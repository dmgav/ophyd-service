import asyncio
import contextlib
import json
import logging
from datetime import datetime

import pydantic
from fastapi import APIRouter, Depends, Security, WebSocket, WebSocketDisconnect
from packaging import version

from ophyd_service import __version__

from ..settings import get_settings
from ..utils import get_api_access_manager, get_current_username, get_resource_access_manager

if version.parse(pydantic.__version__) < version.parse("2.0.0"):
    from pydantic import BaseSettings
else:
    from pydantic_settings import BaseSettings

from ..authentication import (
    authenticate_websocket_first_message,
    get_current_principal,
    get_current_principal_websocket,
)
from ..resources import SERVER_RESOURCES as SR  # noqa: F401

# if version.parse(pydantic.__version__) < version.parse("2.0.0"):
#     from pydantic import BaseSettings
# else:
#     from pydantic_settings import BaseSettings

# from ..resources import SERVER_RESOURCES as SR
# from ..utils import process_exception

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api")


@router.get("/")
@router.get("/ping")
async def ping_handler(payload: dict = {}, principal=Security(get_current_principal, scopes=["read:status"])):
    """
    May be called to get some response from the server. Currently returns status of RE Manager.
    """
    msg = {"success": True, "msg": f"Ophyd-Service: v.{__version__}"}
    return msg


@router.get("/device/read/{device_name:path}")
async def device_read_handler(
    device_name: str,
    principal=Security(get_current_principal, scopes=["read:devices"]),
    settings: BaseSettings = Depends(get_settings),
    api_access_manager=Depends(get_api_access_manager),
    resource_access_manager=Depends(get_resource_access_manager),
):
    """
    Return the name of the device. The name may contain slashes.
    """
    # Subdevices are separated by slashes in the API and by dots in the namespace.
    username = get_current_username(principal=principal, settings=settings, api_access_manager=api_access_manager)[
        0
    ]
    user_group = resource_access_manager.get_resource_group(username)
    device_name = device_name.replace("/", ".")
    logger.info(
        "Device read requested by user '%s' in resource group '%s' for device '%s'.",
        username,
        user_group,
        device_name,
    )
    return {"success": True, "msg": "", "device_name": device_name, "value": {}}


# WebSocket close codes.  4001 = invalid token, 4401 = auth required
# (RFC 6455 leaves 4000-4999 for application use).
_WS_CLOSE_INVALID_TOKEN = 4001
_WS_CLOSE_AUTH_REQUIRED = 4401


async def _authenticate_websocket(websocket, scopes):
    """Resolve a Principal for a WebSocket connection.

    Tries in order:

    1. ``Authorization: Bearer|ApiKey ...`` header (populated by curl/CLI).
    2. ``?access_token=...`` or ``?api_key=...`` query parameter (populated
       by browsers, which cannot set request headers on a WebSocket
       handshake).
    3. First-message handshake: accepts the socket, then reads one JSON
       message of the form
       ``{"type": "auth", "api_key": "..."}`` or
       ``{"type": "auth", "access_token": "..."}``.
       On success the socket stays open; on failure the socket is closed
       with code 4001 and ``None`` is returned.

    Returns ``(principal, accepted)`` where ``accepted`` indicates whether
    the socket has already been ``.accept()``-ed by this helper (True only
    when the first-message path was used).  Callers that receive ``None``
    for the principal have already had the socket closed and should return
    immediately.
    """
    principal = get_current_principal_websocket(websocket=websocket, scopes=scopes)
    if principal is not None:
        return principal, False

    # Fall back to the first-message handshake.  Accept the socket so that we
    # can receive the auth payload; the client is expected to send it as the
    # very first frame.
    await websocket.accept()
    try:
        message = await asyncio.wait_for(websocket.receive_json(), timeout=10)
    except asyncio.TimeoutError:
        await websocket.close(code=_WS_CLOSE_AUTH_REQUIRED, reason="Auth required")
        return None, True
    except WebSocketDisconnect:
        # Client already gone — no close frame needed.
        return None, True
    except Exception:
        logger.exception("Unexpected error receiving WebSocket auth frame")
        await websocket.close(code=_WS_CLOSE_AUTH_REQUIRED, reason="Auth required")
        return None, True

    principal = authenticate_websocket_first_message(websocket, message)
    if principal is None:
        await websocket.close(code=_WS_CLOSE_INVALID_TOKEN, reason="Invalid token")
        return None, True

    return principal, True


def get_timestamp_iso8601():
    """
    Returns current timestamp in ISO 8601 format.
    """
    return datetime.now().isoformat()


class HeartbeatMessageStream:
    def __init__(self, period=1.0):
        self._period = period

    @contextlib.asynccontextmanager
    async def subscribe(self):
        queue = asyncio.Queue()
        task = asyncio.create_task(self._publish_heartbeats(queue))
        try:
            yield queue
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _publish_heartbeats(self, queue):
        while True:
            await queue.put(
                {
                    "heartbeat": {
                        "timestamp": get_timestamp_iso8601(),
                    }
                }
            )
            await asyncio.sleep(self._period)


heartbeat_message_stream = HeartbeatMessageStream()


@router.websocket("/monitor/ws")
async def monitor_websocket_handler(websocket: WebSocket, scopes=["read:devices"]):
    """
    Stream the data on the monitored PVs published by the environment manager. Only the messages
    published while the connection is open are sent to the client. The client may send JSON
    messages to the server over the same connection.
    """

    send_lock = asyncio.Lock()

    async def send_monitor_data(queue):
        while True:
            msg = await queue.get()
            async with send_lock:
                await websocket.send_json(msg)

    async def receive_client_messages():
        while True:
            try:
                msg = await websocket.receive_json()
            except json.JSONDecodeError:
                logger.error("The message received from a monitor websocket client is not valid JSON.")
                continue
            if not isinstance(msg, dict):
                logger.error("The message received from a monitor websocket client is not a JSON object.")
                continue

            device_names = msg.get("monitor_devices")
            if not isinstance(device_names, list) or not all(isinstance(_, str) for _ in device_names):
                logger.error("Invalid request received from a monitor websocket client: %s", msg)
                continue

            logger.info("Message received from a monitor websocket client: %s", msg)
            async with send_lock:
                await websocket.send_json(
                    {
                        "requested_devices": [],
                        "accepted_devices": [],
                    }
                )

    principal, accepted = await _authenticate_websocket(websocket, scopes)
    if not principal:
        return

    if not accepted:
        await websocket.accept()
    async with heartbeat_message_stream.subscribe() as queue:
        tasks = [
            asyncio.ensure_future(send_monitor_data(queue)),
            asyncio.ensure_future(receive_client_messages()),
        ]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                task.result()  # Reraise the exception that stopped the handler.
        except WebSocketDisconnect:
            logger.debug("The client disconnected from the monitor websocket.")
        except RuntimeError as ex:
            # Raised if the connection is closed while the message is sent or received.
            logger.debug("The monitor websocket was closed: %s", ex)
