import asyncio
import json
import logging

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

from ..authentication import get_current_principal
from ..resources import SERVER_RESOURCES as SR

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
    principal=Security(get_current_principal, scopes=["read:status"]),
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
    # logger.debug("User group: %s  Device name: %s", user_group, device_name)
    success, msg, value, req_uid = await SR.environment_manager.device_read(device_name, user_group=user_group)
    return {"success": success, "msg": msg, "device_name": device_name, "value": value}


@router.post("/environment/open")
async def environment_open_handler(principal=Security(get_current_principal, scopes=["write:manager:control"])):
    """
    Open the RE Worker environment: start the worker process and load the startup code.
    """
    success, msg = await SR.environment_manager.open_environment()
    return {"success": success, "msg": msg}


@router.post("/environment/close")
async def environment_close_handler(principal=Security(get_current_principal, scopes=["write:manager:control"])):
    """
    Close the RE Worker environment. The worker process is killed if it fails to exit
    in an orderly way before the timeout expires.
    """
    success, msg = await SR.environment_manager.close_environment()
    return {"success": success, "msg": msg}


@router.websocket("/status")
async def status_websocket_handler(websocket: WebSocket):
    """
    Stream the status messages published by the environment manager. Only the messages
    published while the connection is open are sent to the client.
    """
    await websocket.accept()
    with SR.environment_manager.subscribe_status() as queue:
        try:
            while True:
                await websocket.send_json(await queue.get())
        except WebSocketDisconnect:
            logger.debug("The client disconnected from the status websocket.")
        except RuntimeError as ex:
            # Raised if the connection is closed while the message is sent.
            logger.debug("The status websocket was closed: %s", ex)


@router.websocket("/monitor")
async def monitor_websocket_handler(websocket: WebSocket):
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

    async def receive_client_messages(queue):
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

            logger.debug("Message received from a monitor websocket client: %s", msg)
            result = await SR.environment_manager.subscribe_monitor_devices(device_names, queue)
            accepted_devices = result["accepted_device_names"]
            async with send_lock:
                await websocket.send_json({"accepted_devices": accepted_devices})

    await websocket.accept()
    async with SR.environment_manager.subscribe_monitor() as queue:
        tasks = [
            asyncio.ensure_future(send_monitor_data(queue)),
            asyncio.ensure_future(receive_client_messages(queue)),
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
