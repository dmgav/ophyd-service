import logging

from fastapi import APIRouter, Security, WebSocket, WebSocketDisconnect

from ophyd_service import __version__

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
async def device_read_handler(device_name: str, principal=Security(get_current_principal, scopes=["read:status"])):
    """
    Return the name of the device. The name may contain slashes.
    """
    # Subdevices are separated by slashes in the API and by dots in the namespace.
    device_name = device_name.replace("/", ".")
    logger.info("Device name: %s", device_name)
    success, msg, value, req_uid = await SR.environment_manager.device_read(device_name)
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
