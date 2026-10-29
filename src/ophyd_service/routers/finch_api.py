import asyncio
import json
import logging
import time

import numpy as np
import pydantic
from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from ophyd import EpicsMotor, EpicsSignal, EpicsSignalRO, PseudoPositioner
from ophyd_async.core import SignalW
from packaging import version

from ..resources import SERVER_RESOURCES as SR
from ..service.device_registry import DeviceAccessType
from ..service.service_utils import run_in_bluesky_loop
from ..settings import get_settings
from ..utils import get_api_access_manager, get_current_username, get_resource_access_manager
from .core_api import _authenticate_websocket

if version.parse(pydantic.__version__) < version.parse("2.0.0"):
    from pydantic import BaseSettings
else:
    from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1")


# ********************************************************************************************************
#                    The following websockets are vendored from ophyd-websocket
# ********************************************************************************************************


@router.websocket("/device-socket")
async def device_socket_write_handler(
    websocket: WebSocket,
    scopes=["write:devices"],
    resource_access_manager=Depends(get_resource_access_manager),
    settings: BaseSettings = Depends(get_settings),
    api_access_manager=Depends(get_api_access_manager),
):
    principal, accepted = await _authenticate_websocket(websocket, scopes)
    if not principal:
        return

    if not accepted:
        await websocket.accept()

    username = get_current_username(principal=principal, settings=settings, api_access_manager=api_access_manager)[
        0
    ]
    user_group = resource_access_manager.get_resource_group(username)

    await websocket_endpoint(websocket, user_group=user_group, read_only=False)


@router.websocket("/device-socket/read")
async def device_socket_read_handler(
    websocket: WebSocket,
    scopes=["read:devices"],
    resource_access_manager=Depends(get_resource_access_manager),
    settings: BaseSettings = Depends(get_settings),
    api_access_manager=Depends(get_api_access_manager),
):

    principal, accepted = await _authenticate_websocket(websocket, scopes)
    if not principal:
        return

    if not accepted:
        await websocket.accept()

    username = get_current_username(principal=principal, settings=settings, api_access_manager=api_access_manager)[
        0
    ]
    user_group = resource_access_manager.get_resource_group(username)

    await websocket_endpoint(websocket, user_group=user_group, read_only=True)


async def websocket_endpoint(websocket: WebSocket, *, user_group, read_only=False):

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()

    # Check if device exists in the device registry
    device_registry = SR.device_control.device_registry
    existing_devices = device_registry.existing_devices
    readable_devices = device_registry.allowed_devices.get(user_group, {}).get(DeviceAccessType.READ.value, {})
    subscriptions = {}
    supported_actions = ("subscribe", "unsubscribe", "refresh", "subscribeSafely", "subscribeReadOnly", "set")

    def can_subscribe(device_name):
        description = existing_devices.get(device_name, {})
        return description.get("is_subscribable", False) and device_name in readable_devices

    def is_async(device_name):
        description = existing_devices.get(device_name, {})
        return bool(description.get("is_async", False))

    async def addCallbacks(device_name, device, *, recursive=True, readOnly=False):
        # different ophyd devices may have different event types that are subscribable
        # EpicsSignal: 'setpoint_meta', 'setpoint', 'meta', 'value'
        # EpicsMotor: 'start_moving', 'readback', '_req_done', 'done_moving', 'acq_done'
        # PseudoPositioner: 'start_moving', 'readback', '_req_done', 'done_moving', 'acq_done', 'readback'
        # Component: 'acq_done'
        connection_state = {"connected": None, "last_update": 0}
        device_name_top = device_name

        def get_callbackMd(device_name, device_name_top):
            def callback(**kwargs):
                # Triggered on changes to metadata, including 'connected'
                # Will trigger when device first connects, when the device is disconnected/reconnected
                # Does not trigger when the value changes
                message = dict(kwargs)
                message["obj"] = (
                    # obj is an EpicsSignal which is not JSON serializable, overwrite it so the msg will send
                    device_name
                )
                # Return device or component name
                message["device_name"] = device_name
                # Return the name of the top level device used for recursive subscription
                #   (consistent with original behavior of ophyd-websocket)
                message["device"] = device_name_top
                message["signal"] = (
                    kwargs.get("obj", None).name if kwargs.get("obj", None) else None
                )  # provides the specific signal name within the device

                if message.get("connected") is not None:
                    current_connection = message.get("connected")
                    # Only update connection state & send ws message if it has changed
                    if current_connection != connection_state["connected"]:
                        # if even one signal is disconnected, always send message that device is disconnected
                        if current_connection is False:
                            connection_state["connected"] = current_connection
                            connection_state["last_update"] = time.time()
                            try:
                                asyncio.run_coroutine_threadsafe(websocket.send_json(message), loop)
                            except WebSocketDisconnect:
                                logger.info(f"Connection closed while sending update for device: {device_name}")
                        if current_connection is True:
                            # TODO: verify that the all signals in device are connected
                            #   before sending connected=true message
                            # device.connected
                            try:
                                asyncio.run_coroutine_threadsafe(websocket.send_json(message), loop)
                            except WebSocketDisconnect:
                                logger.info(f"Connection closed while sending update for device: {device_name}")

            return callback

        def get_callbackValue(device_name, device_name_top):
            def callback(value, timestamp, **kwargs):
                if isinstance(value, np.ndarray) and value.dtype.kind in ["i", "u"]:
                    try:
                        # Remove null bytes and convert to string
                        cleaned_array = value[value != 0]  # Remove null terminators
                        if len(cleaned_array) > 0:
                            string_value = "".join(chr(x) for x in cleaned_array)
                            value = string_value
                            logger.debug(f"Converted array to string for device {device_name}: {value}")
                    except (ValueError, OverflowError):
                        # If conversion fails, keep original value
                        pass
                if isinstance(value, tuple):
                    value = value[0]
                message = dict(kwargs)
                message = {
                    "device_name": device_name,
                    "device": device_name_top,
                    "value": value,
                    "timestamp": timestamp,
                    "connected": device.connected,  # might take this out since redundant with meta
                    "read_access": getattr(device, "read_access", None),
                    "write_access": getattr(device, "write_access", None),
                    "signal": kwargs.get("obj", None).name if kwargs.get("obj", None) else None,
                }
                try:
                    asyncio.run_coroutine_threadsafe(websocket.send_json(message), loop)
                except WebSocketDisconnect:
                    logger.info(f"Connection closed while sending update for device: {device_name}")

            return callback

        def get_callbackReadingAsync(device_name, device_name_top, signal):
            # ophyd-async signals pass readings as {signal_name: {"value": ..., "timestamp": ..., ...}}
            def callback(reading):
                for signal_name, signal_reading in reading.items():
                    value = signal_reading["value"]
                    if isinstance(value, np.ndarray) and value.dtype.kind in ["i", "u"]:
                        try:
                            # Remove null terminators and convert to string
                            cleaned_array = value[value != 0]
                            if len(cleaned_array) > 0:
                                value = "".join(chr(x) for x in cleaned_array)
                                logger.debug(f"Converted array to string for device {device_name}: {value}")
                        except (ValueError, OverflowError):
                            pass
                    if isinstance(value, tuple):
                        value = value[0]
                    message = {
                        "device_name": device_name,
                        "device": device_name_top,
                        "value": value,
                        "timestamp": signal_reading["timestamp"],
                        "connected": True,  # Readings are delivered only by connected signals
                        "read_access": True,
                        "write_access": isinstance(signal, SignalW),
                        "signal": signal_name,
                    }
                    try:
                        asyncio.run_coroutine_threadsafe(websocket.send_json(message), loop)
                    except WebSocketDisconnect:
                        logger.info(f"Connection closed while sending update for device: {device_name}")

            return callback

        async def recursively_subscribe(device, device_name, *, top_level=True):
            """
            Subscribe callbacks to the device or, for devices of unsupported types, to its components.
            Subscription is skipped if the device is not subscribable or the user group has no
            READ access to the device. Component names are taken from the flattened list of
            existing devices.

            Parameters
            ----------
            device: object
                Reference to the device object.
            device_name: str
                Full device name, e.g. ``'device.component.subcomponent'``.
            top_level: bool
                ``True`` if the device is the top-level element of the subscription.
            """
            is_subscribed = device_name in subscriptions
            if not is_subscribed:
                subscriptions[device_name] = {
                    "device": device,
                    "recursive": recursive and top_level,
                    "read_only": readOnly,
                    "cids": [],
                }
            cids = subscriptions[device_name]["cids"]

            subscription_allowed = not is_subscribed and can_subscribe(device_name)
            if subscription_allowed:
                logger.debug(f"Subscribing to the device {device_name!r}")

            if is_async(device_name):
                if subscription_allowed:
                    callback = get_callbackReadingAsync(device_name, device_name_top, device)

                    async def subscribe():
                        device.subscribe(callback)

                    await run_in_bluesky_loop(subscribe())
                    # ophyd-async unsubscribes by callback reference, so the callback is saved as 'cid'
                    cids.append(callback)
                if recursive:
                    for name in existing_devices.get(device_name, {}).get("components", []):
                        await recursively_subscribe(
                            getattr(device, name), f"{device_name}.{name}", top_level=False
                        )
            else:
                if isinstance(device, (EpicsSignal, EpicsSignalRO)):
                    if subscription_allowed:
                        cids.append(
                            device.subscribe(get_callbackMd(device_name, device_name_top), event_type="meta")
                        )
                        cids.append(
                            device.subscribe(get_callbackValue(device_name, device_name_top), event_type="value")
                        )
                elif isinstance(device, EpicsMotor):
                    if subscription_allowed:
                        cids.append(
                            device.subscribe(
                                get_callbackValue(device_name, device_name_top), event_type="readback"
                            )
                        )
                        for signal in device.walk_signals():
                            signal_name = f"{device_name}.{signal.dotted_name}"
                            if signal_name not in subscriptions:
                                cid = signal.item.subscribe(
                                    get_callbackMd(device_name, device_name_top), event_type="meta"
                                )
                                subscriptions[signal_name] = {
                                    "device": signal.item,
                                    "recursive": False,
                                    "read_only": readOnly,
                                    "cids": [cid],
                                }
                elif isinstance(device, PseudoPositioner):
                    if subscription_allowed:
                        cids.append(
                            device.subscribe(
                                get_callbackValue(device_name, device_name_top), event_type="readback"
                            )
                        )
                else:
                    if subscription_allowed:
                        for etype in device.event_types:
                            if etype == "meta":
                                cids.append(
                                    device.subscribe(
                                        get_callbackMd(device_name, device_name_top), event_type="meta"
                                    )
                                )
                            elif etype == "value":
                                cids.append(
                                    device.subscribe(
                                        get_callbackValue(device_name, device_name_top), event_type="value"
                                    )
                                )
                    if recursive:
                        for name in existing_devices.get(device_name, {}).get("components", []):
                            await recursively_subscribe(
                                getattr(device, name), f"{device_name}.{name}", top_level=False
                            )

        await recursively_subscribe(device, device_name)

    async def handleSubscribe(data, requireConnection=False, readOnly=False):
        # allows user to subscribe to any device from the device registry
        device_name = data.get("device")
        recursive = bool(data.get("recursive", True))

        if not device_name:
            await websocket.send_json({"error": "No device name specified"})
            return

        if device_name in subscriptions:
            await websocket.send_json({"message": f"Already subscribed to {device_name}"})
            return

        try:
            device, _ = device_registry.get_device(
                device_name, user_group=user_group, access_type=DeviceAccessType.READ
            )
        except RuntimeError as ex:
            allowed_devices = device_registry.allowed_devices.get(user_group, {})
            available_devices = list(allowed_devices.get(DeviceAccessType.READ.value, {}))
            await websocket.send_json(
                {"error": f"Device '{device_name}' is not available: {ex}", "available_devices": available_devices}
            )
            return

        try:
            if requireConnection:
                device.get()  # creates exception if can't connect to the device
        except Exception as e:
            await websocket.send_json({"error": f"Failed to connect to device {device_name}: {str(e)}"})
            if requireConnection:
                return
            await websocket.send_json({"connected": False, "device": device_name})

        await addCallbacks(device_name, device, recursive=recursive, readOnly=readOnly)

        await websocket.send_json({"message": f"Subscribed to device {device_name}"})

    async def handleUnsubscribe(data):
        device_name = data.get("device")

        if not device_name:
            await websocket.send_json({"error": "No device name specified"})
            return

        if device_name not in subscriptions:
            await websocket.send_json({"message": f"Not subscribed to {device_name}"})
            return

        names = [device_name]
        if subscriptions[device_name]["recursive"]:
            prefix = f"{device_name}."
            names.extend(_ for _ in subscriptions if _.startswith(prefix))

        for name in names:
            try:
                info = subscriptions.pop(name)
                for cid in info["cids"]:
                    await remove_subscription(name, info, cid)
            except Exception as ex:
                logger.warning(f"Failed to unsubscribe from {name!r}: {ex}")

        await websocket.send_json({"message": f"Unsubscribed from {device_name}"})

    async def remove_subscription(name, info, cid):
        if is_async(name):

            async def clear_sub():
                info["device"].clear_sub(cid)

            await run_in_bluesky_loop(clear_sub())
        else:
            info["device"].unsubscribe(cid)

    async def unsubscribe_all():
        for name, info in subscriptions.items():
            for cid in info["cids"]:
                try:
                    await remove_subscription(name, info, cid)
                except Exception as ex:
                    logger.warning(f"Failed to unsubscribe from {name!r}: {ex}")
        subscriptions.clear()

    async def handleRefresh(data):
        timeout = data.get("timeout", None)  # Wait indefinitely if the value is None
        if not isinstance(timeout, int | float | None):
            await websocket.send_json({"error": "Timeout must be a number. Could not refresh devices"})
            return

        for name, info in subscriptions.items():
            try:
                if is_async(name):
                    # Only ophyd-async signals have 'get_value()', devices are refreshed via their signals
                    if hasattr(info["device"], "get_value"):
                        await asyncio.wait_for(
                            run_in_bluesky_loop(info["device"].get_value(cached=False)), timeout=timeout
                        )
                else:
                    await asyncio.wait_for(asyncio.to_thread(info["device"].get), timeout=timeout)
            except Exception:
                pass

        await websocket.send_json({"message": "Refreshed all devices"})

    async def handleSet(data):
        device_name = data.get("device")

        if not device_name:
            await websocket.send_json({"error": "No device name specified"})
            return

        if device_name not in subscriptions:
            await websocket.send_json(
                {"error": f"Device {device_name} is not subscribed. Subscribe to device before setting value."}
            )
            return

        if subscriptions[device_name]["read_only"]:
            await websocket.send_json(
                {"error": f"Cannot set value of {device_name!r}: the device is subscribed as read-only"}
            )
            return

        timeout = data.get("timeout", None)  # Use default if the value is None
        if not isinstance(timeout, int | float | None):
            await websocket.send_json(
                {"error": f"Timeout must be a number. Could not set value of {device_name!r}"}
            )
            return

        try:
            permissions = device_registry.get_device_permissions(device_name, user_group=user_group)
        except RuntimeError as ex:
            await websocket.send_json({"error": f"Failed to set the value of {device_name!r}: {ex}"})
            return

        # TODO: this code will need to be reworked when the status of Queue Server becomes available
        write_always = permissions[DeviceAccessType.WRITE_ALWAYS.value]
        write_idle = permissions[DeviceAccessType.WRITE_IDLE.value]
        if not (write_always or write_idle):
            await websocket.send_json(
                {"error": f"Cannot set value of {device_name!r}: write access is not allowed"}
            )
            return

        device = subscriptions[device_name]["device"]

        value = data.get("value")
        try:
            # Try to convert to number if it looks like one
            if isinstance(value, str) and value.replace(".", "").replace("-", "").isdigit():
                value = float(value) if "." in value else int(value)
            elif not isinstance(value, (int, float, str)):
                raise ValueError("Value must be a string or number")
            # If it's already a string, int, or float, keep it as-is
        except ValueError:
            await websocket.send_json(
                {"error": f"Value must be a number. Could not set value of {device_name!r} to {value}"}
            )
            return

        is_numeric = isinstance(value, (int, float))
        if not is_async(device_name) and not isinstance(device, PseudoPositioner) and is_numeric:
            # Some devices (e.g. simulated 'SynAxis') do not define limits.
            low_limit = getattr(device, "low_limit", None)
            high_limit = getattr(device, "high_limit", None)

            if (low_limit is not None and value < low_limit) or (high_limit is not None and value > high_limit):
                # area detector limits have a low limit === high limit by default.
                if low_limit != high_limit:
                    msg = (
                        f"Value {value} is outside of limits for device {device_name}. "
                        f"Low limit: {low_limit}, High limit: {high_limit}"
                    )
                    await websocket.send_json({"error": msg})
                    return

        try:
            # ophyd-async has no 'put()', so 'set()' is used for values of all types
            if isinstance(value, str) and not is_async(device_name):
                await SR.device_control.device_put_handler(
                    device_name, device, value, timeout=timeout, use_complete=True
                )
            else:
                await SR.device_control.device_set_handler(device_name, device, value, timeout=timeout)
        except Exception as error:
            await websocket.send_json({"error": f"Failed to set value of {device_name} to {value}: {str(error)}"})
            return

        await websocket.send_json({"message": f"Successfully set {device_name} to {value}"})

    try:
        while True:
            message = await websocket.receive_text()
            try:
                data = json.loads(message)
                action = data.get("action")
                if action not in supported_actions:
                    await websocket.send_json(
                        {
                            "error": (
                                f"Received action: {action}, actions must be one of: "
                                f"{', '.join(repr(_) for _ in supported_actions)}. "
                                "Example msg: {action: 'subscribe', device: 'motor1'}"
                            )
                        }
                    )
                    continue

                if read_only and action == "set":
                    await websocket.send_json({"error": "Setting values is not allowed on a read-only connection"})
                elif action == "subscribe":
                    await handleSubscribe(data)
                elif action == "subscribeSafely":
                    await handleSubscribe(data, requireConnection=True)
                elif action == "subscribeReadOnly":
                    await handleSubscribe(data, requireConnection=False, readOnly=True)
                elif action == "unsubscribe":
                    await handleUnsubscribe(data)
                elif action == "refresh":
                    await handleRefresh(data)
                elif action == "set":
                    await handleSet(data)
                else:
                    raise RuntimeError(f"Action {action!r} was not handled")

            except json.JSONDecodeError:
                await websocket.send_json({"error": "Invalid JSON format"})
                logger.error(f"Received invalid JSON: {message}")
            except Exception as e:
                await websocket.send_json({"error": f"Unexpected error: {str(e)}"})
                logger.error(f"Unexpected error: {str(e)}")
    except WebSocketDisconnect as ex:
        logger.info(f"WebSocket disconnected (code={ex.code})")
    except Exception as e:
        logger.error(f"Error in websocket loop: {str(e)}")
    finally:
        await unsubscribe_all()
        logger.info("WebSocket connection closed.")
