import asyncio
import inspect
import json
import logging
import re

from ophyd_async.core import SignalW

from .device_registry import DeviceAccessType, DeviceRegistry
from .service_utils import run_in_bluesky_loop

logger = logging.getLogger(__name__)


# Device name is a dotted sequence of identifiers, e.g. 'det1' or 'sim_stage.det.val'.
_device_name_pattern = re.compile(r"[_a-zA-Z][_a-zA-Z0-9]*(\.[_a-zA-Z][_a-zA-Z0-9]*)*")


class DeviceControl:
    """
    Parameters
    ----------
    device_registry: DeviceRegistry
        Registry holding the devices of the ophyd environment.
    """

    def __init__(self, device_registry: DeviceRegistry):
        self._timeout_default = 1
        self._connection_timeout_default = 1
        self._device_registry = device_registry

    @property
    def device_registry(self):
        return self._device_registry

    async def device_info_handler(self, device_name, method, user_group):
        """
        Get information on the device with the name ``device_name``.
        """
        logger.debug("Getting information on the device '%s' ...", device_name)

        success, err_msg, value = False, "", None
        try:
            if not isinstance(device_name, str) or not _device_name_pattern.fullmatch(device_name):
                raise ValueError(f"Invalid device name: {device_name!r}")

            supported_methods = ("properties", "permissions")
            if method not in supported_methods:
                raise ValueError(f"Unsupported method {method!r}. Supported methods: {supported_methods}")

            _, device_properties = self._device_registry.get_device(
                device_name, user_group=user_group, access_type=DeviceAccessType.INFO
            )

            if method == "properties":
                value = device_properties
            elif method == "permissions":
                value = self._device_registry.get_device_permissions(device_name, user_group=user_group)
            else:
                # This exception should never be raised
                raise RuntimeError(f"Unknown method {method!r}")

            json.dumps(value)

            success, err_msg = True, ""

        except Exception as ex:
            success, err_msg = False, f"Error: {ex}"

        return {"success": success, "err_msg": err_msg, "result": value}

    async def device_read_handler(self, device_name, method, user_group):
        """
        Read the device with the name ``device_name``.
        """
        logger.debug("Reading the device '%s' ...", device_name)

        success, err_msg, value = False, "", None
        try:
            # The name is evaluated, so only dotted identifiers are accepted.
            if not isinstance(device_name, str) or not _device_name_pattern.fullmatch(device_name):
                raise ValueError(f"Invalid device name: {device_name!r}")

            supported_methods = ("read", "describe", "get")
            if method not in supported_methods:
                raise ValueError(f"Unsupported method {method!r}. Supported methods: {supported_methods}")

            # self._validate_device_name(device_name, user_group=user_group)

            device_obj, device_properties = self._device_registry.get_device(
                device_name, user_group=user_group, access_type=DeviceAccessType.READ
            )

            def _check_method(_attr):
                if not hasattr(device_obj, _attr):
                    raise RuntimeError(
                        f"Object {device_name!r} has no attribute {_attr!r}. Method {method!r} is not supported"
                    )

            read_task = None
            if method == "read":
                if not device_properties.get("is_readable", False):
                    raise RuntimeError(f"Method {method!r} is not supported for the device {device_name!r}")
                if inspect.iscoroutinefunction(device_obj.read):
                    read_task = run_in_bluesky_loop(device_obj.read())
                else:
                    read_task = asyncio.to_thread(device_obj.read)
            elif method == "describe":
                if not device_properties.get("is_readable", False):
                    raise RuntimeError(f"Method {method!r} is not supported for the device {device_name!r}")
                if inspect.iscoroutinefunction(device_obj.describe):
                    read_task = run_in_bluesky_loop(device_obj.describe())
                else:
                    read_task = asyncio.to_thread(device_obj.describe)
            elif method == "get":
                if inspect.iscoroutinefunction(device_obj.read):
                    _check_method("get_value")
                    read_task = run_in_bluesky_loop(device_obj.get_value())
                else:
                    _check_method("get")
                    read_task = asyncio.to_thread(device_obj.get)
            else:
                # This exception should never be raised
                raise RuntimeError(f"Unknown method {method!r}")

            if read_task is not None:
                value = await read_task
            json.dumps(value)

            success, err_msg = True, ""

        except Exception as ex:
            success, err_msg = False, f"Error: {ex}"

        return {"success": success, "err_msg": err_msg, "result": value}

    async def device_put_handler(self, device_name, device, value, *, timeout=None, use_complete=False):
        """
        Write the value to the device. For Ophyd devices, ``put()`` is called in a separate thread.
        For ophyd-async signals, ``set()`` is awaited (ophyd-async has no ``put()``). Exceptions
        are reraised.

        Parameters
        ----------
        device_name: str
            Full device name, e.g. ``'device.component.subcomponent'``.
        device: object
            Reference to the device object.
        value: any
            The value to write.
        timeout: float or None
            Timeout for the put operation. If ``None``, the default timeout is used.
        use_complete: bool
            Wait for put completion (Ophyd EPICS signals only). Ignored for ophyd-async signals.
        """
        logger.debug("Writing value %r to the device '%s' (put) ...", value, device_name)

        if timeout is None:
            timeout = self._timeout_default

        if isinstance(device, SignalW):
            # 'set()' starts a task in the current loop when called, so it is called in the Bluesky loop.
            async def _set():
                await device.set(value, timeout=timeout)

            await run_in_bluesky_loop(_set())
            return

        put_method = getattr(device, "put", None)
        if not callable(put_method):
            raise RuntimeError(f"Device {device_name!r} does not support 'put' operation")

        # Soft signals warn about unsupported kwargs, so pass EPICS-specific kwargs only if accepted.
        kwargs = {}
        if "use_complete" in inspect.signature(put_method).parameters:
            kwargs.update({"use_complete": use_complete, "wait": use_complete})

        await asyncio.to_thread(put_method, value, timeout=timeout, **kwargs)

    async def device_set_handler(self, device_name, device, value, *, timeout=None):
        """
        Call ``set()`` method of the device and wait for the returned status object to complete.
        For Ophyd devices, ``set()`` is called in a separate thread. For ophyd-async devices,
        ``set()`` is called in the event loop. Exceptions raised by ``set()`` or by the status
        object (including the timeout) are reraised. The operation is not cancelled on timeout.

        Parameters
        ----------
        device_name: str
            Full device name, e.g. ``'device.component.subcomponent'``.
        device: object
            Reference to the device object.
        value: any
            The value to set.
        timeout: float or None
            Maximum time to wait for the status object to complete. If ``None``, the default
            timeout is used.
        """
        logger.debug("Setting value %r of the device '%s' (set) ...", value, device_name)

        if timeout is None:
            timeout = self._timeout_default

        set_method = getattr(device, "set", None)
        if not callable(set_method):
            raise RuntimeError(f"Device {device_name!r} does not support 'set' operation")

        # ophyd-async wraps the coroutine 'set' with 'AsyncStatus.wrap', so check the unwrapped function.
        if inspect.iscoroutinefunction(inspect.unwrap(set_method)):

            async def _set_async():
                await set_method(value)

            status = run_in_bluesky_loop(_set_async())
            try:
                await asyncio.wait_for(asyncio.shield(status), timeout=timeout)
            except asyncio.TimeoutError as ex:
                raise TimeoutError(f"Set operation for {device_name!r} timed out after {timeout} s") from ex
        else:

            def _set():
                set_method(value).wait(timeout=timeout)

            await asyncio.to_thread(_set)
