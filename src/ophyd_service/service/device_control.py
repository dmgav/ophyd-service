import asyncio
import inspect
import json
import logging
import re

from .device_registry import DeviceRegistry

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
        self._device_registry = device_registry

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

            supported_methods = ("read", "get")
            if method not in supported_methods:
                raise ValueError(f"Unsupported method {method!r}. Supported methods: {supported_methods}")

            # self._validate_device_name(device_name, user_group=user_group)

            # In IPython mode the namespace is the user namespace of the kernel.
            ns = self._device_registry.ns
            try:
                device = eval(device_name, ns, ns)  # noqa: S307
            except Exception as ex:
                raise RuntimeError(f"Device '{device_name}' is not found in the namespace: {ex}") from ex

            def _check_method(_attr):
                if not hasattr(device, _attr):
                    raise RuntimeError(
                        f"Object {device_name!r} has no attribute {_attr!r}. Method {method!r} is not supported"
                    )

            if method == "read":
                if inspect.iscoroutinefunction(device.read):
                    _check_method("read")
                    coro = device.read()
                else:
                    _check_method("read")
                    coro = asyncio.to_thread(device.read)
            elif method == "get":
                if inspect.iscoroutinefunction(device.read):
                    _check_method("get_value")
                    coro = device.get_value()
                else:
                    _check_method("get")
                    coro = asyncio.to_thread(device.get)
            else:
                # This exception should never be raised
                raise RuntimeError(f"Unknown method {method!r}")

            value = await coro
            json.dumps(value)

            success, err_msg = True, ""

        except Exception as ex:
            success, err_msg = False, f"Error: {ex}"

        return {"success": success, "err_msg": err_msg, "result": value}
