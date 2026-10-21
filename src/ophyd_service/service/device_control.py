import asyncio
import inspect
import json
import logging
import re

from .device_registry import DeviceAccessType, DeviceRegistry

logger = logging.getLogger(__name__)


# Device name is a dotted sequence of identifiers, e.g. 'det1' or 'sim_stage.det.val'.
_device_name_pattern = re.compile(r"[_a-zA-Z][_a-zA-Z0-9]*(\.[_a-zA-Z][_a-zA-Z0-9]*)*")


class DeviceControl:
    """
    Parameters
    ----------
    device_registry: DeviceRegistry
        Registry holding the devices of the ophyd environment.
    default_timeout: float
        The default timeout used for all methods.
    """

    def __init__(self, device_registry: DeviceRegistry, default_timeout: float = 10.0):
        self._device_registry = device_registry
        self._default_timeout = default_timeout

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
                    read_task = device_obj.read()
                else:
                    read_task = asyncio.to_thread(device_obj.read)
            elif method == "describe":
                if not device_properties.get("is_readable", False):
                    raise RuntimeError(f"Method {method!r} is not supported for the device {device_name!r}")
                if inspect.iscoroutinefunction(device_obj.describe):
                    read_task = device_obj.describe()
                else:
                    read_task = asyncio.to_thread(device_obj.describe)
            elif method == "get":
                if inspect.iscoroutinefunction(device_obj.read):
                    _check_method("get_value")
                    read_task = device_obj.get_value()
                else:
                    _check_method("get")
                    read_task = asyncio.to_thread(device_obj.get)
            else:
                # This exception should never be raised
                raise RuntimeError(f"Unknown method {method!r}")

            if read_task is not None:
                try:
                    value = await asyncio.wait_for(read_task, timeout=self._default_timeout)
                except asyncio.TimeoutError:
                    raise TimeoutError(f"Device {device_name!r}, timed out after {self._default_timeout}s")
            json.dumps(value)

            success, err_msg = True, ""

        except Exception as ex:
            success, err_msg = False, f"Error: {ex}"

        return {"success": success, "err_msg": err_msg, "result": value}
