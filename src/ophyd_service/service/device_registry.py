import enum
import logging
import pprint

from bluesky_queueserver.manager.profile_ops import load_worker_startup_code

from .device_list import (
    existing_device_objects,
    existing_plans_and_devices_from_nspace,
    flatten_allowed_devices,
    flatten_device_tree,
    select_allowed_devices,
)
from .ipython_namespace import load_worker_startup_code_ipython
from .user_permissions import load_user_group_permissions

logger = logging.getLogger(__name__)


class DeviceAccessType(str, enum.Enum):
    INFO = "info"
    READ = "read"
    WRITE_IDLE = "write_idle"
    WRITE_ALWAYS = "write_always"


class DeviceRegistry:
    """
    Loads the startup code and holds the devices of the ophyd environment.

    Parameters
    ----------
    service_config: dict
        Service configuration.
    """

    def __init__(self, service_config):
        self._use_ipython_kernel = service_config["use_ipython_kernel"]
        self._startup_dir = service_config["startup_dir"]
        self._startup_module_name = service_config["startup_module_name"]
        self._startup_script_path = service_config["startup_script_path"]
        self._startup_profile = service_config["startup_profile"]
        self._ipython_dir = service_config["ipython_dir"]
        self._demo_mode = service_config["demo_mode"]
        self._user_group_permissions_path = service_config["user_group_permissions_path"]
        self._device_max_depth = service_config["device_max_depth"]

        self._env_exists = False
        self._ns = {}
        self._existing_devices = {}
        self._existing_devices_obj = {}
        self._allowed_devices = {}

    @property
    def ns(self):
        return self._ns

    @property
    def existing_devices(self):
        return self._existing_devices

    @property
    def existing_devices_obj(self):
        return self._existing_devices_obj

    @property
    def allowed_devices(self):
        return self._allowed_devices

    def load_startup_code(self):
        """
        Load the startup code into the namespace. IPython is used if ``use_ipython_kernel``
        is enabled, otherwise the code is loaded with standard Python.
        """
        self._ns.clear()
        self._env_exists = False

        try:
            logger.info(f"Loading user group permissions from {self._user_group_permissions_path!r}")
            self._user_group_permissions = load_user_group_permissions(self._user_group_permissions_path)

            logger.info("Loading startup code (IPython kernel enabled: %s) ...", self._use_ipython_kernel)
            if self._use_ipython_kernel:
                load_worker_startup_code_ipython(
                    startup_profile=self._startup_profile,
                    ipython_dir=self._ipython_dir,
                    startup_module_name=self._startup_module_name,
                    startup_script_path=self._startup_script_path,
                    user_ns=self._ns,
                )
            else:
                load_worker_startup_code(
                    startup_dir=self._startup_dir,
                    startup_module_name=self._startup_module_name,
                    startup_script_path=self._startup_script_path,
                    nspace=self._ns,
                )

            existing_devices_tree, _ = existing_plans_and_devices_from_nspace(
                nspace=self._ns, max_depth=self._device_max_depth
            )
            existing_devices = flatten_device_tree(existing_devices_tree)
            self._existing_devices = existing_devices
            self._existing_devices_obj = existing_device_objects(
                existing_devices=existing_devices, nspace=self._ns
            )

            allowed_devices_tree = select_allowed_devices(
                existing_devices=existing_devices_tree, user_group_permissions=self._user_group_permissions
            )
            allowed_devices = flatten_allowed_devices(allowed_devices_tree)
            self._allowed_devices = allowed_devices

            # print(f"existing_devices_tree = {pprint.pformat(existing_devices_tree)}")
            # print(f"allowed_devices_tree = {pprint.pformat(allowed_devices_tree)}")
            print(f"existing_devices = {pprint.pformat(existing_devices)}")
            print(f"allowed_devices = {pprint.pformat(allowed_devices)}")

            self._env_exists = True
            logger.info("Startup code was loaded successfully.")

        except Exception as ex:
            logger.error("Failed to populate registry: %s", ex)
            self._ns.clear()
            self._existing_devices.clear()
            self._existing_devices_obj.clear()
            self._allowed_devices.clear()

    def get_device(self, device_name, *, user_group, access_type):
        """
        Return a reference to the device object and the device properties if the requested type
        of access is allowed for the user group. Access of type ``WRITE_IDLE`` is also granted
        if the device is allowed for ``WRITE_ALWAYS`` access. Access of type ``INFO`` is granted
        for any existing device.

        Parameters
        ----------
        device_name: str
            Full device name, e.g. ``'device.component.subcomponent'``.
        user_group: str
            Name of the user group.
        access_type: DeviceAccessType
            Requested type of access.

        Returns
        -------
        object
            Reference to the device object.
        dict
            Device properties.

        Raises
        ------
        RuntimeError
            The device does not exist, access is not allowed or the device can not be obtained.
        """
        access_type = DeviceAccessType(access_type)

        if user_group not in self._allowed_devices:
            raise RuntimeError(f"User group {user_group!r} does not exist")

        if device_name not in self._existing_devices:
            raise RuntimeError(f"Device {device_name!r} does not exist")

        if access_type != DeviceAccessType.INFO:
            if access_type == DeviceAccessType.WRITE_IDLE:
                access_types = (DeviceAccessType.WRITE_IDLE, DeviceAccessType.WRITE_ALWAYS)
            else:
                access_types = (access_type,)

            group_devices = self._allowed_devices[user_group]
            if not any(device_name in group_devices.get(access.value, {}) for access in access_types):
                raise RuntimeError(
                    f"Access of type {access_type.value!r} to device {device_name!r} "
                    f"is not allowed for user group {user_group!r}"
                )

        if device_name not in self._existing_devices_obj:
            raise RuntimeError(f"Reference to device {device_name!r} is not found")
        device_obj = self._existing_devices_obj[device_name]
        if device_obj is None:
            raise RuntimeError(f"Reference to device {device_name!r} is None")

        device_properties = self._existing_devices.get(device_name)
        if device_properties is None:
            raise RuntimeError(f"Properties of device {device_name!r} are not found")

        return device_obj, device_properties

    def get_device_permissions(self, device_name, *, user_group):
        """
        Return permissions of the user group for the device. ``write_idle`` permission is also
        granted if the device is allowed for ``write_always`` access.

        Parameters
        ----------
        device_name: str
            Full device name, e.g. ``'device.component.subcomponent'``.
        user_group: str
            Name of the user group.

        Returns
        -------
        dict
            Dictionary with boolean values for the keys ``'read'``, ``'write_idle'``
            and ``'write_always'``.

        Raises
        ------
        RuntimeError
            The user group or the device does not exist.
        """
        if user_group not in self._allowed_devices:
            raise RuntimeError(f"User group {user_group!r} does not exist")

        if device_name not in self._existing_devices:
            raise RuntimeError(f"Device {device_name!r} does not exist")

        group_devices = self._allowed_devices[user_group]
        access_types = (DeviceAccessType.READ, DeviceAccessType.WRITE_IDLE, DeviceAccessType.WRITE_ALWAYS)
        permissions = {access.value: device_name in group_devices.get(access.value, {}) for access in access_types}
        permissions[DeviceAccessType.WRITE_IDLE.value] |= permissions[DeviceAccessType.WRITE_ALWAYS.value]

        return permissions
