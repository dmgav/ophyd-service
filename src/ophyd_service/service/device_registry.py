import asyncio
import enum
import logging

from bluesky.run_engine import _ensure_event_loop_running, get_bluesky_event_loop, set_bluesky_event_loop
from bluesky_queueserver.manager.profile_ops import load_worker_startup_code

from .device_list import (
    existing_device_objects,
    existing_plans_and_devices_from_nspace,
    flatten_allowed_devices,
    flatten_device_tree,
    merge_allowed_devices,
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
        self._read_only_mode = bool(service_config["read_only_mode"])

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

    @property
    def read_only_mode(self):
        return self._read_only_mode

    def _start_bluesky_event_loop(self):
        """
        Create and start the Bluesky event loop in a background thread. The existing loop
        is reused if it is not closed.
        """
        loop = get_bluesky_event_loop()
        if loop is None or loop.is_closed():
            logger.info("Starting Bluesky event loop ...")
            loop = asyncio.new_event_loop()
            set_bluesky_event_loop(loop)
        # Bluesky's helper registers the thread, so 'RunEngine' can reuse the running loop.
        _ensure_event_loop_running(loop)
        # Wait until the loop is running, otherwise a second thread may be started for it.
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop).result(timeout=10)
        logger.info("Bluesky event loop is running.")

    def _stop_replaced_bluesky_event_loop(self, initial_loop):
        """
        'RunEngine()' created without the 'loop' parameter starts a new loop and makes it the Bluesky
        event loop. Stop and close the loop that was replaced, since it is no longer used.
        """
        loop = get_bluesky_event_loop()
        if initial_loop is None or loop is initial_loop:
            return
        logger.info("The startup code replaced the Bluesky event loop (e.g. by creating 'RunEngine').")
        if initial_loop.is_running():
            initial_loop.call_soon_threadsafe(initial_loop.stop)
            thread = _ensure_event_loop_running.loop_to_thread.pop(initial_loop, None)
            if thread is not None:
                thread.join(timeout=10)
        if not initial_loop.is_running() and not initial_loop.is_closed():
            initial_loop.close()

    def stop_bluesky_event_loop(self):
        """
        Stop the Bluesky event loop. The loop is not closed and is restarted by ``load_startup_code()``.
        """
        loop = get_bluesky_event_loop()
        if loop is not None and loop.is_running():
            logger.info("Stopping Bluesky event loop ...")
            loop.call_soon_threadsafe(loop.stop)

    def load_startup_code(self):
        """
        Load the startup code into the namespace. IPython is used if ``use_ipython_kernel``
        is enabled, otherwise the code is loaded with standard Python.
        """
        self._ns.clear()
        self._env_exists = False

        try:
            self._start_bluesky_event_loop()
            initial_loop = get_bluesky_event_loop()

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
            self._stop_replaced_bluesky_event_loop(initial_loop)

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
            allowed_devices = merge_allowed_devices(flatten_allowed_devices(allowed_devices_tree))
            self._allowed_devices = allowed_devices

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
        of access is allowed for the user group. Access of type ``INFO`` is granted
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
            if device_name not in self._allowed_devices[user_group].get(access_type.value, {}):
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
        Return permissions of the user group for the device.

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
            and ``'write_always'``. Write permissions are ``False`` in read-only mode.

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

        if self._read_only_mode:
            permissions[DeviceAccessType.WRITE_IDLE.value] = False
            permissions[DeviceAccessType.WRITE_ALWAYS.value] = False

        return permissions
