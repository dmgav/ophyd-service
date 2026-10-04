import logging

from bluesky_queueserver.manager.profile_ops import load_worker_startup_code

from .ipython_namespace import load_worker_startup_code_ipython

logger = logging.getLogger(__name__)


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

    @property
    def ns(self):
        return self._ns

    def load_startup_code(self):
        """
        Load the startup code into the namespace. IPython is used if ``use_ipython_kernel``
        is enabled, otherwise the code is loaded with standard Python.
        """
        self._ns.clear()
        self._env_exists = False

        logger.info("Loading startup code (IPython kernel enabled: %s) ...", self._use_ipython_kernel)

        try:
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

            self._env_exists = True
            logger.info("Startup code was loaded successfully.")

        except Exception as ex:
            logger.error("Failed to load startup code: %s", ex)
            self._ns.clear()

    # def _validate_device_name(self, device_name, *, user_group):
    #     """
    #     Check if the device may be accessed by the user. The name is validated using permissions
    #     of the 'root' group and then the permissions of the user group. Raises ``RuntimeError``
    #     if the device name is not allowed.
    #     """
    #     user_groups = self._user_group_permissions.get("user_groups", {})

    #     for group in ("root", user_group):
    #         permissions = user_groups.get(group)
    #         if permissions is None:
    #             raise RuntimeError(f"Permissions for the user group {group!r} are not defined.")

    #         # If the lists are not defined, then no devices are allowed.
    #         allowed = device_name_is_allowed(
    #             device_name,
    #             allow_patterns=permissions.get("allowed_devices_read", []),
    #             disallow_patterns=permissions.get("forbidden_devices_read", []),
    #         )

    #         if not allowed:
    #             raise RuntimeError(f"Device {device_name!r} is not allowed for the user group {group!r}.")
