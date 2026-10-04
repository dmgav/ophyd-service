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
