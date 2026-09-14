"""
Processing of the parameters that define the location of the startup code.
"""

import getpass
import os
import tempfile
from importlib.util import find_spec

from ..config_schemas.loading import ConfigError
from .worker_utils import get_default_startup_dir, get_default_startup_profile

default_existing_pd_fln = "existing_plans_and_devices.yaml"
default_user_group_pd_fln = "user_group_permissions.yaml"


def get_profile_name_from_path(startup_dir):
    """
    Returns name of profile and ipython path based on path to startup directory.
    """
    sd = os.path.abspath(os.path.expanduser(startup_dir))

    profile_name, ip_dir = None, None
    p, d = os.path.split(sd)
    if d == "startup":
        ipd, pr = os.path.split(p)
        if pr.startswith("profile_"):
            profile_name = pr[len("profile_") :]
            ip_dir = ipd

    if not profile_name or not ip_dir:
        raise ConfigError(
            f"Failed to extract IPython directory and profile name from startup directory name: {startup_dir!r}."
        )

    return profile_name, ip_dir


def profile_name_to_startup_dir(profile_name, ipython_dir=None):
    """
    Finds and returns full path to startup directory based on the profile name.
    """
    profile_name = profile_name or "default"

    if ipython_dir:
        path_to_ipython = ipython_dir
    elif find_spec("IPython"):
        import IPython

        path_to_ipython = IPython.paths.get_ipython_dir()
    else:
        raise ConfigError("IPython is not installed. Specify directory using CLI parameters or in config file.")

    ipython_dir = os.path.abspath(path_to_ipython)
    profile_name_full = f"profile_{profile_name}"
    return os.path.join(ipython_dir, profile_name_full, "startup")


def process_startup_options(
    *,
    startup_dir=None,
    startup_module=None,
    startup_script=None,
    startup_profile=None,
    ipython_dir=None,
    use_ipython_kernel=False,
):
    """
    Returns names of startup_dir, startup_module or startup_script. Only one of the name can be not None.
    """

    # Default: startup scripts with simulated plans and devices
    default_startup_dir = get_default_startup_dir()
    default_startup_profile = get_default_startup_profile()

    # The parameters are used as the source of the returned values, which are computed below.
    _dir, _module, _script = startup_dir, startup_module, startup_script
    _profile, _ipdir = startup_profile, ipython_dir

    ipython_dir, startup_profile = None, None
    startup_dir, startup_module, startup_script = None, None, None
    aux_dir = None  # Default directory for lists and permissions (unless explicitly specified)
    demo_mode = False

    if use_ipython_kernel:
        if _dir and (_profile or _ipdir):
            raise ConfigError(
                f"Ambiguous location of startup code: "
                f"startup_dir={_dir!r} startup_profile={_profile!r} ipython_dir={_ipdir!r}"
            )
        if _module and _script:
            raise ConfigError(
                f"Ambiguous location of startup code: startup_module={_module!r} startup_script={_script!r}"
            )

        if _profile or _ipdir:
            startup_profile, ipython_dir = _profile, _ipdir
        elif _dir:
            startup_profile, ipython_dir = get_profile_name_from_path(_dir)

        if _module:
            startup_module = _module
        elif _script:
            startup_script = os.path.abspath(os.path.expanduser(_script))

        # If no location of startup code was specified, then load the default
        #   simulated ipython_sim/profile_collection_sim
        if not any([startup_script, startup_module, startup_profile, ipython_dir]):
            ipython_dir = os.path.join(tempfile.gettempdir(), f"ophyd_service_{getpass.getuser()}", "ipython")
            startup_profile = default_startup_profile
            demo_mode = True

        aux_dir = profile_name_to_startup_dir(startup_profile, ipython_dir)
    else:
        # We ignore profile name if other location is specified
        if _dir and (_profile or _ipdir):
            raise ConfigError(
                f"Ambiguous location of startup code: "
                f"startup_dir={_dir!r} startup_profile={_profile!r} ipython_dir={_ipdir!r}"
            )
        if sum([_ is not None for _ in [_dir, _module, _script]]) > 1:
            raise ConfigError(
                f"Ambiguous location of startup code: "
                f"startup_dir={_dir!r} startup_module={_module!r} startup_script={_script!r}"
            )

        if _module:
            startup_module = _module
        elif _script:
            startup_script = os.path.abspath(os.path.expanduser(_script))
        elif _dir:
            startup_dir = os.path.abspath(os.path.expanduser(_dir))
        elif _profile or _ipdir:
            startup_dir = profile_name_to_startup_dir(_profile, _ipdir)

        startup_profile, ipython_dir = _profile or None, _ipdir or None

        if not any([startup_dir, startup_module, startup_script]):
            startup_dir = default_startup_dir

        # Demo mode: the code is loaded from the built-in startup dir.
        demo_mode = startup_dir == default_startup_dir

        if startup_dir:
            aux_dir = startup_dir
        elif startup_profile or ipython_dir:
            aux_dir = profile_name_to_startup_dir(startup_profile, ipython_dir)

    return startup_dir, startup_module, startup_script, startup_profile, ipython_dir, aux_dir, demo_mode


def adjust_startup_options(worker_config):
    """
    Process the parameters that define the location of the startup code and update
    ``worker_config`` in place. The keys 'aux_dir' and 'demo_mode' are added to the dictionary.
    """
    startup_dir, startup_module, startup_script, startup_profile, ipython_dir, aux_dir, demo_mode = (
        process_startup_options(
            startup_dir=worker_config.get("startup_dir"),
            startup_module=worker_config.get("startup_module_name"),
            startup_script=worker_config.get("startup_script_path"),
            startup_profile=worker_config.get("startup_profile"),
            ipython_dir=worker_config.get("ipython_dir"),
            use_ipython_kernel=worker_config.get("use_ipython_kernel", False),
        )
    )

    existing_plans_and_devices_path = worker_config["existing_plans_and_devices_path"]
    if not existing_plans_and_devices_path:
        existing_plans_and_devices_path = aux_dir
    if not existing_plans_and_devices_path.endswith(".yaml"):
        existing_plans_and_devices_path = os.path.join(existing_plans_and_devices_path, default_existing_pd_fln)
    worker_config["existing_plans_and_devices_path"] = existing_plans_and_devices_path

    user_group_permissions_path = worker_config["user_group_permissions_path"]
    if not user_group_permissions_path:
        user_group_permissions_path = aux_dir
    if not user_group_permissions_path.endswith(".yaml"):
        user_group_permissions_path = os.path.join(user_group_permissions_path, default_user_group_pd_fln)
    worker_config["user_group_permissions_path"] = user_group_permissions_path

    print(f"===================== {startup_dir=}")  ##
    print(f"===================== {aux_dir=}")  ##
    print(f"===================== {demo_mode=}")  ##
    worker_config.update(
        {
            "startup_dir": startup_dir,
            "startup_module_name": startup_module,
            "startup_script_path": startup_script,
            "startup_profile": startup_profile,
            "ipython_dir": ipython_dir,
            "aux_dir": aux_dir,
            "demo_mode": demo_mode,
        }
    )
