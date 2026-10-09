import logging
from collections.abc import Iterable

from bluesky_queueserver.manager.profile_ops import _filter_device_tree, devices_from_nspace, reg_ns_items

logger = logging.getLogger(__name__)


def _prepare_devices(devices, *, max_depth=0, ignore_all_subdevices_if_one_fails=True, expand_areadetectors=False):
    """
    Prepare dictionary of existing devices for saving to YAML file.
    ``max_depth`` is the maximum depth for the components. The default value (50)
    is a very large number.

    Parameters
    ----------
    devices: dict
        Dictionary of devices from the namespace (key - device name, value - reference
        to the device object).
    max_depth: int
        Maximum depth for the device search: 0 - infinite depth, 1 - only top level
        devices, 2 - device and subdevices etc.
    ignore_all_subdevices_if_one_fails: bool
        Ignore all components of devices if at least one component (PVs) can not
        be accessed. It saves a lot of time to ignore all components, since
        stale code may contain devices with many components with non-existing PVs
        and respective timeout may amount to substantial waiting time.
    expand_areadetectors: bool
        Find subdevices of areadetectors. It may take significant time to expand an
        areadetector and it is unlikely that the areadetector subdevices should be
        accessed via plan parameters.

    Returns
    -------
    dict
        List of existing devices (tree of devices and subdevices).
    """
    max_depth = max(0, max_depth)  # must be >= 0

    from bluesky import protocols
    from ophyd.areadetector import ADBase

    def get_device_params(device, device_obj_name):
        movable_protocols = (protocols.Movable,)
        # TODO: remove this check when NamedMovable is available in every Bluesky deployment
        # !!! Checking for NamedMovable involves checking for 'hints' attribute, which tends to
        # !!! instantiate objects (at least on Python 3.10 and 3.11, worked fine on Python 3.12),
        # !!! which is undesirable. NamedMovable object will be detected checked for attributes of
        # !!! Movable protocol.
        # if hasattr(protocols, "NamedMovable"):
        #     movable_protocols = (*movable_protocols, protocols.NamedMovable)

        return {
            "is_readable": isinstance(device, protocols.Readable),
            "is_movable": isinstance(device, movable_protocols),
            "is_flyable": isinstance(device, protocols.Flyable),
            "classname": type(device).__name__,
            "module": type(device).__module__,
            "name": device.name if isinstance(device, protocols.HasName) else device_obj_name,
        }

    def get_device_component_names(device):
        if hasattr(device, "component_names"):
            component_names = device.component_names
            if not isinstance(component_names, Iterable):
                component_names = []
        elif hasattr(device, "children"):
            component_names = [_[0] for _ in device.children()]
        else:
            component_names = []
        return component_names

    def create_device_description(device, device_name, *, depth=0, max_depth=0, is_registered=False):
        description = get_device_params(device, device_name)
        comps = get_device_component_names(device)
        components = {}

        is_areadetector = isinstance(device, ADBase)
        expand = is_registered or not is_areadetector or expand_areadetectors

        if expand and (not max_depth or (depth < max_depth - 1)):
            ignore_subdevices = False
            for comp_name in comps:
                try:
                    if hasattr(device, comp_name):
                        c = getattr(device, comp_name)
                        desc = create_device_description(
                            c,
                            device_name + "." + comp_name,
                            depth=depth + 1,
                            max_depth=max_depth,
                            is_registered=is_registered,
                        )
                        components[comp_name] = desc
                except Exception as ex:
                    ignore_subdevices = ignore_all_subdevices_if_one_fails
                    logger.warning(
                        "Device '%s': component '%s' can not be processed: %s", device_name, comp_name, ex
                    )
                if ignore_subdevices:
                    components = {}  # Ignore all components of the subdevice
                    break
            if components:
                description["components"] = components

        return description

    def process_devices(*, max_depth):
        """
        Process devices one by one based on information from the namespace and
        the dictionary of registered devices.
        """
        reg_devices = reg_ns_items.reg_devices

        devices_selected = {}
        for name in set(devices.keys()).union(set(reg_devices.keys())):
            obj, _max_depth, is_registered = None, max_depth, False
            if name in reg_devices:
                obj = reg_devices[name]["obj"]
                _max_depth = reg_devices[name]["depth"]
                if obj:
                    is_registered = True
            elif name in devices:
                obj = devices[name]
            if obj:
                devices_selected[name] = dict(obj=obj, max_depth=_max_depth, is_registered=is_registered)

        device_dict = {}
        for name, p in devices_selected.items():
            device_dict[name] = create_device_description(
                p["obj"], name, max_depth=p["max_depth"], is_registered=p["is_registered"]
            )

        return device_dict

    return process_devices(max_depth=max_depth)


def existing_plans_and_devices_from_nspace(*, nspace, max_depth=0, ignore_invalid_plans=False):
    """
    Generate lists of existing plans and devices from namespace. The namespace
    must be in the form returned by ``load_worker_startup_code()``.

    Parameters
    ----------
    nspace: dict
        Namespace that contains plans and devices
    max_depth: int
        Default maximum depth for device search: 0 - unlimited depth, 1 - include only top level devices,
        2 - include top level devices and subdevices, etc.
    ignore_invalid_plans: bool
        Ignore plans with unsupported signatures. If the argument is ``False`` (default), then
        an exception is raised otherwise a message is printed and the plan is not included in the list.


    Returns
    -------
    existing_plans : dict
        Dictionary of descriptions of existing plans
    existing_devices : dict
        Dictionary of descriptions of existing devices
    plans_in_nspace : dict
        Dictionary of plans in namespace
    devices_in_nspace : dict
        Dictionary of devices in namespace
    """
    logger.debug("Extracting existing plans and devices from the namespace ...")

    devices_in_nspace = devices_from_nspace(nspace)

    existing_devices = _prepare_devices(devices_in_nspace, max_depth=max_depth)

    return existing_devices, devices_in_nspace


def select_allowed_devices(
    *,
    existing_devices,
    user_group_permissions,
):
    """
    Generate dictionaries of allowed devices for each user group. The function is using
    the list (dict) of existing devices passed as a parameter.

    Parameters
    ----------
    existing_devices: dict
        List (dict) of the existing devices.
    user_group_permissions: dict
        Dictionary with user group permissions. If ``{}`` or ``None``, then dictionary of allowed
        devices will contain one group 'root' with all existing devices set as allowed.

    Returns
    -------
    dict
        Dictionary of allowed devices. Dictionary keys are names of the user groups.
        Dictionary is ``{}`` if no data on existing devices is provided.
        Dictionary contains one group (``root``) if no user perission data is provided.

    """
    allowed_devices = {}

    try:
        # Detect some possible errors
        if not user_group_permissions:
            raise RuntimeError("No user permissions are specified")

        if not isinstance(user_group_permissions, dict):
            raise RuntimeError(
                f"'user_group_permissions' has type {type(user_group_permissions)} instead of 'dict'"
            )

        if "user_groups" not in user_group_permissions:
            raise RuntimeError("No user groups are defined: 'user_groups' keys is not in 'user_group_permissions'")

        user_groups = list(user_group_permissions["user_groups"].keys())
        allowed_dev_keys = ("allowed_devices_read", "allowed_devices_write_idle", "allowed_devices_write_always")
        forbidden_dev_keys = (
            "forbidden_devices_read",
            "forbidden_devices_write_idle",
            "forbidden_devices_write_always",
        )

        # First filter the devices based on the permissions for 'root' user group.
        #   The 'root' user group permissions should be used to exclude all 'junk', i.e.
        #   unused/outdated/not functioning devices collected from namespace, from
        #   the lists available to ALL the users.
        access_types = ("read", "write_idle", "write_always")
        access_keys = list(zip(access_types, allowed_dev_keys, forbidden_dev_keys))

        devices_root = dict.fromkeys(access_types, existing_devices)
        if "root" in user_groups:
            group_permissions_root = user_group_permissions["user_groups"]["root"]
            if existing_devices:
                for access, allowed_key, forbidden_key in access_keys:
                    devices_root[access] = _filter_device_tree(
                        existing_devices,
                        group_permissions_root.get(allowed_key, []),
                        group_permissions_root.get(forbidden_key, []),
                    )

        # Now create lists of allowed devices based on the lists for the 'root' user group
        for group in user_groups:
            selected_devices = {access: {} for access in access_types}
            if group == "root":
                selected_devices = devices_root
            else:
                group_permissions = user_group_permissions["user_groups"][group]

                if existing_devices:
                    for access, allowed_key, forbidden_key in access_keys:
                        selected_devices[access] = _filter_device_tree(
                            devices_root[access],
                            group_permissions.get(allowed_key, []),
                            group_permissions.get(forbidden_key, []),
                        )

            allowed_devices[group] = selected_devices

    except Exception as ex:
        logger.exception("Error occurred while generating the list of allowed devices: %s", ex)

        # The 'default' list in case error occurred while generating lists
        allowed_devices["root"] = {access: {} for access in access_types}

    return allowed_devices


def _flatten_device_tree(devices, *, prefix=""):
    flat_devices = {}
    for name, description in devices.items():
        full_name = f"{prefix}.{name}" if prefix else name
        if not description.get("excluded", False):
            flat_devices[full_name] = {k: v for k, v in description.items() if k != "components"}
        if components := description.get("components"):
            flat_devices.update(_flatten_device_tree(components, prefix=full_name))
    return flat_devices


def flatten_device_tree(devices):
    """
    Flatten a hierarchical tree of device descriptions. Devices and all their components
    and subcomponents are placed at the top level of the returned dictionary. Devices marked
    with ``"excluded": True`` are skipped, but their components are still included.

    Parameters
    ----------
    devices: dict
        Tree of device descriptions (key - device name, value - device description).
        Components of a device are listed in the ``components`` key of its description.

    Returns
    -------
    dict
        Flat dictionary of device descriptions (without the ``components`` key). The keys
        are full device names, e.g. ``'device.component.subcomponent'``.
    """
    return _flatten_device_tree(devices)


def flatten_allowed_devices(allowed_devices):
    """
    Flatten the hierarchical lists of allowed devices returned by ``select_allowed_devices``.

    Parameters
    ----------
    allowed_devices: dict
        Dictionary of allowed devices for each user group and access type.

    Returns
    -------
    dict
        Dictionary with the same user groups and access types. Each list of devices includes
        devices and all their components (without the ``components`` key) at the top level.
        The keys are full device names, e.g. ``'device.component.subcomponent'``.
    """
    return {
        group: {access: flatten_device_tree(devices) for access, devices in group_devices.items()}
        for group, group_devices in allowed_devices.items()
    }
