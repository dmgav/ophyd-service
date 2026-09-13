import importlib
import logging
import os
import re
from datetime import datetime

from bluesky_queueserver.manager.profile_ops import _split_name_pattern

logger = logging.getLogger(__name__)


def get_timestamp_iso8601():
    """
    Returns current timestamp in ISO 8601 format.
    """
    return datetime.now().isoformat()


def get_default_startup_dir():
    """
    Returns the path to the default profile collection that is distributed with the package.
    The function does not guarantee that the directory exists. Used for demo with Python-based worker.
    """
    pc_path = os.path.join(importlib.resources.files("ophyd_service"), "profile_collection_sim", "")
    return pc_path


def get_default_startup_profile():
    """
    Returns the name for the default startup profile. Used for demo with IPython-based worker.
    The startup code is expected to be in ``/tmp/ophyd_service/ipython/profile_collection_sim/startup`` directory.
    """
    return "collection_sim"


def _device_name_matches_pattern(device_name, pattern):
    """
    Check if ``device_name`` matches a single pattern. See ``device_name_matches_patterns()``.
    """
    components, uses_re, _ = _split_name_pattern(pattern)

    if not uses_re:
        # The pattern is an explicitly stated device or subdevice name.
        return device_name == ".".join(_[0] for _ in components)

    name_parts = device_name.split(".")

    for n, (regex, include, is_full_re, depth) in enumerate(components):
        if n >= len(name_parts):
            # The name is shorter than the pattern.
            return False

        if is_full_re:
            # The expression is applied to the remaining part of the name.
            if (depth is not None) and (len(name_parts) - n > depth):
                return False
            return bool(re.search(regex, ".".join(name_parts[n:])))

        if not re.search(regex, name_parts[n]):
            return False

        if n == len(name_parts) - 1:
            # The name ends at this level, so it is selected only if the level is included.
            return include

    # The name is longer than the pattern.
    return False


def device_name_matches_patterns(device_name, patterns):
    """
    Check if the device name (e.g. ``'stage.motor'``) matches at least one of the patterns.
    The patterns in the list are expected to follow the notation used in Queue Server configuration:
    https://blueskyproject.io/bluesky-queueserver/plan_annotation.html#lists-of-device-names

    The device type keywords (e.g. ``'__MOTOR__'``) are accepted, but ignored, since the
    type can not be determined from the name.

    Parameters
    ----------
    device_name: str
        Name of the device or subdevice, e.g. ``'stage.motor'``.
    patterns: list(str)
        List of patterns. The name matches the list if it matches at least one pattern.

    Returns
    -------
    boolean
        Indicates if the device name matches at least one of the patterns.

    Raises
    ------
    TypeError, ValueError
        A pattern is not a string or has invalid format.
    """
    return any(_device_name_matches_pattern(device_name, _) for _ in patterns)


def device_name_is_allowed(device_name, *, allow_patterns, disallow_patterns):
    """
    Check if the device name is allowed: it matches one of the patterns in ``allow_patterns``
    and none of the patterns in ``disallow_patterns``. The function does not raise exceptions:
    the invalid patterns are reported and ignored.

    Following the convention used in user group permissions, the list of patterns containing
    ``None`` as the first element (e.g. ``[None]``) means that all the device names are allowed
    or that no device names are disallowed. An empty ``allow_patterns`` disables access to all
    the devices, an empty ``disallow_patterns`` disallows no devices.

    Parameters
    ----------
    device_name: str
        Name of the device or subdevice, e.g. ``'stage.motor'``.
    allow_patterns: list(str or None)
        List of patterns for the allowed device names.
    disallow_patterns: list(str or None)
        List of patterns for the device names that are not allowed. The patterns are applied
        to the names selected using ``allow_patterns``.

    Returns
    -------
    boolean
        Indicates if the device name is allowed.
    """

    def _matches_any_pattern(patterns):
        for pattern in patterns:
            try:
                if _device_name_matches_pattern(device_name, pattern):
                    return True
            except Exception as ex:
                logger.error("Device name pattern %r is invalid and ignored: %s", pattern, ex)
        return False

    if allow_patterns and (allow_patterns[0] is None):
        allowed = True
    else:
        allowed = _matches_any_pattern(allow_patterns)

    if allowed and disallow_patterns and (disallow_patterns[0] is not None):
        allowed = not _matches_any_pattern(disallow_patterns)

    return allowed
