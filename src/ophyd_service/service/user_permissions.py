import os

import jsonschema
import yaml

_user_group_permission_schema = {
    "type": "object",
    "additionalProperties": False,
    "required": ["user_groups"],
    "properties": {
        "user_groups": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "allowed_devices_read": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                    "forbidden_devices_read": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                    "allowed_devices_write_idle": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                    "forbidden_devices_write_idle": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                    "allowed_devices_write_always": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                    "forbidden_devices_write_always": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                },
            },
        },
    },
}


def _validate_user_group_permissions_schema(user_group_permissions):
    """
    Validate user group permissions schema. Raises exception if validation fails.

    Parameters
    ----------
    user_group_permissions: dict
        A dictionary with user group permissions.
    """
    jsonschema.validate(instance=user_group_permissions, schema=_user_group_permission_schema)


def validate_user_group_permissions(user_group_permissions):
    """
    Validate the dictionary with user group permissions. Exception is raised errors are detected.

    Parameters
    ----------
    user_group_permissions: dict
        The dictionary with user group permissions.
    """
    if not isinstance(user_group_permissions, dict):
        raise TypeError(f"User group permissions: Invalid type '{type(user_group_permissions)}', must be 'dict'")

    _validate_user_group_permissions_schema(user_group_permissions)

    if "root" not in user_group_permissions["user_groups"]:
        raise KeyError("User group permissions: Missing required user group: 'root'")


def load_user_group_permissions(path_to_file=None):
    """
    Load the data on allowed plans and devices for user groups. User group 'root'
    is required. Exception is raised in user group 'root' is missing.

    Parameters
    ----------
    path_to_file: str
        Full path to YAML file that contains user data permissions.

    Returns
    -------
    dict
        Data structure with user permissions. Returns ``{}`` if path is an empty string
        or None.

    Raises
    ------
    OSError
        Error while reading the YAML file.
    """

    if not path_to_file:
        return {}

    try:
        if not os.path.isfile(path_to_file):
            raise OSError(f"File '{path_to_file}' does not exist.")

        with open(path_to_file) as stream:
            user_group_permissions = yaml.safe_load(stream)

        validate_user_group_permissions(user_group_permissions)

        if "root" not in user_group_permissions["user_groups"]:
            raise Exception("Missing required user group: 'root'")

    except Exception as ex:
        msg = f"Error while loading user group permissions from file '{path_to_file}': {str(ex)}"
        raise OSError(msg) from ex

    return user_group_permissions
