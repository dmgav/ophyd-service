import importlib
import os


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
