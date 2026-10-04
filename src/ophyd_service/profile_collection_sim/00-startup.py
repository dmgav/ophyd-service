# flake8: noqa
print(f"Loading file {__file__!r}")

# TODO: The IPython fails to initialize the async Ophyd devices with the RunEngine instance
# The profile loads with/without RunEngine instance in plain Python mode.
from bluesky import RunEngine

RE = RunEngine()
