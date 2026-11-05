# flake8: noqa
print(f"Loading file {__file__!r}")

import asyncio
import random
import time as ttime

import numpy as np
import ophyd
import ophyd.sim
from ophyd import Component as Cpt
from ophyd_async.core import (
    DEFAULT_TIMEOUT,
    AsyncStatus,
    StandardReadable,
    StandardReadableFormat,
    StrictEnum,
    init_devices,
    soft_signal_r_and_setter,
    soft_signal_rw,
)
from ophyd.sim import hw
from ophyd_async.sim import PatternGenerator, SimPointDetector, SimStage

# Load simulated ophyd devices (det1, motor1, ...) into the global namespace.
globals().update(vars(hw()))
rand.start_simulation()
rand2.start_simulation()

# =======================================================================================
#                  Simulated device with periodically updated readings


class SimPeriodicDevice(ophyd.Device):
    """
    Simulated device. The values of the components are updated in background threads
    at the intervals of ``period +/- period_jitter`` seconds.
    """

    noise = Cpt(
        ophyd.sim.SynPeriodicSignal,
        name="noise",
        func=lambda: np.random.normal(loc=1, scale=0.1),
        period=1,
        period_jitter=0.1,
        labels={"detectors"},
    )
    sine = Cpt(
        ophyd.sim.SynPeriodicSignal,
        name="sine",
        func=lambda: np.sin(ttime.monotonic()),
        period=0.5,
        period_jitter=0,
        labels={"detectors"},
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.noise.start_simulation()
        self.sine.start_simulation()


sim_periodic_device1 = SimPeriodicDevice(name="sim_periodic_device1")
sim_periodic_device2 = SimPeriodicDevice(name="sim_periodic_device2")
# Write access to this device is blocked by user group permissions.
sim_periodic_device_no_write = SimPeriodicDevice(name="sim_periodic_device_no_write")

# def callbackValue(value, timestamp, **kwargs):
#     print(f"{value=} {timestamp=}")

# sim_periodic_device1.noise.subscribe(callbackValue, event_type="value")


# =======================================================================================
#                     Additional simulated Ophyd devices (used in tests)


class SynAxisWithLimits(ophyd.sim.SynAxis):
    """Simulated motor with software limits (``low_limit`` and ``high_limit``)."""

    def __init__(self, *args, limits=(-10, 10), **kwargs):
        super().__init__(*args, **kwargs)
        self._low_limit, self._high_limit = limits

    @property
    def low_limit(self):
        return self._low_limit

    @property
    def high_limit(self):
        return self._high_limit


motor_limits = SynAxisWithLimits(name="motor_limits", limits=(-10, 10))

signal_float = ophyd.Signal(name="signal_float", value=0.0)
signal_str = ophyd.Signal(name="signal_str", value="")
signal_ro = ophyd.SignalRO(name="signal_ro", value=0.0)

# Read access to this device is blocked by user group permissions.
forbidden_motor = ophyd.sim.SynAxis(name="forbidden_motor")
# Write access to this device is blocked by user group permissions.
motor_no_write = ophyd.sim.SynAxis(name="motor_no_write")
# 'set' operation takes 2 seconds to complete.
motor_slow = ophyd.sim.SynAxis(name="motor_slow", delay=2)


# =======================================================================================
#                          Simulated 'ophyd-async' devices

_sim_pattern_generator = PatternGenerator()


class RandomNumberDevice(StandardReadable):
    """An ophyd-async Device with a signal updated with a new random value every
    'period' seconds. Reads between the updates return the same value."""

    def __init__(self, period: float = 1.0, name: str = ""):
        self._period = period
        self._update_task: asyncio.Task | None = None
        with self.add_children_as_readables(StandardReadableFormat.HINTED_SIGNAL):
            self.value, self._set_value = soft_signal_r_and_setter(float, self._new_value())
        super().__init__(name)

    def _new_value(self) -> float:
        return random.uniform(0.0, 100.0)

    async def connect(self, mock=False, timeout=DEFAULT_TIMEOUT, force_reconnect=False):
        await super().connect(mock=mock, timeout=timeout, force_reconnect=force_reconnect)
        # Started here so that the task runs in the bluesky event loop.
        if self._update_task is None or self._update_task.done():
            self._update_task = asyncio.create_task(self._update_periodically())

    async def _update_periodically(self):
        while True:
            await asyncio.sleep(self._period)
            self._set_value(self._new_value())


class OnOffMode(StrictEnum):
    OFF = "Off"
    ON = "On"


class SimpleAsyncDevice(StandardReadable):
    """A basic ophyd-async device with soft signals of different types for testing."""

    def __init__(self, name: str = ""):
        with self.add_children_as_readables(StandardReadableFormat.HINTED_SIGNAL):
            self.value_float = soft_signal_rw(float, 0.0)
        with self.add_children_as_readables(StandardReadableFormat.CONFIG_SIGNAL):
            self.value_int = soft_signal_rw(int, 0)
            self.value_str = soft_signal_rw(str, "")
            self.mode = soft_signal_rw(OnOffMode, OnOffMode.OFF)
            self.value_ro, self._set_value_ro = soft_signal_r_and_setter(float, 0.0)
        super().__init__(name)


class SlowAsyncDevice(StandardReadable):
    """An ophyd-async device with 'set' operation that takes 'delay' seconds to complete."""

    def __init__(self, delay: float = 2.0, name: str = ""):
        self._delay = delay
        with self.add_children_as_readables(StandardReadableFormat.HINTED_SIGNAL):
            self.readback, self._set_readback = soft_signal_r_and_setter(float, 0.0)
        super().__init__(name)

    @AsyncStatus.wrap
    async def set(self, value: float):
        await asyncio.sleep(self._delay)
        self._set_readback(value)


with init_devices():
    # Channel values are updated at 10 Hz while the detector is acquiring and depend
    # on the positions of the stage motors.
    sim_stage_async = SimStage(_sim_pattern_generator)
    sim_det_async = SimPointDetector(_sim_pattern_generator)

    rand_async_device1 = RandomNumberDevice(period=1.0)
    rand_async_device2 = RandomNumberDevice(period=1.0)
    rand_async_device3 = RandomNumberDevice(period=1.0)
    rand_async_device4 = RandomNumberDevice(period=1.0)

    simple_async_device = SimpleAsyncDevice()
    slow_async_device = SlowAsyncDevice(delay=2.0)

# =======================================================================================
