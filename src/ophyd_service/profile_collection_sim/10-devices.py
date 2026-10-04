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
    StandardReadable,
    StandardReadableFormat,
    init_devices,
    soft_signal_r_and_setter,
)
from ophyd_async.sim import PatternGenerator, SimPointDetector, SimStage

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


sim_periodic_device = SimPeriodicDevice(name="sim_periodic_device")


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


with init_devices():
    # Channel values are updated at 10 Hz while the detector is acquiring and depend
    # on the positions of the stage motors.
    sim_stage_async = SimStage(_sim_pattern_generator)
    sim_det_async = SimPointDetector(_sim_pattern_generator)

    rand_async_device1 = RandomNumberDevice(period=1.0)
    rand_async_device2 = RandomNumberDevice(period=1.0)
    rand_async_device3 = RandomNumberDevice(period=1.0)
    rand_async_device4 = RandomNumberDevice(period=1.0)

# =======================================================================================
