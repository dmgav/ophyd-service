"""
Management of the worker environment (worker process lifecycle).

The server process owns the communication pipe and the worker process. There is no
watchdog process: the worker is started, monitored and stopped directly from the
process that runs the FastAPI server.
"""

import asyncio
import contextlib
import enum
import logging
import multiprocessing
import threading
import time as ttime
import uuid

from bluesky_queueserver.manager.comms import PipeJsonRpcSendAsync
from bluesky_queueserver.manager.profile_ops import load_user_group_permissions

from .. import __version__
from .parameters import adjust_startup_options
from .worker import RunEngineWorker
from .worker_utils import device_name_is_allowed, get_timestamp_iso8601

logger = logging.getLogger(__name__)

# Maximum time to wait for the worker to exit in an orderly way before it is killed.
DEFAULT_CLOSE_TIMEOUT = 10.0
# Time to let the pipe polling threads exit before the connections are closed.
_COMM_STOP_DELAY = 0.25
# Period of the task that polls the state of the worker process.
_STATE_MONITOR_PERIOD = 1.0
# Maximum number of status messages buffered for a subscriber that fails to read them in time.
_STATUS_QUEUE_MAXSIZE = 100
# Maximum number of monitor messages buffered for a subscriber that fails to read them in time.
_MONITOR_QUEUE_MAXSIZE = 100
# Maximum time to wait for the worker to complete an operation on a device.
_DEVICE_OPERATION_TIMEOUT = 600.0
# The message that tells the thread reading the device queue to exit.
_DEVICE_QUEUE_STOP = "__stop_reading_device_queue__"
# The message that tells the thread reading the stream queue to exit.
_STREAM_QUEUE_STOP = "__stop_reading_stream_queue__"
# Maximum time to wait for the thread reading the device queue to exit.
_DEVICE_QUEUE_STOP_TIMEOUT = 5.0
# Delay before the next attempt to read the device queue after a failure.
_DEVICE_QUEUE_ERROR_DELAY = 0.5


def _generate_uid():
    """
    Generate a new Run List UID.

    Returns
    -------
    str
        Run List UID
    """
    return str(uuid.uuid4())


class EnvState(enum.Enum):
    CLOSED = "closed"
    OPENING = "opening"
    OPEN = "open"
    CLOSING = "closing"


def default_worker_config():
    """
    Default configuration of the worker process. The worker fails to start unless all
    the keys are present, so the defaults are used for the parameters that are not
    explicitly configured.
    """
    return {
        "use_ipython_kernel": False,
        # Startup code: exactly one of the three sources must be set (Python mode).
        "startup_dir": None,
        "startup_module_name": None,
        "startup_script_path": None,
        "startup_profile": None,
        "ipython_dir": None,
        "ipython_matplotlib": None,
        "user_group_permissions_path": "user_group_permissions.yaml",
        "existing_plans_and_devices_path": None,
        "update_existing_plans_devices": "NEVER",
        "ignore_invalid_plans": False,
        "device_max_depth": 0,
        "ipython_kernel_ip": "localhost",
        "ipython_connection_file": None,
        "ipython_connection_dir": None,
        "ipython_shell_port": None,
        "ipython_iopub_port": None,
        "ipython_stdin_port": None,
        "ipython_hb_port": None,
        "ipython_control_port": None,
    }


class EnvironmentManager:
    """
    Starts, monitors and stops the worker process.

    Parameters
    ----------
    worker_config: dict or None
        Configuration of the worker process. Overrides ``default_worker_config()``.
    close_timeout: float
        Maximum time to wait for the orderly exit before the process is killed.
    log_level: int
        Log level passed to the worker process.
    """

    def __init__(
        self,
        *,
        worker_config=None,
        close_timeout=DEFAULT_CLOSE_TIMEOUT,
        log_level=logging.INFO,
    ):
        self._worker_config = default_worker_config()
        self._worker_config.update(worker_config or {})
        adjust_startup_options(self._worker_config)

        self._close_timeout = close_timeout
        self._log_level = log_level

        self._process = None
        self._comm_to_worker = None
        self._env_state_monitor_task = None
        self._device_queue_monitor_task = None
        self._device_queue_bridge_thread = None
        self._stream_queue_monitor_task = None
        self._stream_queue_bridge_thread = None

        # The pipe and the queue are owned by the server process and reused by each new
        # worker process. The pipe is flushed before a new worker process is started.
        self._conn_server, self._conn_worker = multiprocessing.Pipe()
        self._device_queue = multiprocessing.Queue()
        self._stream_queue = multiprocessing.Queue()

        # The messages read from 'self._device_queue' are passed to the loop using this queue.
        self._device_queue_async = asyncio.Queue()
        # The messages read from 'self._stream_queue' are passed to the loop using this queue.
        self._stream_queue_async = asyncio.Queue()

        # One queue per subscriber (e.g. an open websocket connection).
        self._status_subscribers = set()
        self._monitor_subscribers = set()

        # Pending calls to the worker: 'req_uid' -> set
        self._pending_calls = {}

        self._env_state = EnvState.CLOSED
        self._worker_state = None
        self._user_group_permissions = {}
        # Serializes the open/close operations, which may be requested concurrently.
        self._lock = asyncio.Lock()

        # The constructor is called from the running loop (server startup).
        self._loop = asyncio.get_running_loop()
        self._start_state_monitor()
        self._start_device_queue_monitor()
        self._start_stream_queue_monitor()

    @property
    def env_state(self):
        return self._env_state

    @property
    def device_queue(self):
        return self._device_queue

    @property
    def stream_queue(self):
        return self._stream_queue

    @property
    def is_running(self):
        return (self._process is not None) and self._process.is_alive()

    def get_status(self):
        """
        Status of the service in the form of a dictionary.
        """
        worker_state = self._worker_state or {}
        return {
            "msg": f"ophyd-service v{__version__}",
            "time": get_timestamp_iso8601(),
            "manager_state": self._env_state.value,
            "worker_environment_exists": self._env_state == EnvState.OPEN,
            "worker_environment_state": worker_state.get("environment_state", None),
            "status_uid": _generate_uid(),  # New UID each time status is updated
            "devices_existing_uid": None,
            "devices_allowed_uid": None,
        }

    # ------------------------------------------------------------
    #                        Open environment

    async def open_environment(self):
        """
        Start the worker process and wait until the environment is ready. Returns
        ``(success, err_msg)``.
        """

        def _validate_startup_config():
            """
            In Python mode the startup code is loaded by the worker using
            ``load_worker_startup_code()``, which requires exactly one source to be specified.
            """
            if self._worker_config["use_ipython_kernel"]:
                return

            keys = ("startup_dir", "startup_module_name", "startup_script_path")
            if sum(self._worker_config.get(_) is not None for _ in keys) != 1:
                raise ValueError(
                    "Exactly one source of startup code ('startup_dir', 'startup_module_name' "
                    "or 'startup_script_path') must be configured."
                )

        def _flush_pipes():
            for conn in (self._conn_server, self._conn_worker):
                try:
                    while conn.poll():
                        conn.recv_bytes()
                except (EOFError, OSError) as ex:
                    logger.warning("Failed to flush the communication pipe: %s", ex)

        async def _start_worker(user_group_permissions):
            _flush_pipes()

            self._process = RunEngineWorker(
                conn=self._conn_worker,
                device_queue=self._device_queue,
                stream_queue=self._stream_queue,
                name="Worker Process",
                config=self._worker_config,
                log_level=self._log_level,
                user_group_permissions=user_group_permissions,
            )
            await asyncio.to_thread(self._process.start)

            # The object must be created in the running loop.
            self._comm_to_worker = PipeJsonRpcSendAsync(
                conn=self._conn_server,
                use_json=False,
                name="Server-Worker Comm",
            )
            self._comm_to_worker.start()

        async def _wait_until_ready():
            """
            Poll the worker state until the environment is ready. Loading of the startup code
            may take arbitrarily long time, so no timeout is applied. The worker switches to
            the 'closing' state if it fails to load the startup code.
            """
            while True:
                if not self.is_running:
                    return False, "Worker process terminated unexpectedly while opening the environment."

                status = await self._request_worker_state()
                env_state = status.get("environment_state") if status else None

                if env_state == "idle":
                    return True, ""
                if env_state in ("failed", "closing"):
                    return False, "Failed to load the startup code."

                await asyncio.sleep(0.2)

        async with self._lock:
            if (self._env_state != EnvState.CLOSED) or self.is_running:
                return False, "RE Worker environment already exists."

            try:
                _validate_startup_config()
                # Permissions are loaded from disk before the process is created.
                user_group_permissions_path = self._worker_config.get("user_group_permissions_path")
                user_group_permissions = await asyncio.to_thread(
                    load_user_group_permissions, user_group_permissions_path
                )
                self._user_group_permissions = user_group_permissions
            except Exception as ex:
                logger.exception("Failed to open RE Worker environment: %s", ex)
                return False, f"Failed to open RE Worker environment: {ex}"

            self._env_state = EnvState.OPENING
            logger.info("Opening RE Worker environment ...")

            try:
                await _start_worker(user_group_permissions)
                success, err_msg = await _wait_until_ready()
            except Exception as ex:
                logger.exception("Failed to start RE Worker process: %s", ex)
                success, err_msg = False, f"Failed to start RE Worker process: {ex}"

            if success:
                self._env_state = EnvState.OPEN
                logger.info("RE Worker environment was opened successfully")
            else:
                logger.error("Failed to open RE Worker environment: %s", err_msg)
                await self._destroy_worker()
                self._env_state = EnvState.CLOSED

            return success, err_msg

    # ------------------------------------------------------------
    #                       Close environment

    async def _cleanup(self):
        if self._comm_to_worker is not None:
            self._comm_to_worker.stop()
            self._comm_to_worker = None
            # The polling threads raise an error if the connection is closed while in use.
            await asyncio.sleep(_COMM_STOP_DELAY)

        self._process = None
        self._worker_state = None

    async def _destroy_worker(self):
        """
        Kill the worker process and release the resources.
        """
        if self.is_running:
            logger.warning("Killing the worker process ...")
            try:
                self._process.kill()
                await asyncio.to_thread(self._process.join)
            except Exception as ex:
                logger.exception("Failed to kill the worker process: %s", ex)

        await self._cleanup()

    async def close_environment(self):
        """
        Close the environment in an orderly way. The worker process is killed if it fails
        to exit before the timeout expires. Returns ``(success, err_msg)``.
        """

        async def _close_worker(deadline):
            try:
                response = await self._comm_to_worker.send_msg("command_close_env")
            except Exception as ex:
                return False, f"Failed to send the request to close the environment: {ex}"

            if response.get("status") != "accepted":
                return False, response.get("err_msg") or "The request to close the environment was rejected."

            # Wait until the worker is ready to exit and is waiting for the confirmation.
            while ttime.monotonic() < deadline:
                if not self.is_running:
                    return True, ""
                status = await self._request_worker_state()
                if status and status.get("environment_state") == "closing":
                    break
                await asyncio.sleep(0.1)
            else:
                return False, "Timeout while waiting for the worker to prepare to exit."

            try:
                await self._comm_to_worker.send_msg("command_confirm_exit")
            except Exception as ex:
                return False, f"Failed to confirm exit of the worker process: {ex}"

            timeout = max(deadline - ttime.monotonic(), 0)
            await asyncio.to_thread(self._process.join, timeout)

            if self.is_running:
                return False, "Timeout while waiting for the worker process to exit."

            return True, ""

        async with self._lock:
            if (self._env_state != EnvState.OPEN) or not self.is_running:
                return False, "RE Worker environment does not exist."

            self._env_state = EnvState.CLOSING
            logger.info("Closing RE Worker environment ...")

            deadline = ttime.monotonic() + self._close_timeout
            success, err_msg = await _close_worker(deadline)

            if not success or self.is_running:
                logger.error("Failed to close RE Worker environment in an orderly way: %s", err_msg)
                await self._destroy_worker()
                success, err_msg = True, f"The worker process was killed: {err_msg}"
            else:
                await self._cleanup()
                logger.info("RE Worker environment was closed successfully")

            self._env_state = EnvState.CLOSED
            return success, err_msg

    # ------------------------------------------------------------
    #                          Device API

    def _validate_device_name(self, device_name, *, user_group):
        """
        Check if the device may be accessed by the user. The name is validated using permissions
        of the 'root' group and then the permissions of the user group. Raises ``RuntimeError``
        if the device name is not allowed.
        """
        user_groups = self._user_group_permissions.get("user_groups", {})

        for group in ("root", user_group):
            permissions = user_groups.get(group)
            if permissions is None:
                raise RuntimeError(f"Permissions for the user group {group!r} are not defined.")

            # If the lists are not defined, then no devices are allowed.
            allowed = device_name_is_allowed(
                device_name,
                allow_patterns=permissions.get("allowed_devices_read", []),
                disallow_patterns=permissions.get("forbidden_devices_read", []),
            )

            if not allowed:
                raise RuntimeError(f"Device {device_name!r} is not allowed for the user group {group!r}.")

    async def device_read(self, device_name, *, user_group):
        """
        Request the worker to read the device ``device_name`` and wait until the operation is
        completed. The device must be allowed for the user group ``user_group``. Returns
        ``(success, err_msg, value, req_uid)``, where ``req_uid`` is used to identify the result
        of the operation.
        """
        req_uid = _generate_uid()
        value = None
        # The event is set once the result of the operation is received from the worker.
        event, result = asyncio.Event(), {}
        self._pending_calls[req_uid] = (event, result)

        try:
            if (self._env_state != EnvState.OPEN) or (self._comm_to_worker is None):
                raise RuntimeError("RE Worker environment does not exist.")

            self._validate_device_name(device_name, user_group=user_group)

            try:
                response = await self._comm_to_worker.send_msg(
                    "device_read", {"device_name": device_name, "req_uid": req_uid}
                )
            except Exception as ex:
                logger.exception("Failed to send the request to read the device '%s': %s", device_name, ex)
                raise RuntimeError(f"Failed to send the request to read the device: {ex}") from ex

            if response.get("status") != "accepted":
                raise RuntimeError(response.get("err_msg") or "The request to read the device was rejected.")

            try:
                await asyncio.wait_for(event.wait(), timeout=_DEVICE_OPERATION_TIMEOUT)
            except asyncio.TimeoutError as ex:
                raise RuntimeError("Timeout while waiting for the device to be read.") from ex

            if not result.get("success"):
                raise RuntimeError(result.get("err_msg") or "Failed to read the device.")

            value = result.get("result")

            success, err_msg = True, ""
        except RuntimeError as ex:
            success, err_msg = False, str(ex)
        finally:
            self._pending_calls.pop(req_uid, None)

        return success, err_msg, value, req_uid

    # ------------------------------------------------------------
    #                      Device queue monitor

    def _start_device_queue_monitor(self):
        """
        Start the thread that reads the queue shared with the worker process and the task that
        delivers the results of the operations on devices to the callers.
        """
        if self._device_queue_bridge_thread is None:
            self._device_queue_bridge_thread = threading.Thread(
                target=self._device_queue_bridge, name="Device Queue Bridge", daemon=True
            )
            self._device_queue_bridge_thread.start()

        if self._device_queue_monitor_task is None:
            self._device_queue_monitor_task = asyncio.create_task(self._device_queue_monitor())

    async def _stop_device_queue_monitor(self):
        """
        Stop the task and the thread and wait until they exit.
        """
        if self._device_queue_monitor_task is not None:
            self._device_queue_monitor_task.cancel()
            try:
                await self._device_queue_monitor_task
            except asyncio.CancelledError:
                pass
            self._device_queue_monitor_task = None

        if self._device_queue_bridge_thread is not None:
            # The sentinel wakes up the blocking call in the thread.
            self._device_queue.put(_DEVICE_QUEUE_STOP)
            await asyncio.to_thread(self._device_queue_bridge_thread.join, _DEVICE_QUEUE_STOP_TIMEOUT)
            if self._device_queue_bridge_thread.is_alive():
                logger.warning("The thread reading the device queue failed to exit")
            self._device_queue_bridge_thread = None

    def _device_queue_bridge(self):
        """
        Read the messages from the queue shared with the worker process and pass them to the loop.
        'multiprocessing.Queue' has no asynchronous API, so the blocking calls are executed in
        this thread.
        """
        while True:
            try:
                msg = self._device_queue.get()
            except Exception as ex:
                logger.exception("Failed to read the message from the device queue: %s", ex)
                ttime.sleep(_DEVICE_QUEUE_ERROR_DELAY)
                continue

            if msg == _DEVICE_QUEUE_STOP:
                break

            self._loop.call_soon_threadsafe(self._device_queue_async.put_nowait, msg)

    async def _device_queue_monitor(self):
        while True:
            msg = await self._device_queue_async.get()

            req_uid = msg.get("req_uid") if isinstance(msg, dict) else None
            pending_call = self._pending_calls.get(req_uid)

            if pending_call is None:
                # The caller is no longer waiting for the result (e.g. the request timed out).
                logger.warning("Received the result of an unknown request: %s", req_uid)
                continue

            event, result = pending_call
            result.update(msg)
            event.set()

    # ------------------------------------------------------------
    #                      Stream queue monitor

    def _start_stream_queue_monitor(self):
        """
        Start the thread that reads the queue shared with the worker process and the task that
        delivers the data on the monitored PVs to the subscribers.
        """
        if self._stream_queue_bridge_thread is None:
            self._stream_queue_bridge_thread = threading.Thread(
                target=self._stream_queue_bridge, name="Stream Queue Bridge", daemon=True
            )
            self._stream_queue_bridge_thread.start()

        if self._stream_queue_monitor_task is None:
            self._stream_queue_monitor_task = asyncio.create_task(self._stream_queue_monitor())

    async def _stop_stream_queue_monitor(self):
        """
        Stop the task and the thread and wait until they exit.
        """
        if self._stream_queue_monitor_task is not None:
            self._stream_queue_monitor_task.cancel()
            try:
                await self._stream_queue_monitor_task
            except asyncio.CancelledError:
                pass
            self._stream_queue_monitor_task = None

        if self._stream_queue_bridge_thread is not None:
            # The sentinel wakes up the blocking call in the thread.
            self._stream_queue.put(_STREAM_QUEUE_STOP)
            await asyncio.to_thread(self._stream_queue_bridge_thread.join, _DEVICE_QUEUE_STOP_TIMEOUT)
            if self._stream_queue_bridge_thread.is_alive():
                logger.warning("The thread reading the stream queue failed to exit")
            self._stream_queue_bridge_thread = None

    def _stream_queue_bridge(self):
        """
        Read the messages from the queue shared with the worker process and pass them to the loop.
        'multiprocessing.Queue' has no asynchronous API, so the blocking calls are executed in
        this thread.
        """
        while True:
            try:
                msg = self._stream_queue.get()
            except Exception as ex:
                logger.exception("Failed to read the message from the stream queue: %s", ex)
                ttime.sleep(_DEVICE_QUEUE_ERROR_DELAY)
                continue

            if msg == _STREAM_QUEUE_STOP:
                break

            self._loop.call_soon_threadsafe(self._stream_queue_async.put_nowait, msg)

    async def _stream_queue_monitor(self):
        while True:
            msg = await self._stream_queue_async.get()
            self._publish_monitor(msg)

    # ------------------------------------------------------------
    #                            Shutdown

    async def stop(self):
        """
        Close the environment if it exists and release the resources. The manager can not
        be used after this call.
        """
        if self._env_state == EnvState.OPEN:
            success, err_msg = await self.close_environment()
            if not success:
                logger.error("Failed to close the RE Worker environment: %s", err_msg)

        await self._stop_state_monitor()
        await self._stop_device_queue_monitor()
        await self._stop_stream_queue_monitor()

        for conn in (self._conn_server, self._conn_worker):
            try:
                conn.close()
            except Exception as ex:
                logger.debug("Failed to close the communication pipe: %s", ex)

        try:
            self._device_queue.close()
            self._stream_queue.close()
        except Exception as ex:
            logger.debug("Failed to close the queue: %s", ex)

    # ------------------------------------------------------------
    #                        State monitor

    def _start_state_monitor(self):
        """
        Start the task that periodically requests the state of the worker process.
        """
        if self._env_state_monitor_task is None:
            self._env_state_monitor_task = asyncio.create_task(self._env_state_monitor())

    async def _stop_state_monitor(self):
        """
        Stop the task and wait until it exits.
        """
        if self._env_state_monitor_task is not None:
            self._env_state_monitor_task.cancel()
            try:
                await self._env_state_monitor_task
            except asyncio.CancelledError:
                pass
            self._env_state_monitor_task = None

    @contextlib.contextmanager
    def subscribe_status(self):
        """
        Context manager that yields a queue receiving the status messages published while
        the subscription is active. Each subscriber gets its own queue and its own copy of
        every message.
        """
        queue = asyncio.Queue(maxsize=_STATUS_QUEUE_MAXSIZE)
        self._status_subscribers.add(queue)
        try:
            yield queue
        finally:
            self._status_subscribers.discard(queue)

    def _publish_status(self):
        msg = {"status": self.get_status()}
        # logger.debug(f"The number of status subscribers: {len(self._status_subscribers)}")
        for queue in self._status_subscribers:
            if queue.full():
                # The subscriber is not reading the messages fast enough: drop the oldest one.
                queue.get_nowait()
            queue.put_nowait(msg)

    async def _env_state_monitor(self):
        while True:
            await asyncio.sleep(_STATE_MONITOR_PERIOD)
            if self.is_running and (self._comm_to_worker is not None):
                self._worker_state = await self._request_worker_state()
            else:
                self._worker_state = None
            self._publish_status()

    # ------------------------------------------------------------
    #                        Monitored data

    @contextlib.contextmanager
    def subscribe_monitor(self):
        """
        Context manager that yields a queue receiving the data on the monitored PVs published
        while the subscription is active. Each subscriber gets its own queue and its own copy
        of every message.
        """
        queue = asyncio.Queue(maxsize=_MONITOR_QUEUE_MAXSIZE)
        self._monitor_subscribers.add(queue)
        try:
            yield queue
        finally:
            self._monitor_subscribers.discard(queue)

    def _publish_monitor(self, msg):
        for queue in self._monitor_subscribers:
            if queue.full():
                # The subscriber is not reading the messages fast enough: drop the oldest one.
                queue.get_nowait()
            queue.put_nowait(msg)

    # ------------------------------------------------------------

    async def _request_worker_state(self):
        try:
            return await self._comm_to_worker.send_msg("request_state")
        except Exception as ex:
            logger.debug("Failed to load the worker state: %s", ex)
            return None
