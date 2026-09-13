import asyncio
import copy
import enum
import inspect
import json
import logging
import os
import queue
import re
import sys
import threading
import time as ttime
from multiprocessing import Process
from threading import Thread

from bluesky_queueserver.manager.comms import PipeJsonRpcReceive
from bluesky_queueserver.manager.logging_setup import PPrintForLogging as ppfl
from bluesky_queueserver.manager.logging_setup import setup_loggers
from bluesky_queueserver.manager.profile_ops import (
    existing_plans_and_devices_from_nspace,
    load_allowed_plans_and_devices,
    load_worker_startup_code,
    update_existing_plans_and_devices,
)

from .worker_utils import get_timestamp_iso8601

logger = logging.getLogger(__name__)


# State of the worker environment
class EState(enum.Enum):
    INITIALIZING = "initializing"
    IDLE = "idle"
    FAILED = "failed"
    CLOSING = "closing"
    CLOSED = "closed"  # For completeness


# State of IPKernel
class IPKernelState(enum.Enum):
    DISABLED = "disabled"  # Kernel is not started (or worker is in Python mode)
    BUSY = "busy"
    IDLE = "idle"
    STARTING = "starting"


# Device name is a dotted sequence of identifiers, e.g. 'det1' or 'sim_stage.det.val'.
_device_name_pattern = re.compile(r"[_a-zA-Z][_a-zA-Z0-9]*(\.[_a-zA-Z][_a-zA-Z0-9]*)*")

# Maximum time to wait for the thread running the event loop to exit.
_LOOP_STOP_TIMEOUT = 5.0


class RunEngineWorker(Process):
    """
    The class implementing the worker process. The worker loads the startup code and reports
    the lists of existing and allowed devices. It does not execute plans, functions or scripts.

    Parameters
    ----------
    conn: multiprocessing.Connection
        One end of bidirectional (input/output) pipe. The other end is used by RE Manager.
    device_queue: multiprocessing.Queue
        Queue used to pass messages from the worker to the process that owns the worker.
    stream_queue: multiprocessing.Queue
        Queue used to pass the data on the monitored PVs to the process that owns the worker.
    args, kwargs
        `args` and `kwargs` of the `multiprocessing.Process`
    """

    def __init__(
        self,
        *args,
        conn,
        config=None,
        device_queue=None,
        stream_queue=None,
        log_level=logging.DEBUG,
        user_group_permissions=None,
        **kwargs,
    ):
        if not conn:
            raise RuntimeError("Invalid value of parameter 'conn': %S.", str(conn))

        super().__init__(*args, **kwargs)

        self._log_level = log_level
        self._device_queue = device_queue
        self._stream_queue = stream_queue

        self._user_group_permissions = user_group_permissions or {}

        # The end of bidirectional Pipe assigned to the worker (for communication with Manager process)
        self._conn = conn

        self._exit_event = None
        self._exit_confirmed_event = None

        # The following variable determine the state of RE Worker
        self._env_state = EState.CLOSED

        # Class that supports communication over the pipe
        self._comm_to_manager = None

        # Note: 'self._config' is a private attribute of 'multiprocessing.Process'. Overriding
        #   this variable may lead to unpredictable and hard to debug issues.
        self._config_dict = config or {}
        self._existing_plans_and_devices_changed = False
        self._existing_devices = {}
        self._allowed_devices = {}

        self._allowed_items_lock = None  # threading.Lock()
        self._existing_items_lock = None  # threading.Lock()

        # Indicates when to update the existing plans and devices
        update_pd = self._config_dict["update_existing_plans_devices"]
        if update_pd not in ("NEVER", "ENVIRONMENT_OPEN", "ALWAYS"):
            logger.error(
                "Unknown option for updating lists of existing plans and devices: '%s'. "
                "The lists stored on disk are not going to be updated.",
                update_pd,
            )
            update_pd = "NEVER"
        self._update_existing_plans_devices_on_disk = update_pd

        self._use_ipython_kernel = self._config_dict["use_ipython_kernel"]
        self._ip_kernel_app = None  # Reference to IPKernelApp, None if IPython is not used
        self._ip_connect_file = ""  # Filename with connection info for the running IP kernel
        self._ip_connect_info = {}  # Connection info for the running IP Kernel
        self._ip_kernel_client = None  # Kernel client for communication with IP kernel.
        self._ip_kernel_state = IPKernelState.DISABLED
        self._ip_kernel_monitor_stop = False
        # List of message types that are allowed to be printed even if the message does not contain parent header
        self._ip_kernel_monitor_always_allow_types = []
        # The list of collected tracebacks. If the variable is a list, then tracebacks (strings) are appended
        #   to the list. If the variable is None, then tracebacks are not collected.
        self._ip_kernel_monitor_collected_tracebacks = None

        # The event is used to monitor shutdown of IPython kernel.
        self._ip_kernel_is_shut_down_event = None

        self._loop = None  # Event loop used by the worker
        self._loop_thread = None  # Thread that runs the event loop

        self._re_namespace, self._devices_in_nspace = {}, {}

        self._worker_shutdown_initiated = False  # Indicates if shutdown is initiated by request
        self._unexpected_shutdown = False  # Indicates if shutdown is in progress, but it was not requested

        self._success_startup = True  # Indicates if worker startup is proceding successfully

    def _generate_list_of_allowed_devices(self):
        """
        Generate the list of allowed devices based on the existing devices and user permissions.
        """
        logger.info("Generating the list of allowed devices")

        with self._existing_items_lock:
            existing_devices = self._existing_devices

        _, allowed_devices = load_allowed_plans_and_devices(
            existing_plans={},
            existing_devices=existing_devices,
            user_group_permissions=self._user_group_permissions,
        )

        with self._allowed_items_lock:
            self._allowed_devices = allowed_devices

        logger.info("List of allowed devices was successfully generated")

    def _update_existing_pd_file(self, *, options):
        """
        Update the list of existing devices on disk. ``options`` parameter is a list (or tuple)
        of options which are compared to ``self._update_existing_plans_devices_on_disk`` to
        determine if the list should be saved.
        """
        path_pd = self._config_dict["existing_plans_and_devices_path"]

        if self._update_existing_plans_devices_on_disk in options:
            with self._existing_items_lock:
                existing_devices = self._existing_devices
            update_existing_plans_and_devices(
                path_to_file=path_pd,
                existing_plans={},
                existing_devices=existing_devices,
            )

    # =============================================================================
    #               Handlers for messages from RE Manager

    def _request_state_handler(self):
        """
        Returns the state information of RE Worker environment.
        """
        self._stream_queue.put({"heartbeat": {"time": get_timestamp_iso8601()}})

        env_state_str = self._env_state.value
        plans_and_devices_list_updated = self._existing_plans_and_devices_changed
        ip_kernel_state = self._ip_kernel_state.value
        unexpected_shutdown = self._unexpected_shutdown
        msg_out = {
            "environment_state": env_state_str,
            "plans_and_devices_list_updated": plans_and_devices_list_updated,
            "ip_kernel_state": ip_kernel_state,
            "unexpected_shutdown": unexpected_shutdown,
        }
        return msg_out

    def _request_ip_connect_info(self):
        """
        Return IP connect info obtained from IPython kernel. Returns ``{}`` if
        worker is using pure Python.
        """
        connect_info = copy.deepcopy(self._ip_connect_info)
        connect_info["key"] = connect_info["key"].decode("utf-8")
        return {"ip_connect_info": connect_info}

    def _request_plans_and_devices_list_handler(self):
        """
        Returns currents lists of existing plans and devices. Also returns the current
        dictionary of user group permissions. It is assumed that the dictionary of user group
        permissions is relatively small and passing it with the lists of existing plans
        and devices should not affect the performance. If performance becomes an issue, then
        create a separate API for passing user group permissions.
        """
        with self._existing_items_lock:
            existing_devices = self._existing_devices
        msg_out = {
            "existing_devices": existing_devices,
            "user_group_permissions": self._user_group_permissions,
        }
        self._existing_plans_and_devices_changed = False
        return msg_out

    def _command_close_env_handler(self):
        """
        Close RE Worker environment in orderly way.
        """
        # Stop the loop in main thread
        logger.info("Closing RE Worker environment ...")
        err_msg = None

        if self._ip_kernel_state == IPKernelState.BUSY:
            # The condition for IP Kernel 'busy' state accounts for the case when IP is not used.
            status = "rejected"
            err_msg = "IPython kernel is busy and can not be stopped."
        else:
            try:
                if self._use_ipython_kernel:
                    # Send 'quit' command to the kernel, 'exit_event' is set elsewhere.
                    self._ip_kernel_shutdown()
                else:
                    self._exit_event.set()
                self._worker_shutdown_initiated = True  # Shutdown is initiated by request
                status = "accepted"
            except Exception as ex:
                status = "error"
                err_msg = str(ex)
        msg_out = {"status": status, "err_msg": err_msg}
        return msg_out

    def _command_confirm_exit_handler(self):
        """
        Confirm that the environment is closed. The 'accepted' status returned by
        the function indicates that the environment is almost closed. After the command
        is sent two things should happen: the environment is closed completely and the process
        is terminated; the caller may safely assume that the environment does not exist
        (no messages can be sent to the closed environment).
        """
        err_msg = ""
        if self._exit_event.is_set():
            self._exit_confirmed_event.set()
            status = "accepted"
        else:
            status = "rejected"
            err_msg = (
                "Environment closing was not initiated. Use command 'command_close_env' "
                "to initiate closing and wait for RE Worker state: "
                f"environment state is '{self._env_state.value}'"
            )
        msg_out = {"status": status, "err_msg": err_msg}
        return msg_out

    def _command_permissions_reload_handler(self, user_group_permissions):
        """
        Initiate reloading of permissions and computing the new list of allowed devices.
        Computations are performed in a separate thread. The function is not waiting for computations
        to complete. Status ('accepted' or 'rejected') and error message is returned. 'accepted' status
        does not mean that the operation was successful.
        """
        try:
            self._user_group_permissions = user_group_permissions
            th = threading.Thread(target=self._generate_list_of_allowed_devices, daemon=True)
            th.start()
            status, err_msg = "accepted", ""
        except Exception as ex:
            status, err_msg = "rejected", f"Error: {ex}"

        return {"status": status, "err_msg": err_msg}

    def _command_interrupt_kernel_handler(self):
        """
        Send an interrupt request to the IPython kernel. Call fails if the worker is running
        on Python (not IPython kernel).
        """
        logger.debug("Interrupting kernel ...")
        try:
            if not self._use_ipython_kernel:
                raise RuntimeError("The worker is not running an IPython kernel")

            msg = self._ip_kernel_client.session.msg("interrupt_request", content={})
            self._ip_kernel_client.control_channel.send(msg)
            status, err_msg = "accepted", ""
        except Exception as ex:
            status, err_msg = "rejected", f"Error: {ex}"

        return {"status": status, "err_msg": err_msg}

    def _device_read_handler(self, device_name, req_uid):
        """
        Read the device with the name ``device_name``. The name may refer to a subdevice,
        e.g. 'sim_stage.det'. ``req_uid`` is the UID of the request, which is used to
        identify the result of the operation.
        """
        logger.debug("Reading the device '%s' (request UID '%s') ...", device_name, req_uid)
        try:
            # The name is evaluated, so only dotted identifiers are accepted.
            if not isinstance(device_name, str) or not _device_name_pattern.fullmatch(device_name):
                raise ValueError(f"Invalid device name: {device_name!r}")

            # In IPython mode the namespace is the user namespace of the kernel.
            nspace = self._re_namespace
            try:
                device = eval(device_name, nspace, nspace)  # noqa: S307
            except Exception as ex:
                raise RuntimeError(f"Device '{device_name}' is not found in the namespace: {ex}") from ex

            if not hasattr(device, "read"):
                raise RuntimeError(f"Object '{device_name}' has no attribute 'read'")

            if inspect.iscoroutinefunction(device.read):
                coro = self._device_read_async(device_name, device, req_uid)
            else:
                coro = self._device_read_thread(device_name, device, req_uid)

            # The handler is called from the communication thread, the task runs in the worker loop.
            asyncio.run_coroutine_threadsafe(coro, self._loop)

            status, err_msg = "accepted", ""
        except Exception as ex:
            status, err_msg = "rejected", f"Error: {ex}"

        return {"status": status, "err_msg": err_msg}

    async def _device_read_async(self, device_name, device, req_uid):
        """
        Read the device with the asynchronous ``read()`` method (e.g. 'ophyd-async' device).
        """
        try:
            result = await device.read()
            logger.info("Device '%s' was read (request UID '%s'): %s", device_name, req_uid, result)
            # The result is returned to the clients as JSON, so it must be serializable.
            json.dumps(result)
            msg = {"req_uid": req_uid, "success": True, "err_msg": "", "result": result}
        except Exception as ex:
            logger.exception("Failed to read the device '%s' (request UID '%s'): %s", device_name, req_uid, ex)
            msg = {"req_uid": req_uid, "success": False, "err_msg": f"Error: {ex}", "result": None}

        self._device_queue.put(msg)

    async def _device_read_thread(self, device_name, device, req_uid):
        """
        Read the device with the blocking ``read()`` method (e.g. 'ophyd' device). The method
        is executed in a separate thread, so that the loop is not blocked.
        """
        try:
            result = await asyncio.to_thread(device.read)
            # logger.debug("Device '%s' was read (request UID '%s'): %s", device_name, req_uid, result)
            # The result is returned to the clients as JSON, so it must be serializable.
            json.dumps(result)
            msg = {"req_uid": req_uid, "success": True, "err_msg": "", "result": result}
        except Exception as ex:
            logger.exception("Failed to read the device '%s' (request UID '%s'): %s", device_name, req_uid, ex)
            msg = {"req_uid": req_uid, "success": False, "err_msg": f"Error: {ex}", "result": None}

        self._device_queue.put(msg)

    # ------------------------------------------------------------

    def _execute_in_main_thread(self):
        """
        Run this function to block the main thread. No plans or tasks are executed by the worker,
        so the function simply waits until the environment is closed.
        """
        self._exit_event.wait()

    # ------------------------------------------------------------

    def _start_event_loop(self):
        """
        Run the event loop in a separate thread. The loop is used to execute asynchronous tasks,
        such as reading of devices. In Python mode the main thread is blocked until the environment
        is closed, so the loop can not be run in the main thread.
        """

        def _run_loop():
            asyncio.set_event_loop(self._loop)
            self._loop.run_forever()

        self._loop_thread = threading.Thread(target=_run_loop, name="Worker Event Loop", daemon=True)
        self._loop_thread.start()

    def _stop_event_loop(self):
        """
        Stop the event loop and wait until the thread exits. The running tasks are not awaited.
        """
        if self._loop_thread is None:
            return

        self._loop.call_soon_threadsafe(self._loop.stop)
        self._loop_thread.join(timeout=_LOOP_STOP_TIMEOUT)

        if self._loop_thread.is_alive():
            logger.warning("The thread running the event loop failed to exit")
        else:
            self._loop.close()

        self._loop_thread = None

    # ------------------------------------------------------------

    def _worker_prepare_for_startup(self):
        """
        Operations necessary to prepare for worker startup (before loading)
        """
        from bluesky_queueserver.manager.profile_tools import set_ipython_mode, set_re_worker_active

        self._ip_kernel_is_shut_down_event = threading.Event()  # Used with IPython kernel

        # Set the environment variable indicating that RE Worker is active. Status may be
        #   checked using 'is_re_worker_active()' in startup scripts or modules.
        set_re_worker_active()
        set_ipython_mode(self._use_ipython_kernel)

        # Class that supports communication over the pipe
        self._comm_to_manager = PipeJsonRpcReceive(conn=self._conn, use_json=False, name="RE Worker-Manager Comm")

        self._comm_to_manager.add_method(self._request_state_handler, "request_state")
        self._comm_to_manager.add_method(self._request_ip_connect_info, "request_ip_connect_info")
        self._comm_to_manager.add_method(
            self._request_plans_and_devices_list_handler, "request_plans_and_devices_list"
        )
        self._comm_to_manager.add_method(self._command_close_env_handler, "command_close_env")
        self._comm_to_manager.add_method(self._command_confirm_exit_handler, "command_confirm_exit")
        self._comm_to_manager.add_method(self._command_permissions_reload_handler, "command_permissions_reload")
        self._comm_to_manager.add_method(self._command_interrupt_kernel_handler, "command_interrupt_kernel")
        self._comm_to_manager.add_method(self._device_read_handler, "device_read")

        self._comm_to_manager.start()

        self._exit_event = threading.Event()
        self._exit_confirmed_event = threading.Event()

        self._allowed_items_lock = threading.Lock()
        self._existing_items_lock = threading.Lock()

        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        if not self._use_ipython_kernel:
            self._start_event_loop()

    def _worker_startup_code(self):
        """
        Perform startup tasks for the worker.
        """
        from bluesky_queueserver.manager.profile_tools import global_user_namespace

        try:
            startup_dir = self._config_dict.get("startup_dir", None)
            startup_module_name = self._config_dict.get("startup_module_name", None)
            startup_script_path = self._config_dict.get("startup_script_path", None)

            # If IPython kernel is used, the startup code is loaded during kernel initialization.
            if not self._use_ipython_kernel:
                self._re_namespace = load_worker_startup_code(
                    startup_dir=startup_dir,
                    startup_module_name=startup_module_name,
                    startup_script_path=startup_script_path,
                    nspace=self._re_namespace,
                )

            # if "RE" not in self._re_namespace:
            #     raise RuntimeError("Run Engine is not created in the startup code.")

            epd = existing_plans_and_devices_from_nspace(
                nspace=self._re_namespace,
                ignore_invalid_plans=self._config_dict["ignore_invalid_plans"],
                max_depth=self._config_dict["device_max_depth"],
            )
            _, existing_devices, _, devices_in_nspace = epd

            # Descriptions of existing devices
            with self._existing_items_lock:
                self._existing_devices = existing_devices

            # Dictionary of references to devices from the namespace
            self._devices_in_nspace = devices_in_nspace

            # Always download the list of existing devices when loading the new environment
            self._existing_plans_and_devices_changed = True

            logger.info("Startup code was successfully loaded.")

        except BaseException as ex:
            s = "Failed to start RE Worker environment. Error while loading startup code"
            if hasattr(ex, "tb"):  # ScriptLoadingError
                logger.error("%s:\n%s\n", s, ex.tb)
            else:
                logger.exception("%s: %s.", s, ex)
            self._success_startup = False

        if self._success_startup:
            self._generate_list_of_allowed_devices()
            self._update_existing_pd_file(options=("ENVIRONMENT_OPEN", "ALWAYS"))

            try:
                # Make the namespace available to the code running in the worker.
                global_user_namespace.set_user_namespace(
                    user_ns=self._re_namespace, use_ipython=self._use_ipython_kernel
                )

                # If IPython kernel is used, then the environment state should be updated
                #     once the kernel is 'idle'
                if not self._use_ipython_kernel:
                    self._env_state = EState.IDLE

                logger.info("RE Environment is ready")

            except BaseException as ex:
                self._success_startup = False
                logger.exception("Error occurred while initializing the environment: %s.", ex)

        if not self._success_startup:
            self._env_state = EState.FAILED

    def _worker_shutdown_code(self):
        """
        Perform shutdown tasks for the worker.
        """
        from bluesky_queueserver.manager.profile_tools import clear_ipython_mode, clear_re_worker_active

        # If shutdown was not initiated by request from manager, then the manager needs to know this,
        #   since it still needs to send a request to the worker to confirm the orderly exit.
        if not self._worker_shutdown_initiated:
            self._unexpected_shutdown = True

        logger.info("Environment is waiting to be closed ...")
        self._env_state = EState.CLOSING

        # Wait until confirmation is received from RE Manager
        while not self._exit_confirmed_event.is_set():
            ttime.sleep(0.02)

        # Clear the environment variable indicating that RE Worker is active. It is an optional step
        #   since the process is about to close, but we still do it for consistency.
        clear_re_worker_active()
        clear_ipython_mode()

        self._stop_event_loop()
        self._comm_to_manager.stop()

    def _run_loop_python(self):
        """
        Run loop (Python kernel). The loop is blocking the main thread until the environment is closed.
        """
        if self._success_startup:
            self._execute_in_main_thread()
        else:
            self._exit_event.set()

    def _ip_kernel_iopub_monitor_thread(self, output_stream, error_stream):
        while True:
            if self._ip_kernel_monitor_stop:
                break

            try:
                msg = self._ip_kernel_client.get_iopub_msg(timeout=0.5)
                if msg["header"]["msg_type"] == "status":
                    self._ip_kernel_state = IPKernelState(msg["content"]["execution_state"])

                    if (self._env_state == EState.INITIALIZING) and (self._ip_kernel_state == IPKernelState.IDLE):
                        logger.info("IPython kernel is in 'idle' state")
                        self._env_state = EState.IDLE

                try:
                    discard = msg["header"]["msg_type"] not in self._ip_kernel_monitor_always_allow_types
                    if discard and "parent_header" in msg and msg["parent_header"]:
                        session_id = self._ip_kernel_client.session.session
                        discard = msg["parent_header"]["session"] != session_id

                    if not discard:
                        if msg["header"]["msg_type"] == "stream":
                            stream_name = msg["content"]["name"]
                            stream_text = msg["content"]["text"]
                            if stream_name == "stdout":
                                output_stream.write(stream_text)
                            elif stream_name == "stderr":
                                error_stream.write(stream_text)
                        elif msg["header"]["msg_type"] == "error":
                            tb = msg["content"]["traceback"]
                            # Remove escape characters from traceback
                            ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                            tb = [ansi_escape.sub("", _) for _ in tb]
                            tb = "\n".join(tb)
                            if self._ip_kernel_monitor_collected_tracebacks is not None:
                                self._ip_kernel_monitor_collected_tracebacks.append(tb)
                            print(f"Traceback: {tb}", file=error_stream)
                        elif msg["header"]["msg_type"] == "execute_result":
                            res = msg["content"]["data"]["text/plain"]
                            print(f">> {res}", file=output_stream)
                except KeyError:
                    pass
            except queue.Empty:
                pass
            except BaseException as ex:
                logger.exception(ex)

    def _ip_kernel_execute_command(self, *, command: str, except_on: bool = False):
        try:
            self._ip_kernel_client.execute(command, reply=False, store_history=False)
        except Exception as ex:
            if except_on:
                raise
            logger.exception(
                "Error occurred while sending request to IPython kernel: Command: %r.\n%s", command, ex
            )

    def _ip_kernel_shutdown_thread(self):
        """
        Simply sending 'shutdown_request' or 'quit' to the kernel does not always work.
        It was found that on the beamline machines the kernel does not shut down until
        it receives additional command (e.g. the kernel does not quit until jupyter console
        application is started). The following procedure sends periodic requests to the
        kernel for 20 seconds and then kills the kernel (stops ioloop) if it did not quit.
        It may look like overkill, since in simulated environments the kernel quits immediately
        after shutdown request, and on the beamline machines after the first request
        (to execute an empty cell), but it is may be important that the operation of closing
        the environment works reliably.
        """
        logger.info("Requesting kernel to shut down ...")
        self._ip_kernel_client.shutdown()  # Sends 'shutdown_request' to the kernel

        # Alternative method is to send 'quit' command. It seems like 0MQ sockets
        #   of the client may be closed explicitly. TODO: should shutdown or 'quit' be used?
        # self._ip_kernel_execute_command(command="quit")

        timeout = 20  # This is time before the kernel is terminated. It can be parametrized if necessary.
        t_stop = ttime.time() + timeout
        while not self._ip_kernel_is_shut_down_event.wait(1):
            if ttime.time() > t_stop:
                break
            logger.debug("Sending 'quit' command to IP kernel")
            self._ip_kernel_execute_command(command="quit")

        if not self._ip_kernel_is_shut_down_event.is_set():
            logger.info("Kernel failed to stop normaly. Killing the ioloop ...")
            self._ip_kernel_app.io_loop.stop()

        logger.debug("Request to shutdown IP kernel is completed. Exiting the thread ...")

    def _ip_kernel_shutdown(self):
        # The manager is now designed not to send repeated requests to stop the environment.
        #   This code needs to be revised if this behavior is changed.
        self._ip_kernel_is_shut_down_event.clear()

        th = threading.Thread(target=self._ip_kernel_shutdown_thread, daemon=True)
        th.start()

    def run(self):
        """
        Overrides the `run()` function of the `multiprocessing.Process` class. Called
        by the `start` method.
        """
        logging.basicConfig(level=max(logging.WARNING, self._log_level))
        setup_loggers(name="bluesky_queueserver", log_level=self._log_level)
        setup_loggers(name="ophyd_service", log_level=self._log_level)

        self._success_startup = True
        self._env_state = EState.INITIALIZING

        if not self._use_ipython_kernel:
            self._worker_prepare_for_startup()
            self._worker_startup_code()
            self._run_loop_python()
        else:
            import socket

            from bluesky_queueserver.manager.utils import generate_random_port
            from ipykernel.kernelapp import IPKernelApp

            connection_file = self._config_dict["ipython_connection_file"]
            connection_dir = self._config_dict["ipython_connection_dir"]

            if connection_file:
                # IPython kernel is designed to remove connection files after the kernel is closed.
                # This functionality exists for a long time, but apparently it did not work in the past,
                # It appeares to work as expected with Python 3.13, which creates a problem with QS.
                # We specify the name of the connection file if we want to reuse the connection
                # information (including random key), at the next startup, so we need to keep
                # the connection file. The patch removes the functionality from IPKernelApp.
                class IPKernelAppCustom(IPKernelApp):
                    def cleanup_connection_file(self):
                        # Do not remove the connection file
                        self.cleanup_ipc_files()

            else:
                # If the name is not specified, IPython creates files with random names and we
                # don't need them. Use the default functionality.
                IPKernelAppCustom = IPKernelApp

            self._ip_kernel_app = IPKernelAppCustom.instance(user_ns=self._re_namespace)
            out_stream, err_stream = sys.stdout, sys.stderr

            self._worker_prepare_for_startup()

            startup_profile = self._config_dict.get("startup_profile", None)
            startup_module_name = self._config_dict.get("startup_module_name", None)
            startup_script_path = self._config_dict.get("startup_script_path", None)
            ipython_dir = self._config_dict.get("ipython_dir", None)
            ipython_matplotlib = self._config_dict.get("ipython_matplotlib", None)

            if startup_profile:
                self._ip_kernel_app.profile = startup_profile
            if startup_module_name:
                # NOTE: Startup files are still loaded.
                self._ip_kernel_app.module_to_run = startup_module_name
            if startup_script_path:
                # NOTE: Startup files are still loaded.
                self._ip_kernel_app.file_to_run = startup_script_path
            if ipython_dir:
                self._ip_kernel_app.ipython_dir = ipython_dir

            self._ip_kernel_app.matplotlib = ipython_matplotlib if ipython_matplotlib else "agg"

            # Prevent kernel from capturing stdout/stderr, otherwise it creates a mess.
            # See https://github.com/ipython/ipykernel/issues/795
            self._ip_kernel_app.capture_fd_output = False

            # Echo all the output to sys.__stdout__ and sys.__stderr__ during kernel initialization
            self._ip_kernel_app.quiet = False

            def find_kernel_ip(ip_str):
                if ip_str == "localhost":
                    ip = "127.0.0.1"
                elif ip_str == "auto":
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.connect(("8.8.8.8", 80))
                    ip = s.getsockname()[0]
                else:
                    ip = ip_str
                return ip

            shell_port = self._config_dict["ipython_shell_port"]
            iopub_port = self._config_dict["ipython_iopub_port"]
            stdin_port = self._config_dict["ipython_stdin_port"]
            hb_port = self._config_dict["ipython_hb_port"]
            control_port = self._config_dict["ipython_control_port"]

            kernel_ip = self._config_dict["ipython_kernel_ip"]
            kernel_ip = find_kernel_ip(kernel_ip)

            use_connection_file = bool(connection_file)

            if connection_dir:
                self._ip_kernel_app.connection_dir = connection_dir

            if connection_file:
                self._ip_kernel_app.connection_file = connection_file
                abs_cf_name = self._ip_kernel_app.abs_connection_file

                # Check the connection file. Delete the existing connection file if
                #   any of the parameters are not matching the new parameters or if
                #   the file is corrupt.
                if os.path.isfile(abs_cf_name):
                    try:
                        with open(abs_cf_name) as f:
                            cn_info = json.load(f)

                        def _check_value(value, key):
                            if value:
                                if key not in cn_info:
                                    raise Exception(f"Key {key!r} is not found in the connection file")
                                if cn_info[key] != value:
                                    raise Exception(
                                        f"Old value {cn_info[key]=} is does not match the new value {value!r}"
                                    )

                        _check_value(shell_port, "shell_port")
                        _check_value(iopub_port, "iopub_port")
                        _check_value(stdin_port, "stdin_port")
                        _check_value(hb_port, "hb_port")
                        _check_value(control_port, "control_port")
                        _check_value(kernel_ip, "ip")

                    except Exception as ex:
                        logger.error(
                            f"Connection file {abs_cf_name!r} is can't be loaded or out of date. "
                            f"A new connection file will be generated. ({ex})"
                        )
                        use_connection_file = False
                        os.remove(abs_cf_name)
                else:
                    logger.info(f"Connection file {abs_cf_name!r} is not found. Creating a new file.")
                    use_connection_file = False

            try:
                if use_connection_file:
                    logger.info(f"Loading connection file {self._ip_kernel_app.abs_connection_file}")
                    self._ip_kernel_app.load_connection_file(self._ip_kernel_app.abs_connection_file)
                else:
                    logger.info("Generating connection parameters. New file is created ...")
                    self._ip_kernel_app.ip = kernel_ip
                    self._ip_kernel_app.shell_port = shell_port or generate_random_port(kernel_ip)
                    self._ip_kernel_app.iopub_port = iopub_port or generate_random_port(kernel_ip)
                    self._ip_kernel_app.stdin_port = stdin_port or generate_random_port(kernel_ip)
                    self._ip_kernel_app.hb_port = hb_port or generate_random_port(kernel_ip)
                    self._ip_kernel_app.control_port = control_port or generate_random_port(kernel_ip)
                self._ip_connect_info = self._ip_kernel_app.get_connection_info()
            except Exception as ex:
                self._success_startup = False
                logger.error("Failed to generate kernel ports for IP %r: %s", kernel_ip, ex)

            if self._success_startup:

                def start_jupyter_client():
                    from jupyter_client import BlockingKernelClient

                    self._ip_kernel_client = BlockingKernelClient()
                    self._ip_kernel_client.load_connection_info(self._ip_connect_info)
                    logger.info(
                        "Session ID for communication with IP kernel: %s", self._ip_kernel_client.session.session
                    )
                    self._ip_kernel_client.start_channels()

                    ip_kernel_iopub_monitor_thread = Thread(
                        target=self._ip_kernel_iopub_monitor_thread,
                        kwargs=dict(output_stream=out_stream, error_stream=err_stream),
                        daemon=True,
                    )
                    ip_kernel_iopub_monitor_thread.start()

                start_jupyter_client()

                self._ip_kernel_monitor_always_allow_types = ["error"]
                self._ip_kernel_monitor_collected_tracebacks = []

                ttime.sleep(0.5)  # Wait unitl 0MQ monitor is connected to the kernel ports

                logger.info("Initializing IPython kernel ...")
                self._ip_kernel_app.initialize([])
                logger.info("IPython kernel initialization is complete.")

                ttime.sleep(0.2)  # Wait until the error message are delivered (if startup fails)

                self._ip_kernel_monitor_always_allow_types = ["stream", "error", "execute_result"]
                collected_tracebacks = self._ip_kernel_monitor_collected_tracebacks
                self._ip_kernel_monitor_collected_tracebacks = None

                self._ip_connect_file = self._ip_kernel_app.connection_file

                # This is a very naive idea: if no exceptions were raised during kernel initialization
                #   the we consider that startup code was loaded and the environment is fully functional
                #   Otherwise we assume that loading failed and the collected tracebacks are used
                #   to generate report (if needed). TODO: there could be some other non-obvious way to
                #   detect if startup code was loaded. Ideas are appreciated.
                if collected_tracebacks:
                    self._success_startup = False
                    logger.error("The environment can not be opened: failed to load startup code.")

                # Disable echoing, since startup code is already loaded
                self._ip_kernel_app.quiet = True
                self._ip_kernel_app.init_io()

                # Print connect info for the kernel (after kernel initialization)
                cinfo = copy.deepcopy(self._ip_connect_info)
                cinfo["key"] = cinfo["key"].decode("utf-8")
                logger.info("IPython kernel connection info:\n %r", ppfl(cinfo))

            # --------------------------------------------------------------------------
            #               Run startup code outside the IPython kernel1
            if self._success_startup:
                logger.info("Configuring the environment ...")
                self._worker_startup_code()

            if self._success_startup:
                logger.info("Preparing to start IPython kernel ...")
                # The kernel reports the 'idle' state only after it executes a command.
                self._ip_kernel_execute_command(command="pass", except_on=False)
                self._ip_kernel_app.start()

            self._ip_kernel_is_shut_down_event.set()

            self._ip_kernel_state = IPKernelState.DISABLED
            # self._ip_kernel_app.close()  # Does not work well if kernel is shut down
            self._exit_event.set()
            self._ip_kernel_monitor_stop = True

            # Restore 'sys.stdout' and 'sys.stderr' changed during kernel initialization
            sys.stdout, sys.stderr = out_stream, err_stream

        self._worker_shutdown_code()

        logger.info("Run Engine environment was closed successfully")
