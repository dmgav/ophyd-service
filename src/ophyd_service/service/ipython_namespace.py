"""
Stand-alone helper that loads IPython startup files into a namespace without running a kernel.

The module depends only on the standard library and ``ipykernel`` and may be copied
to another project as is.
"""

import logging
import sys
import traceback

import zmq
from ipykernel.kernelapp import IPKernelApp

logger = logging.getLogger(__name__)


class StartupLoadingError(RuntimeError):
    """Raised if at least one startup file failed to load. ``tracebacks`` holds the collected tracebacks."""

    def __init__(self, tracebacks):
        self.tracebacks = list(tracebacks)
        super().__init__("Failed to load startup code:\n" + "\n".join(self.tracebacks))


def load_worker_startup_code_ipython(
    *,
    startup_profile=None,
    ipython_dir=None,
    startup_module_name=None,
    startup_script_path=None,
    matplotlib="agg",
    user_ns=None,
    quiet=False,
    raise_on_error=True,
):
    """
    Load the startup code of an IPython profile and return the namespace with the loaded objects.

    The approach is the same as the one used by the worker in ``ophyd_service.manager.worker``:
    ``IPKernelApp`` is initialized with a user namespace, which makes IPython execute the startup
    files of the profile in that namespace.

    The kernel is initialized, but never started: no requests are processed, but the shell exists,
    so the startup files may use ``get_ipython()``, magics and other IPython features. ``IPKernelApp``
    and the shell are singletons, so ``cleanup_ipykernel_app()`` must be called before the function
    can be called again.

    Parameters
    ----------
    startup_profile: str or None
        Name of the IPython profile. The files from ``<ipython_dir>/profile_<name>/startup`` are
        executed in the alphabetical order. ``None`` selects the default profile.
    ipython_dir: str or None
        Location of the IPython directory (``None`` selects the default location).
    startup_module_name: str or None
        Module to run after the startup files (same as ``ipython -m``).
    startup_script_path: str or None
        Script to run after the startup files (same as ``ipython <script>``).
    matplotlib: str
        Matplotlib backend. The non-interactive ``agg`` backend is used by default.
    user_ns: dict or None
        Existing namespace to load the objects into. A new dictionary is created if ``None``.
    quiet: bool
        If ``True``, the output of the startup code is not echoed to ``sys.__stdout__``
        and ``sys.__stderr__``.
    raise_on_error: bool
        If ``True``, ``StartupLoadingError`` is raised if the startup code fails. Otherwise
        the errors are only logged and the partially filled namespace is returned.

    Returns
    -------
    dict
        The user namespace (``user_ns`` if it was passed).
    """
    if IPKernelApp.initialized():
        raise RuntimeError("IPKernelApp is already initialized: call 'cleanup_ipykernel_app()' before reloading.")

    tracebacks = []

    class _StartupApp(IPKernelApp):
        def init_code(self):
            # IPython reports the errors in startup files by printing the traceback and does not
            #   raise exceptions, so the tracebacks are collected to detect the failures.
            show_traceback = self.shell.showtraceback

            def showtraceback(*args, **kwargs):
                tracebacks.append(traceback.format_exc())
                return show_traceback(*args, **kwargs)

            self.shell.showtraceback = showtraceback
            super().init_code()

        def log_connection_info(self):
            # Suppress the banner: the kernel is never started and nobody can connect to it.
            pass

    if user_ns is None:
        user_ns = {}

    app = _StartupApp.instance(user_ns=user_ns)

    if startup_profile:
        app.profile = startup_profile
    if ipython_dir:
        app.ipython_dir = ipython_dir
    if startup_module_name:
        app.module_to_run = startup_module_name
    if startup_script_path:
        app.file_to_run = startup_script_path

    app.matplotlib = matplotlib or "agg"
    # Prevent the kernel from capturing the output of the process (ipykernel issue 795).
    app.capture_fd_output = False
    app.quiet = quiet
    # Unix sockets avoid binding the kernel ports on the network interface.
    if zmq.has("ipc"):
        app.transport = "ipc"

    # Initialization replaces the standard streams and hooks, they are restored afterwards.
    saved = (sys.stdout, sys.stderr, sys.displayhook, sys.excepthook)
    try:
        app.initialize([])
    finally:
        sys.stdout, sys.stderr, sys.displayhook, sys.excepthook = saved
        app.cleanup_connection_file()

    if tracebacks:
        logger.error("Failed to load startup code:\n%s", "\n".join(tracebacks))
        if raise_on_error:
            raise StartupLoadingError(tracebacks)

    return user_ns


def cleanup_ipykernel_app():
    """
    Release the ``IPKernelApp`` created by ``load_worker_startup_code_ipython()`` and reset the singletons
    (application, kernel and shell), so that the startup files may be loaded again. Does nothing if the app
    is not initialized.

    The namespace dictionary is not modified: pass a new dictionary to the next call (or clear it) to reload
    the objects. After the cleanup ``get_ipython()`` returns ``None``, so the objects in the old namespace
    that keep a reference to the old shell are no longer functional.
    """
    if not IPKernelApp.initialized():
        return

    app = IPKernelApp.instance()
    kernel = getattr(app, "kernel", None)
    shell = getattr(app, "shell", None)

    # Closing the app restores the standard streams, but not the hooks.
    saved = (sys.stdout, sys.stderr, sys.displayhook, sys.excepthook)
    try:
        app.close()
    except Exception:
        logger.exception("Failed to close IPKernelApp")
    finally:
        sys.stdout, sys.stderr, sys.displayhook, sys.excepthook = saved
        app.cleanup_connection_file()

    for obj in (shell, kernel, app):
        if obj is not None:
            type(obj).clear_instance()
