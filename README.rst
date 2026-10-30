=============
ophyd-service
=============

Prototype for REST API Server for Ophyd devices. The server loads the startup code
(profile collection) that creates Ophyd and ophyd-async devices and provides the REST API
and websockets for reading, monitoring and setting the devices.


Installation
============

Clone the repository::

    git clone https://github.com/<organization>/ophyd-service.git
    cd ophyd-service

The recommended way to set up the development environment is `pixi <https://pixi.sh>`_.
Pixi creates the environment and installs the package in editable mode with development
dependencies::

    pixi install

Environments with specific versions of Python are also available (``py311``, ``py312``,
``py313`` and ``py314``)::

    pixi install -e py313

Run commands in the environment using ``pixi run`` (e.g. ``pixi run uvicorn ...``) or activate
the environment using ``pixi shell``.

Alternatively, the package may be installed in an existing Python (>=3.11) environment
using ``pip``::

    pip install -e .           # Basic installation
    pip install -e .[dev]      # Installation with development dependencies


Starting the server
===================

The server is started using ``uvicorn``. By default the server loads the simulated profile
collection distributed with the package (``ophyd_service/profile_collection_sim``), which
contains simulated Ophyd and ophyd-async devices::

    uvicorn --host localhost --port 60620 ophyd_service.server:app

**Single user API key.** The simplest way to access the API is to use a single user API key.
The key is set using the environment variable ``OPHYD_SERVICE_SINGLE_USER_API_KEY``.
If the variable is not set, a random key is generated at startup::

    OPHYD_SERVICE_SINGLE_USER_API_KEY=a uvicorn --host localhost --port 60620 ophyd_service.server:app

The key is passed with each request in the ``Authorization`` header::

    http GET http://localhost:60620/api/v1/ping 'Authorization: ApiKey a'

**Config file.** The server configuration may be loaded from a YAML file. The path to the file
is passed using the environment variable ``OPHYD_SERVICE_CONFIG``::

    OPHYD_SERVICE_CONFIG=config.yml uvicorn --host localhost --port 60620 ophyd_service.server:app

Minimum config file::

    authentication:
        single_user_api_key: a

**Startup code.** If the location of the startup code is not specified, the server starts
in demo mode and loads the demo startup scripts with simulated devices distributed with
the package (``ophyd_service/profile_collection_sim``). For practical deployments, the location
of the startup code (profile collection) must be specified in the ``ophyd_service_configuration``
section of the config file. Only one of the options ``startup_dir``, ``startup_module_name``,
``startup_script_path`` or ``startup_profile`` may be specified::

    authentication:
        single_user_api_key: a
    ophyd_service_configuration:
        startup_dir: ~/.ipython/profile_collection/startup
        # startup_module_name: my_beamline.startup
        # startup_script_path: ~/my_beamline/startup.py
        # startup_profile: collection        # IPython profile name
        # ipython_dir: ~/.ipython            # Used with 'startup_profile'
        user_group_permissions_path: ~/my_beamline/user_group_permissions_ophyd.yaml

The user group permissions (the devices that are readable or writable by each user group)
are loaded from the file ``user_group_permissions_ophyd.yaml``. By default, the file is
expected to be in the startup directory. See
``ophyd_service/profile_collection_sim/user_group_permissions_ophyd.yaml`` for an example.

**Read-only mode.** The server may be started in read-only mode, which disables write
operations on device values regardless of the user permissions. Read-only mode is enabled
in the ``ophyd_service_configuration`` section of the config file (default: ``false``)::

    ophyd_service_configuration:
        read_only_mode: true

In read-only mode, ``write_idle`` and ``write_always`` permissions are reported as ``false``
for all devices and the websocket ``/api/v1/device-socket`` behaves the same way as
``/api/v1/device-socket/read`` (the ``set`` action is not allowed).


Examples of API calls
=====================

The examples use `HTTPie <https://httpie.io>`_ (``http``) and assume that the server is
started with the single user API key ``a`` and the simulated profile collection
(``ophyd_service/profile_collection_sim``). The same requests can be sent with ``curl``::

    curl -H 'Authorization: ApiKey a' http://localhost:60620/api/v1/ping

Check that the server is running::

    http GET http://localhost:60620/api/v1/ping 'Authorization: ApiKey a'

**Ophyd devices.** The simulated profile collection contains the devices from ``ophyd.sim``
(``det1``, ``motor1``, ``rand``, ...) and the devices ``sim_periodic_device1`` and
``sim_periodic_device2`` with the components ``noise`` and ``sine`` updated periodically
in background threads. Read the device (``read()``), its description (``describe()``) or
its value (``get()``)::

    http GET http://localhost:60620/api/v1/device/read/det1 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/describe/det1 'Authorization: ApiKey a'

    http GET http://localhost:60620/api/v1/device/read/motor1 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/get/motor1/readback 'Authorization: ApiKey a'

    http GET http://localhost:60620/api/v1/device/read/sim_periodic_device1 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/get/sim_periodic_device1 'Authorization: ApiKey a'

Components of the device are addressed by appending their names to the URL, separated by
slashes. For example, read or get the component ``sim_periodic_device1.sine``::

    http GET http://localhost:60620/api/v1/device/read/sim_periodic_device1/sine 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/get/sim_periodic_device1/sine 'Authorization: ApiKey a'

**Ophyd-async devices.** The simulated profile collection contains the ophyd-async devices
``rand_async_device1`` ... ``rand_async_device4`` (the signal ``value`` is updated with a new
random number every second), ``sim_stage_async`` (motors ``x`` and ``y``) and
``sim_det_async``. The methods ``read`` and ``describe`` work the same way as for Ophyd
devices. The method ``get`` is supported for signals and returns the result of
``get_value()``::

    http GET http://localhost:60620/api/v1/device/read/rand_async_device1 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/describe/rand_async_device1 'Authorization: ApiKey a'

    http GET http://localhost:60620/api/v1/device/read/rand_async_device1/value 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/get/rand_async_device1/value 'Authorization: ApiKey a'

    http GET http://localhost:60620/api/v1/device/read/sim_stage_async/x 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/get/sim_stage_async/x/user_readback 'Authorization: ApiKey a'

**Device information.** Get the properties of the device stored in the device registry and
the permissions of the user group for the device. These methods are supported for both Ophyd
and ophyd-async devices::

    http GET http://localhost:60620/api/v1/device/properties/motor1 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/permissions/motor1 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/properties/rand_async_device1 'Authorization: ApiKey a'

If the request fails (e.g. the device does not exist or the user group has no access to
the device), the response contains ``"success": false`` and the error message in ``err_msg``.

The examples of using the Finch compatible websocket are included in the description
of the websocket below.


Supported API
=============

All API endpoints have the prefix ``/api/v1``.

Device names
------------

Devices and their components are addressed by full names, e.g. ``det1``, ``motor1`` or
``sim_periodic_device1.sine``. In the REST API URLs the components are separated by slashes
(``sim_periodic_device1/sine``), in websocket messages by dots (``sim_periodic_device1.sine``).
Access to devices is controlled per user group: each device may allow ``read``, ``write_idle``
and ``write_always`` access.

Core API
--------

``GET /api/v1/ping`` (scope ``read:status``)
    Returns the server version::

        {"success": true, "msg": "Ophyd-Service: v.<version>"}

``GET /api/v1/device/{method}/{device_name}`` (scope ``read:devices``)
    Read the device or the information on the device. Supported methods:

    - ``read`` -- result of ``device.read()`` (readable devices only);
    - ``describe`` -- result of ``device.describe()`` (readable devices only);
    - ``get`` -- result of ``device.get()`` (``get_value()`` for ophyd-async signals);
    - ``properties`` -- device properties stored in the device registry;
    - ``permissions`` -- permissions of the user group for the device
      (``{"read": ..., "write_idle": ..., "write_always": ...}``).

    Response::

        {"success": true, "err_msg": "", "method": "read", "device_name": "det1", "value": {...}}

    Example::

        http GET http://localhost:60620/api/v1/device/permissions/motor1 'Authorization: ApiKey a'

``/api/v1/monitor/ws`` (websocket, scope ``read:devices``)
    Experimental. The server sends heartbeat messages once per second::

        {"heartbeat": {"timestamp": "2026-10-19T12:00:00.000000"}}

    The client may send the list of devices to monitor::

        {"monitor_devices": ["det1", "motor1"]}

    Device monitoring is not implemented yet: the server replies with
    ``{"requested_devices": [], "accepted_devices": []}``.

Finch compatible websocket
--------------------------

``/api/v1/device-socket`` (scope ``write:devices``)
    Subscribe to devices, receive streamed value updates and set device values.
    If the server is running in read-only mode, the ``set`` action is not allowed.

``/api/v1/device-socket/read`` (scope ``read:devices``)
    Same as ``/api/v1/device-socket``, but the ``set`` action is not allowed.

The client sends requests as JSON messages with the ``action`` key. Requests on a connection
are processed one at a time in the order they are received.

``subscribe``
    Subscribe to the device and its components::

        {"action": "subscribe", "device": "sim_periodic_device1", "recursive": true}

    If ``recursive`` is ``true`` (default), the components of the device are subscribed
    as well. Only the components with ``read`` access are subscribed. Subscribing to the device
    that is already subscribed (including the components of a recursively subscribed device)
    is not allowed.

``subscribeSafely``
    Same as ``subscribe``, but the request fails if the device is not connected.

``subscribeReadOnly``
    Same as ``subscribe``, but ``set`` requests are rejected for the subscribed devices.

``unsubscribe``
    Unsubscribe from the device::

        {"action": "unsubscribe", "device": "sim_periodic_device1"}

    If the device was subscribed recursively, its components are unsubscribed as well.
    All subscriptions are removed when the connection is closed.

``refresh``
    Call ``get()`` for all subscribed devices (e.g. to reconnect EPICS devices)::

        {"action": "refresh", "timeout": 5}

    ``timeout`` (optional) is the maximum time in seconds to wait for each device. Devices that
    fail or time out are skipped. If ``timeout`` is not specified, the default timeouts of the
    devices are used.

``set``
    Set the value of the subscribed device::

        {"action": "set", "device": "motor1", "value": 2.5, "timeout": 5}

    The device must be subscribed (not with ``subscribeReadOnly``) and the user group must have
    ``write_idle`` or ``write_always`` access to the device. Numbers (or numeric strings) are set
    using ``device.set()`` and the request completes once the operation is finished (e.g. the
    motor stopped moving). Other strings are written using ``device.put()`` with put completion.
    Numeric values are checked against the device limits (if defined). ``timeout`` is
    the maximum time to wait for completion in seconds (default: 1).

Messages sent by the server:

- value updates::

      {"device": "det1", "signal": "det1", "value": 0.85, "timestamp": 1760870000.0,
       "connected": true, "read_access": true, "write_access": true}

- connection state updates::

      {"device": "motor1", "signal": "motor1_user_readback", "connected": false, ...}

- replies to requests::

      {"message": "Subscribed to device det1"}

- errors::

      {"error": "Device 'abc' is not available: ...", "available_devices": ["det1", "motor1", ...]}

The script ``scripts/stream_monitor_finch.py`` may be used to test the websocket::

    pixi run python scripts/stream_monitor_finch.py --api-key a --devices det1 motor1
    pixi run python scripts/stream_monitor_finch.py --api-key a --devices sim_periodic_device1 --recursive false
    pixi run python scripts/stream_monitor_finch.py --api-key a --devices det1 --action refresh
    pixi run python scripts/stream_monitor_finch.py --api-key a --devices motor1 --action set --value 2.5 --timeout 5


