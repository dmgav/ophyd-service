import pytest

from .conftest import request_to_json


def _device_info(method, device_name):
    """
    Send the request to '/v1/device/{method}/{device_name}' and check the common fields of the response.
    Components in ``device_name`` may be separated by slashes or dots.
    """
    resp = request_to_json("get", f"/v1/device/{method}/{device_name}")
    assert resp["method"] == method, resp
    assert resp["device_name"] == device_name.replace("/", "."), resp
    return resp


def _check_error(method, device_name, err_msg):
    """
    Send the request and check that it fails with the error message containing ``err_msg``.
    """
    resp = _device_info(method, device_name)
    assert resp["success"] is False, resp
    assert err_msg in resp["err_msg"], resp
    assert resp["value"] is None, resp


# Devices that do not exist or have invalid names
_NAME_ERRORS = [
    ("no_such_device", "does not exist"),
    ("motor1/no_such_component", "does not exist"),
    ("simple_async_device/no_such_component", "does not exist"),
    ("motor1/__class__", "does not exist"),
    ("motor1;x", "Invalid device name"),
    ("motor1-x", "Invalid device name"),
    ("1motor", "Invalid device name"),
]

# Devices that can not be read by the user group
_ACCESS_ERRORS = [
    ("forbidden_motor", "is not allowed for user group 'primary'"),
    ("forbidden_motor/readback", "is not allowed for user group 'primary'"),
    ("rand_async_device4", "is not allowed for user group 'primary'"),
    ("rand_async_device4/value", "is not allowed for user group 'primary'"),
]

# Ophyd and ophyd-async devices that are not connected
_DISCONNECTED_DEVICES = ["signal_disconnected", "disconnected_async_device"]

# Devices that are not readable (simulated flyers)
_NOT_READABLE_DEVICES = ["flyer1", "trivial_flyer"]


# =======================================================================================
#                               Method 'properties'


# fmt: off
@pytest.mark.parametrize("device_name, expected", [
    # Ophyd
    ("sim_periodic_device1", {
        "is_async": False, "is_readable": True, "is_subscribable": True,
        "classname": "SimPeriodicDevice", "name": "sim_periodic_device1", "components": ["noise", "sine"],
    }),
    ("sim_periodic_device1/sine", {
        "is_async": False, "is_readable": True, "is_subscribable": True,
        "classname": "SynPeriodicSignal", "name": "sim_periodic_device1_sine",
    }),
    ("sim_periodic_device1.sine", {
        "is_async": False, "classname": "SynPeriodicSignal", "name": "sim_periodic_device1_sine",
    }),
    ("motor1", {
        "is_async": False, "is_readable": True, "is_movable": True, "classname": "SynAxis", "name": "motor1",
    }),
    # ophyd-async
    ("simple_async_device", {
        "is_async": True, "is_readable": True, "classname": "SimpleAsyncDevice", "name": "simple_async_device",
    }),
    ("simple_async_device/value_float", {
        "is_async": True, "is_readable": True, "is_movable": True, "is_subscribable": True,
        "classname": "SignalRW", "name": "simple_async_device-value_float",
    }),
    ("simple_async_device/value_ro", {
        "is_async": True, "is_readable": True, "is_movable": False, "is_subscribable": True,
        "classname": "SignalR", "name": "simple_async_device-value_ro",
    }),
])
# fmt: on
def test_device_properties_01(fastapi_server, device_name, expected):
    """
    'properties' returns the properties of Ophyd and ophyd-async devices and their components.
    """
    resp = _device_info("properties", device_name)
    assert resp["success"] is True, resp
    assert resp["err_msg"] == "", resp

    properties = resp["value"]
    for key, value in expected.items():
        assert properties[key] == value, (key, properties)


# fmt: off
@pytest.mark.parametrize("device_name, components", [
    ("motor1", ["readback", "setpoint"]),
    ("simple_async_device", ["value_float", "value_int", "value_str", "mode", "value_ro"]),
])
# fmt: on
def test_device_properties_02(fastapi_server, device_name, components):
    """
    The list of components is included in the properties of Ophyd and ophyd-async devices.
    """
    resp = _device_info("properties", device_name)
    assert resp["success"] is True, resp
    assert set(components).issubset(resp["value"]["components"]), resp


# fmt: off
@pytest.mark.parametrize("device_name", [
    "forbidden_motor", "forbidden_motor/readback",  # Ophyd
    "rand_async_device4", "rand_async_device4/value",  # ophyd-async
])
# fmt: on
def test_device_properties_03(fastapi_server, device_name):
    """
    Properties of the devices that can not be read by the user group are still available.
    """
    resp = _device_info("properties", device_name)
    assert resp["success"] is True, resp
    assert resp["value"]["is_async"] is device_name.startswith("rand_async"), resp


@pytest.mark.parametrize("device_name, err_msg", _NAME_ERRORS)
def test_device_properties_04_errors(fastapi_server, device_name, err_msg):
    """
    'properties' fails for devices that do not exist or have invalid names.
    """
    _check_error("properties", device_name, err_msg)


# =======================================================================================
#                               Method 'permissions'


_ALL = {"read": True, "write_idle": True, "write_always": True, "write": True}
_READ = {"read": True, "write_idle": False, "write_always": False, "write": False}
_NONE = {"read": False, "write_idle": False, "write_always": False, "write": False}


# fmt: off
@pytest.mark.parametrize("device_name, expected", [
    # Ophyd
    ("motor1", _ALL),
    ("motor1/setpoint", _ALL),
    ("sim_periodic_device1.sine", _ALL),
    ("motor_no_write", _READ),
    ("motor_no_write/setpoint", _READ),
    ("sim_periodic_device_no_write/sine", _READ),
    ("forbidden_motor", _NONE),
    ("forbidden_motor/readback", _NONE),
    # ophyd-async
    ("simple_async_device", _ALL),
    ("simple_async_device/value_float", _ALL),
    ("simple_async_device/value_ro", _ALL),  # Permissions do not depend on the type of the signal
    ("rand_async_device4", _NONE),
    ("rand_async_device4/value", _NONE),
])
# fmt: on
def test_device_permissions_01(fastapi_server, device_name, expected):
    """
    'permissions' returns permissions of the user group ('primary' for the single-user API key)
    for Ophyd and ophyd-async devices and their components.
    """
    resp = _device_info("permissions", device_name)
    assert resp["success"] is True, resp
    assert resp["err_msg"] == "", resp
    assert resp["value"] == expected, resp


@pytest.mark.parametrize("device_name, err_msg", _NAME_ERRORS)
def test_device_permissions_02_errors(fastapi_server, device_name, err_msg):
    """
    'permissions' fails for devices that do not exist or have invalid names.
    """
    _check_error("permissions", device_name, err_msg)


# =======================================================================================
#                               Method 'read'


# fmt: off
@pytest.mark.parametrize("device_name, keys", [
    # Ophyd
    ("signal_float", ["signal_float"]),
    ("motor1", ["motor1", "motor1_setpoint"]),
    ("sim_periodic_device1/sine", ["sim_periodic_device1_sine"]),
    ("sim_periodic_device1.sine", ["sim_periodic_device1_sine"]),
    # ophyd-async
    ("simple_async_device", ["simple_async_device-value_float"]),
    ("simple_async_device/value_int", ["simple_async_device-value_int"]),
    ("rand_async_device1", ["rand_async_device1-value"]),
])
# fmt: on
def test_device_read_01(fastapi_server, device_name, keys):
    """
    'read' returns readings of Ophyd and ophyd-async devices and their components.
    """
    resp = _device_info("read", device_name)
    assert resp["success"] is True, resp
    assert resp["err_msg"] == "", resp

    readings = resp["value"]
    assert set(keys).issubset(readings), readings
    for key in keys:
        assert set(readings[key]).issuperset({"value", "timestamp"}), readings[key]


@pytest.mark.parametrize("device_name, err_msg", _NAME_ERRORS + _ACCESS_ERRORS)
def test_device_read_02_errors(fastapi_server, device_name, err_msg):
    """
    'read' fails for devices that do not exist, have invalid names or can not be read by the user group.
    """
    _check_error("read", device_name, err_msg)


@pytest.mark.parametrize("device_name", _DISCONNECTED_DEVICES)
def test_device_read_03_disconnected(fastapi_server, device_name):
    """
    'read' fails for Ophyd and ophyd-async devices that are not connected.
    """
    _check_error("read", device_name, "not connected")


@pytest.mark.parametrize("device_name", _NOT_READABLE_DEVICES)
def test_device_read_04_not_readable(fastapi_server, device_name):
    """
    'read' is not supported for devices that are not readable.
    """
    resp = _device_info("properties", device_name)
    assert resp["value"]["is_readable"] is False, resp
    _check_error("read", device_name, f"Method 'read' is not supported for the device {device_name!r}")


# =======================================================================================
#                               Method 'describe'


# fmt: off
@pytest.mark.parametrize("device_name, descriptions", [
    # Ophyd
    ("signal_float", {"signal_float": {"dtype": "number", "shape": []}}),
    ("signal_str", {"signal_str": {"dtype": "string", "shape": []}}),
    # 'dtype' of the simulated motor depends on the type of the current value (int or float)
    ("motor1", {"motor1": {"shape": []}, "motor1_setpoint": {"shape": []}}),
    ("sim_periodic_device1/sine", {"sim_periodic_device1_sine": {"dtype": "number"}}),
    # ophyd-async
    ("simple_async_device", {"simple_async_device-value_float": {"dtype": "number", "shape": []}}),
    ("simple_async_device/value_int", {"simple_async_device-value_int": {"dtype": "integer", "shape": []}}),
    ("simple_async_device/value_str", {"simple_async_device-value_str": {"dtype": "string", "shape": []}}),
    ("simple_async_device/mode", {"simple_async_device-mode": {"dtype": "string", "choices": ["Off", "On"]}}),
])
# fmt: on
def test_device_describe_01(fastapi_server, device_name, descriptions):
    """
    'describe' returns descriptions of the data keys of Ophyd and ophyd-async devices.
    """
    resp = _device_info("describe", device_name)
    assert resp["success"] is True, resp
    assert resp["err_msg"] == "", resp

    value = resp["value"]
    for key, expected in descriptions.items():
        assert key in value, value
        assert "source" in value[key], value[key]
        for k, v in expected.items():
            assert value[key][k] == v, (key, k, value[key])


@pytest.mark.parametrize("device_name, err_msg", _NAME_ERRORS + _ACCESS_ERRORS)
def test_device_describe_02_errors(fastapi_server, device_name, err_msg):
    """
    'describe' fails for devices that do not exist, have invalid names or can not be read by the user group.
    """
    _check_error("describe", device_name, err_msg)


@pytest.mark.parametrize("device_name", _DISCONNECTED_DEVICES)
def test_device_describe_03_disconnected(fastapi_server, device_name):
    """
    'describe' fails for Ophyd and ophyd-async devices that are not connected.
    """
    _check_error("describe", device_name, "not connected")


@pytest.mark.parametrize("device_name", _NOT_READABLE_DEVICES)
def test_device_describe_04_not_readable(fastapi_server, device_name):
    """
    'describe' is not supported for devices that are not readable.
    """
    resp = _device_info("properties", device_name)
    assert resp["value"]["is_readable"] is False, resp
    _check_error("describe", device_name, f"Method 'describe' is not supported for the device {device_name!r}")


# =======================================================================================
#                               Method 'get'


# fmt: off
@pytest.mark.parametrize("device_name, value_type", [
    # Ophyd
    ("signal_float", float),
    ("signal_str", str),
    ("motor1/readback", (int, float)),
    ("motor1", list),  # Ophyd devices return named tuples
    # ophyd-async
    ("simple_async_device/value_float", float),
    ("simple_async_device/value_int", int),
    ("simple_async_device/value_str", str),
    ("simple_async_device/mode", str),
])
# fmt: on
def test_device_get_01(fastapi_server, device_name, value_type):
    """
    'get' returns values of Ophyd devices and signals and ophyd-async signals.
    """
    resp = _device_info("get", device_name)
    assert resp["success"] is True, resp
    assert resp["err_msg"] == "", resp
    assert isinstance(resp["value"], value_type), resp


def test_device_get_02_async_device(fastapi_server):
    """
    'get' is not supported for ophyd-async devices (only for signals).
    """
    resp = _device_info("get", "simple_async_device")
    assert resp["success"] is False, resp
    assert "has no attribute 'get_value'" in resp["err_msg"], resp


# fmt: off
@pytest.mark.parametrize("device_name, value1, value2", [
    ("signal_float", 1.5, 2.5),  # Ophyd
    ("simple_async_device.value_float", 1.5, 2.5),  # ophyd-async
])
# fmt: on
def test_device_get_03_set_over_socket(fastapi_server, ws_client, device_name, value1, value2):
    """
    'get' and 'read' return the values set over the Finch device socket.
    """
    ws_client.connect()
    ws_client.send({"action": "subscribe", "device": device_name})

    for value in (value1, value2):
        success_msg = f"Successfully set {device_name} to {value}"
        ws_client.clear_messages()
        ws_client.send({"action": "set", "device": device_name, "value": value})

        def set_completed(messages, success_msg=success_msg):
            return any(isinstance(_, dict) and _.get("message") == success_msg for _ in messages)

        assert ws_client.wait_for_condition(set_completed), ws_client.get_messages()

        resp = _device_info("get", device_name)
        assert resp["success"] is True, resp
        assert resp["value"] == value, resp

        resp = _device_info("read", device_name)
        assert resp["success"] is True, resp
        assert [_["value"] for _ in resp["value"].values()] == [value], resp


@pytest.mark.parametrize("device_name, err_msg", _NAME_ERRORS + _ACCESS_ERRORS)
def test_device_get_04_errors(fastapi_server, device_name, err_msg):
    """
    'get' fails for devices that do not exist, have invalid names or can not be read by the user group.
    """
    _check_error("get", device_name, err_msg)


# 'get' is supported only for ophyd-async signals, so the disconnected signal is used instead of the device
@pytest.mark.parametrize("device_name", ["signal_disconnected", "disconnected_async_device/value_disconnected"])
def test_device_get_05_disconnected(fastapi_server, device_name):
    """
    'get' fails for Ophyd and ophyd-async signals that are not connected.
    """
    _check_error("get", device_name, "not connected")


# =======================================================================================
#                               Unsupported methods


def test_device_unsupported_method_01(fastapi_server):
    """
    Unsupported methods are rejected.
    """
    _check_error("write", "motor1", "Unsupported method 'write'")
