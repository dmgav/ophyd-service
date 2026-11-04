import time as ttime

import pytest


def _value_messages(messages, device_name=None):
    """
    Select messages with device values (messages that contain the ``value`` key).

    Parameters
    ----------
    messages: list
        List of received messages.
    device_name: str or None
        Select only messages with the matching ``device_name`` (full device or component name).
        All value messages are selected if ``None``.

    Returns
    -------
    list
        List of selected messages.
    """
    return [
        _
        for _ in messages
        if isinstance(_, dict) and "value" in _ and (device_name is None or _.get("device_name") == device_name)
    ]


def _has_message(text):
    """
    Create a condition for ``WebSocketClient.wait_for_condition()`` that checks if a message
    with the ``message`` key equal to ``text`` was received.

    Parameters
    ----------
    text: str
        Expected text of the message.

    Returns
    -------
    callable
        Function that accepts the list of messages and returns ``True`` if the message is found.
    """

    def condition(messages):
        return any(isinstance(_, dict) and _.get("message") == text for _ in messages)

    return condition


def _subscribe(ws_client, device_name, **kwargs):
    """
    Send the ``subscribe`` request and wait for the confirmation. Fails the test
    if the confirmation is not received.

    Parameters
    ----------
    ws_client: WebSocketClient
        Connected websocket client.
    device_name: str
        Name of the device or component, e.g. ``'device.component'``.
    kwargs: dict
        Additional parameters of the request, e.g. ``recursive=False``.
    """
    ws_client.send({"action": "subscribe", "device": device_name, **kwargs})
    assert ws_client.wait_for_condition(_has_message(f"Subscribed to device {device_name}")), (
        ws_client.get_messages()
    )


def _unsubscribe(ws_client, device_name):
    """
    Send the ``unsubscribe`` request and wait for the confirmation. Fails the test
    if the confirmation is not received.

    Parameters
    ----------
    ws_client: WebSocketClient
        Connected websocket client.
    device_name: str
        Name of the device or component, e.g. ``'device.component'``.
    """
    ws_client.send({"action": "unsubscribe", "device": device_name})
    assert ws_client.wait_for_condition(_has_message(f"Unsubscribed from {device_name}")), ws_client.get_messages()


def _check_no_updates(ws_client, *, delay=0.5, period=2):
    """
    Check that no value updates are received after unsubscribing. Updates that were
    sent before the subscription was removed are discarded after ``delay``.

    Parameters
    ----------
    ws_client: WebSocketClient
        Connected websocket client.
    delay: float
        Time in seconds before the received messages are discarded.
    period: float
        Time in seconds during which no value updates are expected.
    """
    ttime.sleep(delay)
    ws_client.clear_messages()
    ttime.sleep(period)
    assert _value_messages(ws_client.get_messages()) == []


_ENDPOINTS = ["/v1/device-socket", "/v1/device-socket/read"]


# fmt: off
@pytest.mark.parametrize("endpoint", _ENDPOINTS)
@pytest.mark.parametrize("recursive", [None, False, True])
# fmt: on
def test_finch_subscribe_ophyd_01(fastapi_server, ws_client, endpoint, recursive):
    """
    Subscribe and unsubscribe from an Ophyd device: recursive subscription delivers
    updates of the components, non-recursive subscription delivers no updates (the top-level
    device does not generate value events). ``recursive=None`` checks the default value (enabled).
    """
    device_name = "sim_periodic_device1"
    components = ("noise", "sine")
    params = {} if recursive is None else {"recursive": recursive}

    ws_client.connect(endpoint=endpoint)
    _subscribe(ws_client, device_name, **params)

    if recursive is False:
        ttime.sleep(2)
        assert _value_messages(ws_client.get_messages()) == []
        _unsubscribe(ws_client, device_name)
        return

    def updates_received(messages):
        return all(_value_messages(messages, f"{device_name}.{_}") for _ in components)

    assert ws_client.wait_for_condition(updates_received), ws_client.get_messages()

    for cname in components:
        full_name = f"{device_name}.{cname}"
        msg = _value_messages(ws_client.get_messages(), full_name)[0]
        assert msg["device"] == device_name
        assert msg["signal"] == f"{device_name}_{cname}"
        assert msg["connected"] is True
        assert isinstance(msg["value"], float)
        assert isinstance(msg["timestamp"], float)

    _unsubscribe(ws_client, device_name)
    _check_no_updates(ws_client)


@pytest.mark.parametrize("endpoint", _ENDPOINTS)
def test_finch_subscribe_ophyd_02(fastapi_server, ws_client, endpoint):
    """
    Subscribe and unsubscribe from a component of an Ophyd device.
    """
    device_name = "sim_periodic_device1.sine"

    ws_client.connect(endpoint=endpoint)
    _subscribe(ws_client, device_name)
    assert ws_client.wait_for_condition(lambda _: _value_messages(_, device_name)), ws_client.get_messages()

    msg = _value_messages(ws_client.get_messages(), device_name)[0]
    assert msg["device"] == device_name
    assert msg["signal"] == "sim_periodic_device1_sine"
    assert _value_messages(ws_client.get_messages(), "sim_periodic_device1.noise") == []

    _unsubscribe(ws_client, device_name)
    _check_no_updates(ws_client)


# fmt: off
@pytest.mark.parametrize("endpoint", _ENDPOINTS)
@pytest.mark.parametrize("recursive", [None, False, True])
# fmt: on
def test_finch_subscribe_async_01(fastapi_server, ws_client, endpoint, recursive):
    """
    Subscribe and unsubscribe from an ophyd-async device with a periodically updated signal.
    Non-recursive subscription delivers no updates (the top-level device is not subscribable).
    ``recursive=None`` checks the default value (enabled).
    """
    device_name = "rand_async_device1"
    signal_name = f"{device_name}.value"
    params = {} if recursive is None else {"recursive": recursive}

    ws_client.connect(endpoint=endpoint)
    _subscribe(ws_client, device_name, **params)

    if recursive is False:
        ttime.sleep(2)
        assert _value_messages(ws_client.get_messages()) == []
        _unsubscribe(ws_client, device_name)
        return

    # The initial reading and at least one periodic update (the period is 1 s)
    _ = ws_client.wait_for_condition(lambda _: len(_value_messages(_, signal_name)) >= 2)
    assert _, ws_client.get_messages()

    msgs = _value_messages(ws_client.get_messages(), signal_name)
    for msg in msgs:
        assert msg["device"] == device_name
        assert msg["signal"] == f"{device_name}-value"
        assert msg["connected"] is True
        assert msg["read_access"] is True
        assert msg["write_access"] is False
        assert isinstance(msg["value"], float)
    assert msgs[0]["timestamp"] < msgs[-1]["timestamp"]

    _unsubscribe(ws_client, device_name)
    _check_no_updates(ws_client)


@pytest.mark.parametrize("endpoint", _ENDPOINTS)
def test_finch_subscribe_async_02(fastapi_server, ws_client, endpoint):
    """
    Subscribe to an ophyd-async device with signals of different types: the initial reading
    is received for each signal. Check that write access is reported correctly.
    """
    device_name = "simple_async_device"
    write_access = {"value_float": True, "value_int": True, "value_str": True, "mode": True, "value_ro": False}

    ws_client.connect(endpoint=endpoint)
    _subscribe(ws_client, device_name)

    def readings_received(messages):
        return all(_value_messages(messages, f"{device_name}.{_}") for _ in write_access)

    assert ws_client.wait_for_condition(readings_received), ws_client.get_messages()

    for cname, access in write_access.items():
        msg = _value_messages(ws_client.get_messages(), f"{device_name}.{cname}")[0]
        assert msg["device"] == device_name
        assert msg["signal"] == f"{device_name}-{cname}"
        assert msg["write_access"] is access, cname

    _unsubscribe(ws_client, device_name)


# fmt: off
@pytest.mark.parametrize("endpoint", _ENDPOINTS)
@pytest.mark.parametrize("device_name, component_name", [
    ("sim_periodic_device2", "sim_periodic_device2.sine"),
    ("rand_async_device2", "rand_async_device2.value"),
])
# fmt: on
def test_finch_subscribe_repeated_01(fastapi_server, ws_client, endpoint, device_name, component_name):
    """
    Repeated subscription to a device or its component (subscribed recursively) is rejected.
    Unsubscribing from devices that are not subscribed is rejected.
    """
    ws_client.connect(endpoint=endpoint)
    _subscribe(ws_client, device_name)

    for name in (device_name, component_name):
        ws_client.send({"action": "subscribe", "device": name})
        _ = ws_client.wait_for_condition(_has_message(f"Already subscribed to {name}"))
        assert _, ws_client.get_messages()

    _unsubscribe(ws_client, device_name)

    # Components are unsubscribed together with the device
    for name in (device_name, component_name):
        ws_client.send({"action": "unsubscribe", "device": name})
        assert ws_client.wait_for_condition(_has_message(f"Not subscribed to {name}")), ws_client.get_messages()

    # Subscribe again after unsubscribing
    ws_client.clear_messages()
    _subscribe(ws_client, device_name)
    assert ws_client.wait_for_condition(lambda _: _value_messages(_, component_name)), ws_client.get_messages()
    _unsubscribe(ws_client, device_name)
