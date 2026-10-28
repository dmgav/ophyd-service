#!/usr/bin/env python
"""
Subscribe to devices using the '/api/v1/device-socket' websocket of ophyd-service and print the streamed values.

Usage:
    python stream_monitor_finch.py [--url ws://localhost:60620/api/v1/device-socket] [--json]
                                   [--action subscribe|subscribeSafely|subscribeReadOnly|set|refresh]
                                   [--value VALUE] [--timeout SEC] [--recursive true|false]
                                   [--api-key KEY] --devices NAME ...

The actions 'set' and 'refresh' subscribe to the devices (using 'subscribe' action) before
sending the request.


pixi run python stream_monitor_finch.py --devices det1 motor1
pixi run python stream_monitor_finch.py --devices det1 --json
pixi run python stream_monitor_finch.py --devices det1 --action subscribeSafely
pixi run python stream_monitor_finch.py --devices det1 --recursive false
pixi run python stream_monitor_finch.py --devices det1 --action refresh
pixi run python stream_monitor_finch.py --devices motor1 --action set --value 2.5 --timeout 5
pixi run python stream_monitor_finch.py --devices det1 --url ws://localhost:60620/api/v1/device-socket/read
pixi run python stream_monitor_finch.py --devices det1 --api-key <api-key>
"""

import argparse
import asyncio
import contextlib
import json
from datetime import datetime

import websockets

RECONNECT_DELAY = 2.0


def format_timestamp(timestamp):
    try:
        return datetime.fromtimestamp(timestamp).strftime("%H:%M:%S.%f")[:-3]
    except (TypeError, ValueError, OverflowError, OSError):
        return ""


def format_msg(msg):
    """
    Format a message sent by '/device-socket'. The server sends value updates
    (``device``, ``signal``, ``value``, ``timestamp``, ...), metadata updates (``device``,
    ``signal``, ``connected``, ...), replies to requests (``message``) and errors (``error``).
    """
    if "error" in msg:
        text = f"ERROR: {msg['error']}"
        if "available_devices" in msg:
            text += f"\n       Available devices: {msg['available_devices']}"
        return text

    if "message" in msg:
        return f"INFO: {msg['message']}"

    signal = msg.get("signal") or msg.get("device", "")
    ts = format_timestamp(msg.get("timestamp"))

    if "value" in msg:
        return f"{ts:<12} {signal} = {msg['value']!r}"

    if "connected" in msg:
        state = "connected" if msg["connected"] else "DISCONNECTED"
        return f"{ts:<12} {signal}: {state}"

    return json.dumps(msg)


async def monitor(url, as_json, device_names, action, recursive, api_key, *, value, timeout):
    # The key is passed in the header, so that it is not exposed in the URL.
    headers = {"Authorization": f"ApiKey {api_key}"} if api_key else {}
    subscribe_action = "subscribe" if action in ("set", "refresh") else action
    # The value is set only once, not after each reconnect.
    set_pending = action == "set"
    while True:
        try:
            async with websockets.connect(url, additional_headers=headers) as ws:
                print(f"Connected to {url}")
                for name in device_names:
                    print(f"Subscribing to the device {name!r} ({subscribe_action}, recursive={recursive}) ...")
                    await ws.send(json.dumps({"action": subscribe_action, "device": name, "recursive": recursive}))
                if set_pending:
                    for name in device_names:
                        request = {"action": "set", "device": name, "value": value}
                        if timeout is not None:
                            request["timeout"] = timeout
                        print(f"Setting the device {name!r} to {value!r} ...")
                        await ws.send(json.dumps(request))
                    set_pending = False
                elif action == "refresh":
                    print("Refreshing the devices ...")
                    await ws.send(json.dumps({"action": "refresh"}))
                async for message in ws:
                    msg = json.loads(message)
                    if as_json:
                        print(json.dumps(msg, indent=2))
                    else:
                        print(format_msg(msg))
        except (OSError, websockets.exceptions.WebSocketException) as ex:
            # The server may be down or restarting: keep trying to reconnect.
            print(f"Connection failed or closed ({type(ex).__name__}: {ex}). Retrying ...")

        await asyncio.sleep(RECONNECT_DELAY)


def main():
    parser = argparse.ArgumentParser(description="Stream device values from ophyd-service '/device-socket'.")
    parser.add_argument("--url", default="ws://localhost:60620/api/v1/device-socket", help="Websocket URL.")
    parser.add_argument("--json", action="store_true", help="Print the full message as formatted JSON.")
    parser.add_argument(
        "--devices", nargs="+", required=True, metavar="NAME", help="Names of the devices to subscribe to."
    )
    parser.add_argument(
        "--action",
        default="subscribe",
        choices=["subscribe", "subscribeSafely", "subscribeReadOnly", "set", "refresh"],
        help=(
            "Action sent for the devices. 'subscribeSafely' fails if the device is not connected. "
            "'set' and 'refresh' subscribe to the devices first using 'subscribe'."
        ),
    )
    parser.add_argument(
        "--recursive",
        default="true",
        type=str.lower,
        choices=["true", "false"],
        help="Subscribe to the device components recursively.",
    )
    parser.add_argument(
        "--value", help="Value used with 'set' action. Numeric strings are converted by the server."
    )
    parser.add_argument("--timeout", type=float, default=None, help="Timeout (seconds) used with 'set' action.")
    parser.add_argument(
        "--api-key",
        default="",
        help="API key used to authenticate.",
    )
    args = parser.parse_args()

    if args.action == "set" and args.value is None:
        parser.error("'set' action requires --value")

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(
            monitor(
                args.url,
                args.json,
                args.devices,
                args.action,
                args.recursive == "true",
                args.api_key,
                value=args.value,
                timeout=args.timeout,
            )
        )


if __name__ == "__main__":
    main()
