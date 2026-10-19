#!/usr/bin/env python
"""
Subscribe to devices using the '/api/v1/device-socket' websocket of ophyd-service and print the streamed values.

Usage:
    python stream_monitor_finch.py [--url ws://localhost:60620/api/v1/device-socket] [--json]
                                   [--action subscribe|subscribeSafely|subscribeReadOnly]
                                   [--recursive true|false] [--api-key KEY] --devices NAME ...


pixi run python stream_monitor_finch.py --devices det1 motor1
pixi run python stream_monitor_finch.py --devices det1 --json
pixi run python stream_monitor_finch.py --devices det1 --action subscribeSafely
pixi run python stream_monitor_finch.py --devices det1 --recursive false
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


async def monitor(url, as_json, device_names, action, recursive, api_key):
    # The key is passed in the header, so that it is not exposed in the URL.
    headers = {"Authorization": f"ApiKey {api_key}"} if api_key else {}
    while True:
        try:
            async with websockets.connect(url, additional_headers=headers) as ws:
                print(f"Connected to {url}")
                for name in device_names:
                    print(f"Subscribing to the device {name!r} ({action}, recursive={recursive}) ...")
                    await ws.send(json.dumps({"action": action, "device": name, "recursive": recursive}))
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
        choices=["subscribe", "subscribeSafely", "subscribeReadOnly"],
        help="Action used to subscribe to the devices. 'subscribeSafely' fails if the device is not connected.",
    )
    parser.add_argument(
        "--recursive",
        default="true",
        type=str.lower,
        choices=["true", "false"],
        help="Subscribe to the device components recursively.",
    )
    parser.add_argument(
        "--api-key",
        default="",
        help="API key used to authenticate.",
    )
    args = parser.parse_args()

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(
            monitor(args.url, args.json, args.devices, args.action, args.recursive == "true", args.api_key)
        )


if __name__ == "__main__":
    main()
