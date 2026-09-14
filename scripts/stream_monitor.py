#!/usr/bin/env python
"""
Subscribe to the '/api/monitor' websocket of ophyd-service and print the streamed messages.

Usage:
    python stream_monitor.py [--url ws://localhost:60620/api/monitor] [--json] [--devices NAME ...]


pixi run python stream_monitor.py
pixi run python stream_monitor.py --json
pixi run python stream_monitor.py --url ws://some-host:8000/api/monitor
pixi run python stream_monitor.py --devices det1 motor1 motor2
"""

import argparse
import asyncio
import contextlib
import json

import websockets

RECONNECT_DELAY = 2.0


def format_msg(msg):
    if "heartbeat" in msg:
        return f"{msg['heartbeat'].get('time', '')[11:23]:<12} heartbeat"
    return json.dumps(msg)


async def monitor(url, as_json, device_names):
    while True:
        try:
            async with websockets.connect(url) as ws:
                print(f"Connected to {url}")
                if device_names:
                    print(f"Requesting to monitor the devices: {device_names} ...")
                    await ws.send(json.dumps({"monitor_devices": device_names}))
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
    parser = argparse.ArgumentParser(description="Monitor the data stream of ophyd-service.")
    parser.add_argument("--url", default="ws://localhost:60620/api/monitor", help="Websocket URL.")
    parser.add_argument(
        "--json", action="store_true", default=True, help="Print the full message as formatted JSON."
    )
    parser.add_argument(
        "--devices", nargs="*", default=[], metavar="NAME", help="Names of the devices to monitor."
    )
    args = parser.parse_args()

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(monitor(args.url, args.json, args.devices))


if __name__ == "__main__":
    main()
