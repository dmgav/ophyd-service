import json
import threading
import time as ttime

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from .conftest import SERVER_ADDRESS, SERVER_PORT


class WebSocketClient:
    """
    Simple synchronous websocket client intended for integration tests (by default connects
    to the Finch device socket ``/api/v1/device-socket``). Received messages are accumulated
    in memory by a background thread.

    Parameters
    ----------
    endpoint: str
        Socket endpoint relative to ``/api``, e.g. ``"/v1/device-socket"`` or ``"/v1/device-socket/read"``.
    api_key: str or None
        API key used for authentication.
    token: str or None
        Access token used for authentication. Takes precedence over ``api_key``.
    host: str
        Server address.
    port: int or str
        Server port.
    """

    def __init__(
        self,
        *,
        endpoint="/v1/device-socket",
        api_key=None,
        token=None,
        host=SERVER_ADDRESS,
        port=SERVER_PORT,
    ):
        self._endpoint = endpoint
        self._host = host
        self._port = port
        self._uri = None
        self._api_key = api_key
        self._token = token

        self._websocket = None
        self._thread = None
        self._exit_event = threading.Event()
        self._messages = []
        self._messages_lock = threading.Lock()

    @property
    def is_connected(self):
        return self._websocket is not None

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def api_key(self):
        return self._api_key

    @api_key.setter
    def api_key(self, api_key):
        self._api_key = api_key
        self._token = None

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, token):
        self._token = token
        self._api_key = None

    @property
    def uri(self):
        """
        URI of the current connection or ``None`` if the client is not connected.
        """
        return self._uri

    def connect(self, *, endpoint="/v1/device-socket", host=SERVER_ADDRESS, port=SERVER_PORT, timeout=10):
        """
        Connect to the socket and start receiving messages.
        """
        if self._websocket is not None:
            raise RuntimeError("Client is already connected")
        self._endpoint = endpoint
        self._host = host
        self._port = port
        self._uri = f"ws://{self._host}:{self._port}/api{self._endpoint}"
        if self._token is not None:
            headers = {"Authorization": f"Bearer {self._token}"}
        elif self._api_key is not None:
            headers = {"Authorization": f"ApiKey {self._api_key}"}
        else:
            headers = {}
        self._websocket = connect(self._uri, additional_headers=headers, open_timeout=timeout)
        self._exit_event.clear()
        self._thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._thread.start()

    def disconnect(self, *, timeout=5):
        """
        Stop receiving messages and close the connection. Received messages are preserved.
        """
        self._exit_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        if self._websocket is not None:
            try:
                self._websocket.close()
            finally:
                self._websocket = None
        self._uri = None

    def close(self):
        """
        Gracefully disconnect and release all resources, including the stored messages.
        """
        try:
            self.disconnect()
        finally:
            self.clear_messages()

    def send(self, message):
        """
        Send a message to the server. Dictionaries and lists are serialized to JSON,
        strings are sent as is.
        """
        if self._websocket is None:
            raise RuntimeError("Client is not connected")
        if not isinstance(message, str):
            message = json.dumps(message)
        self._websocket.send(message)

    def get_messages(self):
        """
        Returns a copy of the list of received messages. JSON messages are decoded,
        other messages are stored as received.
        """
        with self._messages_lock:
            return list(self._messages)

    def clear_messages(self):
        """
        Remove all stored messages.
        """
        with self._messages_lock:
            self._messages.clear()

    def wait_for_condition(self, condition, *, timeout=5):
        """
        Wait until the condition is satisfied by the received messages.

        Parameters
        ----------
        condition: callable
            Function that accepts the list of received messages and returns ``True``
            if the condition is satisfied.
        timeout: float
            Timeout in seconds.

        Returns
        -------
        bool
            ``True`` if the condition is satisfied, ``False`` on timeout.
        """
        t_stop = ttime.time() + timeout
        while ttime.time() < t_stop:
            if condition(self.get_messages()):
                return True
            ttime.sleep(0.05)
        return False

    def _receive_loop(self):
        websocket = self._websocket
        while not self._exit_event.is_set():
            try:
                msg = websocket.recv(timeout=0.1)
            except TimeoutError:
                continue
            except ConnectionClosed:
                break
            try:
                msg = json.loads(msg)
            except (json.JSONDecodeError, TypeError):
                pass
            with self._messages_lock:
                self._messages.append(msg)

    def __del__(self):
        self.close()
