import json
import pprint
import threading
import time as ttime
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import sessionmaker
from websockets.sync.client import connect

from ophyd_service import authentication as _auth
from ophyd_service.database import orm as db_orm
from ophyd_service.database.core import create_user

from .conftest import (
    SERVER_ADDRESS,
    SERVER_PORT,
    fastapi_server_fs,  # noqa: F401
    request_to_json,
    setup_server_with_config_file,
)

config_toy_test = """
authentication:
    allow_anonymous_access: True
    providers:
        - provider: toy
          authenticator: ophyd_service.authenticators:DictionaryAuthenticator
          args:
              users_to_passwords:
                  bob: bob_password
                  alice: alice_password
                  cara: cara_password
                  tom: tom_password
api_access:
  policy: ophyd_service.authorization:DictionaryAPIAccessControl
  args:
    users:
      bob:
        roles:
          - admin
          - expert
      alice:
        roles: advanced
      tom:
        roles: user
"""


class _ReceiveSystemInfoSocket(threading.Thread):
    """
    Catch streaming console output by connecting to /console_output/ws socket and
    save messages to the buffer.
    """

    def __init__(self, *, endpoint, api_key=None, token=None, **kwargs):
        super().__init__(**kwargs)
        self.received_data_buffer = []
        self._exit = False
        self._api_key = api_key
        self._token = token
        self._endpoint = endpoint

    def run(self):
        websocket_uri = f"ws://{SERVER_ADDRESS}:{SERVER_PORT}/api{self._endpoint}"
        if self._token is not None:
            additional_headers = {"Authorization": f"Bearer {self._token}"}
        elif self._api_key is not None:
            additional_headers = {"Authorization": f"ApiKey {self._api_key}"}
        else:
            additional_headers = {}

        try:
            with connect(websocket_uri, additional_headers=additional_headers) as websocket:
                while not self._exit:
                    try:
                        msg_json = websocket.recv(timeout=0.1, decode=False)
                        try:
                            msg = json.loads(msg_json)
                            self.received_data_buffer.append(msg)
                        except json.JSONDecodeError:
                            pass
                    except TimeoutError:
                        pass
        except Exception as ex:
            print(f"Failed to connect to server: {ex}")

    def stop(self):
        """
        Call this method to stop the thread. Then send a request to the server so that some output
        is printed in ``stdout``.
        """
        self._exit = True

    def __del__(self):
        self.stop()


# fmt: off
@pytest.mark.parametrize("ws_auth_type", ["apikey", "apikey_invalid", "none"])
# fmt: on
def test_websocket_auth_01(
    tmpdir,
    monkeypatch,
    fastapi_server_fs,  # noqa: F811
    ws_auth_type,
):
    """
    Test authentication for websockets. The test is run only on ``/status/ws`` websocket.
    The other websockets are expected to use the same authentication scheme.
    """

    setup_server_with_config_file(config_file_str=config_toy_test, tmpdir=tmpdir, monkeypatch=monkeypatch)
    fastapi_server_fs()

    resp1 = request_to_json("post", "/auth/provider/toy/token", login=("bob", "bob_password"))
    assert "access_token" in pprint.pformat(resp1)
    token = resp1["access_token"]

    resp3 = request_to_json(
        "post", "/auth/apikey", json={"expires_in": 900, "note": "API key for testing"}, token=token
    )
    assert "secret" in resp3, pprint.pformat(resp3)
    assert "note" in resp3, pprint.pformat(resp3)
    assert resp3["note"] == "API key for testing"
    assert resp3["scopes"] == ["inherit"]
    api_key = resp3["secret"]

    endpoint = "/monitor/ws"
    if ws_auth_type == "none":
        ws_params = {}
    elif ws_auth_type == "apikey":
        ws_params = {"api_key": api_key}
    elif ws_auth_type == "apikey_invalid":
        ws_params = {"api_key": "InvalidApiKey"}
    # elif ws_auth_type == "token":
    #     ws_params = {"token": token}
    # elif ws_auth_type == "token_invalid":
    #     ws_params = {"token": "InvalidToken"}
    else:
        assert False, f"Unknown authentication type: {ws_auth_type!r}"

    rsc = _ReceiveSystemInfoSocket(endpoint=endpoint, **ws_params)
    rsc.start()
    ttime.sleep(5)  # Wait until a few messages are collected
    rsc.stop()
    rsc.join()

    buffer = rsc.received_data_buffer
    if ws_auth_type in ("none", "apikey_invalid", "token_invalid"):
        assert len(buffer) == 0
    elif ws_auth_type in ("apikey", "token"):
        assert len(buffer) > 0
        for msg in buffer:
            assert "heartbeat" in msg, msg
            assert "timestamp" in msg["heartbeat"]
    else:
        assert False, f"Unknown authentication type: {ws_auth_type!r}"


def _fake_ws_with_deps(*, api_access_manager=None, authenticators=None, settings=None):
    """Build a minimal fake WebSocket whose ``app.dependency_overrides``
    look like what build_app() installs at runtime, so
    ``authenticate_websocket_first_message`` can retrieve them."""

    from ophyd_service.settings import get_settings
    from ophyd_service.utils import (
        get_api_access_manager,
        get_authenticators,
    )

    class _App:
        state = MagicMock()
        dependency_overrides = {
            get_settings: lambda: settings,
            get_authenticators: lambda: authenticators or {},
            get_api_access_manager: lambda: api_access_manager,
        }

    class _WS:
        app = _App()
        headers = {"host": "localhost:8000"}
        scope = {"scheme": "http", "root_path": ""}
        query_params: dict = {}
        cookies: dict = {}

        def __init__(self):
            # get_current_principal reads request.state.cookies_to_set for a
            # side-effect on the HTTP path.  Provide a stub so that path does
            # not attribute-error on the websocket route.
            self.state = MagicMock()
            self.state.cookies_to_set = []

    return _WS()


def test_authenticate_websocket_first_message_rejects_non_auth_frames():
    ws = _fake_ws_with_deps(settings=MagicMock())
    assert _auth.authenticate_websocket_first_message(ws, {"type": "ping"}) is None
    assert _auth.authenticate_websocket_first_message(ws, "not-a-dict") is None
    assert _auth.authenticate_websocket_first_message(ws, {"type": "auth"}) is None


def test_authenticate_websocket_first_message_accepts_valid_api_key(sqlite_session):
    """Feed a valid API key through the first-message handshake."""
    from ophyd_service.settings import DatabaseSettings

    db = sqlite_session
    principal = create_user(db, "internal", "alice")
    # Generate an API key with the same machinery routes use.
    import hashlib
    import secrets as py_secrets

    secret = py_secrets.token_bytes(4 + 32)
    hashed = hashlib.sha256(secret).digest()
    apikey_orm = db_orm.APIKey(
        principal_id=principal.id,
        first_eight=secret.hex()[:8],
        hashed_secret=hashed,
        scopes=["read:status"],
    )
    db.add(apikey_orm)
    db.commit()

    # Route the sessionmaker used by get_current_principal through our
    # in-memory sqlite engine.
    engine = db.get_bind()

    def _fake_sessionmaker(_db_settings):
        return sessionmaker(bind=engine, autocommit=False, autoflush=False)

    settings = MagicMock()
    settings.database_settings = DatabaseSettings(uri="sqlite://", pool_size=None, pool_pre_ping=None)
    settings.authentication_provider_names = ["internal"]
    settings.secret_keys = ["hmac"]

    api_access_manager = MagicMock()
    api_access_manager.is_user_known.return_value = True
    api_access_manager.get_user_scopes.return_value = {"read:status"}
    api_access_manager.get_user_roles.return_value = {"user"}

    authenticators = {"internal": MagicMock()}  # truthy => multi-user mode
    ws = _fake_ws_with_deps(
        api_access_manager=api_access_manager, authenticators=authenticators, settings=settings
    )

    import ophyd_service.authentication as auth_mod

    saved = auth_mod.get_sessionmaker
    auth_mod.get_sessionmaker = _fake_sessionmaker
    try:
        result = _auth.authenticate_websocket_first_message(ws, {"type": "auth", "api_key": secret.hex()})
    finally:
        auth_mod.get_sessionmaker = saved

    assert result is not None
    assert result.uuid == principal.uuid


def test_authenticate_websocket_first_message_rejects_bad_api_key(sqlite_session):
    """A malformed (non-hex) API key must be rejected without leaking DB
    state.  Uses the same monkey-patched sessionmaker plumbing as the
    happy-path test so we do not accidentally exercise the real
    get_sessionmaker(pool_size=None) code path in unit tests."""
    from ophyd_service.settings import DatabaseSettings

    engine = sqlite_session.get_bind()

    def _fake_sessionmaker(_db_settings):
        return sessionmaker(bind=engine, autocommit=False, autoflush=False)

    settings = MagicMock()
    settings.database_settings = DatabaseSettings(uri="sqlite://", pool_size=5, pool_pre_ping=False)
    settings.authentication_provider_names = ["internal"]
    settings.secret_keys = ["hmac"]

    ws = _fake_ws_with_deps(
        api_access_manager=MagicMock(),
        authenticators={"internal": MagicMock()},
        settings=settings,
    )

    import ophyd_service.authentication as auth_mod

    saved = auth_mod.get_sessionmaker
    auth_mod.get_sessionmaker = _fake_sessionmaker
    try:
        # 'not-hex' fails bytes.fromhex → HTTPException 401 inside get_current_principal.
        assert _auth.authenticate_websocket_first_message(ws, {"type": "auth", "api_key": "not-hex"}) is None
    finally:
        auth_mod.get_sessionmaker = saved
