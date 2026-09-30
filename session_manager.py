"""
session_manager.py  –  TDLib (pytdbot) user-client lifecycle for login + transfer.

KEY DIFFERENCES from the Pyrofork version:
  1. No session strings in TDLib. A TDLib session IS its database directory
     (td.binlog holds the auth key). On successful login we archive the whole
     files_directory (tar.gz → base64) and store it in Mongo's existing
     `session_string` field. Worker dynos decode it back to disk and open a
     TDLib user client on top of it. Everything else (DB schema, /logout,
     /extract_string, watchdog, resume) is unchanged.

  2. No phone_code_hash in TDLib — setAuthenticationPhoneNumber() is enough;
     TDLib tracks the auth transaction internally.

  3. OTP format: users may send "1-2-3-4-5" — dashes are stripped before
     checkAuthenticationCode().

  4. Rate limits arrive as types.Error(code=429) — parsed via
     config.get_retry_after().

  5. use_file_database / use_chat_info_database / use_message_database are
     all False for login + transfer clients: the archived session stays tiny
     (~100 KB) and downloads don't pollute it.

  6. CRITICAL — ONE ClientManager PER PROCESS: TDLib's td_receive() MUST be
     called from a single thread. Every Client that calls .start() on its own
     spawns its own ClientManager → its own receiver thread → the moment a
     second TDLib client exists (bot + login client, or worker bot + user
     client), TDLib aborts the WHOLE PROCESS with:
         "Receive must not be called simultaneously from two different
          threads" → SIGABRT (Heroku: 'Process exited with status 134').
     So main.py / worker.py create ONE shared pytdbot.ClientManager and hand
     it to us via set_client_manager(); every client here is attached with
     manager.add_client(client, start_client=True) instead of client.start().
"""

import asyncio
import base64
import io
import os
import shutil
import tarfile
import time

import config


# ── SHARED CLIENT MANAGER (one per process — see module docstring §6) ────────

_shared_manager = None


def set_client_manager(manager) -> None:
    """main.py / worker.py call this once at startup with the process-wide
    pytdbot.ClientManager that also hosts the bot client."""
    global _shared_manager
    _shared_manager = manager


def get_client_manager():
    return _shared_manager


async def _start_client(client) -> None:
    """Start a pytdbot Client WITHOUT creating a second TDLib receiver thread."""
    if _shared_manager is not None:
        await _shared_manager.add_client(client, start_client=True)
    else:
        # Standalone fallback (single-client scripts/tests only).
        await client.start()


async def _stop_client(client) -> None:
    """Gracefully close a client and detach it from the shared manager."""
    try:
        await client.stop()
    except Exception:
        pass
    if _shared_manager is not None:
        try:
            await _shared_manager.delete_client(client.client_id, close_client=False)
        except Exception:
            pass


async def wait_until_ready(client, timeout: float = 60.0) -> None:
    """Wait until a client reaches authorizationStateReady (is_authenticated).
    Used for BOT clients attached to a shared manager, where Client.start()
    does not block until login."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if getattr(client, 'is_authenticated', False):
            return
        if client.authorization_state in ('authorizationStateClosed',
                                          'authorizationStateClosing'):
            raise SessionExpiredError("TDLib closed during authorization")
        await asyncio.sleep(0.5)
    raise TimeoutError(f"Client auth wait timed out at {client.authorization_state}")


# ── EXCEPTIONS (same names/semantics the old handlers.py catches) ────────────

class SessionPasswordNeeded(Exception):
    """2FA is enabled — TDLib is now in authorizationStateWaitPassword."""

class PhoneCodeInvalid(Exception):
    pass

class PhoneCodeExpired(Exception):
    pass

class PhoneNumberInvalid(Exception):
    pass

class PasswordHashInvalid(Exception):
    pass

class FloodWaitError(Exception):
    def __init__(self, x: int):
        super().__init__(f"FloodWait {x}s")
        self.x = x

class SessionExpiredError(Exception):
    pass


def _raise_for_auth_error(res) -> None:
    """Translate a TDLib Error from an auth call into the legacy exceptions."""
    if not config.is_error(res):
        return
    msg   = (getattr(res, 'message', '') or '').upper()
    retry = config.get_retry_after(res)
    if retry:
        raise FloodWaitError(retry)
    if 'PHONE_NUMBER_INVALID' in msg:
        raise PhoneNumberInvalid(msg)
    if 'PHONE_NUMBER_BANNED' in msg:
        raise Exception("PHONE_NUMBER_BANNED")
    if 'PHONE_CODE_EXPIRED' in msg:
        raise PhoneCodeExpired(msg)
    if 'PHONE_CODE_INVALID' in msg or 'INVALID' in msg and 'CODE' in msg:
        raise PhoneCodeInvalid(msg)
    if 'PASSWORD_HASH_INVALID' in msg:
        raise PasswordHashInvalid(msg)
    raise Exception(getattr(res, 'message', 'Unknown TDLib auth error'))


# ── SESSION ARCHIVE HELPERS ───────────────────────────────────────────────────

def _archive_dir_to_b64(directory: str) -> str:
    """tar.gz the TDLib files_directory and return base64 text."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w:gz') as tar:
        for name in os.listdir(directory):
            tar.add(os.path.join(directory, name), arcname=name)
    return base64.b64encode(buf.getvalue()).decode('ascii')


def _restore_b64_to_dir(blob: str, directory: str) -> None:
    """Restore a base64 tar.gz session blob into a fresh directory."""
    shutil.rmtree(directory, ignore_errors=True)
    os.makedirs(directory, exist_ok=True)
    raw = base64.b64decode(blob.encode('ascii'))
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as tar:
        tar.extractall(directory, filter='data')


async def _wait_auth_state(client, wanted: set[str], timeout: float = 90.0) -> str:
    """
    Poll client.authorization_state until it lands in `wanted`.
    Returns the final state string. Raises TimeoutError otherwise.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = client.authorization_state
        if state in wanted:
            return state
        if state in ('authorizationStateClosed', 'authorizationStateClosing'):
            raise SessionExpiredError("TDLib closed during authorization")
        await asyncio.sleep(0.5)
    raise TimeoutError(f"TDLib auth state stuck at {client.authorization_state}")


class SessionManager:
    def __init__(self):
        # user_id → temp pytdbot Client (active during login flow only)
        self.temp_clients: dict[int, object] = {}
        self.temp_dirs:    dict[int, str]    = {}
        self._semaphore = None

    @property
    def semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(999_999)
        return self._semaphore

    def _make_user_client(self, directory: str):
        from pytdbot import Client
        import progress as progress_mod
        os.makedirs(directory, exist_ok=True)
        client = Client(
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            user_bot=True,
            files_directory=directory,
            database_encryption_key=config.TD_ENCRYPTION_KEY,
            use_file_database=False,
            use_chat_info_database=False,
            use_message_database=False,
            workers=1,
            td_verbosity=1,
        )

        # Bridge updateFile into progress.py so DOWNLOAD progress bars work
        # for transfers running on this user client (worker + in-process).
        @client.on_updateFile()
        async def _on_user_file(c, update):
            try:
                await progress_mod.dispatch_update_file(update.file)
            except Exception:
                pass

        return client

    # ── LOGIN FLOW ────────────────────────────────────────────────────────────

    async def create_temp_client(self, user_id: int):
        """
        Create + start a TDLib user client for the login flow and wait until
        TDLib is ready to receive a phone number.
        """
        await self.remove_temp_client(user_id)

        directory = f"/tmp/td_login_{user_id}_{int(time.time())}"
        shutil.rmtree(directory, ignore_errors=True)

        client = self._make_user_client(directory)
        await _start_client(client)
        self.temp_clients[user_id] = client
        self.temp_dirs[user_id]    = directory

        await _wait_auth_state(client, {
            'authorizationStateWaitPhoneNumber',
            'authorizationStateReady',          # already logged in (stale dir)
        })
        return client

    async def send_code(self, user_id: int, phone: str) -> str:
        """
        Send OTP to the given phone number.
        Returns an empty string — TDLib needs no phone_code_hash; the value is
        stored in LOGIN_STATES only to keep the old handler shape intact.
        """
        client = self.temp_clients[user_id]
        res = await client.setAuthenticationPhoneNumber(phone_number=phone)
        _raise_for_auth_error(res)
        await _wait_auth_state(client, {'authorizationStateWaitCode'})
        return ""

    async def resend_code(self, user_id: int, phone: str, phone_code_hash: str = "") -> str:
        """Resend OTP (user requested a new code)."""
        client = self.temp_clients[user_id]
        res = await client.resendAuthenticationCode()
        _raise_for_auth_error(res)
        return ""

    async def sign_in(self, user_id: int, phone: str, phone_code_hash: str, code: str) -> str:
        """
        Sign in with OTP. Returns the archived TDLib session blob on success.
        Raises SessionPasswordNeeded if 2FA is enabled.
        Raises PhoneCodeInvalid / PhoneCodeExpired on bad/expired OTP.
        """
        client     = self.temp_clients[user_id]
        clean_code = code.replace('-', '').replace(' ', '').strip()

        res = await client.checkAuthenticationCode(code=clean_code)
        _raise_for_auth_error(res)

        state = await _wait_auth_state(client, {
            'authorizationStateWaitPassword',
            'authorizationStateReady',
        })
        if state == 'authorizationStateWaitPassword':
            raise SessionPasswordNeeded()

        return await self._export_and_cleanup(user_id)

    async def check_password(self, user_id: int, password: str) -> str:
        """
        Complete 2FA login. Returns the archived TDLib session blob.
        Raises PasswordHashInvalid on wrong password.
        """
        client = self.temp_clients[user_id]
        res = await client.checkAuthenticationPassword(password=password)
        _raise_for_auth_error(res)
        await _wait_auth_state(client, {'authorizationStateReady'})
        return await self._export_and_cleanup(user_id)

    async def _export_and_cleanup(self, user_id: int) -> str:
        """Stop the temp client, archive its TDLib db dir, return base64 blob."""
        client    = self.temp_clients.pop(user_id, None)
        directory = self.temp_dirs.pop(user_id, None)
        blob = None
        try:
            if client:
                # Let pytdbot's internal post-login getMe() finish BEFORE we
                # close the client — otherwise it logs a noisy (harmless)
                # "'Error' object has no attribute 'first_name'" task crash.
                await asyncio.sleep(1.5)
                await _stop_client(client)
            if directory and os.path.isdir(directory):
                # TDLib flushes td.binlog asynchronously on close — give it a beat.
                await asyncio.sleep(2)
                blob = _archive_dir_to_b64(directory)
        finally:
            if directory:
                shutil.rmtree(directory, ignore_errors=True)
        if not blob:
            raise Exception("Session export failed")
        return blob

    async def get_temp_client(self, user_id: int):
        return self.temp_clients.get(user_id)

    async def remove_temp_client(self, user_id: int):
        """Stop and remove a temporary login client + its directory."""
        client    = self.temp_clients.pop(user_id, None)
        directory = self.temp_dirs.pop(user_id, None)
        if client:
            await _stop_client(client)
        if directory:
            shutil.rmtree(directory, ignore_errors=True)

    # ── TRANSFER SESSION ──────────────────────────────────────────────────────

    async def start_user_session(self, session_blob: str, user_id: int):
        """
        Restore an archived TDLib session from Mongo and start a user client
        on top of it. Used by in-process transfers; worker.py restores the
        blob itself for its own process.
        """
        directory = f"/tmp/td_user_{user_id}_{int(time.time())}"
        try:
            _restore_b64_to_dir(session_blob, directory)
        except Exception as e:
            raise SessionExpiredError(f"Could not restore session archive: {e}")

        client = self._make_user_client(directory)
        async with self.semaphore:
            await _start_client(client)
            try:
                state = await _wait_auth_state(
                    client,
                    {'authorizationStateReady', 'authorizationStateWaitPassword',
                     'authorizationStateWaitPhoneNumber'},
                )
            except Exception as e:
                await _stop_client(client)
                shutil.rmtree(directory, ignore_errors=True)
                raise e

        if state != 'authorizationStateReady':
            await _stop_client(client)
            shutil.rmtree(directory, ignore_errors=True)
            raise SessionExpiredError("Session is no longer authorized")

        # Keep the dir path on the client so stop_user_session can clean up.
        client._src_files_directory = directory
        return client

    async def stop_user_session(self, client) -> None:
        """Stop a transfer user client and delete its restored db directory."""
        directory = getattr(client, '_src_files_directory', None)
        if client:
            await _stop_client(client)
        if directory:
            shutil.rmtree(directory, ignore_errors=True)


# Global singleton
session_manager = SessionManager()
