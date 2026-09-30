"""
heroku_manager.py
Heroku Platform API wrapper — spawn / kill / list per-user dynos.

Required env vars:
  HEROKU_API_TOKEN  – Personal OAuth token (heroku authorizations:create)
  HEROKU_APP_NAME   – App name as it appears on Heroku dashboard
"""

import aiohttp
import logging
import os

logger = logging.getLogger(__name__)

HEROKU_API_TOKEN = os.environ.get("HEROKU_API_TOKEN", "")
HEROKU_APP_NAME  = os.environ.get("HEROKU_APP_NAME", "")
HEROKU_API_BASE  = "https://api.heroku.com"

# Dyno size to use for each user worker.
# "standard-1x" → 512 MB RAM.  "standard-2x" → 1 GB RAM.
WORKER_DYNO_SIZE = os.environ.get("WORKER_DYNO_SIZE", "standard-1x")


class HerokuManager:
    """Thin async wrapper around the Heroku Platform API v3."""

    def __init__(self):
        self.headers = {
            "Authorization": f"Bearer {HEROKU_API_TOKEN}",
            "Content-Type":  "application/json",
            "Accept":        "application/vnd.heroku+json; version=3",
        }

    def _url(self, path: str) -> str:
        return f"{HEROKU_API_BASE}/apps/{HEROKU_APP_NAME}{path}"

    # ── SPAWN ────────────────────────────────────────────────────────────────

    async def spawn_user_dyno(self, user_id: int, task_id: str) -> dict:
        """
        Spawn a one-off dyno that runs worker.py for a specific user.
        Returns the full Heroku dyno object on success, {} on error.
        """
        if not HEROKU_API_TOKEN or not HEROKU_APP_NAME:
            logger.error("HEROKU_API_TOKEN / HEROKU_APP_NAME not set!")
            return {}

        payload = {
            "command": f"python3 worker.py --user-id={user_id} --task-id={task_id}",
            "attach":  False,
            "size":    WORKER_DYNO_SIZE,
            "env":     {},          # inherits app config vars automatically
            "time_to_live": 86400,  # 24-hour safety cap
        }

        try:
            async with aiohttp.ClientSession() as s:
                resp = await s.post(
                    self._url("/dynos"),
                    headers=self.headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                )
                data = await resp.json()

            if resp.status not in (200, 201, 202):
                logger.error(f"Heroku spawn failed ({resp.status}): {data}")
                return {}

            logger.info(f"✅ Spawned dyno '{data.get('name')}' for user {user_id}")
            return data

        except Exception as e:
            logger.error(f"spawn_user_dyno error: {e}")
            return {}

    # ── KILL ─────────────────────────────────────────────────────────────────

    async def kill_dyno(self, dyno_name: str) -> bool:
        """Kill a running dyno by name (e.g. 'run.1234')."""
        try:
            async with aiohttp.ClientSession() as s:
                resp = await s.delete(
                    self._url(f"/dynos/{dyno_name}"),
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                )
            ok = resp.status in (200, 202)
            logger.info(f"Kill dyno '{dyno_name}': {'OK' if ok else 'FAILED'} ({resp.status})")
            return ok
        except Exception as e:
            logger.error(f"kill_dyno error: {e}")
            return False

    # ── LIST ─────────────────────────────────────────────────────────────────

    async def list_dynos(self) -> list:
        """Return list of all dyno objects for this app."""
        try:
            async with aiohttp.ClientSession() as s:
                resp = await s.get(
                    self._url("/dynos"),
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                )
                return await resp.json()
        except Exception as e:
            logger.error(f"list_dynos error: {e}")
            return []

    async def get_dyno_info(self, dyno_name: str) -> dict:
        """Fetch current state of a single dyno."""
        try:
            async with aiohttp.ClientSession() as s:
                resp = await s.get(
                    self._url(f"/dynos/{dyno_name}"),
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                )
                return await resp.json()
        except Exception as e:
            logger.error(f"get_dyno_info error: {e}")
            return {}

    # ── RESTART (RAM CLEANUP helper) ─────────────────────────────────────────

    async def restart_dyno(self, dyno_name: str) -> bool:
        """
        Restart a specific dyno — used for RAM cleanup.
        For one-off worker dynos this kills and lets the worker exit cleanly;
        the admin can restart the transfer manually via /clone.
        """
        return await self.kill_dyno(dyno_name)


# Global singleton
heroku_manager = HerokuManager()
