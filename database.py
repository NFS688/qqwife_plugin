import datetime as dt
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import aiosqlite

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
QQWIFE_DIR = os.path.join(DATA_DIR, "qqwife")
QQWIFE_DB_PATH = os.path.join(QQWIFE_DIR, "qqwife.db")
WALLET_DB_PATH = os.path.join(DATA_DIR, "wallet", "wallet.db")
MAIBOT_DB_PATH = os.path.join(DATA_DIR, "MaiBot.db")


def canonical_pair(uid: str, target: str) -> Tuple[str, str]:
    left = str(uid).strip()
    right = str(target).strip()
    if not left:
        left = "0"
    if not right:
        right = "0"
    if left.isdigit() and right.isdigit():
        li = int(left)
        ri = int(right)
        return (left, right) if li >= ri else (right, left)
    return (left, right) if left >= right else (right, left)


@dataclass
class GroupSettings:
    group_id: str
    last_reset_date: str
    can_match: int = 1
    can_ntr: int = 1
    cd_hours: float = 12.0


@dataclass
class MarriageRecord:
    group_id: str
    user_id: str
    target_id: str
    username: str
    target_name: str
    married_date: str
    married_at: str


@dataclass
class ActiveUser:
    group_id: str
    user_id: str
    nickname: str
    last_seen: float


class QQWifeDB:
    def __init__(self) -> None:
        self.conn: Optional[aiosqlite.Connection] = None
        self._tx_depth = 0
        self._has_maibot = False

    async def _ensure_conn(self) -> None:
        if self.conn is not None:
            return
        os.makedirs(QQWIFE_DIR, exist_ok=True)
        self.conn = await aiosqlite.connect(QQWIFE_DB_PATH)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode = WAL")
        await self.conn.execute("PRAGMA synchronous = NORMAL")
        await self.conn.execute("PRAGMA busy_timeout = 2000")
        await self.conn.execute("PRAGMA temp_store = MEMORY")
        await self.conn.execute("PRAGMA foreign_keys = ON")

        wallet_dir = os.path.dirname(WALLET_DB_PATH)
        os.makedirs(wallet_dir, exist_ok=True)
        await self.conn.execute("ATTACH DATABASE ? AS wallet", (WALLET_DB_PATH,))
        if os.path.exists(MAIBOT_DB_PATH):
            try:
                await self.conn.execute("ATTACH DATABASE ? AS maibot", (MAIBOT_DB_PATH,))
                self._has_maibot = True
            except Exception:
                self._has_maibot = False
        await self._init_db()

    async def _init_db(self) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS group_settings (
                group_id TEXT PRIMARY KEY,
                last_reset_date TEXT NOT NULL DEFAULT '',
                can_match INTEGER NOT NULL DEFAULT 1,
                can_ntr INTEGER NOT NULL DEFAULT 1,
                cd_hours REAL NOT NULL DEFAULT 12
            )
            """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS marriages (
                group_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                username TEXT NOT NULL DEFAULT '',
                target_name TEXT NOT NULL DEFAULT '',
                married_date TEXT NOT NULL DEFAULT '',
                married_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (group_id, user_id)
            )
            """
        )
        await self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_marriages_group_target ON marriages(group_id, target_id)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_marriages_group_date ON marriages(group_id, married_date, married_at)"
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cd_records (
                group_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                mode_id TEXT NOT NULL,
                last_ts REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (group_id, user_id, mode_id)
            )
            """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS favorability (
                user_a TEXT NOT NULL,
                user_b TEXT NOT NULL,
                favor INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_a, user_b)
            )
            """
        )
        await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_favor_user_a ON favorability(user_a)")
        await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_favor_user_b ON favorability(user_b)")
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS active_users (
                group_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                nickname TEXT NOT NULL DEFAULT '',
                last_seen REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (group_id, user_id)
            )
            """
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_active_group_time ON active_users(group_id, last_seen DESC)"
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet.wallet_data (
                uid INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL UNIQUE,
                coins INTEGER DEFAULT 0
            )
            """
        )
        await self.conn.commit()

    def _in_transaction(self) -> bool:
        return self._tx_depth > 0

    async def _commit_if_needed(self) -> None:
        assert self.conn is not None
        if not self._in_transaction():
            await self.conn.commit()

    @asynccontextmanager
    async def transaction(self):
        await self._ensure_conn()
        assert self.conn is not None
        root = self._tx_depth == 0
        if root:
            await self.conn.execute("BEGIN IMMEDIATE")
        self._tx_depth += 1
        try:
            yield
        except Exception:
            self._tx_depth -= 1
            if root:
                await self.conn.rollback()
            raise
        else:
            self._tx_depth -= 1
            if root:
                await self.conn.commit()

    async def get_group_settings(self, group_id: str, default_cd_hours: float = 12.0) -> GroupSettings:
        await self._ensure_conn()
        assert self.conn is not None
        async with self.conn.execute(
            "SELECT group_id, last_reset_date, can_match, can_ntr, cd_hours FROM group_settings WHERE group_id = ?",
            (group_id,),
        ) as cursor:
            row = await cursor.fetchone()
        today = dt.date.today().isoformat()
        if row is None:
            settings = GroupSettings(
                group_id=group_id,
                last_reset_date=today,
                can_match=1,
                can_ntr=1,
                cd_hours=max(0.1, float(default_cd_hours)),
            )
            await self.save_group_settings(settings)
            return settings
        return GroupSettings(
            group_id=str(row["group_id"]),
            last_reset_date=str(row["last_reset_date"] or today),
            can_match=int(row["can_match"]),
            can_ntr=int(row["can_ntr"]),
            cd_hours=float(row["cd_hours"]),
        )

    async def save_group_settings(self, settings: GroupSettings) -> None:
        await self._ensure_conn()
        assert self.conn is not None
        await self.conn.execute(
            """
            INSERT INTO group_settings (group_id, last_reset_date, can_match, can_ntr, cd_hours)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(group_id) DO UPDATE SET
                last_reset_date = excluded.last_reset_date,
                can_match = excluded.can_match,
                can_ntr = excluded.can_ntr,
                cd_hours = excluded.cd_hours
            """,
            (
                settings.group_id,
                settings.last_reset_date,
                int(settings.can_match),
                int(settings.can_ntr),
                float(settings.cd_hours),
            ),
        )
        await self._commit_if_needed()

    async def ensure_daily_reset(self, group_id: str, default_cd_hours: float = 12.0) -> GroupSettings:
        async with self.transaction():
            settings = await self.get_group_settings(group_id, default_cd_hours=default_cd_hours)
            today = dt.date.today().isoformat()
            if settings.last_reset_date != today:
                await self.conn.execute("DELETE FROM marriages WHERE group_id = ?", (group_id,))  # type: ignore[arg-type]
                settings.last_reset_date = today
                await self.save_group_settings(settings)
            return settings

    async def upsert_active_user(
        self,
        group_id: str,
        user_id: str,
        nickname: str,
        *,
        seen_ts: Optional[float] = None,
    ) -> None:
        await self._ensure_conn()
        assert self.conn is not None
        now = float(seen_ts if seen_ts is not None else dt.datetime.now().timestamp())
        await self.conn.execute(
            """
            INSERT INTO active_users (group_id, user_id, nickname, last_seen)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(group_id, user_id) DO UPDATE SET
                nickname = CASE WHEN excluded.nickname != '' THEN excluded.nickname ELSE active_users.nickname END,
                last_seen = CASE
                    WHEN excluded.last_seen > active_users.last_seen THEN excluded.last_seen
                    ELSE active_users.last_seen
                END
            """,
            (group_id, user_id, nickname.strip(), now),
        )
        await self._commit_if_needed()

    async def get_user_display_name(self, group_id: str, user_id: str, fallback: Optional[str] = None) -> str:
        await self._ensure_conn()
        assert self.conn is not None
        async with self.conn.execute(
            """
            SELECT nickname
            FROM active_users
            WHERE group_id = ? AND user_id = ?
            LIMIT 1
            """,
            (group_id, user_id),
        ) as cursor:
            row = await cursor.fetchone()
        if row and str(row["nickname"]).strip():
            return str(row["nickname"]).strip()
        async with self.conn.execute(
            """
            SELECT nickname
            FROM active_users
            WHERE user_id = ?
            ORDER BY last_seen DESC
            LIMIT 1
            """,
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row and str(row["nickname"]).strip():
            return str(row["nickname"]).strip()
        return str(fallback or user_id)

    async def get_married_member_ids(self, group_id: str) -> set[str]:
        await self._ensure_conn()
        assert self.conn is not None
        members: set[str] = set()
        async with self.conn.execute(
            "SELECT user_id, target_id FROM marriages WHERE group_id = ?",
            (group_id,),
        ) as cursor:
            rows = await cursor.fetchall()
        for row in rows:
            user_id = str(row["user_id"])
            target_id = str(row["target_id"])
            members.add(user_id)
            if target_id and target_id != "0":
                members.add(target_id)
        return members

    async def get_recent_active_users(
        self,
        group_id: str,
        limit: int = 30,
        exclude_user_ids: Optional[Sequence[str]] = None,
    ) -> List[ActiveUser]:
        await self._ensure_conn()
        assert self.conn is not None
        excludes = {str(i) for i in (exclude_user_ids or [])}
        max_scan = max(limit * 3, 30)
        users: List[ActiveUser] = []
        async with self.conn.execute(
            """
            SELECT group_id, user_id, nickname, last_seen
            FROM active_users
            WHERE group_id = ?
            ORDER BY last_seen DESC
            LIMIT ?
            """,
            (group_id, max_scan),
        ) as cursor:
            rows = await cursor.fetchall()
        for row in rows:
            uid = str(row["user_id"])
            if uid in excludes:
                continue
            users.append(
                ActiveUser(
                    group_id=str(row["group_id"]),
                    user_id=uid,
                    nickname=str(row["nickname"] or uid),
                    last_seen=float(row["last_seen"] or 0.0),
                )
            )
            if len(users) >= limit:
                break
        return users

    async def get_recent_group_speakers_from_messages(
        self,
        group_id: str,
        platform: str,
        limit: int = 30,
        exclude_user_ids: Optional[Sequence[str]] = None,
    ) -> List[ActiveUser]:
        await self._ensure_conn()
        if not self._has_maibot:
            return []
        assert self.conn is not None
        excludes = {str(i) for i in (exclude_user_ids or [])}
        users: List[ActiveUser] = []
        try:
            async with self.conn.execute(
                """
                SELECT
                    user_id,
                    COALESCE(NULLIF(user_cardname, ''), NULLIF(user_nickname, ''), user_id) AS nickname,
                    MAX(time) AS last_seen
                FROM maibot.messages
                WHERE
                    chat_info_group_id = ?
                    AND chat_info_platform = ?
                    AND message_id != 'notice'
                    AND user_id IS NOT NULL
                    AND user_id != ''
                GROUP BY user_id
                ORDER BY last_seen DESC
                LIMIT ?
                """,
                (group_id, platform, max(limit * 3, 30)),
            ) as cursor:
                rows = await cursor.fetchall()
        except Exception:
            return []

        for row in rows:
            uid = str(row["user_id"])
            if uid in excludes:
                continue
            users.append(
                ActiveUser(
                    group_id=group_id,
                    user_id=uid,
                    nickname=str(row["nickname"] or uid),
                    last_seen=float(row["last_seen"] or 0.0),
                )
            )
            if len(users) >= limit:
                break
        return users

    async def get_member_marriage(self, group_id: str, user_id: str) -> Optional[MarriageRecord]:
        await self._ensure_conn()
        assert self.conn is not None
        async with self.conn.execute(
            """
            SELECT group_id, user_id, target_id, username, target_name, married_date, married_at
            FROM marriages
            WHERE group_id = ? AND (user_id = ? OR target_id = ?)
            ORDER BY CASE WHEN user_id = ? THEN 0 ELSE 1 END
            LIMIT 1
            """,
            (group_id, user_id, user_id, user_id),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        return MarriageRecord(
            group_id=str(row["group_id"]),
            user_id=str(row["user_id"]),
            target_id=str(row["target_id"]),
            username=str(row["username"] or ""),
            target_name=str(row["target_name"] or ""),
            married_date=str(row["married_date"] or ""),
            married_at=str(row["married_at"] or ""),
        )

    async def register_marriage(
        self,
        group_id: str,
        user_id: str,
        target_id: str,
        username: str,
        target_name: str,
    ) -> bool:
        await self._ensure_conn()
        assert self.conn is not None
        now = dt.datetime.now()
        try:
            await self.conn.execute(
                """
                INSERT INTO marriages (
                    group_id, user_id, target_id, username, target_name, married_date, married_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    group_id,
                    user_id,
                    target_id,
                    username,
                    target_name,
                    now.date().isoformat(),
                    now.strftime("%H:%M:%S"),
                ),
            )
            await self._commit_if_needed()
            return True
        except Exception:
            return False

    async def delete_marriage_by_user(self, group_id: str, user_id: str) -> bool:
        await self._ensure_conn()
        assert self.conn is not None
        await self.conn.execute(
            "DELETE FROM marriages WHERE group_id = ? AND user_id = ?",
            (group_id, user_id),
        )
        async with self.conn.execute("SELECT changes() AS cnt") as cursor:
            row = await cursor.fetchone()
        await self._commit_if_needed()
        return bool(row and int(row["cnt"]) > 0)

    async def delete_marriage_by_target(self, group_id: str, target_id: str) -> bool:
        await self._ensure_conn()
        assert self.conn is not None
        await self.conn.execute(
            "DELETE FROM marriages WHERE group_id = ? AND target_id = ?",
            (group_id, target_id),
        )
        async with self.conn.execute("SELECT changes() AS cnt") as cursor:
            row = await cursor.fetchone()
        await self._commit_if_needed()
        return bool(row and int(row["cnt"]) > 0)

    async def list_group_marriages(self, group_id: str, include_single: bool = False) -> List[MarriageRecord]:
        await self._ensure_conn()
        assert self.conn is not None
        if include_single:
            query = """
                SELECT group_id, user_id, target_id, username, target_name, married_date, married_at
                FROM marriages
                WHERE group_id = ?
                ORDER BY married_at ASC
            """
            params = (group_id,)
        else:
            query = """
                SELECT group_id, user_id, target_id, username, target_name, married_date, married_at
                FROM marriages
                WHERE group_id = ? AND target_id != '0'
                ORDER BY married_at ASC
            """
            params = (group_id,)
        records: List[MarriageRecord] = []
        async with self.conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        for row in rows:
            records.append(
                MarriageRecord(
                    group_id=str(row["group_id"]),
                    user_id=str(row["user_id"]),
                    target_id=str(row["target_id"]),
                    username=str(row["username"] or ""),
                    target_name=str(row["target_name"] or ""),
                    married_date=str(row["married_date"] or ""),
                    married_at=str(row["married_at"] or ""),
                )
            )
        return records

    async def clear_group_roster(self, group_id: str) -> None:
        await self._ensure_conn()
        assert self.conn is not None
        await self.conn.execute("DELETE FROM marriages WHERE group_id = ?", (group_id,))
        await self.conn.execute("DELETE FROM cd_records WHERE group_id = ?", (group_id,))
        await self._commit_if_needed()

    async def clear_all_rosters(self) -> None:
        await self._ensure_conn()
        assert self.conn is not None
        await self.conn.execute("DELETE FROM marriages")
        await self.conn.execute("DELETE FROM cd_records")
        await self.conn.execute("DELETE FROM active_users")
        await self.conn.execute("DELETE FROM group_settings")
        await self._commit_if_needed()

    async def check_cd(self, group_id: str, user_id: str, mode_id: str, cd_hours: float) -> bool:
        await self._ensure_conn()
        assert self.conn is not None
        if cd_hours <= 0:
            return True
        async with self.conn.execute(
            """
            SELECT last_ts FROM cd_records
            WHERE group_id = ? AND user_id = ? AND mode_id = ?
            LIMIT 1
            """,
            (group_id, user_id, mode_id),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return True
        last_ts = float(row["last_ts"] or 0.0)
        if dt.datetime.now().timestamp() - last_ts >= cd_hours * 3600:
            await self.conn.execute(
                "DELETE FROM cd_records WHERE group_id = ? AND user_id = ? AND mode_id = ?",
                (group_id, user_id, mode_id),
            )
            await self._commit_if_needed()
            return True
        return False

    async def record_cd(self, group_id: str, user_id: str, mode_id: str) -> None:
        await self._ensure_conn()
        assert self.conn is not None
        now = dt.datetime.now().timestamp()
        await self.conn.execute(
            """
            INSERT INTO cd_records (group_id, user_id, mode_id, last_ts)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(group_id, user_id, mode_id) DO UPDATE SET
                last_ts = excluded.last_ts
            """,
            (group_id, user_id, mode_id, now),
        )
        await self._commit_if_needed()

    async def get_favor(self, uid: str, target: str) -> int:
        await self._ensure_conn()
        assert self.conn is not None
        user_a, user_b = canonical_pair(uid, target)
        async with self.conn.execute(
            """
            SELECT favor
            FROM favorability
            WHERE user_a = ? AND user_b = ?
            LIMIT 1
            """,
            (user_a, user_b),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return 0
        return int(row["favor"])

    async def update_favor(self, uid: str, target: str, score: int) -> int:
        await self._ensure_conn()
        assert self.conn is not None
        user_a, user_b = canonical_pair(uid, target)
        async with self.conn.execute(
            "SELECT favor FROM favorability WHERE user_a = ? AND user_b = ? LIMIT 1",
            (user_a, user_b),
        ) as cursor:
            row = await cursor.fetchone()
        base = int(row["favor"]) if row else 0
        value = max(0, min(100, base + int(score)))
        await self.conn.execute(
            """
            INSERT INTO favorability (user_a, user_b, favor)
            VALUES (?, ?, ?)
            ON CONFLICT(user_a, user_b) DO UPDATE SET
                favor = excluded.favor
            """,
            (user_a, user_b, value),
        )
        await self._commit_if_needed()
        return value

    async def list_favor_for_user(self, uid: str, limit: int = 10) -> List[Tuple[str, int]]:
        await self._ensure_conn()
        assert self.conn is not None
        query_limit = max(1, int(limit))
        pairs: List[Tuple[str, int]] = []
        async with self.conn.execute(
            """
            SELECT
                CASE WHEN user_a = ? THEN user_b ELSE user_a END AS target_id,
                favor
            FROM favorability
            WHERE user_a = ? OR user_b = ?
            ORDER BY favor DESC
            LIMIT ?
            """,
            (uid, uid, uid, query_limit),
        ) as cursor:
            rows = await cursor.fetchall()
        for row in rows:
            pairs.append((str(row["target_id"]), int(row["favor"])))
        return pairs

    async def cleanup_favorability_data(self) -> Tuple[int, int]:
        await self._ensure_conn()
        assert self.conn is not None
        async with self.conn.execute("SELECT user_a, user_b, favor FROM favorability") as cursor:
            rows = await cursor.fetchall()
        merged: Dict[Tuple[str, str], int] = {}
        rewritten = 0
        for row in rows:
            user_a = str(row["user_a"])
            user_b = str(row["user_b"])
            favor = int(row["favor"])
            key = canonical_pair(user_a, user_b)
            old = merged.get(key)
            if old is None or favor > old:
                merged[key] = favor
            if key != (user_a, user_b):
                rewritten += 1

        async with self.transaction():
            await self.conn.execute("DELETE FROM favorability")  # type: ignore[arg-type]
            if merged:
                await self.conn.executemany(
                    "INSERT INTO favorability (user_a, user_b, favor) VALUES (?, ?, ?)",
                    [(a, b, v) for (a, b), v in merged.items()],
                )
        return len(rows), rewritten

    async def get_balance(self, user_id: str) -> int:
        await self._ensure_conn()
        assert self.conn is not None
        async with self.conn.execute(
            "SELECT coins FROM wallet.wallet_data WHERE user_id = ? LIMIT 1",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return 0
        return int(row["coins"])

    async def change_balance(self, user_id: str, delta: int) -> Tuple[bool, int]:
        await self._ensure_conn()
        assert self.conn is not None
        if delta == 0:
            return True, await self.get_balance(user_id)

        own_tx = not self._in_transaction()
        try:
            if own_tx:
                await self.conn.execute("BEGIN IMMEDIATE")

            if delta < 0:
                await self.conn.execute(
                    """
                    UPDATE wallet.wallet_data
                    SET coins = coins + ?
                    WHERE user_id = ? AND coins + ? >= 0
                    """,
                    (delta, user_id, delta),
                )
                async with self.conn.execute("SELECT changes() AS cnt") as cursor:
                    row = await cursor.fetchone()
                changed = int(row["cnt"]) if row else 0
                if changed == 0:
                    async with self.conn.execute(
                        "SELECT coins FROM wallet.wallet_data WHERE user_id = ?",
                        (user_id,),
                    ) as cursor:
                        row = await cursor.fetchone()
                    if own_tx:
                        await self.conn.rollback()
                    return False, int(row["coins"]) if row else 0
            else:
                await self.conn.execute(
                    "INSERT OR IGNORE INTO wallet.wallet_data (user_id, coins) VALUES (?, 0)",
                    (user_id,),
                )
                await self.conn.execute(
                    "UPDATE wallet.wallet_data SET coins = coins + ? WHERE user_id = ?",
                    (delta, user_id),
                )

            async with self.conn.execute(
                "SELECT coins FROM wallet.wallet_data WHERE user_id = ?",
                (user_id,),
            ) as cursor:
                row = await cursor.fetchone()
            balance = int(row["coins"]) if row else 0
            if own_tx:
                await self.conn.commit()
            return True, balance
        except Exception:
            if own_tx:
                await self.conn.rollback()
            raise


QQWIFE_DB = QQWifeDB()
