import asyncio
import random
import re
from contextlib import asynccontextmanager
from typing import Dict, List, Optional, Sequence, Tuple, Type

from src.common.logger import get_logger
from src.plugin_system import (
    BaseCommand,
    BaseEventHandler,
    BasePlugin,
    ComponentInfo,
    ConfigField,
    EventType,
    MaiMessages,
    register_plugin,
)

from .database import GroupSettings, MarriageRecord, QQWIFE_DB

logger = get_logger("qqwife_plugin")


USER_OP_LOCKS: Dict[str, asyncio.Lock] = {}
USER_OP_LOCKS_GUARD = asyncio.Lock()
GROUP_OP_LOCKS: Dict[str, asyncio.Lock] = {}
GROUP_OP_LOCKS_GUARD = asyncio.Lock()

SUCCESS_TEXTS = [
    "是个勇敢的孩子，今天的好运都降临在你身边了。",
    "对方答应了你，并表示愿意当今天的CP。",
]
FAIL_TEXTS = [
    "今天的运气有一点背，明天再试试吧。",
    "下次还有机会，抱抱你。",
    "今天失败了，明天还有机会。",
]
NTR_SUCCESS_TEXTS = [
    "因为你的个人魅力，今天 ta 就是你的了。",
]
DIVORCE_FAIL_TEXTS = [
    "打是亲骂是爱，不打不亲不相爱。答应我不要分手。",
    "床头打架床尾和，夫妻没有隔夜仇。安啦安啦，不要闹别扭。",
]
DIVORCE_SUCCESS_TEXTS = [
    "离婚成功。话说你不考虑当个1吗？",
    "离婚成功。天涯何处无芳草，何必单恋一枝花。",
]

CQ_AT_PATTERN = re.compile(r"\[CQ:at,[^\]]*?(?:qq|id)=(\d+)[^\]]*?\]")
NUMERIC_ID_PATTERN = re.compile(r"(?<!\d)(\d{5,20})(?!\d)")

MODE_PROPOSE = "propose"
MODE_MISTRESS = "mistress"
MODE_MATCHMAKER = "matchmaker"
MODE_DIVORCE = "divorce"
MODE_GIFT = "gift"


def is_single_noble(record: Optional[MarriageRecord]) -> bool:
    if record is None:
        return False
    return record.target_id == "0" or record.user_id == "0"


def unique_order(seq: Sequence[str]) -> List[str]:
    result: List[str] = []
    seen: set[str] = set()
    for item in seq:
        value = str(item).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


async def _try_acquire_lock(
    lock_map: Dict[str, asyncio.Lock],
    guard: asyncio.Lock,
    key: str,
) -> Optional[asyncio.Lock]:
    async with guard:
        lock = lock_map.get(key)
        if lock is None:
            lock = asyncio.Lock()
            lock_map[key] = lock
        if lock.locked():
            return None
        await lock.acquire()
        return lock


async def _release_lock(
    lock_map: Dict[str, asyncio.Lock],
    guard: asyncio.Lock,
    key: str,
    lock: asyncio.Lock,
) -> None:
    async with guard:
        if lock.locked():
            lock.release()
        if lock_map.get(key) is lock and not lock.locked():
            lock_map.pop(key, None)


class QQWifeBaseCommand(BaseCommand):
    def get_user_id(self) -> str:
        return str(self.message.message_info.user_info.user_id)

    def get_user_name(self) -> str:
        user = self.message.message_info.user_info
        card = str(getattr(user, "user_cardname", "") or "").strip()
        nick = str(getattr(user, "user_nickname", "") or "").strip()
        return card or nick or self.get_user_id()

    def get_group_id(self) -> str:
        group_info = self.message.message_info.group_info
        if group_info is None:
            return ""
        return str(group_info.group_id or "").strip()

    def get_platform(self) -> str:
        return str(self.message.message_info.platform or "").strip()

    def get_message_text(self) -> str:
        return str(getattr(self.message, "processed_plain_text", "") or "").strip()

    def get_wallet_name(self) -> str:
        return str(self.get_config("components.wallet_name", "麦币"))

    def get_default_cd_hours(self) -> float:
        try:
            value = float(self.get_config("components.default_cd_hours", 12.0))
        except Exception:
            value = 12.0
        return max(0.1, min(168.0, value))

    def get_candidate_pool_size(self) -> int:
        try:
            value = int(self.get_config("components.recent_candidate_limit", 30))
        except Exception:
            value = 30
        return max(5, min(200, value))

    def get_gift_max_cost(self) -> int:
        try:
            value = int(self.get_config("components.gift_max_cost", 100))
        except Exception:
            value = 100
        return max(1, min(1000, value))

    def allow_message_fallback_candidates(self) -> bool:
        return bool(self.get_config("components.allow_message_fallback_candidates", True))

    def get_admin_user_ids(self) -> set[str]:
        raw = self.get_config("components.admin_user_ids", [])
        if isinstance(raw, str):
            values = re.split(r"[\s,，]+", raw)
        elif isinstance(raw, list):
            values = [str(i) for i in raw]
        else:
            values = []
        return {v.strip() for v in values if str(v).strip()}

    def get_super_user_ids(self) -> set[str]:
        raw = self.get_config("components.super_user_ids", [])
        if isinstance(raw, str):
            values = re.split(r"[\s,，]+", raw)
        elif isinstance(raw, list):
            values = [str(i) for i in raw]
        else:
            values = []
        return {v.strip() for v in values if str(v).strip()}

    def is_super_user(self) -> bool:
        return self.get_user_id() in self.get_super_user_ids()

    def is_group_admin(self) -> bool:
        if self.is_super_user():
            return True
        uid = self.get_user_id()
        if uid in self.get_admin_user_ids():
            return True
        add_cfg = getattr(self.message.message_info, "additional_config", None) or {}
        if not isinstance(add_cfg, dict):
            return False
        role = str(add_cfg.get("role") or add_cfg.get("user_role") or add_cfg.get("permission") or "").lower()
        if role in {"owner", "admin", "group_admin", "super_admin"}:
            return True
        return bool(add_cfg.get("is_owner")) or bool(add_cfg.get("is_admin"))

    async def require_group(self) -> bool:
        if self.get_group_id():
            return True
        await self.send_text("该命令只能在群聊中使用。")
        return False

    async def require_group_admin(self) -> bool:
        if not await self.require_group():
            return False
        if self.is_group_admin():
            return True
        await self.send_text("你没有权限执行该命令。")
        return False

    async def require_super_user(self) -> bool:
        if self.is_super_user():
            return True
        await self.send_text("该命令仅超级用户可用。")
        return False

    @asynccontextmanager
    async def guard_user_operation(self, key: str):
        lock = await _try_acquire_lock(USER_OP_LOCKS, USER_OP_LOCKS_GUARD, key)
        if lock is None:
            await self.send_text("你的操作正在处理中，请稍后再试。")
            yield False
            return
        try:
            yield True
        finally:
            await _release_lock(USER_OP_LOCKS, USER_OP_LOCKS_GUARD, key, lock)

    @asynccontextmanager
    async def guard_group_operation(self, group_id: str):
        lock = await _try_acquire_lock(GROUP_OP_LOCKS, GROUP_OP_LOCKS_GUARD, group_id)
        if lock is None:
            await self.send_text("群内有操作正在处理，请稍后再试。")
            yield False
            return
        try:
            yield True
        finally:
            await _release_lock(GROUP_OP_LOCKS, GROUP_OP_LOCKS_GUARD, group_id, lock)

    def _extract_ids_from_text(self, text: str) -> List[str]:
        if not text:
            return []
        ids: List[str] = []
        ids.extend(CQ_AT_PATTERN.findall(text))
        ids.extend(NUMERIC_ID_PATTERN.findall(text))
        return unique_order(ids)

    def _collect_ids_from_segments(self) -> List[str]:
        result: List[str] = []

        def walk(seg) -> None:
            if seg is None:
                return
            seg_type = str(getattr(seg, "type", "") or "")
            seg_data = getattr(seg, "data", None)

            if seg_type in {"at", "mention", "mention_user"}:
                if isinstance(seg_data, dict):
                    for key in ("user_id", "id", "qq", "uid"):
                        value = str(seg_data.get(key) or "").strip()
                        if value:
                            result.append(value)
                else:
                    value = str(seg_data or "").strip()
                    if value:
                        result.append(value)
                return

            if seg_type == "text":
                result.extend(self._extract_ids_from_text(str(seg_data or "")))
                return

            if seg_type == "seglist" and isinstance(seg_data, list):
                for child in seg_data:
                    walk(child)

        walk(getattr(self.message, "message_segment", None))
        return unique_order(result)

    def _collect_ids_from_additional_config(self) -> List[str]:
        add_cfg = getattr(self.message.message_info, "additional_config", None) or {}
        if not isinstance(add_cfg, dict):
            return []
        result: List[str] = []
        for key in ("at_user_ids", "mentioned_user_ids", "at_users", "mentions"):
            value = add_cfg.get(key)
            if isinstance(value, list):
                result.extend(str(i).strip() for i in value if str(i).strip())
            elif isinstance(value, str):
                result.extend(self._extract_ids_from_text(value))
        return unique_order(result)

    def _collect_target_ids(self, extra_text: str = "") -> List[str]:
        values: List[str] = []
        values.extend(self._collect_ids_from_segments())
        values.extend(self._collect_ids_from_additional_config())
        values.extend(self._extract_ids_from_text(str(getattr(self.message, "raw_message", "") or "")))
        values.extend(self._extract_ids_from_text(self.get_message_text()))
        if extra_text:
            values.extend(self._extract_ids_from_text(extra_text))
        return unique_order(values)

    def parse_single_target(self, raw_text: str) -> Optional[str]:
        ids = self._collect_target_ids(raw_text)
        return ids[0] if ids else None

    def parse_two_targets(self, raw_a: str, raw_b: str) -> Tuple[Optional[str], Optional[str]]:
        merged = self._collect_target_ids(f"{raw_a} {raw_b}")
        if len(merged) >= 2:
            return merged[0], merged[1]

        a = self.parse_single_target(raw_a)
        b = self.parse_single_target(raw_b)
        if a and b:
            return a, b

        if len(merged) == 1:
            if not a:
                a = merged[0]
            elif not b and merged[0] != a:
                b = merged[0]
        return a, b

    async def ensure_daily(self, group_id: str) -> GroupSettings:
        return await QQWIFE_DB.ensure_daily_reset(group_id, default_cd_hours=self.get_default_cd_hours())

    async def display_name(self, group_id: str, user_id: str, fallback: Optional[str] = None) -> str:
        return await QQWIFE_DB.get_user_display_name(group_id, user_id, fallback=fallback or user_id)

    async def check_cd_or_reply(
        self,
        group_id: str,
        user_id: str,
        mode_id: str,
        settings: GroupSettings,
    ) -> bool:
        ok = await QQWIFE_DB.check_cd(group_id, user_id, mode_id, settings.cd_hours)
        if ok:
            return True
        await self.send_text("你的技能还在CD中。")
        return False

    async def validate_single_propose(
        self,
        group_id: str,
        user_id: str,
        target_id: str,
        settings: GroupSettings,
    ) -> bool:
        if settings.can_match == 0:
            await self.send_text("本群已禁止自由恋爱。")
            return False

        user_info = await QQWIFE_DB.get_member_marriage(group_id, user_id)
        if is_single_noble(user_info):
            await self.send_text("今天的你是单身贵族哦。")
            return False
        if user_info and (user_info.user_id == target_id or user_info.target_id == target_id):
            await self.send_text("笨蛋！你们已经在一起了。")
            return False
        if user_info and user_info.user_id == user_id:
            await self.send_text("笨蛋，你家里还有个吃白饭的。")
            return False
        if user_info and user_info.target_id == user_id:
            await self.send_text("你今天已经是0了，别折腾了。")
            return False

        target_info = await QQWIFE_DB.get_member_marriage(group_id, target_id)
        if is_single_noble(target_info):
            await self.send_text("今天的 ta 是单身贵族哦。")
            return False
        if target_info and target_info.user_id == target_id:
            await self.send_text("ta有对象了，你该放下了。")
            return False
        if target_info and target_info.target_id == target_id:
            await self.send_text("ta被别人娶了，你来晚啦。")
            return False
        return True

    async def validate_mistress(
        self,
        group_id: str,
        user_id: str,
        target_id: str,
        settings: GroupSettings,
    ) -> bool:
        if settings.can_ntr == 0:
            await self.send_text("本群已禁止牛头人行为。")
            return False

        target_info = await QQWIFE_DB.get_member_marriage(group_id, target_id)
        if target_info is None:
            await self.send_text("ta现在还是单身，快向ta表白吧。")
            return False
        if is_single_noble(target_info):
            await self.send_text("今天的 ta 是单身贵族哦。")
            return False
        if target_info.user_id == user_id or target_info.target_id == user_id:
            await self.send_text("笨蛋！你们已经在一起了。")
            return False

        user_info = await QQWIFE_DB.get_member_marriage(group_id, user_id)
        if is_single_noble(user_info):
            await self.send_text("今天的你是单身贵族哦。")
            return False
        if user_info and user_info.user_id == user_id:
            await self.send_text("不允许纳小三。")
            return False
        if user_info and user_info.target_id == user_id:
            await self.send_text("你今天已经是0了，别折腾了。")
            return False
        return True

    async def validate_matchmaker(
        self,
        group_id: str,
        user_id: str,
        left_id: str,
        right_id: str,
    ) -> bool:
        if left_id == user_id or right_id == user_id:
            await self.send_text("禁止给自己做媒。")
            return False
        if left_id == right_id:
            await self.send_text("你这媒人有点怪，不能这样配。")
            return False

        left_info = await QQWIFE_DB.get_member_marriage(group_id, left_id)
        if is_single_noble(left_info):
            await self.send_text("今天的攻方是单身贵族。")
            return False
        if left_info and (left_info.target_id == right_id or left_info.user_id == right_id):
            await self.send_text("笨蛋，ta们已经在一起了。")
            return False
        if left_info is not None:
            await self.send_text("攻方不是单身，不允许给这种人做媒。")
            return False

        right_info = await QQWIFE_DB.get_member_marriage(group_id, right_id)
        if is_single_noble(right_info):
            await self.send_text("今天的受方是单身贵族。")
            return False
        if right_info is not None:
            await self.send_text("受方不是单身，不允许给这种人做媒。")
            return False
        return True


class QQWifeRandomCommand(QQWifeBaseCommand):
    command_name = "qqwife_random"
    command_description = "随机娶群友"
    command_pattern = r"^娶群友$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_group():
            return True, "group_only", 1

        gid = self.get_group_id()
        uid = self.get_user_id()
        uname = self.get_user_name()
        platform = self.get_platform()

        async with self.guard_group_operation(gid) as locked:
            if not locked:
                return True, "group_busy", 1

            async with QQWIFE_DB.transaction():
                await QQWIFE_DB.upsert_active_user(gid, uid, uname)
                await self.ensure_daily(gid)

                status = await QQWIFE_DB.get_member_marriage(gid, uid)
                if is_single_noble(status):
                    await self.send_text("今天你是单身贵族。")
                    return True, "single_noble", 1
                if status and status.user_id == uid:
                    name = status.target_name or await self.display_name(gid, status.target_id, status.target_id)
                    await self.send_text(f"今天你在 {status.married_at} 娶了群友\n[{name}]({status.target_id})")
                    return True, "already_husband", 1
                if status and status.target_id == uid:
                    name = status.username or await self.display_name(gid, status.user_id, status.user_id)
                    await self.send_text(f"今天你在 {status.married_at} 被群友\n[{name}]({status.user_id}) 娶了")
                    return True, "already_wife", 1

                married_ids = await QQWIFE_DB.get_married_member_ids(gid)
                candidates = await QQWIFE_DB.get_recent_active_users(
                    gid,
                    limit=self.get_candidate_pool_size(),
                    exclude_user_ids=tuple(married_ids),
                )
                if self.allow_message_fallback_candidates() and len(candidates) < 2:
                    fallback = await QQWIFE_DB.get_recent_group_speakers_from_messages(
                        gid,
                        platform,
                        limit=self.get_candidate_pool_size(),
                        exclude_user_ids=tuple(married_ids),
                    )
                    exists = {item.user_id for item in candidates}
                    for item in fallback:
                        if item.user_id in exists:
                            continue
                        candidates.append(item)
                        exists.add(item.user_id)
                        if len(candidates) >= self.get_candidate_pool_size():
                            break

                if not candidates:
                    await self.send_text("群里暂无可匹配对象，先多聊聊再来试试。")
                    return True, "no_candidate", 1

                if len(candidates) == 1 and candidates[0].user_id == uid:
                    await self.send_text("群里没有ta人是单身了，明天再试试吧。")
                    return True, "only_self", 1

                fiancee = random.choice(candidates)
                fid = str(fiancee.user_id)
                fname = fiancee.nickname or await self.display_name(gid, fid, fid)

                if fid == uid:
                    if random.randint(0, 9) == 1:
                        ok = await QQWIFE_DB.register_marriage(gid, uid, "0", uname, "")
                        if ok:
                            await self.send_text("今日获得成就：单身贵族")
                        else:
                            await self.send_text("系统繁忙，请稍后再试。")
                        return True, "single_noble_roll", 1
                    await self.send_text("唔，没娶到，你可以再试一次。")
                    return True, "self_miss", 1

                ok = await QQWIFE_DB.register_marriage(gid, uid, fid, uname, fname)
                if not ok:
                    await self.send_text("ta好像刚被别人抢先一步了，稍后再试试。")
                    return True, "race_lost", 1

                favor = await QQWIFE_DB.update_favor(uid, fid, random.randint(1, 5))
                await self.send_text(f"今天你的群老婆是\n[{fname}]({fid})\n当前你们好感度为 {favor}")
                return True, "ok", 1


class QQWifeRosterCommand(QQWifeBaseCommand):
    command_name = "qqwife_roster"
    command_description = "查看群老婆列表"
    command_pattern = r"^群老婆列表$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_group():
            return True, "group_only", 1

        gid = self.get_group_id()
        async with QQWIFE_DB.transaction():
            await self.ensure_daily(gid)
            records = await QQWIFE_DB.list_group_marriages(gid, include_single=False)

        if not records:
            await self.send_text("今天还没有人结婚。")
            return True, "empty", 1

        lines = ["群老婆列表", "--------------------"]
        for idx, item in enumerate(records, start=1):
            left = item.username or await self.display_name(gid, item.user_id, item.user_id)
            right = item.target_name or await self.display_name(gid, item.target_id, item.target_id)
            lines.append(f"{idx}. {left}({item.user_id}) -> {right}({item.target_id})")
        await self.send_text("\n".join(lines))
        return True, "ok", 1


class QQWifeProposeCommand(QQWifeBaseCommand):
    command_name = "qqwife_propose"
    command_description = "娶或嫁指定对象"
    command_pattern = r"^(?P<action>娶|嫁)\s*(?P<target>.+)$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_group():
            return True, "group_only", 1

        action = str(self.matched_groups.get("action", "") or "")
        raw_target = str(self.matched_groups.get("target", "") or "")
        target_id = self.parse_single_target(raw_target)
        if not target_id:
            await self.send_text("目标不存在，请输入对方QQ号或@对方。")
            return True, "invalid_target", 1

        gid = self.get_group_id()
        uid = self.get_user_id()
        uname = self.get_user_name()
        user_lock_key = f"{gid}:{uid}"

        async with self.guard_user_operation(user_lock_key) as user_locked:
            if not user_locked:
                return True, "user_busy", 1

            async with self.guard_group_operation(gid) as group_locked:
                if not group_locked:
                    return True, "group_busy", 1

                async with QQWIFE_DB.transaction():
                    await QQWIFE_DB.upsert_active_user(gid, uid, uname)
                    settings = await self.ensure_daily(gid)

                    if not await self.check_cd_or_reply(gid, uid, MODE_PROPOSE, settings):
                        return True, "cd", 1

                    if not await self.validate_single_propose(gid, uid, target_id, settings):
                        return True, "validate_failed", 1

                    await QQWIFE_DB.record_cd(gid, uid, MODE_PROPOSE)

                    if target_id == uid:
                        if random.randint(0, 2) == 1:
                            ok = await QQWIFE_DB.register_marriage(gid, uid, "0", uname, "")
                            if ok:
                                await self.send_text("今日获得成就：单身贵族")
                            else:
                                await self.send_text("系统繁忙，请稍后再试。")
                            return True, "single_noble", 1
                        await self.send_text("今日获得成就：自恋狂")
                        return True, "self_love", 1

                    favor = await QQWIFE_DB.get_favor(uid, target_id)
                    if favor < 30:
                        favor = 30
                    if random.randint(0, 100) >= favor:
                        await self.send_text(random.choice(FAIL_TEXTS))
                        return True, "failed", 1

                    target_name = await self.display_name(gid, target_id, target_id)
                    if action == "娶":
                        ok = await QQWIFE_DB.register_marriage(gid, uid, target_id, uname, target_name)
                        relation = "群老婆"
                    else:
                        ok = await QQWIFE_DB.register_marriage(gid, target_id, uid, target_name, uname)
                        relation = "群老公"
                    if not ok:
                        await self.send_text("ta刚被别人抢先一步了。")
                        return True, "race_lost", 1

                    favor = await QQWIFE_DB.update_favor(uid, target_id, random.randint(1, 5))
                    await self.send_text(
                        f"{random.choice(SUCCESS_TEXTS)}\n\n今天你的{relation}是\n[{target_name}]({target_id})\n当前你们好感度为 {favor}"
                    )
                    return True, "ok", 1


class QQWifeMistressCommand(QQWifeBaseCommand):
    command_name = "qqwife_mistress"
    command_description = "当ta的小三"
    command_pattern = r"^当\s*(?P<target>.+?)\s*的小三$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_group():
            return True, "group_only", 1

        raw_target = str(self.matched_groups.get("target", "") or "")
        target_id = self.parse_single_target(raw_target)
        if not target_id:
            await self.send_text("目标不存在，请输入对方QQ号或@对方。")
            return True, "invalid_target", 1

        gid = self.get_group_id()
        uid = self.get_user_id()
        uname = self.get_user_name()
        user_lock_key = f"{gid}:{uid}"

        async with self.guard_user_operation(user_lock_key) as user_locked:
            if not user_locked:
                return True, "user_busy", 1

            async with self.guard_group_operation(gid) as group_locked:
                if not group_locked:
                    return True, "group_busy", 1

                try:
                    async with QQWIFE_DB.transaction():
                        await QQWIFE_DB.upsert_active_user(gid, uid, uname)
                        settings = await self.ensure_daily(gid)

                        if not await self.check_cd_or_reply(gid, uid, MODE_MISTRESS, settings):
                            return True, "cd", 1
                        if not await self.validate_mistress(gid, uid, target_id, settings):
                            return True, "validate_failed", 1

                        await QQWIFE_DB.record_cd(gid, uid, MODE_MISTRESS)

                        if target_id == uid:
                            await self.send_text("今日获得成就：自我攻略")
                            return True, "self_attack", 1

                        favor = await QQWIFE_DB.get_favor(uid, target_id)
                        if favor < 30:
                            favor = 30
                        threshold = max(1, favor // 3)
                        if random.randint(0, 100) >= threshold:
                            await self.send_text("失败了，可惜。")
                            return True, "failed", 1

                        target_info = await QQWIFE_DB.get_member_marriage(gid, target_id)
                        if target_info is None:
                            await self.send_text("数据已变化，请重试。")
                            return True, "not_found", 1

                        ntr_user_id = uid
                        new_target_id = target_id
                        green_id = ""
                        relation_text = "老婆"

                        if target_info.user_id == target_id:
                            old_partner = target_info.target_id
                            removed = await QQWIFE_DB.delete_marriage_by_user(gid, target_id)
                            if not removed:
                                raise RuntimeError("ta不想和原来的对象分手。")
                            ntr_user_id = target_id
                            new_target_id = uid
                            green_id = old_partner
                            relation_text = "老公"
                        elif target_info.target_id == target_id:
                            old_partner = target_info.user_id
                            removed = await QQWIFE_DB.delete_marriage_by_target(gid, target_id)
                            if not removed:
                                raise RuntimeError("ta不想和原来的对象分手。")
                            ntr_user_id = uid
                            new_target_id = target_id
                            green_id = old_partner
                            relation_text = "老婆"
                        else:
                            raise RuntimeError("婚姻数据异常。")

                        ntr_name = await self.display_name(gid, ntr_user_id, ntr_user_id)
                        new_target_name = await self.display_name(gid, new_target_id, new_target_id)
                        ok = await QQWIFE_DB.register_marriage(gid, ntr_user_id, new_target_id, ntr_name, new_target_name)
                        if not ok:
                            raise RuntimeError("复婚登记失败，请稍后重试。")

                        favor = await QQWIFE_DB.update_favor(uid, target_id, -5)
                        if green_id and green_id != "0":
                            await QQWIFE_DB.update_favor(uid, green_id, 5)

                        target_name = await self.display_name(gid, target_id, target_id)
                        await self.send_text(
                            f"{random.choice(NTR_SUCCESS_TEXTS)}\n\n今天你的群{relation_text}是\n"
                            f"[{target_name}]({target_id})\n当前你们好感度为 {favor}"
                        )
                        return True, "ok", 1
                except RuntimeError as exc:
                    await self.send_text(str(exc))
                    return True, "failed", 1


class QQWifeMatchmakerCommand(QQWifeBaseCommand):
    command_name = "qqwife_matchmaker"
    command_description = "做媒"
    command_pattern = r"^做媒(?P<body>.*)$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_group_admin():
            return True, "permission_denied", 1

        body = str(self.matched_groups.get("body", "") or "")
        left_id, right_id = self.parse_two_targets(body, body)
        if not left_id or not right_id:
            await self.send_text("格式错误，请使用：做媒 @攻方 @受方")
            return True, "invalid_targets", 1

        gid = self.get_group_id()
        uid = self.get_user_id()
        uname = self.get_user_name()
        user_lock_key = f"{gid}:{uid}"

        async with self.guard_user_operation(user_lock_key) as user_locked:
            if not user_locked:
                return True, "user_busy", 1

            async with self.guard_group_operation(gid) as group_locked:
                if not group_locked:
                    return True, "group_busy", 1

                async with QQWIFE_DB.transaction():
                    await QQWIFE_DB.upsert_active_user(gid, uid, uname)
                    settings = await self.ensure_daily(gid)
                    if not await self.check_cd_or_reply(gid, uid, MODE_MATCHMAKER, settings):
                        return True, "cd", 1
                    if not await self.validate_matchmaker(gid, uid, left_id, right_id):
                        return True, "validate_failed", 1

                    await QQWIFE_DB.record_cd(gid, uid, MODE_MATCHMAKER)

                    favor = await QQWIFE_DB.get_favor(left_id, right_id)
                    if favor < 30:
                        favor = 30
                    if random.randint(0, 100) >= favor:
                        await QQWIFE_DB.update_favor(uid, left_id, -1)
                        await QQWIFE_DB.update_favor(uid, right_id, -1)
                        await self.send_text(random.choice(FAIL_TEXTS))
                        return True, "failed", 1

                    left_name = await self.display_name(gid, left_id, left_id)
                    right_name = await self.display_name(gid, right_id, right_id)
                    ok = await QQWIFE_DB.register_marriage(gid, left_id, right_id, left_name, right_name)
                    if not ok:
                        await self.send_text("做媒失败，ta们可能刚被别人抢先了。")
                        return True, "race_lost", 1

                    await QQWIFE_DB.update_favor(uid, left_id, 1)
                    await QQWIFE_DB.update_favor(uid, right_id, 1)
                    await QQWIFE_DB.update_favor(left_id, right_id, 1)

                    await self.send_text(
                        "恭喜你成功撮合了一对CP。\n"
                        f"攻方: [{left_name}]({left_id})\n"
                        f"受方: [{right_name}]({right_id})"
                    )
                    return True, "ok", 1


class QQWifeDivorceCommand(QQWifeBaseCommand):
    command_name = "qqwife_divorce"
    command_description = "闹离婚/办离婚"
    command_pattern = r"^(?:闹离婚|办离婚)$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_group():
            return True, "group_only", 1

        gid = self.get_group_id()
        uid = self.get_user_id()
        uname = self.get_user_name()
        user_lock_key = f"{gid}:{uid}"

        async with self.guard_user_operation(user_lock_key) as user_locked:
            if not user_locked:
                return True, "user_busy", 1

            async with self.guard_group_operation(gid) as group_locked:
                if not group_locked:
                    return True, "group_busy", 1

                async with QQWIFE_DB.transaction():
                    await QQWIFE_DB.upsert_active_user(gid, uid, uname)
                    settings = await self.ensure_daily(gid)
                    if not await self.check_cd_or_reply(gid, uid, MODE_DIVORCE, settings):
                        return True, "cd", 1

                    user_info = await QQWIFE_DB.get_member_marriage(gid, uid)
                    if user_info is None:
                        await self.send_text("今天你还没结婚。")
                        return True, "not_married", 1

                    spouse_id = ""
                    user_pos = -1
                    if user_info.user_id == uid:
                        spouse_id = user_info.target_id
                        user_pos = 1
                    elif user_info.target_id == uid:
                        spouse_id = user_info.user_id
                        user_pos = 0
                    else:
                        await self.send_text("婚姻数据异常。")
                        return True, "invalid_data", 1

                    await QQWIFE_DB.record_cd(gid, uid, MODE_DIVORCE)
                    favor = await QQWIFE_DB.get_favor(uid, spouse_id)
                    if favor < 20:
                        favor = 10
                    if random.randint(0, 100) > (110 - favor):
                        await self.send_text(random.choice(DIVORCE_FAIL_TEXTS))
                        return True, "failed", 1

                    if user_pos == 1:
                        ok = await QQWIFE_DB.delete_marriage_by_user(gid, uid)
                    else:
                        ok = await QQWIFE_DB.delete_marriage_by_target(gid, uid)
                    if not ok:
                        await self.send_text("离婚失败，记录可能已被修改。")
                        return True, "not_found", 1

                    await self.send_text(random.choice(DIVORCE_SUCCESS_TEXTS))
                    return True, "ok", 1


class QQWifeGiftCommand(QQWifeBaseCommand):
    command_name = "qqwife_gift"
    command_description = "买礼物给ta"
    command_pattern = r"^买礼物给\s*(?P<target>.+)$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_group():
            return True, "group_only", 1

        gid = self.get_group_id()
        uid = self.get_user_id()
        uname = self.get_user_name()
        target_id = self.parse_single_target(str(self.matched_groups.get("target", "") or ""))
        if not target_id:
            await self.send_text("目标不存在，请输入对方QQ号或@对方。")
            return True, "invalid_target", 1
        if target_id == uid:
            await self.send_text("你想给自己买什么礼物呢？")
            return True, "self_target", 1

        user_lock_key = f"{gid}:{uid}"
        async with self.guard_user_operation(user_lock_key) as user_locked:
            if not user_locked:
                return True, "user_busy", 1

            async with QQWIFE_DB.transaction():
                await QQWIFE_DB.upsert_active_user(gid, uid, uname)
                settings = await self.ensure_daily(gid)

                if not await self.check_cd_or_reply(gid, uid, MODE_GIFT, settings):
                    return True, "cd", 1

                favor = await QQWIFE_DB.get_favor(uid, target_id)
                balance = await QQWIFE_DB.get_balance(uid)
                if balance < 1:
                    await self.send_text("你钱包没钱啦。")
                    return True, "balance_empty", 1

                cost = random.randint(1, min(balance, self.get_gift_max_cost()))
                favor_delta = 1
                mood_max = 2
                if favor > 50:
                    favor_delta = cost % 10
                else:
                    mood_max = 5
                    favor_delta += random.randint(0, max(0, cost - 1))
                mood = random.randint(0, mood_max - 1)
                if mood == 0:
                    favor_delta = -favor_delta

                ok, _ = await QQWIFE_DB.change_balance(uid, -cost)
                if not ok:
                    await self.send_text("扣款失败，你的钱包可能被并发修改，请重试。")
                    return True, "charge_failed", 1

                last_favor = await QQWIFE_DB.update_favor(uid, target_id, favor_delta)
                await QQWIFE_DB.record_cd(gid, uid, MODE_GIFT)

                wallet_name = self.get_wallet_name()
                if mood == 0:
                    await self.send_text(
                        f"你花了 {cost} {wallet_name} 买了礼物送给 ta，ta不太喜欢。你们的好感度降低至 {last_favor}"
                    )
                else:
                    await self.send_text(
                        f"你花了 {cost} {wallet_name} 买了礼物送给 ta，ta很喜欢。你们的好感度升至 {last_favor}"
                    )
                return True, "ok", 1


class QQWifeSetCDCommand(QQWifeBaseCommand):
    command_name = "qqwife_set_cd"
    command_description = "设置群内CD时间"
    command_pattern = r"^设置CD为(?P<hours>\d+(?:\.\d+)?)小时$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_group_admin():
            return True, "permission_denied", 1
        gid = self.get_group_id()
        try:
            hours = float(self.matched_groups.get("hours", "12"))
        except Exception:
            await self.send_text("请输入纯数字。")
            return True, "invalid_hours", 1
        hours = max(0.1, min(168.0, hours))

        async with QQWIFE_DB.transaction():
            settings = await QQWIFE_DB.get_group_settings(gid, default_cd_hours=self.get_default_cd_hours())
            settings.cd_hours = hours
            await QQWIFE_DB.save_group_settings(settings)
        await self.send_text(f"设置成功：CD 已更新为 {hours:g} 小时。")
        return True, "ok", 1


class QQWifeSwitchCommand(QQWifeBaseCommand):
    command_name = "qqwife_switch"
    command_description = "允许/禁止自由恋爱或牛头人"
    command_pattern = r"^(?P<status>允许|禁止)(?P<mode>自由恋爱|牛头人)$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_group_admin():
            return True, "permission_denied", 1
        gid = self.get_group_id()
        status = str(self.matched_groups.get("status", "") or "")
        mode = str(self.matched_groups.get("mode", "") or "")
        enabled = 1 if status == "允许" else 0

        async with QQWIFE_DB.transaction():
            settings = await QQWIFE_DB.get_group_settings(gid, default_cd_hours=self.get_default_cd_hours())
            if mode == "自由恋爱":
                settings.can_match = enabled
            else:
                settings.can_ntr = enabled
            await QQWIFE_DB.save_group_settings(settings)
        await self.send_text("设置成功。")
        return True, "ok", 1


class QQWifeResetRosterCommand(QQWifeBaseCommand):
    command_name = "qqwife_reset_roster"
    command_description = "重置花名册"
    command_pattern = r"^重置(?P<scope>所有|本群|\d+)?花名册$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_super_user():
            return True, "permission_denied", 1

        scope = str(self.matched_groups.get("scope", "") or "").strip()
        if scope == "所有":
            async with QQWIFE_DB.transaction():
                await QQWIFE_DB.clear_all_rosters()
            await self.send_text("重置成功：已清理所有群花名册。")
            return True, "ok", 1

        if scope in {"", "本群"}:
            gid = self.get_group_id()
            if not gid:
                await self.send_text("请在群聊使用，或指定群号。")
                return True, "group_required", 1
        else:
            gid = scope
            if not gid.isdigit():
                await self.send_text("请输入正确的群号。")
                return True, "invalid_group", 1

        async with QQWIFE_DB.transaction():
            await QQWIFE_DB.clear_group_roster(gid)
        await self.send_text("重置成功。")
        return True, "ok", 1


class QQWifeQueryFavorCommand(QQWifeBaseCommand):
    command_name = "qqwife_query_favor"
    command_description = "查询好感度"
    command_pattern = r"^查好感度\s*(?P<target>.+)$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_group():
            return True, "group_only", 1
        uid = self.get_user_id()
        target = self.parse_single_target(str(self.matched_groups.get("target", "") or ""))
        if not target:
            await self.send_text("目标不存在，请输入对方QQ号或@对方。")
            return True, "invalid_target", 1
        favor = await QQWIFE_DB.get_favor(uid, target)
        await self.send_text(f"当前你们好感度为 {favor}")
        return True, "ok", 1


class QQWifeFavorListCommand(QQWifeBaseCommand):
    command_name = "qqwife_favor_list"
    command_description = "好感度列表"
    command_pattern = r"^好感度列表$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_group():
            return True, "group_only", 1
        gid = self.get_group_id()
        uid = self.get_user_id()
        pairs = await QQWIFE_DB.list_favor_for_user(uid, limit=10)
        if not pairs:
            await self.send_text("你还没有任何好感度记录。")
            return True, "empty", 1

        lines = ["你的好感度排行列表", "--------------------"]
        for idx, (target_id, favor) in enumerate(pairs, start=1):
            name = await self.display_name(gid, target_id, target_id)
            bar = "#" * max(1, favor // 10) if favor > 0 else "-"
            lines.append(f"{idx}. {name}({target_id}) {favor:>3} [{bar}]")
        await self.send_text("\n".join(lines))
        return True, "ok", 1


class QQWifeFavorCleanupCommand(QQWifeBaseCommand):
    command_name = "qqwife_favor_cleanup"
    command_description = "好感度数据整理"
    command_pattern = r"^好感度数据整理$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        if not await self.require_super_user():
            return True, "permission_denied", 1
        async with QQWIFE_DB.transaction():
            total, rewritten = await QQWIFE_DB.cleanup_favorability_data()
        await self.send_text(f"整理完成：扫描 {total} 条，好感度规范化 {rewritten} 条。")
        return True, "ok", 1


class QQWifeActivityTracker(BaseEventHandler):
    event_type = EventType.ON_MESSAGE_PRE_PROCESS
    handler_name = "qqwife_activity_tracker"
    handler_description = "记录群成员活跃数据"

    async def execute(
        self,
        message: MaiMessages | None,
    ) -> Tuple[bool, bool, Optional[str], None, None]:
        if message is None or not message.is_group_message:
            return True, True, None, None, None
        try:
            group_id = str(message.message_base_info.get("group_id") or "").strip()
            user_id = str(message.message_base_info.get("user_id") or "").strip()
            if not group_id or not user_id:
                return True, True, None, None, None
            user_card = str(message.message_base_info.get("user_cardname") or "").strip()
            user_nick = str(message.message_base_info.get("user_nickname") or "").strip()
            nickname = user_card or user_nick or user_id
            await QQWIFE_DB.upsert_active_user(group_id, user_id, nickname)
        except Exception as exc:
            logger.debug(f"[qqwife_activity_tracker] update failed: {exc}")
        return True, True, None, None, None


@register_plugin
class QQWifePlugin(BasePlugin):
    plugin_name = "qqwife_plugin"
    enable_plugin = True
    dependencies: List[str] = []
    python_dependencies = ["aiosqlite"]
    config_file_name = "config.toml"

    config_section_descriptions = {
        "plugin": "插件基础配置",
        "components": "功能配置",
    }
    config_schema = {
        "plugin": {
            "config_version": ConfigField(type=str, default="1.0.0", description="配置版本"),
            "enabled": ConfigField(type=bool, default=True, description="是否启用插件"),
        },
        "components": {
            "command_prefixes": ConfigField(
                type=list,
                default=["#", "/", "!"],
                description="命令触发前缀列表（配合强制开关决定是否必须带前缀）",
                item_type="string",
            ),
            "force_command_prefix_match": ConfigField(
                type=bool,
                default=False,
                description="是否强制命令必须带前缀",
            ),
            "wallet_name": ConfigField(type=str, default="麦币", description="钱包货币名称"),
            "default_cd_hours": ConfigField(type=float, default=12.0, description="默认技能CD（小时）"),
            "recent_candidate_limit": ConfigField(type=int, default=30, description="随机娶群友候选人数上限"),
            "gift_max_cost": ConfigField(type=int, default=100, description="单次送礼最大花费"),
            "allow_message_fallback_candidates": ConfigField(
                type=bool,
                default=True,
                description="活跃缓存不足时是否允许从消息数据库回退取候选",
            ),
            "enable_activity_tracker": ConfigField(type=bool, default=True, description="是否启用活跃用户追踪"),
            "admin_user_ids": ConfigField(type=list, default=[], description="额外群管理用户ID列表", item_type="string"),
            "super_user_ids": ConfigField(type=list, default=[], description="超级用户ID列表", item_type="string"),
        },
    }

    def _get_command_prefixes(self) -> List[str]:
        raw_value = self.get_config("components.command_prefixes", ["#", "/", "!"])
        if isinstance(raw_value, str):
            candidates = [part for part in re.split(r"[\s,，]+", raw_value) if part]
        elif isinstance(raw_value, list):
            candidates = [str(part) for part in raw_value]
        else:
            candidates = []

        prefixes: List[str] = []
        for prefix in candidates:
            value = prefix.strip()
            if not value or value in prefixes:
                continue
            prefixes.append(value)
        return prefixes

    def _get_force_command_prefix_match(self) -> bool:
        return bool(self.get_config("components.force_command_prefix_match", False))

    def _apply_command_prefixes(self, raw_pattern: str, prefixes: List[str], *, force: bool) -> str:
        if not prefixes:
            return raw_pattern
        escaped_prefixes = "|".join(re.escape(prefix) for prefix in prefixes)
        prefix_expr = f"(?:{escaped_prefixes})"

        body = raw_pattern
        if body.startswith("^"):
            body = body[1:]
        if body.endswith("$"):
            body = body[:-1]
        if force:
            return f"^{prefix_expr}\\s*{body}$"
        return f"^(?:{prefix_expr}\\s*)?{body}$"

    def _build_command_components(self, prefixes: List[str], *, force_prefix: bool) -> List[Tuple[ComponentInfo, Type]]:
        command_classes: List[Type[BaseCommand]] = [
            QQWifeRandomCommand,
            QQWifeRosterCommand,
            QQWifeProposeCommand,
            QQWifeMistressCommand,
            QQWifeMatchmakerCommand,
            QQWifeDivorceCommand,
            QQWifeGiftCommand,
            QQWifeQueryFavorCommand,
            QQWifeFavorListCommand,
            QQWifeSetCDCommand,
            QQWifeSwitchCommand,
            QQWifeResetRosterCommand,
            QQWifeFavorCleanupCommand,
        ]
        components: List[Tuple[ComponentInfo, Type]] = []
        for command_cls in command_classes:
            info = command_cls.get_command_info()
            info.command_pattern = self._apply_command_prefixes(
                info.command_pattern,
                prefixes,
                force=force_prefix,
            )
            components.append((info, command_cls))
        return components

    def get_plugin_components(self) -> List[Tuple[ComponentInfo, Type]]:
        if not self.get_config("plugin.enabled", True):
            return []

        components = self._build_command_components(
            self._get_command_prefixes(),
            force_prefix=self._get_force_command_prefix_match(),
        )
        if bool(self.get_config("components.enable_activity_tracker", True)):
            components.append((QQWifeActivityTracker.get_handler_info(), QQWifeActivityTracker))
        return components
