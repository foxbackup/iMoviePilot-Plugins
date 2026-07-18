import os
import copy
import re
import threading
import traceback
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.chain.media import MediaChain
from app.modules.filemanager import FileManagerModule
from app.core.config import settings
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.modules.filemanager.storages.u115 import U115Pan
from app.plugins import _PluginBase
from app.schemas.types import NotificationType


class LocalUploadRequiredError(Exception):
    """需要本地分片上传异常。"""


class Transient115Error(Exception):
    """115 临时错误异常，可稍后重试。"""


class PermanentUploadError(Exception):
    """永久上传错误异常，需要人工处理。"""


class Instant115(_PluginBase):
    """秒传115插件。"""

    plugin_name = "秒传115"
    plugin_desc = "监控 qBittorrent 完成任务，支持按标签映射 115 目录、MoviePilot 智能重命名与 SHA1/preid 秒传缓存，只接受 115 秒传，非秒传自动冷却重试。"
    plugin_icon = "upload_a.png"
    plugin_version = "1.4.1"
    plugin_author = "local"
    plugin_label = "网盘"
    plugin_config_prefix = "instant115_"
    plugin_order = 12
    auth_level = 1

    _enabled = False
    _onlyonce = False
    _notify = True
    _target_path = "/PT"
    _skip_tags = "已上传115"
    _uploaded_tag = "已上传115"
    _tag_path_mappings = ""
    _cooldown_minutes = 30
    _cron = "*/10 * * * *"
    _max_retry = 0
    _max_tasks_per_scan = 3
    _record_keep_days = 30
    _clear_records = False
    _running_lock_timeout_minutes = 180
    _smart_rename = False
    _smart_rename_fallback = True

    _record_key = "records"
    _runtime_key = "runtime"
    _queue_key = "queue"
    _instant_cache_key = "instant_cache"
    _manifest_key = "manifests"
    _scheduler: Optional[BackgroundScheduler] = None
    _running = False
    _running_lock = threading.Lock()

    def init_plugin(self, config: dict = None) -> None:
        """根据配置初始化插件运行状态。"""
        self.stop_service()
        self._enabled = False
        self._onlyonce = False
        self._notify = True
        self._target_path = "/PT"
        self._skip_tags = "已上传115"
        self._uploaded_tag = "已上传115"
        self._tag_path_mappings = ""
        self._cooldown_minutes = 30
        self._cron = "*/10 * * * *"
        self._max_retry = 0
        self._max_tasks_per_scan = 3
        self._record_keep_days = 30
        self._clear_records = False
        self._running_lock_timeout_minutes = 180
        self._smart_rename = False
        self._smart_rename_fallback = True
        if config:
            self._enabled = bool(config.get("enabled", False))
            self._onlyonce = bool(config.get("onlyonce", False))
            self._notify = bool(config.get("notify", True))
            configured_target = self._normalize_115_path(str(config.get("target_path") or "/PT"))
            if configured_target:
                self._target_path = configured_target
            else:
                logger.warning(f"秒传115忽略不安全的默认目标目录：{config.get('target_path')}，使用 /PT")
            self._skip_tags = str(config.get("skip_tags") or "")
            self._uploaded_tag = str(config.get("uploaded_tag") or "已上传115")
            self._tag_path_mappings = str(config.get("tag_path_mappings") or "")
            self._cooldown_minutes = self._safe_int(config.get("cooldown_minutes"), 30, minimum=1)
            self._cron = str(config.get("cron") or config.get("scan_cron") or "*/10 * * * *").strip()
            self._max_retry = self._safe_int(config.get("max_retry"), 0, minimum=0)
            self._max_tasks_per_scan = self._safe_int(config.get("max_tasks_per_scan"), 3, minimum=1)
            self._record_keep_days = self._safe_int(config.get("record_keep_days"), 30, minimum=1)
            self._running_lock_timeout_minutes = self._safe_int(config.get("running_lock_timeout_minutes"), 180, minimum=10)
            self._clear_records = bool(config.get("clear_records", False))
            self._smart_rename = bool(config.get("smart_rename", False))
            self._smart_rename_fallback = bool(config.get("smart_rename_fallback", True))
        if self._clear_records:
            self.del_data(self._record_key)
            self._clear_records = False
            self._save_config()
            logger.info("秒传115已清空历史记录")
        if self._onlyonce:
            self._onlyonce = False
            self._save_config()
            self._start_once_scheduler()

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件 API 列表。"""
        return [
            {"path": f"/{self.__class__.__name__}/status", "endpoint": self.api_status, "methods": ["GET"], "auth": "bear"},
            {"path": f"/{self.__class__.__name__}/run", "endpoint": self.api_run, "methods": ["POST"], "auth": "bear"},
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回插件 JSON 配置表单。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件", "hint": "启用后按扫描间隔自动检查 qB 完成任务。", "persistent-hint": True}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次", "hint": "保存配置后触发一次后台扫描。", "persistent-hint": True}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "发送通知", "hint": "秒传成功后发送插件通知。", "persistent-hint": True}}]},
                        ],
                    },
                    {"component": "VDivider", "props": {"class": "my-3"}},
                    {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "class": "mb-3", "text": "工作方式：先全量筛选 qB 种子，再计算 SHA1/preid/size 缓存；写入最终 115 目录时只接受 status=2 秒传。若需要分片上传，则跳过并冷却。"}},
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "target_path", "label": "115 目标上传目录", "placeholder": "/PT", "hint": "会在该目录下创建与种子名相同的文件夹。", "persistent-hint": True}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "uploaded_tag", "label": "上传完成后添加 qB 标签", "placeholder": "已上传115", "hint": "秒传成功后给 qBittorrent 任务添加的标签。", "persistent-hint": True}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VTextField", "props": {"model": "skip_tags", "label": "跳过的 qB 标签", "placeholder": "多个标签用英文逗号分隔，如：MOVIEPILOT,已上传115", "hint": "任务包含任意一个标签时跳过，不进入队列。", "persistent-hint": True}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VTextarea", "props": {"model": "tag_path_mappings", "label": "标签目标目录映射", "placeholder": "ADE => /PT/ADE\naudiences => /PT/Audiences", "rows": 4, "auto-grow": True, "hint": "一行一条，格式：标签 => 115目录。任务有多个标签时从上到下第一条命中生效；未命中则使用默认目标目录。", "persistent-hint": True}}]},
                        ],
                    },
                    {"component": "VDivider", "props": {"class": "my-3"}},
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "cron", "label": "扫描 Cron", "placeholder": "*/10 * * * *", "hint": "五位 cron 表达式，例如 */10 * * * * 表示每 10 分钟。", "persistent-hint": True}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "max_tasks_per_scan", "label": "每轮最多处理任务数", "type": "number", "min": 1}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "cooldown_minutes", "label": "冷却重试（分钟）", "type": "number", "min": 1, "hint": "检测到需要 115 分片上传时进入冷却。", "persistent-hint": True}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "max_retry", "label": "最大重试次数", "type": "number", "min": 0, "hint": "0 表示不限次数。", "persistent-hint": True}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "record_keep_days", "label": "记录保留天数", "type": "number", "min": 1}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "running_lock_timeout_minutes", "label": "运行锁超时（分钟）", "type": "number", "min": 10, "hint": "异常中断或热重载后，超过该时间自动释放运行锁。", "persistent-hint": True}}]},
                        ],
                    },
                    {"component": "VDivider", "props": {"class": "my-3"}},
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VSwitch", "props": {"model": "smart_rename", "label": "上传前使用 MoviePilot 智能重命名", "hint": "仅改变提交给 115 /open/upload/init 的 file_name，本地文件名不变。", "persistent-hint": True}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VSwitch", "props": {"model": "smart_rename_fallback", "label": "智能命名失败时回退原文件名", "hint": "关闭后智能命名失败会直接使该文件上传失败。", "persistent-hint": True}}]},
                        ],
                    },
                    {"component": "VDivider", "props": {"class": "my-3"}},
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "clear_records", "label": "保存后清空历史记录", "color": "warning"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 8}, "content": [{"component": "VAlert", "props": {"type": "warning", "variant": "tonal", "text": "插件不会调用 U115Pan.upload()，也不会进行 OSS 分片上传。若 115 返回非秒传，会跳过任务并冷却；仅清理本轮新建且为空的目标目录。"}}]},
                        ],
                    },
                ],
            }
        ], self._current_config()

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页面。"""
        status = self.api_status()
        summary = status.get("summary", {})
        queue = status.get("queue", [])[:50]
        records = status.get("records", [])[:30]
        queue_rows = []
        for idx, item in enumerate(queue, 1):
            queue_rows.append({"component": "tr", "content": [
                {"component": "td", "text": str(idx)},
                {"component": "td", "text": item.get("name", "-")},
                {"component": "td", "text": item.get("reason", "待上传")},
                {"component": "td", "text": item.get("path", "-")},
            ]})
        rows = []
        for item in records:
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "text": item.get("name", "-")},
                    {"component": "td", "text": item.get("status_text", "-")},
                    {"component": "td", "text": str(item.get("retry", 0))},
                    {"component": "td", "text": item.get("time_text", "-")},
                ],
            })
        return [
            {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": f"运行中：{summary.get('running', False)}；待上传队列：{summary.get('queue_count', 0)}；已完成：{summary.get('uploaded', 0)}；冷却中：{summary.get('cooldown', 0)}；失败：{summary.get('failed', 0)}；最后扫描：{self._format_time(int(summary.get('last_scan', 0) or 0))}"}},
            {"component": "VCard", "props": {"variant": "tonal", "class": "mb-3"}, "content": [
                {"component": "VCardTitle", "text": "本轮待上传队列"},
                {"component": "VTable", "content": [
                    {"component": "thead", "content": [{"component": "tr", "content": [
                        {"component": "th", "text": "序号"}, {"component": "th", "text": "任务"}, {"component": "th", "text": "入队原因"}, {"component": "th", "text": "本地路径"}
                    ]}]},
                    {"component": "tbody", "content": queue_rows or [{"component": "tr", "content": [{"component": "td", "props": {"colspan": 4}, "text": "暂无待上传队列"}]}]}
                ]}
            ]},
            {"component": "VTable", "content": [
                {"component": "thead", "content": [{"component": "tr", "content": [
                    {"component": "th", "text": "任务"}, {"component": "th", "text": "状态"}, {"component": "th", "text": "重试"}, {"component": "th", "text": "时间"}
                ]}]},
                {"component": "tbody", "content": rows or [{"component": "tr", "content": [{"component": "td", "props": {"colspan": 4}, "text": "暂无记录"}]}]}
            ]}
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时扫描服务。"""
        if not self._enabled:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron)
        except Exception as err:
            logger.error(f"秒传115 Cron 表达式无效：{self._cron} - {err}")
            return []
        return [{"id": self.__class__.__name__, "name": "秒传115扫描", "trigger": trigger, "func": self.scan_and_upload, "kwargs": {}}]

    def stop_service(self) -> None:
        """停止插件后台调度器并取消插件内检测事件。"""
        if self._scheduler:
            try:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    getattr(self._scheduler, "shutdown")(wait=False)
            except Exception as err:
                logger.warning(f"秒传115停止调度器失败：{err}")
            self._scheduler = None

    def api_status(self) -> Dict[str, Any]:
        """返回插件运行状态和最近记录。"""
        records = self._load_records()
        runtime = self.get_data(self._runtime_key) or {}
        now = int(time.time())
        rows = []
        counts = {"uploaded": 0, "cooldown": 0, "failed": 0}
        for key, item in records.items():
            if not isinstance(item, dict):
                continue
            status = item.get("status") or "unknown"
            if status == "uploaded":
                counts["uploaded"] += 1
            elif status == "cooldown" and int(item.get("cooldown_until", 0) or 0) > now:
                counts["cooldown"] += 1
            elif status == "failed":
                counts["failed"] += 1
            rows.append({"key": key, "name": item.get("name") or key, "status": status, "status_text": self._status_text(item), "retry": item.get("retry", 0), "time": item.get("time", 0), "time_text": self._format_time(int(item.get("time", 0) or 0))})
        rows.sort(key=lambda x: int(x.get("time", 0) or 0), reverse=True)
        queue = self.get_data(self._queue_key) or []
        return {"summary": {**counts, "running": bool(runtime.get("running")), "last_scan": runtime.get("last_scan"), "queue_count": len(queue) if isinstance(queue, list) else 0}, "queue": queue if isinstance(queue, list) else [], "records": rows[:80]}

    def api_run(self) -> Dict[str, Any]:
        """通过 API 触发一次后台扫描。"""
        thread = threading.Thread(target=self.scan_and_upload, name="Instant115ApiRun", daemon=True)
        thread.start()
        return {"success": True, "message": "已触发后台扫描"}

    def scan_and_upload(self) -> None:
        """扫描 qBittorrent 完成任务并尝试上传到 115。"""
        if not self._acquire_running_lock():
            return
        try:
            self._cleanup_old_records()
            service = self._get_qb_service()
            if not service:
                logger.error("秒传115未找到 qBittorrent 下载器")
                return
            downloader = service.instance
            torrents, error = downloader.get_torrents()
            if error:
                logger.error(f"秒传115获取 qBittorrent 任务失败：{error}")
                return
            u115 = self._get_115()
            if not u115:
                logger.error("秒传115初始化 115 失败，请检查 115 登录状态")
                return
            if not u115.get_folder(Path(self._target_path)):
                logger.error(f"秒传115创建或获取 115 目录失败：{self._target_path}")
                return
            queue = self._build_upload_queue(torrents or [])
            self._save_queue(queue)
            logger.info(f"秒传115本轮检查完成：总数 {len(torrents or [])}，符合规则 {len(queue)}，每轮上限 {self._max_tasks_per_scan}")
            for idx, item in enumerate(queue[:self._max_tasks_per_scan], 1):
                logger.info(f"秒传115开始处理队列 {idx}/{min(len(queue), self._max_tasks_per_scan)}：{item.get('name')}")
                self._process_torrent(u115, item.get('torrent'))
            self._save_queue(queue[self._max_tasks_per_scan:])
        except Exception as err:
            logger.error(f"秒传115扫描异常：{err}\n{traceback.format_exc()}")
        finally:
            self._release_running_lock()



    def _acquire_running_lock(self) -> bool:
        """获取运行锁，防止扫描和上传任务并发执行。"""
        now = int(time.time())
        runtime = self.get_data(self._runtime_key) or {}
        lock_started = int(runtime.get("lock_started", 0) or 0) if isinstance(runtime, dict) else 0
        lock_timeout = self._running_lock_timeout_minutes * 60
        if isinstance(runtime, dict) and runtime.get("running") and lock_started and now - lock_started < lock_timeout:
            logger.info(f"秒传115持久化运行锁仍有效，本次扫描跳过；锁定开始：{self._format_time(lock_started)}，超时：{self._running_lock_timeout_minutes} 分钟")
            return False
        if isinstance(runtime, dict) and runtime.get("running") and lock_started and now - lock_started >= lock_timeout:
            logger.warning(f"秒传115检测到运行锁超时，自动释放旧锁；锁定开始：{self._format_time(lock_started)}")
        if not self._running_lock.acquire(blocking=False):
            logger.info("秒传115线程运行锁已被占用，本次扫描跳过")
            return False
        if self._running:
            self._running_lock.release()
            logger.info("秒传115已有任务正在运行，本次扫描跳过")
            return False
        self._running = True
        self.save_data(self._runtime_key, {"running": True, "last_scan": now, "lock_started": now, "lock_owner": self.__class__.__name__})
        logger.info(f"秒传115已获取运行锁，锁定开始：{self._format_time(now)}")
        return True

    def _release_running_lock(self) -> None:
        """释放运行锁并更新运行状态。"""
        now = int(time.time())
        self._running = False
        self.save_data(self._runtime_key, {"running": False, "last_scan": now, "lock_started": 0, "lock_owner": ""})
        if self._running_lock.locked():
            try:
                self._running_lock.release()
            except RuntimeError:
                pass
        logger.info(f"秒传115已释放运行锁，结束时间：{self._format_time(now)}")

    def _build_upload_queue(self, torrents: List[Any]) -> List[Dict[str, Any]]:
        """先完整检查所有种子并构建待上传队列。"""
        queue: List[Dict[str, Any]] = []
        records = self._load_records()
        now = int(time.time())
        logger.info(f"秒传115开始全量检查 qB 种子，共 {len(torrents)} 个")
        for torrent in torrents:
            allowed, reason = self._check_torrent_rule(torrent, records=records, now=now)
            name = getattr(torrent, "name", "") or getattr(torrent, "hash", "unknown")
            tags = getattr(torrent, "tags", "") or ""
            progress = float(getattr(torrent, "progress", 0) or 0)
            state = getattr(torrent, "state", "") or ""
            path = getattr(torrent, "content_path", "") or getattr(torrent, "save_path", "") or ""
            logger.debug(f"秒传115检查种子：{name} | state={state} progress={progress:.4f} tags={tags} path={path} | 结果={'入队' if allowed else '剔除'} | 原因={reason}")
            if allowed:
                queue.append({"hash": getattr(torrent, "hash", ""), "name": name, "reason": reason, "path": path, "torrent": torrent, "time": int(time.time())})
        return queue

    def _save_queue(self, queue: List[Dict[str, Any]]) -> None:
        """保存可展示的待上传队列。"""
        view = [{k: v for k, v in item.items() if k != "torrent"} for item in queue]
        self.save_data(self._queue_key, view)

    def _check_torrent_rule(self, torrent, records: Optional[Dict[str, Any]] = None, now: Optional[int] = None) -> Tuple[bool, str]:
        """检查单个种子是否符合上传规则。"""
        if not self._is_completed(torrent):
            return False, "未完成"
        if self._is_skip_tagged(torrent):
            return False, "包含跳过标签"
        records = records if isinstance(records, dict) else self._load_records()
        item = records.get(torrent.hash)
        now = int(now if now is not None else time.time())
        if isinstance(item, dict):
            if item.get("status") == "uploaded":
                return False, "已上传记录"
            if item.get("status") == "cooldown" and int(item.get("cooldown_until", 0) or 0) > now:
                return False, f"冷却中至 {self._format_time(int(item.get('cooldown_until', 0) or 0))}"
        local_path = Path(getattr(torrent, "content_path", "") or getattr(torrent, "save_path", "") or "")
        if not local_path.exists():
            return False, f"本地路径不存在：{local_path}"
        return True, "符合规则"


    def _parse_tag_path_mappings(self) -> List[Tuple[str, str]]:
        """解析 qB 标签到 115 目录的映射配置。"""
        mappings: List[Tuple[str, str]] = []
        for raw_line in str(self._tag_path_mappings or "").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=>" not in line:
                logger.warning(f"秒传115忽略无效标签目录映射：{line}")
                continue
            tag, path = [part.strip() for part in line.split("=>", 1)]
            normalized_path = self._normalize_115_path(path)
            if not tag or not normalized_path:
                logger.warning(f"秒传115忽略不完整或不安全的标签目录映射：{line}")
                continue
            mappings.append((tag, normalized_path))
        return mappings

    def _resolve_target_path(self, torrent) -> str:
        """根据 qB 标签解析当前任务应使用的 115 目标目录。"""
        tags = {tag.strip() for tag in str(getattr(torrent, "tags", "") or "").split(",") if tag.strip()}
        for tag, path in self._parse_tag_path_mappings():
            if tag in tags:
                logger.info(f"秒传115任务命中标签路径映射：{getattr(torrent, 'name', '')} | {tag} => {path}")
                return path
        logger.info(f"秒传115任务未命中标签路径映射，使用默认目录：{getattr(torrent, 'name', '')} -> {self._target_path}")
        return self._target_path

    @staticmethod
    def _normalize_115_path(path: str) -> Optional[str]:
        """规范化 115 绝对路径并拒绝父目录跳转。"""
        value = str(path or "").strip().replace("\\", "/")
        if not value.startswith("/"):
            return None
        parts = [part for part in value.split("/") if part]
        if any(part in {".", ".."} for part in parts):
            return None
        return "/" + "/".join(parts) if parts else "/"

    def _process_torrent(self, u115: U115Pan, torrent) -> None:
        """处理单个 qBittorrent 任务并支持文件级断点恢复。"""
        torrent_hash = torrent.hash
        name = torrent.name or torrent_hash
        local_path = Path(torrent.content_path or torrent.save_path or "")
        if not local_path.exists():
            self._write_record(torrent_hash, name, "failed", reason=f"本地路径不存在：{local_path}")
            return
        logger.info(f"秒传115开始处理：{name}，构建任务清单并恢复文件级状态")
        self._write_record(torrent_hash, name, "checking", reason="正在构建 manifest 和 SHA1/preid/size 缓存")
        ok, file_count, reason, instant_cache = self._build_instant_cache(local_path)
        if not ok:
            self._write_record(torrent_hash, name, "failed", reason=reason)
            return
        base_target_path = self._resolve_target_path(torrent)
        target_path = Path(base_target_path) / self._safe_name(name)
        existed_dir = u115.get_item(target_path)
        target_dir = u115.get_folder(target_path)
        created_by_plugin = not bool(existed_dir) and bool(target_dir)
        if not target_dir:
            self._write_record(torrent_hash, name, "failed", reason="创建 115 同名目录失败")
            return
        manifest = self._prepare_manifest(torrent_hash, local_path, target_dir, instant_cache)
        pending_count = sum(1 for item in manifest if item.get("status") != "confirmed")
        logger.info(f"秒传115任务 manifest：总数 {len(manifest)}，已确认 {len(manifest) - pending_count}，待处理 {pending_count}")
        self._write_record(torrent_hash, name, "uploading", reason=f"准备处理 {pending_count}/{file_count} 个文件")
        ok, confirmed_count, result_code, result_reason = self._upload_manifest(u115, target_dir, torrent_hash, manifest)
        if ok:
            self._clear_instant_cache_for_path(local_path)
            self._delete_manifest(torrent_hash)
            self._write_record(torrent_hash, name, "uploaded", files=confirmed_count, reason=result_reason)
            self._tag_uploaded_torrent(torrent)
            self._notify_msg("秒传115上传完成", f"{name}\n文件数：{confirmed_count}\n结果：{result_reason}")
            return
        records = self._load_records()
        retry = int((records.get(torrent_hash) or {}).get("retry", 0) or 0) + 1
        if result_code in {"non_instant", "transient"}:
            if self._max_retry and retry > self._max_retry:
                self._write_record(torrent_hash, name, "failed", retry=retry, reason=f"超过最大重试次数：{result_reason}")
                return
            cooldown_until = int(time.time()) + self._cooldown_minutes * 60
            error_type = "非秒传" if result_code == "non_instant" else "115 临时错误"
            self._write_record(torrent_hash, name, "cooldown", retry=retry, cooldown_until=cooldown_until, reason=f"{error_type}：{result_reason}；冷却至 {self._format_time(cooldown_until)}")
            logger.warning(f"秒传115任务进入冷却：{name} - {error_type}：{result_reason}")
            self._cleanup_created_empty_dir(u115, target_dir, created_by_plugin)
            return
        self._write_record(torrent_hash, name, "failed", retry=retry, reason=f"永久错误：{result_reason}")
        self._cleanup_created_empty_dir(u115, target_dir, created_by_plugin)

    def _prepare_manifest(self, task_key: str, local_path: Path, target_dir, instant_cache: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """建立任务 manifest，并合并之前持久化的文件级状态。"""
        all_manifests = self._load_manifests()
        old_manifest = all_manifests.get(task_key) if isinstance(all_manifests.get(task_key), dict) else {}
        old_files = old_manifest.get("files") if isinstance(old_manifest.get("files"), dict) else {}
        files = [local_path] if local_path.is_file() else [Path(root) / name for root, _, names in os.walk(local_path) for name in names]
        manifest: List[Dict[str, Any]] = []
        for file_path in sorted(files, key=lambda item: item.as_posix()):
            key = file_path.as_posix()
            meta = instant_cache.get(key) or {}
            relative = Path(file_path.name) if local_path.is_file() else file_path.relative_to(local_path)
            parent_relative = relative.parent.as_posix()
            old = old_files.get(key) if isinstance(old_files.get(key), dict) else {}
            status = "confirmed" if old.get("status") == "confirmed" and old.get("file_sha1") == meta.get("file_sha1") and int(old.get("file_size", -1) or -1) == int(meta.get("file_size", -2) or -2) else "pending"
            manifest.append({
                "local_path": key, "relative_path": relative.as_posix(), "parent_relative": parent_relative,
                "original_name": file_path.name, "upload_name": old.get("upload_name") or "",
                "file_size": int(meta.get("file_size", 0) or 0), "file_sha1": str(meta.get("file_sha1") or ""),
                "file_preid": str(meta.get("file_preid") or ""), "mtime": int(meta.get("mtime", 0) or 0),
                "status": status, "remote_file_id": old.get("remote_file_id"), "remote_path": old.get("remote_path"),
                "updated": int(time.time()),
            })
        self._save_manifest(task_key, target_dir.path, manifest)
        return manifest

    def _load_manifests(self) -> Dict[str, Dict[str, Any]]:
        """读取全部任务 manifest。"""
        data = self.get_data(self._manifest_key)
        return data if isinstance(data, dict) else {}

    def _save_manifest(self, task_key: str, target_path: str, manifest: List[Dict[str, Any]]) -> None:
        """持久化单个任务的文件级 manifest。"""
        all_manifests = self._load_manifests()
        all_manifests[task_key] = {"target_path": target_path, "updated": int(time.time()), "files": {item["local_path"]: dict(item) for item in manifest}}
        self.save_data(self._manifest_key, all_manifests)

    def _delete_manifest(self, task_key: str) -> None:
        """任务全部确认成功后删除其 manifest。"""
        all_manifests = self._load_manifests()
        if task_key in all_manifests:
            all_manifests.pop(task_key, None)
            self.save_data(self._manifest_key, all_manifests)

    def _update_manifest_item(self, task_key: str, item: Dict[str, Any]) -> None:
        """更新并立即持久化一个文件的处理状态。"""
        all_manifests = self._load_manifests()
        task = all_manifests.get(task_key) if isinstance(all_manifests.get(task_key), dict) else {"files": {}}
        files = task.get("files") if isinstance(task.get("files"), dict) else {}
        item["updated"] = int(time.time())
        files[item["local_path"]] = dict(item)
        task["files"] = files
        task["updated"] = int(time.time())
        all_manifests[task_key] = task
        self.save_data(self._manifest_key, all_manifests)

    def _upload_manifest(self, u115: U115Pan, target_dir, task_key: str, manifest: List[Dict[str, Any]]) -> Tuple[bool, int, str, str]:
        """按 manifest 上传未确认文件，并在每个文件成功后持久化状态。"""
        folder_cache = {".": target_dir}
        confirmed = 0
        try:
            for index, item in enumerate(manifest, 1):
                relative_parent = item.get("parent_relative") or "."
                remote_dir = folder_cache.get(relative_parent)
                if remote_dir is None:
                    remote_dir = u115.get_folder(Path(target_dir.path) / relative_parent)
                    folder_cache[relative_parent] = remote_dir
                if not remote_dir:
                    raise PermanentUploadError(f"创建远端子目录失败：{relative_parent}")
                local_file = Path(item["local_path"])
                upload_name = item.get("upload_name") or self._resolve_upload_file_name(local_file, item["original_name"])
                item["upload_name"] = upload_name
                remote_path = Path(remote_dir.path) / upload_name
                item["remote_path"] = remote_path.as_posix()
                existing = u115.get_item(remote_path)
                if existing:
                    if not self._remote_matches(u115, existing, item):
                        raise PermanentUploadError(f"远端同名文件大小或 SHA1 冲突：{remote_path}")
                    item.update({"status": "confirmed", "remote_file_id": getattr(existing, "fileid", None)})
                    self._update_manifest_item(task_key, item)
                    confirmed += 1
                    logger.info(f"秒传115幂等命中远端文件 {index}/{len(manifest)}：{remote_path}")
                    continue
                item["status"] = "pending"
                result = self._upload_file_instant_only(u115, remote_dir, local_file, item)
                item.update({"status": "confirmed", "remote_file_id": getattr(result, "fileid", None)})
                self._update_manifest_item(task_key, item)
                confirmed += 1
            return True, confirmed, "success", "全部文件已由远端确认"
        except LocalUploadRequiredError as err:
            return False, confirmed, "non_instant", str(err)
        except Transient115Error as err:
            return False, confirmed, "transient", str(err)
        except PermanentUploadError as err:
            return False, confirmed, "permanent", str(err)
        except Exception as err:
            logger.error(f"秒传115 manifest 上传异常：{err}\n{traceback.format_exc()}")
            return False, confirmed, "transient", str(err)

    def _remote_matches(self, u115: U115Pan, fileitem, item: Dict[str, Any]) -> bool:
        """通过远端文件 ID 校验同名文件的大小和可用 SHA1。"""
        file_id = getattr(fileitem, "fileid", None)
        if not file_id:
            return False
        try:
            info = u115._request_api("GET", "/open/folder/get_info", "data", params={"file_id": int(file_id)})
        except Exception as err:
            raise Transient115Error(f"查询远端同名文件失败：file_id={file_id} - {err}") from err
        if not info:
            raise Transient115Error(f"查询远端同名文件无响应：file_id={file_id}")
        size_value = self._select_remote_size_value(info)
        if not self._file_size_matches(size_value, int(item.get("file_size", -2) or -2)):
            return False
        remote_sha1 = str(info.get("sha1") or info.get("file_sha1") or "").upper()
        local_sha1 = str(item.get("file_sha1") or "").upper()
        return not remote_sha1 or remote_sha1 == local_sha1

    @staticmethod
    def _select_remote_size_value(info: Dict[str, Any]) -> Any:
        """优先选择远端响应中的原始字节数字段。"""
        for key in ("file_size", "size_bytes", "size_byte", "fs", "size"):
            value = info.get(key)
            if value not in (None, ""):
                return value
        return None

    @staticmethod
    def _parse_file_size(value: Any) -> Optional[int]:
        """解析远端整数或带 B/KB/MB/GB/TB 单位的文件大小。"""
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        text = str(value).strip().replace(",", "")
        if re.fullmatch(r"[+-]?\d+", text):
            return int(text)
        match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*(B|KB|MB|GB|TB)", text, flags=re.IGNORECASE)
        if not match:
            return None
        multiplier = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}[match.group(2).upper()]
        return int(float(match.group(1)) * multiplier)

    @staticmethod
    def _formatted_size_tolerance(value: Any) -> Optional[int]:
        """根据格式化大小的小数位计算四舍五入允许误差。"""
        text = str(value or "").strip().replace(",", "")
        match = re.fullmatch(r"([0-9]+)(?:\.([0-9]+))?\s*(B|KB|MB|GB|TB)", text, flags=re.IGNORECASE)
        if not match:
            return None
        decimals = len(match.group(2) or "")
        multiplier = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}[match.group(3).upper()]
        return max(1, int(0.5 * multiplier / (10 ** decimals)))

    @classmethod
    def _file_size_matches(cls, remote_value: Any, local_size: int) -> bool:
        """精确比较原始字节数，并对格式化大小使用显示精度容差。"""
        parsed = cls._parse_file_size(remote_value)
        if parsed is None:
            raise Transient115Error(f"无法解析远端文件大小：{remote_value}")
        tolerance = cls._formatted_size_tolerance(remote_value)
        if tolerance is None:
            return parsed == int(local_size)
        return abs(parsed - int(local_size)) <= tolerance

    def _build_instant_cache(self, local_path: Path) -> Tuple[bool, int, str, Dict[str, Dict[str, Any]]]:
        """构建或复用路径内所有文件的 SHA1、preid 和 size 秒传缓存。"""
        try:
            files = [local_path] if local_path.is_file() else [Path(root) / name for root, _, names in os.walk(local_path) for name in names]
            if not files:
                return False, 0, "没有可上传文件", {}
            self._cleanup_old_instant_cache()
            persisted_cache = self._load_instant_cache()
            instant_cache: Dict[str, Dict[str, Any]] = {}
            changed = False
            reused = 0
            calculated = 0
            logger.info(f"秒传115开始构建秒传缓存：{local_path}，文件数：{len(files)}")
            u115 = U115Pan()
            for index, file_path in enumerate(files, 1):
                file_key = file_path.as_posix()
                stat = file_path.stat()
                file_size = int(stat.st_size)
                file_mtime = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1000000000)))
                cached = persisted_cache.get(file_key) if isinstance(persisted_cache.get(file_key), dict) else None
                if cached and int(cached.get("file_size", -1) or -1) == file_size and int(cached.get("mtime", -1) or -1) == file_mtime and cached.get("file_sha1") and cached.get("file_preid"):
                    logger.debug(f"秒传115复用文件特征缓存 {index}/{len(files)}：{file_path}")
                    item = dict(cached)
                    item.update({"file_name": file_path.name, "file_size": file_size, "mtime": file_mtime, "time": int(time.time())})
                    persisted_cache[file_key] = item
                    instant_cache[file_key] = item
                    reused += 1
                    changed = True
                    continue
                logger.info(f"秒传115计算文件特征 {index}/{len(files)}：{file_path}")
                file_sha1 = u115._calc_sha1(file_path)
                file_preid = u115._calc_sha1(file_path, 128 * 1024 * 1024)
                item = {
                    "file_name": file_path.name,
                    "file_size": file_size,
                    "mtime": file_mtime,
                    "file_sha1": file_sha1,
                    "file_preid": file_preid,
                    "time": int(time.time()),
                }
                instant_cache[file_key] = item
                persisted_cache[file_key] = item
                calculated += 1
                changed = True
            if changed:
                self.save_data(self._instant_cache_key, persisted_cache)
            logger.info(f"秒传115文件特征缓存完成：{local_path}，文件数：{len(files)}，复用：{reused}，新算：{calculated}")
            return True, len(files), f"文件特征缓存完成，复用 {reused} 个，新算 {calculated} 个", instant_cache
        except Exception as err:
            logger.error(f"秒传115构建秒传缓存异常：{err}\n{traceback.format_exc()}")
            return False, 0, str(err), {}

    def _load_instant_cache(self) -> Dict[str, Dict[str, Any]]:
        """读取持久化秒传文件特征缓存。"""
        data = self.get_data(self._instant_cache_key)
        return data if isinstance(data, dict) else {}

    def _cleanup_old_instant_cache(self) -> None:
        """按记录保留天数清理过期秒传文件特征缓存。"""
        cache = self._load_instant_cache()
        if not cache:
            return
        expire_before = int(time.time()) - self._record_keep_days * 86400
        new_cache = {}
        for key, item in cache.items():
            if not isinstance(item, dict):
                continue
            if int(item.get("time", 0) or 0) < expire_before:
                continue
            if not Path(key).exists():
                continue
            new_cache[key] = item
        if len(new_cache) != len(cache):
            self.save_data(self._instant_cache_key, new_cache)
            logger.info(f"秒传115已清理过期文件特征缓存：{len(cache) - len(new_cache)} 条")

    def _clear_instant_cache_for_path(self, local_path: Path) -> None:
        """秒传成功后删除当前任务涉及文件的特征缓存。"""
        cache = self._load_instant_cache()
        if not cache:
            return
        files = [local_path] if local_path.is_file() else [Path(root) / name for root, _, names in os.walk(local_path) for name in names]
        keys = {file_path.as_posix() for file_path in files}
        removed = 0
        for key in list(keys):
            if key in cache:
                cache.pop(key, None)
                removed += 1
        if removed:
            self.save_data(self._instant_cache_key, cache)
            logger.info(f"秒传115已删除秒传成功任务的文件特征缓存：{removed} 条")

    @staticmethod
    def _is_remote_dir_empty(u115: U115Pan, target_dir) -> bool:
        """判断 115 目录是否为空。"""
        try:
            return len(u115.list(target_dir) or []) == 0
        except Exception as err:
            logger.warning(f"秒传115检查 115 目录是否为空失败：{target_dir.path} - {err}")
            return False

    def _cleanup_created_empty_dir(self, u115: U115Pan, target_dir, created_by_plugin: bool) -> None:
        """清理本轮插件创建且仍为空的 115 目标目录。"""
        if not created_by_plugin or not target_dir:
            return
        try:
            if self._is_remote_dir_empty(u115, target_dir):
                if u115.delete(target_dir):
                    logger.info(f"秒传115已清理本轮创建的空目录：{target_dir.path}")
                else:
                    logger.warning(f"秒传115清理空目录失败：{target_dir.path}")
            else:
                logger.info(f"秒传115目标目录非空，不执行清理：{target_dir.path}")
        except Exception as err:
            logger.warning(f"秒传115清理空目录异常：{target_dir.path} - {err}")

    def _handle_115_sign_check(self, u115: U115Pan, local_path: Path, init_data: Dict[str, Any], init_result: Dict[str, Any]) -> Dict[str, Any]:
        """处理 115 上传初始化的二次认证。"""
        sign_check = init_result.get("sign_check")
        if init_result.get("code") not in [700, 701] or not sign_check:
            return init_result
        pick_code = init_result.get("pick_code")
        sign_key = init_result.get("sign_key")
        start, end = [int(v) for v in sign_check.split("-")[:2]]
        from cryptography.hazmat.primitives import hashes
        with open(local_path, "rb") as fileobj:
            fileobj.seek(start)
            chunk = fileobj.read(end - start + 1)
            sha1 = hashes.Hash(hashes.SHA1())
            sha1.update(chunk)
            sign_val = sha1.finalize().hex().upper()
        retry_data = copy.deepcopy(init_data)
        retry_data.update({"pick_code": pick_code, "sign_key": sign_key, "sign_val": sign_val})
        init_resp = u115._request_api("POST", "/open/upload/init", data=retry_data)
        if not init_resp or not init_resp.get("state"):
            logger.warning(f"【115】预检二次认证失败：{local_path.name} - {init_resp}")
            return {}
        return init_resp.get("data") or {}

    def _upload_file_instant_only(self, u115: U115Pan, target_dir, local_path: Path, meta: Dict[str, Any]):
        """执行仅秒传上传，并以 115 返回文件 ID 确认远端文件。"""
        target_name = str(meta.get("upload_name") or local_path.name)
        file_size = int(meta["file_size"])
        init_data = {
            "file_name": target_name, "file_size": file_size, "target": f"U_1_{target_dir.fileid}",
            "fileid": str(meta["file_sha1"]), "preid": str(meta["file_preid"]),
        }
        try:
            init_resp = u115._request_api("POST", "/open/upload/init", data=init_data)
        except Exception as err:
            raise Transient115Error(f"初始化请求异常：{target_name} - {err}") from err
        if not init_resp:
            raise Transient115Error(f"初始化请求无响应：{target_name}")
        if not init_resp.get("state"):
            error = init_resp.get("error") or init_resp.get("message") or init_resp.get("msg") or "未知错误"
            raise Transient115Error(f"初始化失败：{target_name} - {error}")
        init_result = self._handle_115_sign_check(u115, local_path, init_data, init_resp.get("data") or {})
        if not init_result:
            raise Transient115Error(f"二次认证失败：{target_name}")
        if init_result.get("status") != 2:
            code = init_result.get("code")
            message = init_result.get("message") or init_result.get("msg") or init_result.get("tip") or "需要本地分片上传"
            raise LocalUploadRequiredError(f"{target_name}（status={init_result.get('status')}，code={code}，{message}）")
        file_id = init_result.get("file_id")
        if not file_id:
            raise Transient115Error(f"秒传响应缺少 file_id：{target_name}")
        info_resp = None
        try:
            info_resp = u115._request_api("GET", "/open/folder/get_info", "data", params={"file_id": int(file_id)})
        except Exception as err:
            raise Transient115Error(f"远端确认请求异常：{target_name} - {err}") from err
        if not info_resp:
            raise Transient115Error(f"远端 ID 暂未确认：{target_name}，file_id={file_id}")
        remote_size_value = self._select_remote_size_value(info_resp)
        if not self._file_size_matches(remote_size_value, file_size):
            raise PermanentUploadError(f"远端确认大小不一致：{target_name}，本地={file_size}，远端={remote_size_value}")
        remote_size = self._parse_file_size(remote_size_value)
        from app import schemas
        confirmed_item = schemas.FileItem(
            storage=u115.schema.value, fileid=str(info_resp["file_id"]),
            path=(Path(target_dir.path) / info_resp["file_name"]).as_posix(), type="file",
            name=info_resp["file_name"], basename=Path(info_resp["file_name"]).stem,
            extension=Path(info_resp["file_name"]).suffix[1:], pickcode=info_resp.get("pick_code"),
            size=remote_size, modify_time=info_resp.get("utime"),
        )
        logger.info(f"【115】{local_path.name} 秒传成功并通过远端 ID 确认，115 文件名：{target_name}，file_id={file_id}")
        return confirmed_item

    def _resolve_upload_file_name(self, local_path: Path, original_name: str) -> str:
        """解析提交给 115 上传初始化接口的文件名。"""
        if not self._smart_rename:
            return original_name
        try:
            context = MediaChain().recognize_by_path(local_path.as_posix())
            meta = getattr(context, "meta_info", None) if context else None
            mediainfo = getattr(context, "media_info", None) if context else None
            if not meta or not mediainfo:
                raise ValueError("未识别到完整媒体信息")
            recommend_name = FileManagerModule.recommend_name(meta=meta, mediainfo=mediainfo)
            if not recommend_name:
                raise ValueError("MoviePilot 未生成推荐文件名")
            upload_name = Path(str(recommend_name)).name
            if not upload_name:
                raise ValueError(f"推荐文件名无效：{recommend_name}")
            if not Path(upload_name).suffix and local_path.suffix:
                upload_name = f"{upload_name}{local_path.suffix}"
            logger.info(f"秒传115智能重命名：{local_path.name} -> {upload_name}")
            return upload_name
        except Exception as err:
            logger.warning(f"秒传115智能重命名失败：{local_path} - {err}")
            if self._smart_rename_fallback:
                logger.info(f"秒传115智能重命名失败，按配置回退原文件名：{original_name}")
                return original_name
            raise

    @staticmethod
    def _is_completed(torrent) -> bool:
        """判断 qBittorrent 任务是否已完成。"""
        progress = float(getattr(torrent, "progress", 0) or 0)
        state = str(getattr(torrent, "state", "") or "").lower()
        return progress >= 1 or state in {"completed", "uploading", "stalledup", "pausedup", "queuedup", "forcedup"}

    def _is_skip_tagged(self, torrent) -> bool:
        """判断任务是否包含跳过标签。"""
        tags = {tag.strip() for tag in str(getattr(torrent, "tags", "") or "").split(",") if tag.strip()}
        skip_tags = {tag.strip() for tag in self._skip_tags.split(",") if tag.strip()}
        return bool(tags & skip_tags)

    @staticmethod
    def _get_qb_service():
        """获取 qBittorrent 下载器服务。"""
        services = DownloaderHelper().get_services()
        for _, service in (services or {}).items():
            if DownloaderHelper().is_downloader(service_type="qbittorrent", service=service):
                return service
        return None

    @staticmethod
    def _get_115() -> Optional[U115Pan]:
        """获取 115 存储实例。"""
        try:
            u115 = U115Pan()
            if u115.check():
                return u115
        except Exception as err:
            logger.error(f"秒传115初始化 115 失败：{err}")
        return None

    def _tag_uploaded_torrent(self, torrent) -> None:
        """上传成功后给 qBittorrent 种子添加标签。"""
        if not self._uploaded_tag:
            return
        try:
            torrent.add_tags(self._uploaded_tag)
            logger.info(f"秒传115已添加 qB 标签 {self._uploaded_tag}：{torrent.name}")
        except Exception as err:
            logger.warning(f"秒传115添加 qB 标签失败：{torrent.name} - {err}")

    def _load_records(self) -> dict:
        """读取持久化处理记录。"""
        data = self.get_data(self._record_key)
        return data if isinstance(data, dict) else {}

    def _write_record(self, key: str, name: str, status: str, **kwargs) -> None:
        """写入单条处理记录。"""
        records = self._load_records()
        old = records.get(key) if isinstance(records.get(key), dict) else {}
        item = {**old, "hash": key, "name": name, "status": status, "time": int(time.time()), **kwargs}
        records[key] = item
        self.save_data(self._record_key, records)
        logger.info(f"秒传115记录：{name} -> {self._status_text(item)}")

    def _cleanup_old_records(self) -> None:
        """清理过期记录。"""
        records = self._load_records()
        expire_before = int(time.time()) - self._record_keep_days * 86400
        new_records = {k: v for k, v in records.items() if isinstance(v, dict) and int(v.get("time", 0) or 0) >= expire_before}
        if len(new_records) != len(records):
            self.save_data(self._record_key, new_records)

    def _current_config(self) -> Dict[str, Any]:
        """返回当前配置。"""
        return {"enabled": self._enabled, "onlyonce": False, "notify": self._notify, "target_path": self._target_path, "skip_tags": self._skip_tags, "uploaded_tag": self._uploaded_tag, "tag_path_mappings": self._tag_path_mappings, "cooldown_minutes": self._cooldown_minutes, "cron": self._cron, "max_retry": self._max_retry, "max_tasks_per_scan": self._max_tasks_per_scan, "record_keep_days": self._record_keep_days, "running_lock_timeout_minutes": self._running_lock_timeout_minutes, "smart_rename": self._smart_rename, "smart_rename_fallback": self._smart_rename_fallback, "clear_records": False}

    def _save_config(self) -> None:
        """保存当前配置。"""
        self.update_config(self._current_config())

    def _start_once_scheduler(self) -> None:
        """启动一次性后台执行调度器。"""
        self._scheduler = BackgroundScheduler(timezone=settings.TZ)
        self._scheduler.add_job(func=self.scan_and_upload, trigger="date", run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=2), name="秒传115立即运行")
        self._scheduler.start()

    def _notify_msg(self, title: str, text: str) -> None:
        """发送插件通知。"""
        if self._notify:
            self.post_message(mtype=NotificationType.Plugin, title=title, text=text)

    @staticmethod
    def _safe_int(value: Any, default: int, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
        """安全解析整数配置并限制取值范围。"""
        try:
            result = int(str(value).strip()) if value not in (None, "") else int(default)
        except (TypeError, ValueError):
            logger.warning(f"秒传115忽略无效整数配置：{value}，使用默认值 {default}")
            result = int(default)
        if minimum is not None:
            result = max(minimum, result)
        if maximum is not None:
            result = min(maximum, result)
        return result

    @staticmethod
    def _safe_name(name: str) -> str:
        """生成适用于 115 目录的安全名称。"""
        value = re.sub(r'[^\w\-.\u4e00-\u9fff\[\]()（）【】 ]+', "_", name or "unknown").strip().strip(".")
        return value[:180] or "unknown"

    @staticmethod
    def _format_time(timestamp: int) -> str:
        """格式化时间戳。"""
        if not timestamp:
            return "-"
        return datetime.fromtimestamp(timestamp, tz=pytz.timezone(settings.TZ)).strftime("%m-%d %H:%M:%S")

    def _status_text(self, item: Dict[str, Any]) -> str:
        """生成状态显示文本。"""
        status = item.get("status")
        mapping = {"checking": "校验中", "uploaded": "已完成", "uploading": "上传中", "cooldown": "冷却中", "failed": "失败"}
        text = mapping.get(status, str(status))
        reason = item.get("reason")
        return f"{text}：{reason}" if reason else text
