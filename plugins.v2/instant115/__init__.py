import os
import copy
import re
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.core.config import global_vars, settings
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.modules.filemanager.storages.u115 import U115Pan
from app.plugins import _PluginBase
from app.schemas.types import NotificationType


class LocalUploadRequiredError(Exception):
    """需要本地分片上传异常。"""


class Instant115(_PluginBase):
    """秒传115插件。"""

    plugin_name = "秒传115"
    plugin_desc = "监控 qBittorrent 完成任务，先全量筛选队列，只接受 115 秒传；检测到需要分片上传时自动跳过并冷却重试。"
    plugin_icon = "upload_a.png"
    plugin_version = "1.0.6"
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
    _cooldown_minutes = 30
    _scan_interval = 10
    _max_retry = 0
    _max_tasks_per_scan = 3
    _record_keep_days = 30
    _clear_records = False
    _running_lock_timeout_minutes = 180

    _record_key = "records"
    _runtime_key = "runtime"
    _queue_key = "queue"
    _scheduler: Optional[BackgroundScheduler] = None
    _running = False
    _running_lock = threading.Lock()
    _cancel_events: Dict[str, threading.Event] = {}

    def init_plugin(self, config: dict = None) -> None:
        """根据配置初始化插件运行状态。"""
        self.stop_service()
        self._enabled = False
        self._onlyonce = False
        self._notify = True
        self._target_path = "/PT"
        self._skip_tags = "已上传115"
        self._uploaded_tag = "已上传115"
        self._cooldown_minutes = 30
        self._scan_interval = 10
        self._max_retry = 0
        self._max_tasks_per_scan = 3
        self._record_keep_days = 30
        self._clear_records = False
        self._running_lock_timeout_minutes = 180
        if config:
            self._enabled = bool(config.get("enabled", False))
            self._onlyonce = bool(config.get("onlyonce", False))
            self._notify = bool(config.get("notify", True))
            self._target_path = str(config.get("target_path") or "/PT")
            self._skip_tags = str(config.get("skip_tags") or "")
            self._uploaded_tag = str(config.get("uploaded_tag") or "已上传115")
            self._cooldown_minutes = max(1, int(config.get("cooldown_minutes") or 30))
            self._scan_interval = max(1, int(config.get("scan_interval") or 10))
            self._max_retry = max(0, int(config.get("max_retry") or 0))
            self._max_tasks_per_scan = max(1, int(config.get("max_tasks_per_scan") or 3))
            self._record_keep_days = max(1, int(config.get("record_keep_days") or 30))
            self._running_lock_timeout_minutes = max(10, int(config.get("running_lock_timeout_minutes") or 180))
            self._clear_records = bool(config.get("clear_records", False))
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
                    {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}},
                    {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}},
                    {"component": "VSwitch", "props": {"model": "notify", "label": "上传成功后发送通知"}},
                    {"component": "VTextField", "props": {"model": "target_path", "label": "115目标上传目录", "placeholder": "/PT"}},
                    {"component": "VTextField", "props": {"model": "skip_tags", "label": "跳过的 qB 标签", "placeholder": "多个标签用英文逗号分隔，如：MOVIEPILOT,已上传115"}},
                    {"component": "VTextField", "props": {"model": "uploaded_tag", "label": "上传完成后添加 qB 标签", "placeholder": "已上传115"}},
                    {"component": "VTextField", "props": {"model": "cooldown_minutes", "label": "冷却重试时间 分钟", "type": "number", "hint": "检测到 115 进入分片上传时跳过并冷却。"}},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "scan_interval", "label": "扫描间隔 分钟", "type": "number"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "max_tasks_per_scan", "label": "每轮最多处理任务数", "type": "number"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "max_retry", "label": "最大重试次数", "type": "number", "hint": "0 表示不限次数。"}}]}
                    ]},
                    {"component": "VTextField", "props": {"model": "record_keep_days", "label": "记录保留天数", "type": "number"}},
                    {"component": "VTextField", "props": {"model": "running_lock_timeout_minutes", "label": "运行锁超时 分钟", "type": "number", "hint": "插件异常中断或热重载后，超过该时间的运行锁会自动释放。"}},
                    {"component": "VSwitch", "props": {"model": "clear_records", "label": "保存后清空历史记录"}},
                    {"component": "VAlert", "props": {"type": "warning", "variant": "tonal", "text": "本插件不再按上传流量阈值判断，而是在调用 115 OpenAPI 初始化上传后，只接受 status=2 秒传；如果需要进入 OSS 分片上传，立即判定为非秒传并进入冷却，不继续占用本地上传带宽。"}}
                ]
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
        return [{"id": "Instant115", "name": "秒传115扫描", "trigger": IntervalTrigger(minutes=self._scan_interval), "func": self.scan_and_upload, "kwargs": {}}]

    def stop_service(self) -> None:
        """停止插件后台调度器并取消插件内检测事件。"""
        for event in self._cancel_events.values():
            event.set()
        self._cancel_events.clear()
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
            logger.exception(f"秒传115扫描异常：{err}")
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
        logger.info(f"秒传115开始全量检查 qB 种子，共 {len(torrents)} 个")
        for torrent in torrents:
            allowed, reason = self._check_torrent_rule(torrent)
            name = getattr(torrent, "name", "") or getattr(torrent, "hash", "unknown")
            tags = getattr(torrent, "tags", "") or ""
            progress = float(getattr(torrent, "progress", 0) or 0)
            state = getattr(torrent, "state", "") or ""
            path = getattr(torrent, "content_path", "") or getattr(torrent, "save_path", "") or ""
            logger.info(f"秒传115检查种子：{name} | state={state} progress={progress:.4f} tags={tags} path={path} | 结果={'入队' if allowed else '剔除'} | 原因={reason}")
            if allowed:
                queue.append({"hash": getattr(torrent, "hash", ""), "name": name, "reason": reason, "path": path, "torrent": torrent, "time": int(time.time())})
        return queue

    def _save_queue(self, queue: List[Dict[str, Any]]) -> None:
        """保存可展示的待上传队列。"""
        view = [{k: v for k, v in item.items() if k != "torrent"} for item in queue]
        self.save_data(self._queue_key, view)

    def _check_torrent_rule(self, torrent) -> Tuple[bool, str]:
        """检查单个种子是否符合上传规则。"""
        if not self._is_completed(torrent):
            return False, "未完成"
        if self._is_skip_tagged(torrent):
            return False, "包含跳过标签"
        records = self._load_records()
        item = records.get(torrent.hash)
        now = int(time.time())
        if isinstance(item, dict):
            if item.get("status") == "uploaded":
                return False, "已上传记录"
            if item.get("status") == "cooldown" and int(item.get("cooldown_until", 0) or 0) > now:
                return False, f"冷却中至 {self._format_time(int(item.get('cooldown_until', 0) or 0))}"
        local_path = Path(getattr(torrent, "content_path", "") or getattr(torrent, "save_path", "") or "")
        if not local_path.exists():
            return False, f"本地路径不存在：{local_path}"
        return True, "符合规则"

    def _process_torrent(self, u115: U115Pan, torrent) -> None:
        """处理单个 qBittorrent 任务。"""
        torrent_hash = torrent.hash
        name = torrent.name or torrent_hash
        local_path = Path(torrent.content_path or torrent.save_path or "")
        if not local_path.exists():
            self._write_record(torrent_hash, name, "failed", reason=f"本地路径不存在：{local_path}")
            return
        logger.info(f"秒传115开始处理：{name}，先进行秒传预检，预检通过前不创建 115 目标文件夹")
        self._write_record(torrent_hash, name, "checking", reason="正在进行秒传预检")
        ok, file_count, reason = self._precheck_path_instant(u115, local_path)
        if not ok:
            if reason == "non_instant_upload_required":
                records = self._load_records()
                retry = int((records.get(torrent_hash) or {}).get("retry", 0) or 0) + 1
                if self._max_retry and retry > self._max_retry:
                    self._write_record(torrent_hash, name, "failed", retry=retry, reason="超过最大重试次数")
                    return
                cooldown_until = int(time.time()) + self._cooldown_minutes * 60
                self._write_record(torrent_hash, name, "cooldown", retry=retry, cooldown_until=cooldown_until, reason=f"检测到需要 115 分片上传，已跳过并冷却至 {self._format_time(cooldown_until)}")
                logger.warning(f"秒传115预检发现需要 115 分片上传，未创建目标文件夹并已冷却：{name}")
                return
            self._write_record(torrent_hash, name, "failed", reason=reason)
            return
        target_dir = u115.get_folder(Path(self._target_path) / self._safe_name(name))
        if not target_dir:
            self._write_record(torrent_hash, name, "failed", reason="秒传预检通过，但创建 115 同名目录失败")
            return
        logger.info(f"秒传115预检通过，开始创建目标目录并秒传：{name} -> {target_dir.path}")
        self._write_record(torrent_hash, name, "uploading", reason=f"秒传预检通过，准备写入 {file_count} 个文件")
        ok, file_count, reason = self._upload_path_with_guard(u115, target_dir, local_path, torrent_hash)
        if ok:
            self._write_record(torrent_hash, name, "uploaded", files=file_count, reason=reason)
            self._tag_uploaded_torrent(torrent)
            self._notify_msg("秒传115上传完成", f"{name}\n文件数：{file_count}\n结果：{reason}")
            return
        if reason == "non_instant_upload_required":
            records = self._load_records()
            retry = int((records.get(torrent_hash) or {}).get("retry", 0) or 0) + 1
            if self._max_retry and retry > self._max_retry:
                self._write_record(torrent_hash, name, "failed", retry=retry, reason="超过最大重试次数")
                return
            cooldown_until = int(time.time()) + self._cooldown_minutes * 60
            self._write_record(torrent_hash, name, "cooldown", retry=retry, cooldown_until=cooldown_until, reason=f"检测到需要 115 分片上传，已跳过并冷却至 {self._format_time(cooldown_until)}")
            logger.warning(f"秒传115检测到需要 115 分片上传，已跳过并冷却：{name}")
            return
        self._write_record(torrent_hash, name, "failed", reason=reason)


    def _precheck_path_instant(self, u115: U115Pan, local_path: Path) -> Tuple[bool, int, str]:
        """预检查路径内所有文件是否都支持 115 秒传。"""
        try:
            files = [local_path] if local_path.is_file() else [Path(root) / name for root, _, names in os.walk(local_path) for name in names]
            if not files:
                return False, 0, "没有可上传文件"
            logger.info(f"秒传115开始秒传预检：{local_path}，文件数：{len(files)}")
            for index, file_path in enumerate(files, 1):
                logger.info(f"秒传115预检文件 {index}/{len(files)}：{file_path}")
                if not self._check_file_instant_available(u115, file_path):
                    logger.warning(f"秒传115预检失败，文件需要 115 分片上传：{file_path}")
                    return False, 0, "non_instant_upload_required"
            logger.info(f"秒传115秒传预检通过：{local_path}，文件数：{len(files)}")
            return True, len(files), "秒传预检通过"
        except LocalUploadRequiredError:
            return False, 0, "non_instant_upload_required"
        except Exception as err:
            logger.exception(f"秒传115秒传预检异常：{err}")
            return False, 0, str(err)

    def _check_file_instant_available(self, u115: U115Pan, local_path: Path) -> bool:
        """检查单个文件是否可被 115 秒传，不创建最终目录。"""
        file_size = local_path.stat().st_size
        file_sha1 = u115._calc_sha1(local_path)
        file_preid = u115._calc_sha1(local_path, 128 * 1024 * 1024)
        target_cid = self._get_precheck_target_cid(u115)
        init_data = {"file_name": local_path.name, "file_size": file_size, "target": f"U_1_{target_cid}", "fileid": file_sha1, "preid": file_preid}
        init_resp = u115._request_api("POST", "/open/upload/init", data=init_data)
        if not init_resp or not init_resp.get("state"):
            logger.warning(f"【115】预检初始化上传失败：{local_path.name} - {init_resp}")
            return False
        init_result = init_resp.get("data") or {}
        init_result = self._handle_115_sign_check(u115, local_path, init_data, init_result)
        return bool(init_result and init_result.get("status") == 2)

    def _get_precheck_target_cid(self, u115: U115Pan) -> str:
        """获取 115 秒传预检使用的目标目录 CID。"""
        target_root = u115.get_item(Path(self._target_path)) or u115.get_folder(Path(self._target_path))
        if not target_root:
            raise RuntimeError(f"获取 115 预检目录失败：{self._target_path}")
        return target_root.fileid

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

    def _upload_path_with_guard(self, u115: U115Pan, target_dir, local_path: Path, task_key: str) -> Tuple[bool, int, str]:
        """上传路径并在进入分片上传前取消。"""
        del task_key
        try:
            ok, count = self._upload_path(u115, target_dir, local_path)
            return ok, count, "秒传成功" if ok else "上传失败"
        except LocalUploadRequiredError:
            return False, 0, "non_instant_upload_required"
        except Exception as err:
            logger.exception(f"秒传115上传异常：{err}")
            return False, 0, str(err)

    def _upload_path(self, u115: U115Pan, target_dir, local_path: Path) -> Tuple[bool, int]:
        """上传单个文件或目录。"""
        if local_path.is_file():
            return (self._upload_file_instant_only(u115, target_dir, local_path) is not None), 1
        file_count = 0
        failed = 0
        folder_cache = {".": target_dir}
        for root, _, files in os.walk(local_path):
            root_path = Path(root)
            relative_dir = root_path.relative_to(local_path)
            cache_key = relative_dir.as_posix()
            remote_dir = folder_cache.get(cache_key)
            if remote_dir is None:
                remote_dir = u115.get_folder(Path(target_dir.path) / relative_dir)
                folder_cache[cache_key] = remote_dir
            if not remote_dir:
                failed += len(files)
                continue
            for filename in files:
                file_path = root_path / filename
                if self._upload_file_instant_only(u115, remote_dir, file_path):
                    file_count += 1
                else:
                    failed += 1
        return failed == 0 and file_count > 0, file_count


    def _upload_file_instant_only(self, u115: U115Pan, target_dir, local_path: Path):
        """只执行 115 秒传，检测到需要 OSS 分片上传时抛出冷却异常。"""
        target_name = local_path.name
        target_path = Path(target_dir.path) / target_name
        file_size = local_path.stat().st_size
        file_sha1 = u115._calc_sha1(local_path)
        file_preid = u115._calc_sha1(local_path, 128 * 1024 * 1024)
        init_data = {
            "file_name": target_name,
            "file_size": file_size,
            "target": f"U_1_{target_dir.fileid}",
            "fileid": file_sha1,
            "preid": file_preid,
        }
        init_resp = u115._request_api("POST", "/open/upload/init", data=init_data)
        if not init_resp or not init_resp.get("state"):
            logger.warning(f"【115】初始化上传失败，跳过：{target_name} - {init_resp}")
            return None
        init_result = init_resp.get("data") or {}
        init_result = self._handle_115_sign_check(u115, local_path, init_data, init_result)
        if init_result.get("status") == 2:
            logger.info(f"【115】{target_name} 秒传成功")
            time.sleep(2)
            uploaded_item = u115.get_item(target_path)
            if uploaded_item:
                return uploaded_item
            return u115._U115Pan__build_uploaded_fileitem(target_path, local_path, file_size)
        logger.warning(f"秒传115检测到 {target_name} 需要进入 115 分片上传，立即跳过并冷却")
        raise LocalUploadRequiredError(target_name)

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

    @staticmethod
    def _process_sent_bytes() -> int:
        """统计当前进程及子进程累计发送字节。"""
        total = 0
        try:
            proc = psutil.Process(os.getpid())
            procs = [proc] + proc.children(recursive=True)
            for item in procs:
                try:
                    counters = item.net_io_counters()
                    total += int(getattr(counters, "bytes_sent", 0) or 0)
                except Exception:
                    continue
        except Exception:
            try:
                total = int(psutil.net_io_counters().bytes_sent)
            except Exception:
                total = 0
        return total

    @staticmethod
    def _cancel_upload(local_path: Path) -> None:
        """取消当前插件提交的 115 上传任务。"""
        for method_name in ("set_transfer_stop", "set_transfer_stopped"):
            method = getattr(global_vars, method_name, None)
            if callable(method):
                try:
                    method(local_path.as_posix())
                    return
                except Exception:
                    continue
        logger.warning("秒传115未找到可用的上传取消接口")

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
        return {"enabled": self._enabled, "onlyonce": False, "notify": self._notify, "target_path": self._target_path, "skip_tags": self._skip_tags, "uploaded_tag": self._uploaded_tag, "cooldown_minutes": self._cooldown_minutes, "scan_interval": self._scan_interval, "max_retry": self._max_retry, "max_tasks_per_scan": self._max_tasks_per_scan, "record_keep_days": self._record_keep_days, "running_lock_timeout_minutes": self._running_lock_timeout_minutes, "clear_records": False}

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
        mapping = {"uploaded": "已完成", "uploading": "上传中", "cooldown": "冷却中", "failed": "失败"}
        text = mapping.get(status, str(status))
        reason = item.get("reason")
        return f"{text}：{reason}" if reason else text
