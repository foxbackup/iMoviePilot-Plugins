import os
import re
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import Event, eventmanager
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.modules.filemanager.storages.u115 import U115Pan
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType


class QbSeed115Uploader(_PluginBase):
    """qBittorrent做种上传115插件。"""

    plugin_name = "做种上传115"
    plugin_desc = "定时把qBittorrent内做种达到指定时长的种子上传到115，并清理达到保种天数的种子和本地文件。"
    plugin_icon = "upload_a.png"
    plugin_version = "1.7.0"
    plugin_author = "local"
    author_url = ""
    plugin_config_prefix = "qbseed115uploader_"
    plugin_order = 11
    auth_level = 1

    _enabled = False
    _onlyonce = False
    _notify = True
    _cron = "0 2 * * *"
    _upload_hours = 48
    _delete_days = 7
    _target_path = "/PT"
    _exclude_tags = ""
    _exclude_keywords = ""
    _keep_folder = True
    _uploaded_tag = "已上传115"
    _record_keep_days = 30
    _clear_records = False

    _record_key = "upload_records"
    _lock_key = "running_lock"
    _scheduler: Optional[BackgroundScheduler] = None
    _running = False

    def init_plugin(self, config: dict = None):
        """根据配置初始化插件。"""
        self.stop_service()
        if config:
            self._enabled = bool(config.get("enabled", False))
            self._onlyonce = bool(config.get("onlyonce", False))
            self._notify = bool(config.get("notify", True))
            self._cron = str(config.get("cron") or "0 2 * * *")
            self._upload_hours = int(config.get("upload_hours") or 48)
            self._delete_days = int(config.get("delete_days") or 7)
            self._target_path = str(config.get("target_path") or "/PT")
            self._exclude_tags = str(config.get("exclude_tags") or "")
            self._exclude_keywords = str(config.get("exclude_keywords") or "")
            self._keep_folder = bool(config.get("keep_folder", True))
            self._uploaded_tag = str(config.get("uploaded_tag") or "已上传115")
            self._record_keep_days = int(config.get("record_keep_days") or 30)
            self._clear_records = bool(config.get("clear_records", False))

        if self._clear_records:
            removed = len(self._load_records())
            self._save_records({})
            logger.info(f"已手动清空全部上传/删除记录 {removed} 条")
            self._clear_records = False
            self._save_config()

        if self._onlyonce:
            logger.info("做种上传115收到立即运行一次请求，2秒后开始执行")
            self._onlyonce = False
            self._save_config()
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.upload_and_cleanup,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=2),
                name="qB做种上传115立即运行",
            )
            self._scheduler.start()

    def get_state(self) -> bool:
        """返回插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """注册远程命令。"""
        return [
            {
                "cmd": "/qbseed115",
                "event": EventType.PluginAction,
                "desc": "执行qB做种上传115",
                "category": "115",
                "data": {"action": "qbseed115"},
            }
        ]

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event):
        """处理远程命令。"""
        if not event or not event.event_data:
            return
        if event.event_data.get("action") != "qbseed115":
            return
        self.upload_and_cleanup()

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件API。"""
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时服务。"""
        if not self._enabled or not self._cron:
            return []
        return [
            {
                "id": "QbSeed115Uploader",
                "name": "做种上传115",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.upload_and_cleanup,
                "kwargs": {},
            }
        ]

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        """返回配置表单。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [{"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "keep_folder", "label": "创建同名文件夹"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "clear_records", "label": "保存后清空记录"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "cron", "label": "定时周期（cron）", "placeholder": "0 2 * * *"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "target_path", "label": "115上传目录", "placeholder": "/PT"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "upload_hours", "label": "上传做种阈值（小时）", "type": "number", "placeholder": "48"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "delete_days", "label": "删除做种阈值（天）", "type": "number", "placeholder": "7"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "exclude_tags", "label": "排除标签（逗号分隔）", "placeholder": "MOVIEPILOT,已整理"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "exclude_keywords", "label": "排除关键词（逗号分隔）", "placeholder": "test,example"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VTextField", "props": {"model": "uploaded_tag", "label": "上传后添加标签", "placeholder": "已上传115"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VTextField", "props": {"model": "record_keep_days", "label": "记录保留天数", "type": "number", "placeholder": "30"}}]},
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "notify": True,
            "cron": "0 2 * * *",
            "upload_hours": 48,
            "delete_days": 7,
            "target_path": "/PT",
            "exclude_tags": "",
            "exclude_keywords": "",
            "keep_folder": True,
            "uploaded_tag": "已上传115",
            "record_keep_days": 30,
            "clear_records": False,
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回详情页面。"""
        records = self._load_records()
        upload_count = sum(1 for item in records.values() if isinstance(item, dict) and item.get("status") == "uploaded")
        delete_count = sum(1 for item in records.values() if isinstance(item, dict) and item.get("status") == "deleted")
        uploading_count = sum(1 for item in records.values() if isinstance(item, dict) and item.get("status") == "uploading")
        rows = []
        for item in sorted(records.values(), key=lambda x: int(x.get("time", 0)) if isinstance(x, dict) else 0, reverse=True)[:30]:
            title = item.get("name") or item.get("hash") or "未知种子"
            status = item.get("status") or "uploaded"
            status_text = "上传" if status == "uploaded" else "删除" if status == "deleted" else status
            dt = datetime.fromtimestamp(int(item.get("time", 0)), tz=pytz.timezone(settings.TZ)).strftime("%m-%d %H:%M")
            rows.append({
                "component": "VListItem",
                "props": {"title": title[:80], "subtitle": f"{dt} | {status_text}"},
            })
        return [
            {"component": "VAlert", "props": {"type": "info", "text": f"上传阈值：{self._upload_hours}小时；删除阈值：{self._delete_days}天；目标目录：{self._target_path}；上传后标签：{self._uploaded_tag}；记录保留：{self._record_keep_days}天；记录数：{len(records)}；上传：{upload_count}；删除：{delete_count}；上传中：{uploading_count}"}},
            {"component": "VCard", "props": {"class": "mt-3"}, "content": [
                {"component": "VCardTitle", "props": {"text": "最近上传/清理记录"}},
                {"component": "VDivider"},
                {"component": "VList", "content": rows or [{"component": "VListItem", "props": {"title": "暂无记录"}}]},
            ]},
        ]

    def stop_service(self):
        """停止插件后台调度器。"""
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                self._scheduler = None
        except Exception as err:
            logger.warning(f"停止做种上传115调度器失败：{err}")

    def upload_and_cleanup(self):
        """执行上传与清理流程，带内存锁和持久化锁防止并发重复上传。"""
        if self._running:
            logger.warning("做种上传115已有任务正在运行，本次触发跳过")
            return
        if not self._acquire_running_lock():
            logger.warning("做种上传115检测到持久化运行锁，本次触发跳过")
            return
        self._running = True
        try:
            self._upload_and_cleanup_inner()
        finally:
            self._running = False
            self.del_data(self._lock_key)

    def _acquire_running_lock(self) -> bool:
        """获取持久化运行锁，超过6小时的旧锁自动失效。"""
        now = int(time.time())
        lock = self.get_data(self._lock_key)
        if isinstance(lock, dict):
            lock_time = int(lock.get("time", 0) or 0)
            if lock_time and now - lock_time < 21600:
                return False
        self.save_data(self._lock_key, {"time": now})
        return True

    def _upload_and_cleanup_inner(self):
        """执行实际上传与清理流程。"""
        qb_service = self._get_qb_service()
        if not qb_service:
            self._notify_msg("做种上传115", "未找到可用的 qBittorrent 下载器")
            return
        downloader = qb_service.instance
        torrents, error = downloader.get_torrents()
        if error:
            self._notify_msg("做种上传115", f"获取种子失败：{error}")
            return
        torrents = torrents or []

        self._cleanup_old_records()
        upload_report = self._upload_due_torrents(torrents)
        cleanup_report = self._cleanup_due_torrents(downloader, torrents)

        text = self._build_report(upload_report, cleanup_report)
        logger.info(text)
        self._notify_msg("做种上传115执行完成", text)

    def _upload_due_torrents(self, torrents: list) -> dict:
        """上传达到做种阈值的种子。"""
        records = self._load_records()
        u115 = self._get_115()
        if not u115:
            return {"success": [], "failed": ["115未登录或初始化失败"], "skipped": []}
        target_root = u115.get_folder(Path(self._target_path))
        if not target_root:
            return {"success": [], "failed": [f"创建115目录失败：{self._target_path}"], "skipped": []}

        success, failed, skipped = [], [], []
        threshold = self._upload_hours * 3600
        due_count = sum(1 for t in torrents if (getattr(t, "seeding_time", 0) or 0) >= threshold)
        logger.info(f"做种超过上传阈值 {self._upload_hours} 小时的种子总数：{due_count}")
        for torrent in torrents:
            name = torrent.name or "未知"
            torrent_hash = torrent.hash
            if not torrent.seeding_time or torrent.seeding_time < threshold:
                continue
            if self._is_excluded(torrent):
                reason = "排除标签/关键词"
                logger.info(f"跳过上传：{name}，原因：{reason}")
                skipped.append(f"{name}（{reason}）")
                continue
            record = records.get(torrent_hash)
            if isinstance(record, dict) and record.get("status") in {"uploaded", "uploading"}:
                reason = "已上传" if record.get("status") == "uploaded" else "正在上传"
                logger.info(f"跳过上传：{name}，原因：{reason}")
                skipped.append(f"{name}（{reason}）")
                continue
            if torrent_hash in records and not isinstance(record, dict):
                reason = "旧格式记录已上传"
                logger.info(f"跳过上传：{name}，原因：{reason}")
                skipped.append(f"{name}（{reason}）")
                continue
            if torrent.state_enum.is_paused:
                reason = "种子已暂停"
                logger.info(f"跳过上传：{name}，原因：{reason}")
                skipped.append(f"{name}（{reason}）")
                continue
            local_path = Path(torrent.content_path or torrent.save_path or "")
            if not local_path.exists():
                reason = "本地文件不存在"
                logger.info(f"跳过上传：{name}，原因：{reason}，路径：{local_path}")
                failed.append(f"{name}（{reason}）")
                continue
            records[torrent_hash] = {
                "hash": torrent_hash,
                "name": name,
                "time": int(time.time()),
                "status": "uploading",
                "files": 0,
            }
            self._save_records(records)
            ok, file_count = self._upload_path(u115, target_root, local_path, name)
            if ok:
                records = self._load_records()
                records[torrent_hash] = {
                    "hash": torrent_hash,
                    "name": name,
                    "time": int(time.time()),
                    "status": "uploaded",
                    "files": file_count,
                }
                self._save_records(records)
                self._tag_uploaded_torrent(torrent)
                success.append(f"{name}（{file_count}个文件）")
            else:
                records = self._load_records()
                if isinstance(records.get(torrent_hash), dict) and records[torrent_hash].get("status") == "uploading":
                    records.pop(torrent_hash, None)
                    self._save_records(records)
                failed.append(name)
        return {"success": success, "failed": failed, "skipped": skipped}

    def _cleanup_due_torrents(self, downloader, torrents: list) -> dict:
        """删除达到保种天数的种子和本地文件，并写入删除记录。"""
        records = self._load_records()
        deleted, failed = [], []
        threshold = self._delete_days * 86400
        for torrent in torrents:
            name = torrent.name or "未知"
            record = records.get(torrent.hash)
            if not isinstance(record, dict) or record.get("status") != "uploaded":
                continue
            if not torrent.seeding_time or torrent.seeding_time < threshold:
                continue
            try:
                downloader.delete_torrents(delete_files=True, ids=torrent.hash)
                self._remove_local_residual(torrent.content_path)
                records[f"deleted:{torrent.hash}:{int(time.time())}"] = {
                    "hash": torrent.hash,
                    "name": name,
                    "time": int(time.time()),
                    "status": "deleted",
                }
                self._save_records(records)
                deleted.append(name)
            except Exception as err:
                logger.error(f"删除种子失败：{name} - {err}")
                failed.append(name)
        return {"deleted": deleted, "failed": failed}

    def _upload_path(self, u115: U115Pan, target_root, local_path: Path, torrent_name: str) -> Tuple[bool, int]:
        """上传文件或目录到115，目录上传时保留相对子目录结构。"""
        base_dir = target_root
        if self._keep_folder:
            base_dir = u115.get_folder(Path(self._target_path) / self._safe_name(torrent_name))
            if not base_dir:
                return False, 0
        if local_path.is_file():
            return (u115.upload(base_dir, local_path) is not None), 1

        file_count = 0
        failed = 0
        folder_cache = {".": base_dir}
        for root, _, files in os.walk(local_path):
            root_path = Path(root)
            relative_dir = root_path.relative_to(local_path)
            cache_key = relative_dir.as_posix()
            target_dir = folder_cache.get(cache_key)
            if target_dir is None:
                remote_dir = Path(base_dir.path) / relative_dir
                target_dir = u115.get_folder(remote_dir)
                folder_cache[cache_key] = target_dir
            if not target_dir:
                failed += len(files)
                continue
            for filename in files:
                file_path = root_path / filename
                if u115.upload(target_dir, file_path):
                    file_count += 1
                else:
                    failed += 1
        return failed == 0 and file_count > 0, file_count

    def _is_excluded(self, torrent) -> bool:
        """判断种子是否命中排除条件。"""
        name = (torrent.name or "").lower()
        tags = [tag.strip() for tag in (torrent.tags or "").split(",") if tag.strip()]
        exclude_tags = [tag.strip() for tag in self._exclude_tags.split(",") if tag.strip()]
        exclude_keywords = [kw.strip().lower() for kw in self._exclude_keywords.split(",") if kw.strip()]
        if any(tag in tags for tag in exclude_tags):
            return True
        return any(keyword in name for keyword in exclude_keywords)

    def _get_qb_service(self):
        """获取qBittorrent服务。"""
        services = DownloaderHelper().get_services()
        for _, service in (services or {}).items():
            if DownloaderHelper().is_downloader(service_type="qbittorrent", service=service):
                return service
        return None

    @staticmethod
    def _get_115() -> Optional[U115Pan]:
        """获取115存储实例。"""
        try:
            u115 = U115Pan()
            if u115.check():
                return u115
        except Exception as err:
            logger.error(f"初始化115失败：{err}")
        return None

    @staticmethod
    def _remove_local_residual(path: str):
        """清理可能残留的本地文件。"""
        if not path:
            return
        local_path = Path(path)
        if not local_path.exists():
            return
        try:
            if local_path.is_dir():
                shutil.rmtree(local_path)
            else:
                local_path.unlink()
        except Exception as err:
            logger.warning(f"清理残留文件失败：{local_path} - {err}")

    def _load_records(self) -> dict:
        """读取上传记录。"""
        data = self.get_data(self._record_key)
        return data if isinstance(data, dict) else {}

    def _save_records(self, records: dict):
        """保存上传记录。"""
        self.save_data(self._record_key, records)

    def _cleanup_old_records(self):
        """按记录保留天数清理过期上传/删除记录。"""
        records = self._load_records()
        if not records:
            return
        expire_before = int(time.time()) - self._record_keep_days * 86400
        new_records = {}
        removed = 0
        for key, item in records.items():
            record_time = int(item.get("time", 0)) if isinstance(item, dict) else 0
            if record_time and record_time < expire_before:
                removed += 1
                continue
            new_records[key] = item
        if removed:
            self._save_records(new_records)
            logger.info(f"已清理过期记录 {removed} 条，保留 {len(new_records)} 条")

    def _save_config(self):
        """保存当前配置。"""
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": False,
            "notify": self._notify,
            "cron": self._cron,
            "upload_hours": self._upload_hours,
            "delete_days": self._delete_days,
            "target_path": self._target_path,
            "exclude_tags": self._exclude_tags,
            "exclude_keywords": self._exclude_keywords,
            "keep_folder": self._keep_folder,
            "uploaded_tag": self._uploaded_tag,
            "record_keep_days": self._record_keep_days,
            "clear_records": False,
        })

    def _tag_uploaded_torrent(self, torrent) -> None:
        """上传成功后给 qB 种子添加标签。"""
        if not self._uploaded_tag:
            return
        try:
            torrent.add_tags(self._uploaded_tag)
            logger.info(f"已给种子添加标签 {self._uploaded_tag}: {torrent.name}")
        except Exception as err:
            logger.warning(f"给种子添加标签失败: {torrent.name} - {err}")

    def _build_report(self, upload_report: dict, cleanup_report: dict) -> str:
        """生成上传和清理明细通知文本。"""
        lines = [
            f"上传成功：{len(upload_report.get('success', []))} 个",
            *[f"  ✅ {item}" for item in upload_report.get("success", [])[:20]],
            f"上传失败：{len(upload_report.get('failed', []))} 个",
            *[f"  ❌ {item}" for item in upload_report.get("failed", [])[:10]],
            f"删除种子：{len(cleanup_report.get('deleted', []))} 个",
            *[f"  🗑️ {item}" for item in cleanup_report.get("deleted", [])[:20]],
        ]
        return "\n".join(lines)

    def _notify_msg(self, title: str, text: str):
        """发送通知消息。"""
        if self._notify:
            self.post_message(mtype=NotificationType.SiteMessage, title=title, text=text)

    @staticmethod
    def _safe_name(name: str) -> str:
        """生成安全的115目录名。"""
        value = re.sub(r'[<>:"/\\|?*]', "_", name or "unknown").strip()
        return value[:180] or "unknown"
