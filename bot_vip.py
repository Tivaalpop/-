"""
Telegram media downloader for YouTube, TikTok, Instagram, X/Twitter,
Pinterest, and every other public source supported by yt-dlp.

Use only for videos you are allowed to download and share.
"""

from __future__ import annotations

import sys
import subprocess

import asyncio
import html
import json
# Ensure deno and node are in PATH for yt-dlp JS runtime
import os as _os
_existing_path = _os.environ.get("PATH", "")
import shutil as _shutil
_node_bin = _shutil.which("node") or ""
if _node_bin:
    _node_dir = _os.path.dirname(_node_bin)
    if _node_dir not in _existing_path:
        _os.environ["PATH"] = f"{_node_dir}:{_existing_path}"

import logging
import math
import os
import re
import requests
import secrets
import shutil
import sqlite3
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from typing import Any

import httpx
import yt_dlp
from pyrogram import Client
from pyrogram.types import (
    InlineKeyboardMarkup as PyroInlineKeyboardMarkup,
    InlineKeyboardButton as PyroInlineKeyboardButton,
)

# ---------------------------------------------------------------------------
# Runtime configuration: keep credentials outside the source code.
# Place config.json beside this file, or provide environment variables.
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"

def _load_runtime_config() -> dict:
    data = {}
    if CONFIG_PATH.is_file():
        try:
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"تعذر قراءة config.json: {exc}") from exc
    data["bot_token"] = os.environ.get("BOT_TOKEN", data.get("bot_token", "")).strip()
    data["api_id"] = os.environ.get("API_ID", data.get("api_id", 0))
    data["api_hash"] = os.environ.get("API_HASH", data.get("api_hash", "")).strip()
    data["owner_id"] = os.environ.get("OWNER_ID", data.get("owner_id", 0))
    try:
        data["api_id"] = int(data["api_id"] or 0)
        data["owner_id"] = int(data["owner_id"] or 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("api_id و owner_id يجب أن يكونا أرقامًا صحيحة") from exc
    if not data["bot_token"] or ":" not in data["bot_token"]:
        raise RuntimeError("أضف bot_token الصحيح داخل config.json أو متغير BOT_TOKEN")
    if data["owner_id"] <= 0:
        raise RuntimeError("أضف owner_id الصحيح داخل config.json أو متغير OWNER_ID")
    return data

CONFIG = _load_runtime_config()
BOT_TOKEN = CONFIG["bot_token"]
API_ID = CONFIG["api_id"]
API_HASH = CONFIG["api_hash"]
OWNER_ID = CONFIG["owner_id"]

# Pyrogram is optional: normal Bot API uploads remain available without API_ID/API_HASH.
pyro_client = None
if API_ID > 0 and API_HASH:
    try:
        pyro_client = Client(
            str(BASE_DIR / "bot_session"), api_id=API_ID,
            api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True,
        )
    except Exception as exc:
        logging.getLogger("telegram-downloader").warning("Pyrogram initialization skipped: %s", exc)

def _to_pyro_markup(markup: Any) -> "PyroInlineKeyboardMarkup | None":
    """Convert a python-telegram-bot InlineKeyboardMarkup to Pyrogram's format."""
    if markup is None:
        return None
    try:
        rows = []
        for row in markup.inline_keyboard:
            pyro_row = []
            for btn in row:
                pyro_row.append(
                    PyroInlineKeyboardButton(
                        text=btn.text,
                        callback_data=btn.callback_data or "",
                        url=getattr(btn, "url", None),
                    )
                )
            rows.append(pyro_row)
        return PyroInlineKeyboardMarkup(rows)
    except Exception:
        return None


async def upload_large_media(
    chat_id: int,
    media_path: Path,
    media_type: str,
    caption: str,
    reply_to_message_id: int | None = None,
    reply_markup: Any | None = None,
    duration: int | None = None,
    performer: str | None = None,
    title: str | None = None,
    thumb: Path | None = None,
) -> Any:
    """Upload large files using Pyrogram which supports up to 2GB (or 4GB with Premium)."""
    if pyro_client is None:
        return None
    pyro_markup = _to_pyro_markup(reply_markup)
    if not pyro_client.is_connected:
        try:
            await pyro_client.start()
        except Exception as e:
            logger.error("Pyrogram reconnect failed: %s", e)
            return None
    
    try:
        if media_type == "video":
            return await pyro_client.send_video(
                chat_id=chat_id,
                video=str(media_path),
                caption=caption,
                duration=duration or 0,
                thumb=str(thumb) if thumb and thumb.exists() else None,
                reply_to_message_id=reply_to_message_id,
                reply_markup=pyro_markup,
                supports_streaming=True
            )
        elif media_type == "audio":
            return await pyro_client.send_audio(
                chat_id=chat_id,
                audio=str(media_path),
                caption=caption,
                duration=duration or 0,
                performer=performer,
                title=title,
                thumb=str(thumb) if thumb and thumb.exists() else None,
                reply_to_message_id=reply_to_message_id,
                reply_markup=pyro_markup
            )
        else:
            return await pyro_client.send_document(
                chat_id=chat_id,
                document=str(media_path),
                caption=caption,
                thumb=str(thumb) if thumb and thumb.exists() else None,
                reply_to_message_id=reply_to_message_id,
                reply_markup=pyro_markup
            )
    except Exception as e:
        logging.getLogger("telegram-downloader").error(f"Pyrogram upload failed: {e}")
        return None

# ---------------------------------------------------------------------------
# Download counter — SQLite (thread-safe, persistent across restarts)
# ---------------------------------------------------------------------------
_DB_PATH = BASE_DIR / "downloads.db"

def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS downloads "
        "(user_id INTEGER PRIMARY KEY, count INTEGER NOT NULL DEFAULT 0)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users "
        "(user_id INTEGER PRIMARY KEY, first_name TEXT DEFAULT '', "
        "last_name TEXT DEFAULT '', username TEXT DEFAULT '', "
        "joined_at TEXT, is_banned INTEGER NOT NULL DEFAULT 0, "
        "is_stopped INTEGER NOT NULL DEFAULT 0)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS platform_cookies "
        "(platform TEXT PRIMARY KEY, cookie_path TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    conn.commit()
    return conn

_db = _db_connect()
_db_lock = asyncio.Lock()

async def increment_download(user_id: int) -> None:
    """Add 1 to the download count for this user."""
    async with _db_lock:
        _db.execute(
            "INSERT INTO downloads (user_id, count) VALUES (?, 1) "
            "ON CONFLICT(user_id) DO UPDATE SET count = count + 1",
            (user_id,),
        )
        _db.commit()

async def get_download_count(user_id: int) -> int:
    """Return total successful downloads for this user."""
    async with _db_lock:
        row = _db.execute(
            "SELECT count FROM downloads WHERE user_id = ?", (user_id,)
        ).fetchone()
    return row[0] if row else 0

# ---------------------------------------------------------------------------
# Owner & admin state
# ---------------------------------------------------------------------------

_admin_broadcast_pending:       set[int]        = set()
_admin_cookie_platform_pending: set[int]        = set()
_admin_cookie_content_pending:  dict[int, str]  = {}   # admin_id → platform
_admin_send_msg_pending:        dict[int, int]  = {}   # admin_id → target_id
_admin_cookie_selected_platform: dict[int, str] = {}   # admin_id → selected platform from button

PLATFORM_COOKIES: dict[str, Path] = {}
COOKIES_DIR = BASE_DIR / "cookies"

# ---------------------------------------------------------------------------
# Supported platforms for cookie upload (used in inline buttons)
# ---------------------------------------------------------------------------
SUPPORTED_PLATFORMS_BUTTONS = [
    ("YouTube", "yt"),
    ("TikTok", "tiktok"),
    ("Instagram", "instagram"),
    ("X / Twitter", "twitter"),
    ("Pinterest", "pinterest"),
    ("Facebook", "facebook"),
    ("Snapchat", "snapchat"),
    ("Threads", "threads"),
    ("Likee", "likee"),
    ("Reddit", "reddit"),
]

# ---------------------------------------------------------------------------
# DB helpers — users
# ---------------------------------------------------------------------------

async def db_register_user(user: Any) -> None:
    if user is None:
        return
    async with _db_lock:
        _db.execute(
            "INSERT INTO users (user_id,first_name,last_name,username,joined_at) "
            "VALUES (?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET "
            "first_name=excluded.first_name, last_name=excluded.last_name, "
            "username=excluded.username",
            (user.id, user.first_name or "", user.last_name or "",
             user.username or "", datetime.now().strftime("%Y-%m-%d %H:%M")),
        )
        _db.commit()

async def db_get_all_users() -> list[dict]:
    async with _db_lock:
        rows = _db.execute(
            "SELECT user_id,first_name,last_name,username,joined_at,is_banned,is_stopped "
            "FROM users ORDER BY rowid DESC"
        ).fetchall()
    return [{"id":r[0],"first":r[1],"last":r[2],"username":r[3],
             "joined":r[4],"banned":bool(r[5]),"stopped":bool(r[6])} for r in rows]

async def db_get_user(uid: int) -> dict | None:
    async with _db_lock:
        row = _db.execute(
            "SELECT user_id,first_name,last_name,username,joined_at,is_banned,is_stopped "
            "FROM users WHERE user_id=?", (uid,)
        ).fetchone()
    if not row:
        return None
    return {"id":row[0],"first":row[1],"last":row[2],"username":row[3],
            "joined":row[4],"banned":bool(row[5]),"stopped":bool(row[6])}

async def db_set_ban(uid: int, ban: bool) -> None:
    async with _db_lock:
        _db.execute("UPDATE users SET is_banned=? WHERE user_id=?", (int(ban), uid))
        _db.commit()

async def db_set_stop(uid: int, stop: bool) -> None:
    async with _db_lock:
        _db.execute("UPDATE users SET is_stopped=? WHERE user_id=?", (int(stop), uid))
        _db.commit()

async def db_is_banned(uid: int) -> bool:
    if uid == OWNER_ID:
        return False
    async with _db_lock:
        row = _db.execute("SELECT is_banned FROM users WHERE user_id=?", (uid,)).fetchone()
    return bool(row[0]) if row else False

async def db_is_stopped(uid: int) -> bool:
    if uid == OWNER_ID:
        return False
    async with _db_lock:
        row = _db.execute("SELECT is_stopped FROM users WHERE user_id=?", (uid,)).fetchone()
    return bool(row[0]) if row else False

# ---------------------------------------------------------------------------
# DB helpers — cookies
# ---------------------------------------------------------------------------

async def db_save_cookie(platform: str, content: str) -> Path:
    COOKIES_DIR.mkdir(exist_ok=True)
    safe = re.sub(r"[^\w\-]", "_", platform.lower())
    path = COOKIES_DIR / f"{safe}.txt"
    path.write_text(content, encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass
    async with _db_lock:
        _db.execute(
            "INSERT INTO platform_cookies (platform,cookie_path,updated_at) VALUES (?,?,?) "
            "ON CONFLICT(platform) DO UPDATE SET cookie_path=excluded.cookie_path,"
            "updated_at=excluded.updated_at",
            (platform, str(path), datetime.now().strftime("%Y-%m-%d %H:%M")),
        )
        _db.commit()
    PLATFORM_COOKIES[platform] = path
    return path

async def db_get_all_cookies() -> list[tuple]:
    async with _db_lock:
        return _db.execute(
            "SELECT platform,updated_at FROM platform_cookies ORDER BY platform"
        ).fetchall()

async def db_delete_cookie(platform: str) -> bool:
    """Delete a cookie entry from DB and remove the file."""
    async with _db_lock:
        row = _db.execute(
            "SELECT cookie_path FROM platform_cookies WHERE platform=?", (platform,)
        ).fetchone()
        if row:
            p = Path(row[0])
            if p.is_file():
                try:
                    p.unlink()
                except Exception:
                    pass
            _db.execute("DELETE FROM platform_cookies WHERE platform=?", (platform,))
            _db.commit()
            PLATFORM_COOKIES.pop(platform, None)
            return True
    return False

def _load_platform_cookies() -> None:
    rows = _db.execute("SELECT platform,cookie_path FROM platform_cookies").fetchall()
    for plat, pstr in rows:
        p = Path(pstr)
        if p.is_file():
            PLATFORM_COOKIES[plat] = p

# ---------------------------------------------------------------------------
# YouTube cookies — embedded directly so no external file is needed.
# Written to a temp file once at startup; the path is stored in
# YOUTUBE_COOKIES_FILE after _init_youtube_cookies() is called.
# ---------------------------------------------------------------------------
_YOUTUBE_COOKIES_CONTENT = """
Cooookiii
"""

def _init_youtube_cookies() -> Path:
    """Write the embedded YouTube cookies to a temp file and return its path."""
    fd, path = tempfile.mkstemp(suffix=".txt", prefix="yt_cookies_")
    try:
        os.write(fd, _YOUTUBE_COOKIES_CONTENT.encode())
    finally:
        os.close(fd)
    return Path(path)

# ---------------------------------------------------------------------------
# Pinterest cookies — embedded directly so no external file is needed.
# Written to a temp file once at startup; the path is stored in
# PINTEREST_COOKIES_FILE after _init_pinterest_cookies() is called.
# ---------------------------------------------------------------------------
_PINTEREST_COOKIES_CONTENT = """\
Cooookiii
"""

def _init_facebook_cookies() -> Path:
    """Write the embedded Facebook cookies to a temp file and return its path."""
    fd, path = tempfile.mkstemp(suffix=".txt", prefix="fb_cookies_")
    try:
        os.write(fd, _FACEBOOK_COOKIES_CONTENT.encode())
    finally:
        os.close(fd)
    return Path(path)


from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    Update,
)
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


MAX_FILE_BYTES = max(100, int(CONFIG.get("max_file_mb", 1500))) * 1024 * 1024
MAX_CONCURRENT_DOWNLOADS = max(1, min(int(CONFIG.get("max_concurrent_downloads", 2)), 3))
DOWNLOAD_TIMEOUT_SECONDS = 20 * 60
# Pinterest cookies are embedded in the source — no external file needed.
PINTEREST_COOKIES_FILE: Path = Path("cookies/pinterest.txt") if Path("cookies/pinterest.txt").is_file() else _init_pinterest_cookies()
# Facebook cookies are embedded in the source — no external file needed.
FACEBOOK_COOKIES_FILE: Path = Path("cookies/facebook.txt") if Path("cookies/facebook.txt").is_file() else _init_facebook_cookies()
# YouTube cookies are embedded in the source — no external file needed.
YOUTUBE_COOKIES_FILE: Path = Path("cookies/youtube.txt") if Path("cookies/youtube.txt").is_file() else _init_youtube_cookies()
_load_platform_cookies()   # load admin-uploaded cookies from DB

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("telegram-downloader")
logging.getLogger("httpx").setLevel(logging.WARNING)
download_slots = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

URL_PATTERN = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
AUDIO_BUTTON_LABEL = "تحـميـل المـلف الصـوتـي"
RENAME_BUTTON_LABEL = "إعـادة تسـميـة المـلف"
COVER_BUTTON_LABEL = "وضـع غـلاف"
DESC_BUTTON_LABEL = "وضـع وصـف للمـلف"

# session_key → {url, chat_id, message_id}
audio_sessions: dict[str, dict] = {}
# (chat_id, user_id) → url  — waiting for the user to type a new name
rename_pending: dict[tuple[int, int], str] = {}
# (chat_id, user_id) → session_key — waiting for user to send a photo
cover_pending: dict[tuple[int, int], str] = {}
# (chat_id, user_id) → session_key — waiting for user to type a description
desc_pending: dict[tuple[int, int], str] = {}

# Keep the old audio_url_store for video→audio button (unchanged behaviour)
audio_url_store: dict[str, str] = {}

PLATFORM_HOSTS = {
    "youtube": {"youtube.com", "youtu.be", "youtube-nocookie.com"},
    "tiktok": {"tiktok.com"},
    "instagram": {"instagram.com"},
    "twitter": {"twitter.com", "x.com"},
    "pinterest": {"pinterest.com", "pin.it"},
    "facebook": {"facebook.com", "fb.com", "fb.watch", "m.facebook.com"},
}

MAX_PLAYLIST_VIDEOS = 0   # 0 = no cap — download the full playlist
PLAYLIST_WORKERS = max(1, min(int(CONFIG.get("playlist_workers", 1)), 2))  # Android-safe


class FileTooLargeError(RuntimeError):
    """Raised when a media file cannot fit Telegram's cloud upload limit."""


def extract_media_url(text: str) -> str | None:
    """Return the first HTTP(S) URL; yt-dlp determines whether it is supported."""
    for raw_url in URL_PATTERN.findall(text):
        candidate = raw_url.rstrip(".,!?؛،)]}")
        parsed = urlparse(candidate)
        if parsed.scheme in {"http", "https"} and parsed.hostname and not parsed.username and not parsed.password:
            return candidate
    return None


def platform_name(url: str) -> str:
    """Return a friendly platform name without restricting yt-dlp sources."""
    hostname = (urlparse(url).hostname or "").lower()
    for name, domains in PLATFORM_HOSTS.items():
        if any(hostname == domain or hostname.endswith(f".{domain}") for domain in domains):
            if name == "twitter":
                return "X / Twitter"
            return name.capitalize()
    return "المصدر"


def is_youtube_playlist(url: str) -> bool:
    """Return True when the URL points to a YouTube playlist (has list= param)."""
    from urllib.parse import parse_qs
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    is_yt = any(hostname == d or hostname.endswith(f".{d}") for d in PLATFORM_HOSTS["youtube"])
    if not is_yt:
        return False
    qs = parse_qs(parsed.query)
    return bool(qs.get("list")) and not qs.get("v") == []  # list= must be present


def extract_playlist_entries(url: str) -> list[dict]:
    """Return ALL entry dicts from a playlist (no size cap)."""
    options: dict = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": "in_playlist",
        "noplaylist": False,
        "socket_timeout": 20,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 Chrome/131 Safari/537.36"
            )
        },
    }
    with yt_dlp.YoutubeDL(options) as ydl:
        info = ydl.extract_info(url, download=False)
        if not isinstance(info, dict):
            return []
        entries = info.get("entries") or []
        result = []
        for e in entries:
            if not e:
                continue
            vid_id = e.get("id")
            entry_url = e.get("url") or e.get("webpage_url")
            if not entry_url and vid_id:
                entry_url = f"https://www.youtube.com/watch?v={vid_id}"
            if entry_url:
                result.append({"url": entry_url, "title": e.get("title") or vid_id or ""})
        return result


def format_count(value: Any) -> str:
    if value is None:
        return "غير متاح"
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def format_duration(value: Any) -> str:
    if value is None:
        return "غير متاح"
    try:
        seconds = int(float(value))
    except (TypeError, ValueError):
        return str(value)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return (
        f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        if hours
        else f"{minutes:02d}:{seconds:02d}"
    )


def format_bytes(value: Any) -> str:
    if value is None:
        return "غير متاح"
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return str(value)
    units = ("B", "KB", "MB", "GB", "TB")
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return "غير متاح"


def format_date(value: Any) -> str:
    if not value:
        return "غير متاح"
    value = str(value)
    try:
        return datetime.strptime(value, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return value


def source_metadata(info: dict[str, Any], media_path: Path) -> dict[str, str]:
    """Create safe, human-readable metadata for the Telegram caption."""
    width = info.get("width")
    height = info.get("height")
    resolution = (
        f"{width}x{height}" if width and height else info.get("resolution")
    )
    uploader = (
        info.get("uploader")
        or info.get("channel")
        or info.get("creator")
        or "غير متاح"
    )
    platform = (
        info.get("extractor_key")
        or info.get("extractor")
        or urlparse(str(info.get("webpage_url") or "")).netloc
        or "غير معروف"
    )
    return {
        "title": str(info.get("title") or media_path.stem),
        "uploader": str(uploader),
        "uploader_id": str(info.get("uploader_id") or info.get("channel_id") or "غير متاح"),
        "platform": str(platform),
        "duration": format_duration(info.get("duration")),
        "duration_seconds": str(int(float(info.get("duration") or 0))),
        "resolution": str(resolution or "غير متاح"),
        "format": str(info.get("ext") or media_path.suffix.lstrip(".") or "غير متاح"),
        "format_note": str(info.get("format_note") or "غير متاح"),
        "video_codec": str(info.get("vcodec") or "غير متاح"),
        "audio_codec": str(info.get("acodec") or "غير متاح"),
        "size": format_bytes(media_path.stat().st_size),
        "published": format_date(info.get("upload_date")),
        "views": format_count(info.get("view_count")),
        "likes": format_count(info.get("like_count")),
        "comments": format_count(info.get("comment_count")),
        "id": str(info.get("id") or "غير متاح"),
        "url": str(info.get("webpage_url") or ""),
    }


def safe_html(value: Any, limit: int = 180) -> str:
    """Escape source metadata before putting it in an HTML Telegram caption."""
    text = str(value or "غير متاح").strip()
    return html.escape(text[:limit], quote=False)


def build_caption(metadata: dict[str, str]) -> str:
    """Compact caption attached to the video file (title + uploader + platform)."""
    title = safe_html(metadata["title"], 200)
    uploader = safe_html(metadata["uploader"], 60)
    platform = safe_html(metadata["platform"], 40)
    return f"<b>{title}</b>\n<i>{uploader}</i>  ·  <code>{platform}</code>"


def build_audio_caption(metadata: dict[str, str]) -> str:
    """Compact caption attached to the audio file (title + uploader + platform)."""
    title = safe_html(metadata["title"], 200)
    uploader = safe_html(metadata["uploader"], 60)
    platform = safe_html(metadata["platform"], 40)
    return f"<b>{title}</b>\n<i>{uploader}</i>  ·  <code>{platform}</code>"


def build_rich_details_html(metadata: dict[str, str], mode: str) -> str:
    """Build sendRichMessage HTML with <details>/<summary> collapsible sections."""
    title = safe_html(metadata["title"], 120)
    uploader = safe_html(metadata["uploader"], 60)
    platform = safe_html(metadata["platform"], 40)

    if mode == "audio":
        tech_rows = (
            f"<p>"
            f"<b>المدة</b>       <code>{safe_html(metadata['duration'], 20)}</code>\n"
            f"<b>الجودة</b>      <code>{safe_html(metadata['format_note'], 40)}</code>\n"
            f"<b>الترميز</b>     <code>{safe_html(metadata['audio_codec'], 30)}</code>\n"
            f"<b>الحجم</b>       <code>{safe_html(metadata['size'], 20)}</code>\n"
            f"<b>النشر</b>       <code>{safe_html(metadata['published'], 20)}</code>"
            f"</p>"
        )
    else:
        tech_rows = (
            f"<p>"
            f"<b>المدة</b>       <code>{safe_html(metadata['duration'], 20)}</code>\n"
            f"<b>الدقة</b>       <code>{safe_html(metadata['resolution'], 20)}</code>\n"
            f"<b>الصيغة</b>      <code>{safe_html(metadata['format'], 10)} — {safe_html(metadata['format_note'], 30)}</code>\n"
            f"<b>الفيديو</b>     <code>{safe_html(metadata['video_codec'], 30)}</code>\n"
            f"<b>الصوت</b>       <code>{safe_html(metadata['audio_codec'], 30)}</code>\n"
            f"<b>الحجم</b>       <code>{safe_html(metadata['size'], 20)}</code>\n"
            f"<b>النشر</b>       <code>{safe_html(metadata['published'], 20)}</code>"
            f"</p>"
        )

    stats_rows = (
        f"<p>"
        f"<b>{safe_html(metadata['views'], 15)}</b>   "
        f"<b>{safe_html(metadata['likes'], 15)}</b>   "
        f"<b>{safe_html(metadata['comments'], 15)}</b>"
        f"</p>"
    )

    return (
        f"<h4>{title}</h4>"
        f"<p><i>{uploader}</i>  ·  <code>{platform}</code></p>"
        f"<details>"
        f"<summary>التفاصيل التقنية</summary>"
        f"{tech_rows}"
        f"</details>"
        f"<details>"
        f"<summary>إحصائيات التفاعل</summary>"
        f"{stats_rows}"
        f"</details>"
    )


async def send_rich_message_raw(
    bot_token: str,
    chat_id: int,
    html: str,
    reply_to_message_id: int | None = None,
) -> int | None:
    """Send a Rich Message (Bot API 9+) and return its message_id.
    Falls back to sendMessage if sendRichMessage is not supported."""
    payload: dict = {
        "chat_id": chat_id,
        "rich_message": {"html": html},
    }
    if reply_to_message_id is not None:
        payload["reply_parameters"] = {"message_id": reply_to_message_id}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"https://api.telegram.org/bot{bot_token}/sendRichMessage",
                json=payload,
            )
            if resp.is_success and resp.json().get("ok"):
                return resp.json().get("result", {}).get("message_id")
            logger.warning("sendRichMessage failed (%s): %s", resp.status_code, resp.text[:300])
    except Exception as exc:
        logger.warning("sendRichMessage error: %s", exc)

    # Fallback: sendMessage with plain text so the loading indicator still appears
    try:
        fallback_payload: dict = {
            "chat_id": chat_id,
            "text": "⏳ جاري التحميل...",
            "parse_mode": "HTML",
        }
        if reply_to_message_id is not None:
            fallback_payload["reply_parameters"] = {"message_id": reply_to_message_id}
        async with httpx.AsyncClient(timeout=30) as client:
            resp2 = await client.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json=fallback_payload,
            )
            if resp2.is_success and resp2.json().get("ok"):
                return resp2.json().get("result", {}).get("message_id")
    except Exception as exc2:
        logger.warning("sendMessage fallback error: %s", exc2)
    return None


async def delete_message_raw(bot_token: str, chat_id: int, message_id: int) -> None:
    """Delete a Telegram message silently."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"https://api.telegram.org/bot{bot_token}/deleteMessage",
                json={"chat_id": chat_id, "message_id": message_id},
            )
    except Exception as exc:
        logger.warning("deleteMessage error: %s", exc)


async def send_rich_details(
    bot_token: str,
    chat_id: int,
    reply_to_message_id: int,
    metadata: dict[str, str],
    mode: str,
) -> None:
    """Send collapsible metadata as a Rich Message reply (Bot API 10.1+)."""
    rich_html = build_rich_details_html(metadata, mode)
    await send_rich_message_raw(bot_token, chat_id, rich_html, reply_to_message_id)


async def send_media_with_rich_caption(
    bot_token: str,
    chat_id: int,
    media_path: Path,
    media_type: str,
    rich_html: str,
    reply_to_message_id: int | None = None,
    reply_markup_obj: InlineKeyboardMarkup | None = None,
    supports_streaming: bool = False,
    audio_title: str | None = None,
    audio_performer: str | None = None,
    audio_duration: int | None = None,
    audio_filename: str | None = None,
    thumbnail_path: Path | None = None,
) -> dict | None:
    """Send a media file with a rich HTML caption embedded in the same message."""
    endpoint = {
        "video": "sendVideo",
        "audio": "sendAudio",
        "document": "sendDocument",
    }[media_type]

    data: dict[str, str] = {
        "chat_id": str(chat_id),
        "rich_caption": json.dumps({"html": rich_html}),
    }
    if reply_to_message_id is not None:
        data["reply_parameters"] = json.dumps({"message_id": reply_to_message_id})
    if reply_markup_obj is not None:
        data["reply_markup"] = json.dumps(reply_markup_obj.to_dict())
    if supports_streaming:
        data["supports_streaming"] = "true"
    if audio_title:
        data["title"] = audio_title
    if audio_performer:
        data["performer"] = audio_performer
    if audio_duration is not None:
        data["duration"] = str(audio_duration)
    if audio_filename:
        data["filename"] = audio_filename

    mime_map = {
        "video": "video/mp4",
        "audio": "audio/mpeg",
        "document": "application/octet-stream",
    }

    # Build a plain-text fallback caption in case rich_caption isn't supported.
    plain_caption = (
        f"{audio_title or ''}\n{audio_performer or ''}"
        if media_type == "audio"
        else ""
    ).strip()

    def _build_files(open_media, open_thumb):
        """Build the files list from open file handles (streaming, no full read)."""
        files: list = [(media_type, (media_path.name, open_media, mime_map[media_type]))]
        if open_thumb is not None:
            files.append(("thumbnail", (thumbnail_path.name, open_thumb, "image/jpeg")))  # type: ignore[union-attr]
        return files

    try:
        async with httpx.AsyncClient(timeout=600) as client:
            with open(media_path, "rb") as fmedia:
                thumb_handle = open(thumbnail_path, "rb") if thumbnail_path and thumbnail_path.is_file() else None
                try:
                    resp = await client.post(
                        f"https://api.telegram.org/bot{bot_token}/{endpoint}",
                        data=data,
                        files=_build_files(fmedia, thumb_handle),
                    )
                finally:
                    if thumb_handle:
                        thumb_handle.close()

        if resp.is_success:
            return resp.json().get("result")

        # Retry with plain caption (rich_caption may not be supported on this server).
        logger.warning(
            "send_media_with_rich_caption attempt 1 failed (%s): %s — retrying with plain caption",
            resp.status_code, resp.text[:300],
        )
        fallback_data = {k: v for k, v in data.items() if k != "rich_caption"}
        if plain_caption:
            fallback_data["caption"] = plain_caption

        async with httpx.AsyncClient(timeout=600) as client:
            with open(media_path, "rb") as fmedia2:
                thumb_handle2 = open(thumbnail_path, "rb") if thumbnail_path and thumbnail_path.is_file() else None
                try:
                    resp2 = await client.post(
                        f"https://api.telegram.org/bot{bot_token}/{endpoint}",
                        data=fallback_data,
                        files=_build_files(fmedia2, thumb_handle2),
                    )
                finally:
                    if thumb_handle2:
                        thumb_handle2.close()

        if resp2.is_success:
            return resp2.json().get("result")
        logger.warning(
            "send_media_with_rich_caption attempt 2 failed (%s): %s",
            resp2.status_code, resp2.text[:300],
        )
    except Exception as exc:
        logger.exception("send_media_with_rich_caption error: %s", exc)
    return None


def clean_filename(value: str) -> str:
    """Make a user-provided audio name safe while preserving Arabic text."""
    cleaned = re.sub(r"[\\/:*?\"<>|{}\[\]\x00-\x1f]", " ", value)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    if cleaned.lower().endswith(".mp3"):
        cleaned = cleaned[:-4].rstrip(" .")
    return (cleaned[:90] or "ملف صوتي") + ".mp3"


def _prune_store(store: dict, limit: int = 1000) -> None:
    if len(store) > limit:
        del store[next(iter(store))]


def make_audio_markup(session_key: str) -> InlineKeyboardMarkup:
    """Create the three audio action buttons, keyed by session_key."""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    RENAME_BUTTON_LABEL,
                    callback_data=f"rename:{session_key}",
                )
            ],
            [
                InlineKeyboardButton(
                    COVER_BUTTON_LABEL,
                    callback_data=f"cover:{session_key}",
                ),
                InlineKeyboardButton(
                    DESC_BUTTON_LABEL,
                    callback_data=f"desc:{session_key}",
                ),
            ],
        ]
    )


TELEGRAM_MAX_BYTES = 49 * 1024 * 1024  # 49 MB — Telegram Bot API upload limit


def split_video_into_parts(source: Path, max_bytes: int = TELEGRAM_MAX_BYTES) -> list[Path]:
    """NO LONGER SPLITTING: Returning the original source directly for Pyrogram upload."""
    return [source]
    if source.stat().st_size <= max_bytes:
        return [source]
    try:
        probe = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "csv=p=0",
                str(source),
            ],
            capture_output=True, text=True, timeout=30,
        )
        duration = float(probe.stdout.strip() or "0")
        if duration <= 0:
            return [source]

        n_parts = math.ceil(source.stat().st_size / max_bytes)
        seg_duration = duration / n_parts
        parts: list[Path] = []

        for i in range(n_parts):
            out = source.parent / f"{source.stem}_part{i + 1}of{n_parts}.mp4"
            subprocess.run(
                [
                    "ffmpeg", "-y",
                    "-ss", f"{i * seg_duration:.3f}",
                    "-i", str(source),
                    "-t", f"{seg_duration:.3f}",
                    "-c", "copy",          # stream-copy — no re-encode, very fast
                    "-avoid_negative_ts", "make_zero",
                    str(out),
                ],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=300,
            )
            if out.is_file() and out.stat().st_size > 0:
                parts.append(out)

        logger.info("Split video into %d parts (%.1f MB each)", len(parts), max_bytes / 1024 / 1024)
        return parts if parts else [source]
    except Exception as exc:
        logger.warning("split_video_into_parts failed: %s", exc)
        return [source]


def convert_cover_to_jpeg(source: Path, directory: Path) -> Path | None:
    """Convert a downloaded thumbnail to a Telegram-compatible JPEG."""
    target = directory / "cover.jpg"
    try:
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(source),
                "-vf",
                "scale='min(600,iw)':-2",
                "-q:v",
                "4",
                str(target),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return target if target.is_file() and target.stat().st_size > 0 else None


# ---------------------------------------------------------------------------
# cobalt.tools — free, open-source, no API key, supports YouTube + all major
# platforms. Used as the primary downloader for YouTube; yt-dlp is fallback.
# ---------------------------------------------------------------------------
COBALT_INSTANCES = [
    "https://cobalt.tools/",
    "https://api.cobalt.tools/",
    "https://cobalt.privacyredirect.com/",
    "https://cbl.f95zone.to/",
    "https://cobalt.catvibers.me/",
]


async def _cobalt_get_link(url: str, mode: str) -> tuple[str, str] | None:
    """
    Call cobalt API instances in order and return (download_url, filename).
    Returns None if every instance fails.
    """
    download_mode = "audio" if mode == "audio" else "auto"
    payload: dict = {
        "url": url,
        "videoQuality": "max",
        "audioFormat": "mp3" if mode == "audio" else "best",
        "audioBitrate": "320",
        "filenameStyle": "basic",
        "downloadMode": download_mode,
        "youtubeVideoCodec": "h264",
        "alwaysProxy": False,
        "disableMetadata": False,
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    for instance in COBALT_INSTANCES:
        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                resp = await client.post(instance, json=payload, headers=headers)
            if not resp.is_success:
                logger.warning("Cobalt %s → HTTP %s", instance, resp.status_code)
                continue
            data = resp.json()
            status = data.get("status", "")
            if status in ("tunnel", "redirect"):
                return data.get("url", ""), data.get("filename", "video.mp4")
            if status == "picker":
                items = data.get("picker", [])
                for item in items:
                    if item.get("type") == "video":
                        return item["url"], "video.mp4"
                if items:
                    return items[0]["url"], items[0].get("filename", "video.mp4")
            logger.warning("Cobalt %s → unexpected status %r: %s", instance, status, data)
        except Exception as exc:
            logger.warning("Cobalt instance %s failed: %s", instance, exc)
    return None


async def download_via_cobalt(
    url: str, mode: str = "video"
) -> tuple[Path, dict[str, str], None] | None:
    """
    Download via cobalt.tools API.
    Returns (file_path, metadata_dict, None) on success, or None on failure.
    """
    link = await _cobalt_get_link(url, mode)
    if not link:
        logger.warning("All cobalt instances failed for %s", url)
        return None

    dl_url, filename = link
    ext = Path(filename).suffix or (".mp3" if mode == "audio" else ".mp4")
    fd, tmp_path = tempfile.mkstemp(suffix=ext, prefix="cobalt-")
    os.close(fd)
    dest = Path(tmp_path)

    try:
        async with httpx.AsyncClient(timeout=600, follow_redirects=True) as client:
            async with client.stream("GET", dl_url) as resp:
                resp.raise_for_status()
                with dest.open("wb") as fh:
                    async for chunk in resp.aiter_bytes(1024 * 1024):
                        fh.write(chunk)

        if not dest.is_file() or dest.stat().st_size == 0:
            dest.unlink(missing_ok=True)
            logger.warning("Cobalt download produced empty file for %s", url)
            return None

        logger.info(
            "Cobalt downloaded %s (%.1f MB)",
            filename,
            dest.stat().st_size / 1024 / 1024,
        )
        stem = Path(filename).stem[:200] or "video"
        metadata: dict[str, str] = {
            "title": stem,
            "uploader": "غير متاح",
            "uploader_id": "غير متاح",
            "platform": platform_name(url),
            "duration": "غير متاح",
            "duration_seconds": "0",
            "resolution": "غير متاح",
            "format": ext.lstrip("."),
            "format_note": "cobalt",
            "video_codec": "غير متاح",
            "audio_codec": "غير متاح",
            "size": format_bytes(dest.stat().st_size),
            "published": "غير متاح",
            "views": "غير متاح",
            "likes": "غير متاح",
            "comments": "غير متاح",
            "id": "غير متاح",
            "url": url,
        }
        return dest, metadata, None

    except Exception as exc:
        dest.unlink(missing_ok=True)
        logger.warning("Cobalt stream download failed for %s: %s", url, exc)
        return None


def download_media(
    url: str, mode: str
) -> tuple[Path, dict[str, str], Path | None]:
    """Download media and return its path plus source metadata."""
    is_youtube = platform_name(url) == "Youtube"

    # YouTube needs separate profiles: web+cookies (full quality) then
    # android fallback (only supports "best", not split-stream selectors).
    if is_youtube:
        quality_profiles = (
            ("bestvideo+bestaudio/best",          "أفضل جودة متاحة"),
            ("bestvideo[height<=1080]+bestaudio/best", "جودة 1080p"),
            ("bestvideo[height<=720]+bestaudio/best",  "جودة 720p"),
            ("best",                               "أفضل صيغة متاحة"),
        )
    else:
        quality_profiles = (
            ("bestvideo+bestaudio/best",           "أفضل جودة متاحة"),
            ("bestvideo[height<=1080]+bestaudio/best[height<=1080]/best", "جودة 1080p"),
            ("bestvideo[height<=720]+bestaudio/best[height<=720]/best",   "جودة 720p"),
            ("best",                               "أفضل صيغة متاحة"),
        )

    with tempfile.TemporaryDirectory(prefix="telegram-download-") as temp_dir:
        directory = Path(temp_dir)
        last_error: Exception | None = None

        for format_selector, quality_label in quality_profiles:
            try:
                if mode == "audio":
                    format_selector = "bestaudio/best"

                output_template = str(directory / "%(title).80s-%(id)s.%(ext)s")

                options: dict = {
                    "format": format_selector,
                    "outtmpl": output_template,
                    "merge_output_format": "mp4",
                    "noplaylist": True,
                    "quiet": True,
                    "no_warnings": True,
                    "retries": 10,
                    "fragment_retries": 10,
                    "extractor_retries": 5,
                    "concurrent_fragment_downloads": 32,
                    "buffersize": 16 * 1024 * 1024,
                    "http_chunk_size": 10 * 1024 * 1024,
                    "file_access_retries": 5,
                    "socket_timeout": 30,
                    "continuedl": True,
                    "writethumbnail": mode == "audio",
                    "sleep_interval": 0,
                    "sleep_interval_requests": 0,
                    "restrictfilenames": True,
                    "overwrites": True,
                    "http_headers": {
                        "User-Agent": (
                            "Mozilla/5.0 (X11; Linux x86_64) "
                            "AppleWebKit/537.36 Chrome/131 Safari/537.36"
                        )
                    },
                }

                # YouTube: mobile/TV clients bypass bot-detection and JS challenges
                # without needing cookies. Try them first; use web+cookies as last resort.
                if is_youtube:
                    options["geo_bypass"] = True
                    options["extractor_args"] = {
                        "youtube": {
                            "player_client": [
                                "ios",
                                "android",
                                "tv_embedded",
                                "android_vr",
                                "mweb",
                            ],
                        }
                    }
                    # Only attach cookies on the last quality fallback (plain "best")
                    # so mobile clients get a clean shot first.
                    if format_selector == "best" and YOUTUBE_COOKIES_FILE.is_file():
                        options["cookiefile"] = str(YOUTUBE_COOKIES_FILE)
                        options["extractor_args"]["youtube"]["player_client"] = [
                            "web", "ios", "android"
                        ]

                # Pinterest: always use cookies + geo_bypass for best results.
                if platform_name(url) == "Pinterest":
                    options["geo_bypass"] = True
                    options["http_headers"]["Referer"] = "https://www.pinterest.com/"
                    if PINTEREST_COOKIES_FILE.is_file():
                        options["cookiefile"] = str(PINTEREST_COOKIES_FILE)

                # Use Facebook cookies when available (helps with restricted videos).
                if (
                    platform_name(url) == "Facebook"
                    and FACEBOOK_COOKIES_FILE.is_file()
                ):
                    options["cookiefile"] = str(FACEBOOK_COOKIES_FILE)

                # Dynamic cookies uploaded via admin panel (override built-ins)
                # Match by normalized platform name for flexibility
                dyn_plat_raw = platform_name(url)
                dyn_plat_key = None
                for key in PLATFORM_COOKIES:
                    if dyn_plat_raw.lower() == key.lower() or dyn_plat_raw == key:
                        dyn_plat_key = key
                        break
                    # Also try partial match for "X / Twitter" etc.
                    if dyn_plat_raw.lower() in key.lower() or key.lower() in dyn_plat_raw.lower():
                        dyn_plat_key = key
                if dyn_plat_key and PLATFORM_COOKIES[dyn_plat_key].is_file():
                    options["cookiefile"] = str(PLATFORM_COOKIES[dyn_plat_key])

                if mode == "audio":
                    options["postprocessors"] = [
                        {
                            "key": "FFmpegExtractAudio",
                            "preferredcodec": "mp3",
                            "preferredquality": "192",
                        }
                    ]

                with yt_dlp.YoutubeDL(options) as downloader:
                    info = downloader.extract_info(url, download=True)
                    if not isinstance(info, dict):
                        raise RuntimeError("مصدر التنزيل لم يُرجع بيانات صالحة.")

                candidates = [
                    path
                    for path in directory.iterdir()
                    if (
                        path.is_file()
                        and path.suffix.lower()
                        not in {".part", ".ytdl", ".jpg", ".jpeg", ".png", ".webp"}
                    )
                ]
                if not candidates:
                    raise RuntimeError("لم يتم إنشاء ملف التنزيل.")

                media_path = max(candidates, key=lambda path: path.stat().st_mtime)
                if media_path.stat().st_size > MAX_FILE_BYTES:
                    raise FileTooLargeError(
                        f"حجم الملف ({format_bytes(media_path.stat().st_size)}) يتجاوز الحد المحلي "
                        f"({format_bytes(MAX_FILE_BYTES)}). قلّل الجودة أو ارفع max_file_mb بعد التأكد من مساحة الهاتف."
                    )
                logger.info(
                    "Downloaded %s (%s, %.1f MB)",
                    info.get("title") or "video",
                    quality_label,
                    media_path.stat().st_size / 1024 / 1024,
                )
                # Copy out of TemporaryDirectory so the caller can send it.
                file_descriptor, persistent_name = tempfile.mkstemp(
                    suffix=media_path.suffix
                )
                os.close(file_descriptor)
                persistent_path = Path(persistent_name)
                shutil.copy2(media_path, persistent_path)
                metadata = source_metadata(info, persistent_path)
                persistent_cover: Path | None = None
                if mode == "audio":
                    cover_candidates = [
                        path
                        for path in directory.iterdir()
                        if path.is_file()
                        and path.suffix.lower()
                        in {".jpg", ".jpeg", ".png", ".webp"}
                    ]
                    if cover_candidates:
                        cover_path = convert_cover_to_jpeg(
                            max(
                                cover_candidates,
                                key=lambda path: path.stat().st_mtime,
                            ),
                            directory,
                        )
                        if cover_path is not None:
                            cover_descriptor, cover_name = tempfile.mkstemp(
                                suffix=".jpg"
                            )
                            os.close(cover_descriptor)
                            persistent_cover = Path(cover_name)
                            shutil.copy2(cover_path, persistent_cover)
                return persistent_path, metadata, persistent_cover

            except Exception as error:  # yt-dlp raises several exception types.
                last_error = error
                logger.warning("Download profile failed: %s", error)
                for old_file in directory.iterdir():
                    old_file.unlink(missing_ok=True)
                if mode == "audio":
                    break

        if last_error is not None:
            raise last_error
        raise RuntimeError("فشل التنزيل لسبب غير معروف.")


def download_tiktok_photos(url: str) -> tuple[list[Path], dict]:
    """
    Download TikTok photo slideshow images.
    Returns (image_paths, metadata) if the post is a photo slideshow,
    or ([], {}) if it is a regular video post.
    """
    with tempfile.TemporaryDirectory(prefix="tiktok-photo-") as temp_dir:
        directory = Path(temp_dir)
        options: dict = {
            "outtmpl": str(directory / "%(autonumber)03d_%(id)s.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
            "retries": 5,
            "socket_timeout": 30,
            "http_headers": {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 Chrome/131 Safari/537.36"
                )
            },
        }
        # Apply TikTok cookies if available
        for key in PLATFORM_COOKIES:
            if "tiktok" in key.lower() and PLATFORM_COOKIES[key].is_file():
                options["cookiefile"] = str(PLATFORM_COOKIES[key])
                break

        try:
            with yt_dlp.YoutubeDL(options) as ydl:
                info = ydl.extract_info(url, download=True)
            if not isinstance(info, dict):
                return [], {}
        except Exception as exc:
            logger.warning("TikTok photo extraction attempt: %s", exc)
            return [], {}

        image_exts = {".jpg", ".jpeg", ".png", ".webp"}
        skip_exts = {".json", ".part", ".ytdl", ".tmp"}
        all_files = [p for p in directory.iterdir() if p.is_file()]

        # If any non-image media file exists → this is a regular video post
        non_images = [
            p for p in all_files
            if p.suffix.lower() not in image_exts and p.suffix.lower() not in skip_exts
        ]
        if non_images:
            return [], {}

        images = sorted(
            [p for p in all_files if p.suffix.lower() in image_exts],
            key=lambda p: p.name,
        )
        if not images:
            return [], {}

        # Copy images out of the temp dir (Telegram album limit: 10)
        result: list[Path] = []
        for src in images[:10]:
            fd, dst_name = tempfile.mkstemp(suffix=src.suffix)
            os.close(fd)
            dst = Path(dst_name)
            shutil.copy2(src, dst)
            result.append(dst)

        metadata = source_metadata(info, images[0])
        return result, metadata


LOADING_HTML = """
<h1><b>Downloading</b></h1>

<img src="https://ik.imagekit.io/apkqvdjhg/dc9cbf707332beb64f20a53184669977%20(1).gif">

<h1><b>يـرجـى التحـلي بالـصـبر</b></h1>
"""

ERROR_HTML = """
<h1><b>Something went wrong</b></h1>

<img src="https://ik.imagekit.io/apkqvdjhg/2cad6a9eb1edab1dfbe1fb0db9f50d86.gif">

<h2><b>خـطأ فـي التحـميـل !</b></h2>

<small><b>يـرجـى المـحاولـه فـي وقـتٍ لاحـق أو تأكـد مـن صحـه الرابـط و أن الـفيديـو لـم يُحـذف بالفـعـل.</b></small>
"""


async def send_downloaded_media(
    message: Any,
    context: ContextTypes.DEFAULT_TYPE,
    url: str,
    mode: str = "video",
    custom_filename: str | None = None,
    custom_thumbnail: Any | None = None,
    custom_caption: str | None = None,
    custom_performer: str | None = None,
    show_loading: bool = True,
) -> None:
    if message is None:
        return

    bot_token = BOT_TOKEN

    # Send rich loading message only when requested (skipped for playlist items)
    loading_msg_id = None
    if show_loading:
        loading_msg_id = await send_rich_message_raw(
            bot_token, message.chat_id, LOADING_HTML, message.message_id
        )

    async def delete_loading() -> None:
        if loading_msg_id:
            await delete_message_raw(bot_token, message.chat_id, loading_msg_id)

    downloaded_path: Path | None = None
    cover_path: Path | None = None

    try:
        # --- TikTok photo slideshow ---
        if mode == "video" and platform_name(url) == "Tiktok":
            photo_paths: list[Path] = []
            photo_meta: dict = {}
            async with download_slots:
                await context.bot.send_chat_action(
                    chat_id=message.chat_id, action=ChatAction.UPLOAD_PHOTO
                )
                photo_paths, photo_meta = await asyncio.wait_for(
                    asyncio.to_thread(download_tiktok_photos, url),
                    timeout=DOWNLOAD_TIMEOUT_SECONDS,
                )
            if photo_paths:
                file_handles: list[Any] = []
                try:
                    title = (photo_meta.get("title") or "").strip()
                    media_group = []
                    for i, p in enumerate(photo_paths):
                        fh = open(p, "rb")
                        file_handles.append(fh)
                        media_group.append(
                            InputMediaPhoto(
                                media=fh,
                                caption=f"<b>{html.escape(title)}</b>" if i == 0 and title else None,
                                parse_mode=ParseMode.HTML if i == 0 and title else None,
                            )
                        )
                    await context.bot.send_media_group(
                        chat_id=message.chat_id,
                        media=media_group,
                        reply_to_message_id=message.message_id,
                    )
                    await delete_loading()
                    try:
                        uid = message.from_user.id if message.from_user else None
                        if uid:
                            await increment_download(uid)
                    except Exception:
                        logger.exception("Could not update download counter")
                    return True
                finally:
                    for fh in file_handles:
                        try:
                            fh.close()
                        except Exception:
                            pass
                    for p in photo_paths:
                        p.unlink(missing_ok=True)

        async with download_slots:
            await context.bot.send_chat_action(
                chat_id=message.chat_id,
                action=ChatAction.UPLOAD_VIDEO,
            )
            downloaded_path, metadata, cover_path = await asyncio.wait_for(
                asyncio.to_thread(download_media, url, mode),
                timeout=DOWNLOAD_TIMEOUT_SECONDS,
            )

        if mode == "audio":
            thumbnail_to_use = custom_thumbnail if custom_thumbnail is not None else cover_path
            rich_html = build_rich_details_html(metadata, "audio") if not custom_caption else None

            session_key = secrets.token_urlsafe(8)
            audio_sessions[session_key] = {"url": url}
            _prune_store(audio_sessions)
            audio_markup = make_audio_markup(session_key)

            # Pyrogram first for large audio; Bot API fallback for files within its limit.
            pyro_result = await upload_large_media(
                chat_id=message.chat_id,
                media_path=downloaded_path,
                media_type="audio",
                caption=build_audio_caption(metadata),
                reply_to_message_id=message.message_id,
                reply_markup=audio_markup,
                duration=int(metadata["duration_seconds"]),
                performer=(custom_performer or metadata["uploader"])[:64],
                title=metadata["title"][:64],
                thumb=thumbnail_to_use if isinstance(thumbnail_to_use, Path) else None,
            )
            if pyro_result:
                result_chat_id = getattr(getattr(pyro_result, "chat", None), "id", message.chat_id)
                result_message_id = getattr(pyro_result, "id", None)
                if rich_html and result_message_id:
                    await send_rich_details(bot_token, message.chat_id, result_message_id, metadata, "audio")
            else:
                if downloaded_path.stat().st_size > 49 * 1024 * 1024:
                    raise FileTooLargeError("الملف أكبر من حد Bot API وفشل رفع Pyrogram. راجع api_id وapi_hash واتصالك.")
                result = await send_media_with_rich_caption(
                    bot_token=bot_token,
                    chat_id=message.chat_id,
                    media_path=downloaded_path,
                    media_type="audio",
                    rich_html=rich_html or (custom_caption or ""),
                    reply_to_message_id=message.message_id,
                    reply_markup_obj=audio_markup,
                    audio_title=metadata["title"][:64],
                    audio_performer=(custom_performer or metadata["uploader"])[:64],
                    audio_duration=int(metadata["duration_seconds"]),
                    audio_filename=custom_filename or clean_filename(metadata["title"]),
                    thumbnail_path=thumbnail_to_use if isinstance(thumbnail_to_use, Path) else None,
                )
                if not result:
                    raise RuntimeError("تعذر رفع الملف الصوتي إلى تيليجرام")
                result_chat_id = result.get("chat", {}).get("id", message.chat_id)
                result_message_id = result.get("message_id")
            audio_sessions[session_key]["chat_id"] = result_chat_id
            audio_sessions[session_key]["message_id"] = result_message_id
        else:
            if downloaded_path.stat().st_size > 49 * 1024 * 1024 and pyro_client is None:
                raise FileTooLargeError("الملف أكبر من حد الرفع عبر Bot API. أضف api_id و api_hash لتفعيل Pyrogram.")
            button_key = secrets.token_urlsafe(8)
            audio_url_store[button_key] = url
            _prune_store(audio_url_store)
            audio_markup = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            AUDIO_BUTTON_LABEL,
                            callback_data=f"audio:{button_key}",
                        )
                    ]
                ]
            )
            rich_html = build_rich_details_html(metadata, "video")
            media_type = "video" if downloaded_path.suffix.lower() == ".mp4" else "document"

            # NO LONGER SPLITTING: Use Pyrogram for large files
            parts = [downloaded_path]

            try:
                for idx, part_path in enumerate(parts, 1):
                    part_html = rich_html if idx == 1 else f"<b>الجزء {idx} / {len(parts)}</b>"
                    part_markup = audio_markup if idx == 1 else None
                    
                    # Convert HTML for Pyrogram (simple replacement for common tags)
                    pyro_caption = part_html.replace("<h4>", "<b>").replace("</h4>", "</b>")
                    pyro_caption = pyro_caption.replace("<p>", "").replace("</p>", "\n")
                    pyro_caption = pyro_caption.replace("<details>", "").replace("</details>", "")
                    pyro_caption = pyro_caption.replace("<summary>", "<b>").replace("</summary>", "</b>")
                    
                    # Try Pyrogram first for consistency and large file support
                    res = await upload_large_media(
                        chat_id=message.chat_id,
                        media_path=part_path,
                        media_type="video",
                        caption=pyro_caption,
                        reply_to_message_id=message.message_id,
                        reply_markup=part_markup,
                        duration=int(metadata["duration_seconds"]),
                        thumb=cover_path
                    )
                    
                    if not res and part_path.stat().st_size > 49 * 1024 * 1024:
                        raise FileTooLargeError("فشل رفع الملف الكبير عبر Pyrogram؛ لم تتم محاولة رفعه عبر Bot API.")
                    if not res:
                        # Fallback to original method if Pyrogram fails for small files.
                        res = await send_media_with_rich_caption(
                            bot_token=bot_token,
                            chat_id=message.chat_id,
                            media_path=part_path,
                            media_type="video",
                            rich_html=part_html,
                            reply_to_message_id=message.message_id,
                            reply_markup_obj=part_markup,
                            supports_streaming=True,
                        )
                    if not res:
                        raise RuntimeError("تعذر رفع الملف إلى تيليجرام")
            finally:
                # Clean up split parts (not the original downloaded_path — handled by caller).
                for part_path in parts:
                    if part_path != downloaded_path:
                        part_path.unlink(missing_ok=True)

        await delete_loading()
        # عدّاد التحميلات — يزيد فقط عند النجاح
        try:
            uid = message.from_user.id if message.from_user else None
            if uid:
                await increment_download(uid)
        except Exception:
            logger.exception("Could not update download counter")
        return True
    except asyncio.TimeoutError:
        await delete_loading()
        try:
            await send_rich_message_raw(bot_token, message.chat_id, ERROR_HTML, message.message_id)
        except Exception:
            pass
        return False
    except Exception as error:
        logger.exception("Could not process URL: %s", error)
        await delete_loading()
        try:
            await send_rich_message_raw(bot_token, message.chat_id, ERROR_HTML, message.message_id)
        except Exception:
            pass
        return False
    finally:
        if downloaded_path is not None:
            downloaded_path.unlink(missing_ok=True)
        if cover_path is not None:
            cover_path.unlink(missing_ok=True)


async def audio_button(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if query is None or query.message is None:
        return

    await query.answer("جاري تجهيز الملف الصوتي...")
    callback_data = query.data or ""
    button_key = callback_data.removeprefix("audio:")
    url = audio_url_store.pop(button_key, None)
    if not url:
        await query.message.reply_text(
            "انتهت صلاحية هذا الزر. أرسل الرابط مرة أخرى."
        )
        return
    await send_downloaded_media(
        query.message,
        context,
        url,
        mode="audio",
    )


async def rename_button(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if query is None or query.message is None:
        return

    await query.answer()
    callback_data = query.data or ""
    token = callback_data.removeprefix("rename:")
    session = audio_sessions.get(token)
    if not session:
        await query.message.reply_text(
            "انتهت صلاحية هذا الزر. أرسل الرابط مرة أخرى."
        )
        return

    user_id = query.from_user.id if query.from_user else 0
    pending_key = (query.message.chat_id, user_id)
    rename_pending[pending_key] = session["url"]
    _prune_store(rename_pending)

    await query.message.reply_text(
        "<blockquote><b>إعادة التسمية</b>\n"
        "أرسل الاسم الجديد — عربي أو إنجليزي\n"
        "<i>سأضيف ‎.mp3‎ تلقائيًا</i></blockquote>",
        parse_mode=ParseMode.HTML,
    )


async def cover_button(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ask the user to send a photo that will be used as the audio thumbnail."""
    query = update.callback_query
    if query is None or query.message is None:
        return

    await query.answer()
    callback_data = query.data or ""
    token = callback_data.removeprefix("cover:")
    session = audio_sessions.get(token)
    if not session:
        await query.message.reply_text(
            "انتهت صلاحية هذا الزر. أرسل الرابط مرة أخرى."
        )
        return

    user_id = query.from_user.id if query.from_user else 0
    pending_key = (query.message.chat_id, user_id)
    cover_pending[pending_key] = token
    _prune_store(cover_pending)

    await query.message.reply_text(
        "<blockquote><b>وضع غلاف</b>\n"
        "أرسل الصورة التي تريد تعيينها غلافًا للملف الصوتي</blockquote>",
        parse_mode=ParseMode.HTML,
    )


async def desc_button(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ask the user to type a description/caption for the audio message."""
    query = update.callback_query
    if query is None or query.message is None:
        return

    await query.answer()
    callback_data = query.data or ""
    token = callback_data.removeprefix("desc:")
    session = audio_sessions.get(token)
    if not session:
        await query.message.reply_text(
            "انتهت صلاحية هذا الزر. أرسل الرابط مرة أخرى."
        )
        return

    user_id = query.from_user.id if query.from_user else 0
    pending_key = (query.message.chat_id, user_id)
    desc_pending[pending_key] = token
    _prune_store(desc_pending)

    await query.message.reply_text(
        "<blockquote><b>وضع وصف</b>\n"
        "أرسل الوصف الذي تريد إضافته للملف الصوتي</blockquote>",
        parse_mode=ParseMode.HTML,
    )


def account_markup() -> dict:
    """Return a raw reply_markup dict with a red (destructive) back button."""
    return {
        "inline_keyboard": [
            [{"text": "رجـوع", "callback_data": "back_to_start", "style": "danger"}]
        ]
    }


async def account_button(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if query is None or query.message is None:
        return

    await query.answer()
    user = query.from_user
    user_name  = (user.full_name if user else "") or "غير متاح"
    user_id    = user.id if user else 0
    dl_count   = await get_download_count(user_id) if user_id else 0
    profile_caption = (
        f"<b>ملف {user_name} الشخصي :\n\n"
        f"الاسم  :  {user_name}\n"
        f"المعرف  :  {user_id}\n"
        f"التحميلات  :  {dl_count}</b>"
    )
    async with httpx.AsyncClient(timeout=30) as client:
        await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendAnimation",
            json={
                "chat_id": query.message.chat_id,
                "animation": (
                    "https://ik.imagekit.io/apkqvdjhg/"
                    "Untitled%20(@galvesgal)%20on%20Tumblr.gif?updatedAt=1782915802655"
                ),
                "caption": profile_caption,
                "parse_mode": "HTML",
                "reply_markup": account_markup(),
                "reply_parameters": {"message_id": query.message.message_id},
            },
        )


async def back_to_start(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if query is None or query.message is None:
        return

    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass
    await start(update, context)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    name = update.effective_user.first_name if update.effective_user else ""
    token = BOT_TOKEN

    HTML = f"""
<h1><b>Welcome To MS Downloader Bot</b></h1>

<img src="https://ik.imagekit.io/apkqvdjhg/bcc1844731b3e467b8de50e1e61a4850.jpg">

<p><b>أهـلا بـك {name} في بـوت تـحميـل فـيديـوهات و صـوت من كـل مـنـصات الـتواصـل الإجتـماعي بدون عـلامـة مائيـة.</b></p>

<details>
<summary><b>المنصات المدعومه</b></summary>

<ul>
<li><b>TikTok</b></li>
<li><b>Instagram</b></li>
<li><b>Facebook</b></li>
<li><b>YouTube</b></li>
<li><b>X (Twitter)</b></li>
<li><b>Snapchat</b></li>
<li><b>Threads</b></li>
<li><b>Pinterest</b></li>
<li><b>Likee</b></li>
<li><b>Reddit</b></li>
<li><b>و العديــــد من المـنصـات الأخـرى...</b></li>
</ul>

</details>

<h1><b>قـم بإرسـال الـرابـط فـوراً أو إخـتر ما تـريده من الأسفـل</b></h1>
"""

    start_markup = {
        "inline_keyboard": [
            [{"text": "حسـابـي", "callback_data": "account", "style": "success"}],
            [{"text": "لتـنـصيـب بـوت", "url": "tg://user?id=8220915719", "style": "danger"}]
        ]
    }
    # Try sendRichMessage first (Telegram Bot API 9+), fall back to sendMessage
    sent = False
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"https://api.telegram.org/bot{token}/sendRichMessage",
                json={
                    "chat_id": update.effective_chat.id,
                    "rich_message": {"html": HTML},
                    "reply_markup": start_markup,
                },
            )
            if resp.is_success and resp.json().get("ok"):
                sent = True
    except Exception:
        pass

    if not sent:
        # Standard sendMessage fallback — works on all Telegram servers
        plain_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("حسـابـي", callback_data="account")],
            [InlineKeyboardButton("لتـنـصيـب بـوت", url="tg://user?id=8220915719")]
        ])
        plain_text = (
            f"<b>أهـلاً {html.escape(name)} 👋</b>\n\n"
            "أرسل رابط الفيديو وسأحمّله لك فوراً من:\n"
            "TikTok • Instagram • YouTube • Facebook\n"
            "X (Twitter) • Pinterest • Snapchat • وأكثر..."
        )
        if update.effective_message:
            await update.effective_message.reply_text(
                plain_text,
                parse_mode=ParseMode.HTML,
                reply_markup=plain_markup,
            )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start(update, context)


async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = " ".join(context.args)
    url = extract_media_url(text)
    if not url:
        if update.effective_message:
            await update.effective_message.reply_text(
                "<blockquote><b>الاستخدام الصحيح:</b>\n<code>/audio https://...</code></blockquote>",
                parse_mode=ParseMode.HTML,
            )
        return
    await send_downloaded_media(
        update.effective_message,
        context,
        url,
        mode="audio",
    )


async def photo_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle incoming photos: used for the 'set cover' flow."""
    message = update.effective_message
    if message is None:
        return

    user_id = message.from_user.id if message.from_user else 0
    pending_key = (message.chat_id, user_id)
    session_key = cover_pending.pop(pending_key, None)
    if not session_key:
        # Not in a cover flow — ignore the photo.
        return

    session = audio_sessions.get(session_key)
    if not session:
        await message.reply_text("انتهت صلاحية هذا الطلب. أرسل الرابط مرة أخرى.")
        return

    # Use the largest available photo size.
    photo = message.photo[-1] if message.photo else None
    if photo is None:
        await message.reply_text("لم أتمكن من قراءة الصورة، حاول مجددًا.")
        return

    photo_file = await context.bot.get_file(photo.file_id)

    # Download the photo to a temp file so we can pass it as a thumbnail.
    fd, tmp_path = tempfile.mkstemp(suffix=".jpg")
    os.close(fd)
    tmp = Path(tmp_path)
    try:
        await photo_file.download_to_drive(tmp)
        await send_downloaded_media(
            message,
            context,
            session["url"],
            mode="audio",
            custom_thumbnail=tmp,
        )
    finally:
        tmp.unlink(missing_ok=True)


async def send_playlist(
    message: Any,
    context: ContextTypes.DEFAULT_TYPE,
    url: str,
) -> None:
    """Extract a YouTube playlist and download each video sequentially."""
    bot_token = BOT_TOKEN
    await message.reply_text(
        "<blockquote><b>جاري جلب قائمة التشغيل…</b>\nيرجى الانتظار</blockquote>",
        parse_mode=ParseMode.HTML,
    )
    try:
        entries = await asyncio.wait_for(
            asyncio.to_thread(extract_playlist_entries, url),
            timeout=60,
        )
    except Exception as err:
        logger.warning("Playlist extraction failed: %s", err)
        await message.reply_text(
            "<blockquote><b>تعذّر جلب قائمة التشغيل</b>\n"
            "تأكد أن الرابط عام ثم أعد المحاولة.</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        return

    total = len(entries)
    if total == 0:
        await message.reply_text(
            "<blockquote><b>لم يُعثر على فيديوهات في قائمة التشغيل.</b></blockquote>",
            parse_mode=ParseMode.HTML,
        )
        return

    await message.reply_text(
        f"<blockquote><b>تم العثور على {total} فيديو</b></blockquote>",
        parse_mode=ParseMode.HTML,
    )

    # Shared progress counters
    done_count = 0
    fail_count = 0
    counter_lock = asyncio.Lock()

    # Send a live progress message we'll keep editing
    progress_msg = await message.reply_text(
        f"<blockquote><b>0 / {total}</b>  تم تحميله</blockquote>",
        parse_mode=ParseMode.HTML,
    )

    async def _update_progress() -> None:
        try:
            await progress_msg.edit_text(
                f"<blockquote><b>{done_count} / {total}</b>  تم تحميله"
                + (f"  ·  <b>{fail_count} فشل</b>" if fail_count else "")
                + "</blockquote>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    # Per-playlist semaphore — many workers, separate from global download_slots
    playlist_sem = asyncio.Semaphore(PLAYLIST_WORKERS)

    async def _download_one(idx: int, entry: dict) -> None:
        nonlocal done_count, fail_count
        vid_url = entry["url"]
        async with playlist_sem:
            try:
                succeeded = await send_downloaded_media(message, context, vid_url, show_loading=False)
                async with counter_lock:
                    done_count += 1
                    if not succeeded:
                        fail_count += 1
            except Exception as err:
                logger.warning("Playlist item %d failed: %s", idx, err)
                async with counter_lock:
                    done_count += 1
                    fail_count += 1
            await _update_progress()

    # Create only a small rolling set of tasks; huge playlists must not exhaust RAM.
    for idx, entry in enumerate(entries, start=1):
        await _download_one(idx, entry)

    try:
        await progress_msg.delete()
    except Exception:
        pass

    result_line = (
        f"<b>اكتمل تحميل قائمة التشغيل</b>\n"
        f"تم تنزيل {total - fail_count} فيديو بنجاح"
        + (f"  ·  فشل {fail_count}" if fail_count else "")
    )
    await message.reply_text(f"<blockquote>{result_line}</blockquote>", parse_mode=ParseMode.HTML)


async def url_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None or not message.text:
        return

    user_id = message.from_user.id if message.from_user else 0
    pending_key = (message.chat_id, user_id)

    # --- rename flow ---
    pending_url = rename_pending.pop(pending_key, None)
    if pending_url:
        await send_downloaded_media(
            message,
            context,
            pending_url,
            mode="audio",
            custom_filename=clean_filename(message.text),
        )
        return

    # --- description (performer) flow ---
    desc_token = desc_pending.pop(pending_key, None)
    if desc_token:
        session = audio_sessions.get(desc_token)
        if session and session.get("url"):
            await send_downloaded_media(
                message,
                context,
                session["url"],
                mode="audio",
                custom_performer=message.text.strip()[:64],
            )
        else:
            await message.reply_text(
                "<blockquote><b>انتهت صلاحية الطلب — أرسل الرابط مجددًا</b></blockquote>",
                parse_mode=ParseMode.HTML,
            )
        return

    # --- normal URL flow ---
    url = extract_media_url(message.text)
    if url:
        if is_youtube_playlist(url):
            await send_playlist(message, context, url)
        else:
            await send_downloaded_media(message, context, url)
    else:
        await message.reply_text(
            "<blockquote><b>أرسل رابطًا عامًا صحيحًا للوسائط</b></blockquote>",
            parse_mode=ParseMode.HTML,
        )


# ===========================================================================
# ADMIN PANEL
# ===========================================================================

USERS_PER_PAGE = 30


def _admin_main_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("المستخدمون",  callback_data="adm_users_0"),
            InlineKeyboardButton("الكوكيز",     callback_data="adm_cookies"),
        ],
        [InlineKeyboardButton("إذاعة لكل المستخدمين", callback_data="adm_broadcast")],
    ])


async def _admin_panel_text() -> str:
    users = await db_get_all_users()
    total   = len(users)
    banned  = sum(1 for u in users if u["banned"])
    stopped = sum(1 for u in users if u["stopped"])
    active  = total - banned - stopped
    return (
        "<b>لوحة التحكم</b>\n"
        "———————Mo———————\n"
        f"المستخدمون  :  <b>{total}</b>\n"
        f"نشطون       :  <b>{active}</b>\n"
        f"محظورون     :  <b>{banned}</b>\n"
        f"موقوفون     :  <b>{stopped}</b>"
    )


async def admin_panel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or update.effective_user.id != OWNER_ID:
        return
    text = await _admin_panel_text()
    await update.effective_message.reply_text(
        text, parse_mode=ParseMode.HTML, reply_markup=_admin_main_markup()
    )


async def adm_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if q is None:
        return
    await q.answer()
    text = await _admin_panel_text()
    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=_admin_main_markup())


# ── Users list (paginated) ──────────────────────────────────────────────────

async def adm_users_page(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if q is None:
        return
    await q.answer()
    page = int(q.data.split("_")[-1])
    users = await db_get_all_users()
    total = len(users)

    if total == 0:
        await q.message.edit_text(
            "لا يوجد مستخدمون بعد.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("رجوع", callback_data="adm_back")]]
            ),
        )
        return

    start = page * USERS_PER_PAGE
    page_users = users[start: start + USERS_PER_PAGE]
    total_pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)

    # 3-per-row user buttons
    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for u in page_users:
        name = (u["first"] or str(u["id"]))[:14]
        tag = " [محظور]" if u["banned"] else (" [موقوف]" if u["stopped"] else "")
        row.append(InlineKeyboardButton(f"{name}{tag}", callback_data=f"adm_user_{u['id']}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    # Navigation
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("السابق", callback_data=f"adm_users_{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page + 1} / {total_pages}", callback_data="adm_noop"))
    if start + USERS_PER_PAGE < total:
        nav.append(InlineKeyboardButton("التالي", callback_data=f"adm_users_{page + 1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton("رجوع", callback_data="adm_back")])

    await q.message.edit_text(
        f"<b>المستخدمون</b>  —  {total} مستخدم  |  صفحة {page + 1} / {total_pages}",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


# ── Single user profile ─────────────────────────────────────────────────────

async def _show_user(q: Any, uid: int) -> None:
    user = await db_get_user(uid)
    if not user:
        await q.message.edit_text("المستخدم غير موجود.")
        return
    dl = await get_download_count(uid)
    full = f"{user['first']} {user['last']}".strip() or "—"
    uname = f"@{user['username']}" if user["username"] else "—"
    status = "محظور" if user["banned"] else ("موقوف" if user["stopped"] else "نشط")
    text = (
        f"<b>{html.escape(full)}</b>\n"
        "————————Mo——————\n"
        f"المعرف       :  <code>{uid}</code>\n"
        f"اليوزر       :  {html.escape(uname)}\n"
        f"الانضمام     :  {user['joined'] or '—'}\n"
        f"التحميلات    :  <b>{dl}</b>\n"
        f"الحالة       :  <b>{status}</b>"
    )
    ban_btn = (
        InlineKeyboardButton("فك الحظر",  callback_data=f"adm_unban_{uid}")
        if user["banned"] else
        InlineKeyboardButton("حظر",       callback_data=f"adm_ban_{uid}")
    )
    stop_btn = (
        InlineKeyboardButton("استئناف",   callback_data=f"adm_resume_{uid}")
        if user["stopped"] else
        InlineKeyboardButton("إيقاف سري", callback_data=f"adm_stop_{uid}")
    )
    markup = InlineKeyboardMarkup([
        [ban_btn, stop_btn],
        [InlineKeyboardButton("إرسال رسالة",   callback_data=f"adm_msg_{uid}")],
        [InlineKeyboardButton("رجوع للقائمة",  callback_data="adm_users_0")],
    ])
    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


async def adm_user_detail(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if q is None:
        return
    await q.answer()
    uid = int(q.data.split("_")[-1])
    await _show_user(q, uid)


async def adm_ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if q is None:
        return
    uid = int(q.data.split("_")[-1])
    await db_set_ban(uid, True)
    await q.answer("تم الحظر")
    await _show_user(q, uid)


async def adm_unban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if q is None:
        return
    uid = int(q.data.split("_")[-1])
    await db_set_ban(uid, False)
    await q.answer("تم فك الحظر")
    await _show_user(q, uid)


async def adm_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if q is None:
        return
    uid = int(q.data.split("_")[-1])
    await db_set_stop(uid, True)
    await q.answer("تم الإيقاف")
    await _show_user(q, uid)


async def adm_resume(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if q is None:
        return
    uid = int(q.data.split("_")[-1])
    await db_set_stop(uid, False)
    await q.answer("تم الاستئناف")
    await _show_user(q, uid)


async def adm_msg_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin taps 'Send Message' — saves target and asks for message text."""
    q = update.callback_query
    if q is None:
        return
    uid = int(q.data.split("_")[-1])
    _admin_send_msg_pending[OWNER_ID] = uid
    await q.answer()
    await q.message.reply_text(
        f"أرسل الرسالة التي تريد إيصالها للمستخدم <code>{uid}</code>\n"
        "(أي نوع: نص، صورة، فيديو، صوت…)",
        parse_mode=ParseMode.HTML,
    )


# ── Cookies panel ───────────────────────────────────────────────────────────

async def adm_cookies_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if q is None:
        return
    await q.answer()
    rows = await db_get_all_cookies()
    lines = "\n".join(f"- <b>{html.escape(r[0])}</b>  <i>({r[1]})</i>" for r in rows) or "لا توجد كوكيز محفوظة بعد."
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("إضافة / تحديث كوكيز", callback_data="adm_cookie_add")],
        [InlineKeyboardButton("رجوع",                 callback_data="adm_back")],
    ])
    await q.message.edit_text(
        f"<b>الكوكيز المحفوظة</b>\n———————Mo———————\n{lines}",
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


def _cookie_platform_buttons() -> InlineKeyboardMarkup:
    """Build inline keyboard with platform buttons for cookie selection (2 per row)."""
    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for display_name, platform_key in SUPPORTED_PLATFORMS_BUTTONS:
        row.append(
            InlineKeyboardButton(
                display_name,
                callback_data=f"adm_cookie_plat_{platform_key}",
            )
        )
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("رجوع", callback_data="adm_cookies")])
    return InlineKeyboardMarkup(buttons)


async def adm_cookie_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show platform selection buttons instead of asking for text input."""
    q = update.callback_query
    if q is None:
        return
    await q.answer()
    await q.message.reply_text(
        "<b>اختَر المنصة التي تريد إضافة / تحديث كوكيز لها:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=_cookie_platform_buttons(),
    )


async def adm_cookie_platform_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """User (admin) tapped a platform button — ask for TXT file upload."""
    q = update.callback_query
    if q is None:
        return
    await q.answer()
    # Extract platform key from callback data
    platform_key = q.data.removeprefix("adm_cookie_plat_")
    # Find the display name for this key
    platform_name_display = platform_key
    for display_name, key in SUPPORTED_PLATFORMS_BUTTONS:
        if key == platform_key:
            platform_name_display = display_name
            break

    _admin_cookie_selected_platform[OWNER_ID] = platform_name_display
    await q.message.edit_text(
        f"<b>المنصة: {html.escape(platform_name_display)}</b>\n"
        "——————————————\n"
        "أرسِل <b>ملف TXT</b> يحتوي على الكوكيز\n"
        "(يجب أن يكون الملف بصيغة <code>.txt</code>)",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("إلغاء", callback_data="adm_cookies")]
        ]),
    )


async def adm_cookie_document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle incoming .txt document when admin is waiting for cookie file."""
    message = update.effective_message
    if message is None:
        return
    user_id = message.from_user.id if message.from_user else 0
    if user_id != OWNER_ID:
        return

    platform = _admin_cookie_selected_platform.pop(user_id, None)
    if not platform:
        return

    doc = message.document
    if doc is None:
        await message.reply_text("لم يتم إرسال ملف. أرسِل ملف TXT فقط.")
        return

    filename = (doc.file_name or "").lower()
    if not filename.endswith(".txt"):
        await message.reply_text(
            "يجب أن يكون الملف بصيغة <code>.txt</code> فقط.",
            parse_mode=ParseMode.HTML,
        )
        # Re-set the pending state so they can try again
        _admin_cookie_selected_platform[user_id] = platform
        return

    # Reject unusually large cookie files before downloading them.
    if doc.file_size and doc.file_size > 2 * 1024 * 1024:
        await message.reply_text("حجم ملف الكوكيز أكبر من الحد المسموح (2 ميجابايت).")
        _admin_cookie_selected_platform[user_id] = platform
        return

    # Download the document
    try:
        fd, tmp_path = tempfile.mkstemp(suffix=".txt")
        os.close(fd)
        tmp = Path(tmp_path)
        try:
            doc_file = await context.bot.get_file(doc.file_id)
            await doc_file.download_to_drive(tmp)
            content = tmp.read_text(encoding="utf-8", errors="replace")
            if not content.strip():
                await message.reply_text("الملف فارغ. أرسِل ملف يحتوي على كوكيز.")
                _admin_cookie_selected_platform[user_id] = platform
                return
        finally:
            tmp.unlink(missing_ok=True)

        path = await db_save_cookie(platform, content)
        await message.reply_text(
            f"تم حفظ كوكيز <b>{html.escape(platform)}</b>\n"
            f"المسار: <code>{path}</code>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:
        logger.exception("Cookie file upload failed: %s", exc)
        await message.reply_text(
            "حدث خطأ أثناء حفظ الكوكيز. حاول مرة أخرى.",
        )
        _admin_cookie_selected_platform[user_id] = platform


# ── Broadcast ───────────────────────────────────────────────────────────────

async def adm_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if q is None:
        return
    await q.answer()
    _admin_broadcast_pending.add(OWNER_ID)
    users = await db_get_all_users()
    active = sum(1 for u in users if not u["banned"] and not u["stopped"])
    await q.message.reply_text(
        f"<b>الإذاعة</b>\n"
        "——————————————\n"
        f"سيُرسَل لـ <b>{active}</b> مستخدم نشط.\n\n"
        "أرسل الرسالة الآن — أي تنسيق مدعوم:\n"
        "نص، صور، فيديو، صوت، اقتباسات، <b>عريض</b>، <i>مائل</i>، <code>كود</code>… الخ",
        parse_mode=ParseMode.HTML,
    )


async def _do_broadcast(source_msg: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
    users = await db_get_all_users()
    targets = [u for u in users if not u["banned"] and not u["stopped"] and u["id"] != OWNER_ID]
    total = len(targets)

    prog = await source_msg.reply_text(
        f"جارية الإذاعة...  0 / {total}", parse_mode=ParseMode.HTML
    )
    success = fail = 0
    for i, u in enumerate(targets, 1):
        try:
            await context.bot.copy_message(
                chat_id=u["id"],
                from_chat_id=source_msg.chat_id,
                message_id=source_msg.message_id,
            )
            success += 1
        except Exception:
            fail += 1
        if i % 15 == 0 or i == total:
            try:
                await prog.edit_text(
                    f"جارية الإذاعة...  نجح: {success}  فشل: {fail}  /  {total}",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        await asyncio.sleep(0.04)

    await prog.edit_text(
        f"<b>اكتملت الإذاعة</b>\n"
        "——————————————\n"
        f"أُرسلت   :  {success}\n"
        f"فشل      :  {fail}\n"
        f"المجموع  :  {total}",
        parse_mode=ParseMode.HTML,
    )


# ── Global pre-handler (group -1) ───────────────────────────────────────────
# Runs before every message: registers users, handles admin states, blocks banned/stopped.

async def admin_callback_guard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Block forged admin callback data from every non-owner account."""
    from telegram.ext import ApplicationHandlerStop
    query = update.callback_query
    if query and (query.from_user is None or query.from_user.id != OWNER_ID):
        try:
            await query.answer("غير مصرح لك باستخدام لوحة الإدارة", show_alert=True)
        finally:
            raise ApplicationHandlerStop


async def global_pre_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    from telegram.ext import ApplicationHandlerStop

    # Callback queries identify the clicking user via query.from_user, not message.from_user.
    if update.callback_query is not None:
        query = update.callback_query
        user = query.from_user
        if user is None:
            await query.answer()
            raise ApplicationHandlerStop
        await db_register_user(user)
        if user.id != OWNER_ID and await db_is_banned(user.id):
            await query.answer("عذرًا، تم إيقاف صلاحيتك في هذا البوت.", show_alert=True)
            raise ApplicationHandlerStop
        if user.id != OWNER_ID and await db_is_stopped(user.id):
            await query.answer("الخدمة متوقفة لحسابك حاليًا.", show_alert=True)
            raise ApplicationHandlerStop
        return

    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    user    = msg.from_user
    user_id = user.id

    # Always register / update user record
    await db_register_user(user)

    # ---- Owner admin pending states ----------------------------------------
    if user_id == OWNER_ID:
        # Broadcast
        if user_id in _admin_broadcast_pending:
            _admin_broadcast_pending.discard(user_id)
            await _do_broadcast(msg, context)
            raise ApplicationHandlerStop

        # Cookie file upload — check if waiting for a document upload
        if user_id in _admin_cookie_selected_platform:
            # Let the dedicated TXT document handler receive valid uploads.
            if msg.document and (msg.document.file_name or "").lower().endswith(".txt"):
                return
            await msg.reply_text(
                "أرسِل <b>ملف TXT</b> فقط، وليس نصًا أو وسائط أخرى.",
                parse_mode=ParseMode.HTML,
            )
            raise ApplicationHandlerStop

        # Send message to a specific user
        if user_id in _admin_send_msg_pending:
            target_id = _admin_send_msg_pending.pop(user_id)
            try:
                await context.bot.copy_message(
                    chat_id=target_id,
                    from_chat_id=msg.chat_id,
                    message_id=msg.message_id,
                )
                await msg.reply_text(f"أُرسلت إلى <code>{target_id}</code>", parse_mode=ParseMode.HTML)
            except Exception as e:
                await msg.reply_text(f"فشل الإرسال: {e}")
            raise ApplicationHandlerStop

        return  # owner, no pending state → normal handlers

    # ---- Non-owner: ban / stop checks --------------------------------------
    if await db_is_banned(user_id):
        await msg.reply_text("عذرًا، تم إيقاف صلاحيتك في هذا البوت.")
        raise ApplicationHandlerStop

    if await db_is_stopped(user_id):
        raise ApplicationHandlerStop


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle Telegram API errors gracefully."""
    from telegram.error import TimedOut as TelegramTimedOut, Conflict as TelegramConflict, NetworkError
    error = context.error
    if isinstance(error, TelegramTimedOut):
        logger.warning("Telegram TimedOut — ignoring (temporary connectivity issue)")
        return
    if isinstance(error, TelegramConflict):
        # Another instance is polling — not an error on our side, we will win eventually
        logger.warning("Telegram Conflict — another instance detected, retrying polling")
        return
    if isinstance(error, NetworkError):
        logger.warning("Telegram NetworkError — ignoring: %s", error)
        return
    logger.exception("Unhandled Telegram error", exc_info=error)


def main() -> None:
    token = BOT_TOKEN

    from telegram.request import HTTPXRequest
    request = HTTPXRequest(
        connection_pool_size=16,
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=30.0,
    )
    async def _post_init(app) -> None:
        """Start Pyrogram inside the bot's event loop (correct loop)."""
        if pyro_client is None:
            logger.info("Pyrogram disabled; Bot API upload fallback will be used.")
            return
        try:
            await pyro_client.start()
            logger.info("Pyrogram client started successfully.")
        except Exception as e:
            logger.warning("Pyrogram unavailable; Bot API fallback enabled: %s", e)

    async def _post_shutdown(app) -> None:
        """Stop Pyrogram cleanly on shutdown."""
        try:
            if pyro_client is not None and pyro_client.is_connected:
                await pyro_client.stop()
        except Exception:
            logger.exception("Pyrogram shutdown failed")

    application = (
        Application.builder()
        .token(token)
        .concurrent_updates(8)
        .request(request)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )
    # group -1: runs first — user registration + admin states + ban/stop gate
    application.add_handler(
        MessageHandler(filters.ALL, global_pre_handler), group=-1
    )

    # Admin callback authorization must run before all admin callback handlers.
    application.add_handler(CallbackQueryHandler(admin_callback_guard, pattern=r"^adm_"), group=-2)

    # Commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("audio", audio_command))
    application.add_handler(CommandHandler("M", admin_panel_cmd))

    # Existing inline callbacks
    application.add_handler(CallbackQueryHandler(audio_button,   pattern=r"^audio:"))
    application.add_handler(CallbackQueryHandler(rename_button,  pattern=r"^rename:"))
    application.add_handler(CallbackQueryHandler(cover_button,   pattern=r"^cover:"))
    application.add_handler(CallbackQueryHandler(desc_button,    pattern=r"^desc:"))
    application.add_handler(CallbackQueryHandler(account_button, pattern=r"^account$"))
    application.add_handler(CallbackQueryHandler(back_to_start,  pattern=r"^back_to_start$"))

    # Admin panel callbacks
    application.add_handler(CallbackQueryHandler(adm_back,             pattern=r"^adm_back$"))
    application.add_handler(CallbackQueryHandler(adm_users_page,       pattern=r"^adm_users_\d+$"))
    application.add_handler(CallbackQueryHandler(adm_user_detail,      pattern=r"^adm_user_\d+$"))
    application.add_handler(CallbackQueryHandler(adm_ban,              pattern=r"^adm_ban_\d+$"))
    application.add_handler(CallbackQueryHandler(adm_unban,            pattern=r"^adm_unban_\d+$"))
    application.add_handler(CallbackQueryHandler(adm_stop,             pattern=r"^adm_stop_\d+$"))
    application.add_handler(CallbackQueryHandler(adm_resume,           pattern=r"^adm_resume_\d+$"))
    application.add_handler(CallbackQueryHandler(adm_msg_start,        pattern=r"^adm_msg_\d+$"))
    application.add_handler(CallbackQueryHandler(adm_cookies_panel,    pattern=r"^adm_cookies$"))
    application.add_handler(CallbackQueryHandler(adm_cookie_add_start, pattern=r"^adm_cookie_add$"))
    application.add_handler(CallbackQueryHandler(adm_cookie_platform_selected, pattern=r"^adm_cookie_plat_\w+$"))
    application.add_handler(CallbackQueryHandler(adm_broadcast_start,  pattern=r"^adm_broadcast$"))
    application.add_handler(CallbackQueryHandler(lambda u, c: u.callback_query.answer(), pattern=r"^adm_noop$"))

    # Message handlers — order matters! Document before text
    application.add_handler(MessageHandler(filters.Document.FileExtension("txt"), adm_cookie_document_handler))
    application.add_handler(MessageHandler(filters.PHOTO, photo_message))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, url_message))

    application.add_error_handler(error_handler)

    # Do not force-delete webhooks or consume getUpdates here. A second active
    # instance should fail visibly with Telegram Conflict instead of deleting updates.

    logger.info("Telegram downloader bot is starting.")

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
