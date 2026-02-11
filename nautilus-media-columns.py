#!/usr/bin/env python3

import atexit
import os
import pathlib
import sqlite3
import time
from typing import NamedTuple, Optional, Tuple

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GstPbutils", "1.0")
from gi.repository import GObject, Nautilus, Gst, GstPbutils, GLib

# Image backends:
# - Prefer GExiv2: metadata-only, orientation-aware, matches Nautilus internals
# - Fallback to GdkPixbuf header probe to avoid hard dependencies or extension failure
HAVE_GEXIV2 = False
try:
    gi.require_version("GExiv2", "0.10")
    from gi.repository import GExiv2

    HAVE_GEXIV2 = True
except (ValueError, ImportError):
    GExiv2 = None

try:
    gi.require_version("GdkPixbuf", "2.0")
    from gi.repository import GdkPixbuf
except ImportError:
    GdkPixbuf = None


# Logging
import logging

_LOG_DOMAIN = "nautilus-media-columns"
JOURNAL_AVAILABLE = False

_debug_env = (GLib.getenv("G_MESSAGES_DEBUG") or "").strip()
_DEBUG_ENABLED = _debug_env == "all" or _LOG_DOMAIN in _debug_env.split(",")

try:
    from systemd.journal import JournalHandler

    _journal_logger = logging.getLogger(_LOG_DOMAIN)
    _journal_logger.setLevel(logging.DEBUG)

    journal_handler = JournalHandler(SYSLOG_IDENTIFIER=_LOG_DOMAIN)
    journal_handler.setLevel(logging.DEBUG)
    journal_handler.setFormatter(
        logging.Formatter("[%(levelname)s] %(message)s")
    )

    _journal_logger.addHandler(journal_handler)

    JOURNAL_AVAILABLE = True
except ImportError:
    pass


def _journal(level, msg):
    if JOURNAL_AVAILABLE and (level != logging.DEBUG or _DEBUG_ENABLED):
        _journal_logger.log(level, msg)


def _glib_log(level_flag, msg):
    GLib.log_default_handler(_LOG_DOMAIN, level_flag, msg, None)


def log_debug(msg):
    if not _DEBUG_ENABLED:
        return
    _glib_log(GLib.LogLevelFlags.LEVEL_DEBUG, msg)
    _journal(logging.DEBUG, msg)


def log_info(msg):
    _glib_log(GLib.LogLevelFlags.LEVEL_MESSAGE, msg)
    _journal(logging.INFO, msg)


def log_warn(msg):
    _glib_log(GLib.LogLevelFlags.LEVEL_WARNING, msg)
    _journal(logging.WARNING, msg)


def log_error(msg):
    _glib_log(GLib.LogLevelFlags.LEVEL_CRITICAL, msg)
    _journal(logging.ERROR, msg)


# Initialize GStreamer
Gst.init(None)

# Config
VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".avi", ".webm", ".m4v"}
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tiff"}
SUPPORTED_EXTS = VIDEO_EXTS | IMAGE_EXTS

DISCOVER_TIMEOUT_NS = 2 * Gst.SECOND

DB_TIMEOUT_S = 2.0
DB_BUSY_TIMEOUT_MS = 2000
CACHE_TTL_DAYS = 90
CACHE_MAX_ROWS = 50000
CACHE_DIR = os.path.join(os.path.expanduser("~/.cache"), "nautilus-media-columns")
CACHE_DB = os.path.join(CACHE_DIR, "media.sqlite3")

# In-process cache to avoid repeated SQLite hits while Nautilus is running
_MEM_CACHE_MAX = 2048

COMMIT_EVERY = 50

# Globals
_DISCOVERER = None
IMAGE_BACKEND = None
_MEM_CACHE = {}
_DB = None
_pending_writes = 0


if HAVE_GEXIV2 and GExiv2 is not None:
    IMAGE_BACKEND = "GExiv2"
elif GdkPixbuf is not None:
    IMAGE_BACKEND = "GdkPixbuf"

if IMAGE_BACKEND:
    log_info(f"Image backend selected: {IMAGE_BACKEND}")
else:
    log_warn("No image backend available; image dimensions will be empty")


# Database
def _get_db() -> sqlite3.Connection:
    """Open the cache database if needed and return the connection."""
    global _DB
    if _DB is not None:
        return _DB

    pathlib.Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
    log_info(f"Opening cache DB: {CACHE_DB}")
    conn = sqlite3.connect(CACHE_DB, timeout=DB_TIMEOUT_S)
    try:
        conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error as e:
        log_debug(f"SQLite PRAGMA failed (non-fatal): {type(e).__name__}: {e}")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cache(
          path TEXT PRIMARY KEY,
          mtime_ns INTEGER NOT NULL,
          size INTEGER NOT NULL,
          dims TEXT,
          dur TEXT,
          fps TEXT,
          last_access_ns INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(cache)").fetchall()}
        if "last_access_ns" not in cols:
            log_info("Upgrading cache schema: adding last_access_ns")
            conn.execute("ALTER TABLE cache ADD COLUMN last_access_ns INTEGER NOT NULL DEFAULT 0")
            conn.commit()
    except sqlite3.Error as e:
        log_debug(f"SQLite schema check/upgrade failed (non-fatal): {type(e).__name__}: {e}")

    _DB = conn
    _cache_prune(conn)
    return _DB


def _flush_and_close_db() -> None:
    """Write any pending cache data and close the database."""
    global _DB, _pending_writes
    if _DB is None:
        return
    try:
        if _pending_writes:
            log_debug(f"Flushing pending DB writes: {_pending_writes}")
            _DB.commit()
            _pending_writes = 0
    except sqlite3.Error as e:
        log_warn(f"SQLite commit failed (non-fatal): {type(e).__name__}: {e}")
    try:
        _DB.close()
        log_debug("Closed cache DB")
    except sqlite3.Error as e:
        log_warn(f"SQLite close failed (non-fatal): {type(e).__name__}: {e}")
    _DB = None


atexit.register(_flush_and_close_db)


# Cache
def _mem_cache_put(
    path: str,
    mtime_ns: int,
    size: int,
    dimensions: str,
    duration: str,
    framerate: str,
) -> None:
    """Save a cache entry in memory for fast reuse."""
    _MEM_CACHE[path] = (mtime_ns, size, dimensions, duration, framerate)
    if len(_MEM_CACHE) > _MEM_CACHE_MAX:
        # FIFO eviction (dict keeps insertion order in Python 3.7+)
        evicted = next(iter(_MEM_CACHE))
        _MEM_CACHE.pop(evicted, None)
        log_debug(f"Mem-cache evicted: {evicted}")


def _cache_get(path: str, mtime_ns: int, size: int) -> Optional[Tuple[str, str, str]]:
    """Get cached values for a file if they are still valid."""
    global _pending_writes
    cached = _MEM_CACHE.get(path)
    if cached and cached[0] == mtime_ns and cached[1] == size:
        log_debug(f"Cache hit (mem): {path}")
        return cached[2], cached[3], cached[4]

    try:
        conn = _get_db()
        row = conn.execute(
            "SELECT mtime_ns,size,dims,dur,fps FROM cache WHERE path=?",
            (path,),
        ).fetchone()
    except sqlite3.Error as e:
        log_debug(f"Cache query failed (non-fatal): {type(e).__name__}: {e}")
        return None

    if not row:
        log_debug(f"Cache miss (db): {path}")
        return None

    if int(row[0]) != mtime_ns or int(row[1]) != size:
        log_debug(f"Cache stale -> delete: {path}")
        try:
            conn.execute("DELETE FROM cache WHERE path=?", (path,))
        except sqlite3.Error as e:
            log_debug(f"Cache stale delete failed (non-fatal): {type(e).__name__}: {e}")
        return None

    dimensions, duration, framerate = row[2] or "", row[3] or "", row[4] or ""
    try:
        conn.execute(
            "UPDATE cache SET last_access_ns=? WHERE path=?",
            (time.time_ns(), path),
        )
        _pending_writes += 1
        if _pending_writes >= COMMIT_EVERY:
            log_debug(f"DB commit threshold reached: {_pending_writes}")
            conn.commit()
            _pending_writes = 0
    except sqlite3.Error as e:
        log_debug(f"Cache last_access update failed (non-fatal): {type(e).__name__}: {e}")

    _mem_cache_put(path, mtime_ns, size, dimensions, duration, framerate)
    log_debug(f"Cache hit (db): {path}")
    return dimensions, duration, framerate


def _cache_put(
    path: str,
    mtime_ns: int,
    size: int,
    dimensions: str,
    duration: str,
    framerate: str,
) -> None:
    """Save media metadata to the cache."""
    global _pending_writes
    _mem_cache_put(path, mtime_ns, size, dimensions, duration, framerate)

    try:
        conn = _get_db()
        conn.execute(
            "INSERT OR REPLACE INTO cache(path,mtime_ns,size,dims,dur,fps,last_access_ns) VALUES(?,?,?,?,?,?,?)",
            (path, mtime_ns, size, dimensions, duration, framerate, time.time_ns()),
        )
        _pending_writes += 1
        if _pending_writes >= COMMIT_EVERY:
            log_debug(f"DB commit threshold reached: {_pending_writes}")
            conn.commit()
            _pending_writes = 0
    except sqlite3.Error as e:
        log_debug(f"Cache put failed (non-fatal): {type(e).__name__}: {e}")


def _cache_prune(conn: sqlite3.Connection) -> None:
    """Remove stale or excess cache entries based on age and total size limits."""
    global _pending_writes
    now = time.time_ns()
    cutoff = now - (CACHE_TTL_DAYS * 24 * 60 * 60 * 1_000_000_000)
    deleted = 0
    try:
        delete_cursor = conn.execute(
            "DELETE FROM cache WHERE last_access_ns > 0 AND last_access_ns < ?",
            (cutoff,),
        )
        deleted += max(0, delete_cursor.rowcount or 0)
        delete_cursor = conn.execute(
            "DELETE FROM cache WHERE rowid IN ("
            "SELECT rowid FROM cache ORDER BY last_access_ns ASC LIMIT ("
            "SELECT MAX(0, COUNT(*)-?) FROM cache))",
            (CACHE_MAX_ROWS,),
        )
        deleted += max(0, delete_cursor.rowcount or 0)
        if deleted:
            log_info(f"Cache pruned: {deleted} rows removed")
            _pending_writes += 1
        else:
            log_debug("Cache prune: no rows removed")
    except sqlite3.Error as e:
        log_debug(f"Cache prune failed (non-fatal): {type(e).__name__}: {e}")


# Formatting
def _fmt_duration_ns(duration_ns: int) -> str:
    """Turn a nanosecond duration into a readable time string."""
    try:
        duration_ns = int(duration_ns)
    except Exception:
        return ""
    if duration_ns <= 0:
        return ""
    total_seconds = duration_ns // int(Gst.SECOND)
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _fmt_framerate_ratio(numerator: int, denominator: int) -> str:
    """Turn a framerate ratio into a whole-number FPS string."""
    try:
        numerator = int(numerator)
        denominator = int(denominator)
    except Exception:
        return ""
    if denominator == 0 or numerator == 0:
        return ""
    framerate = numerator / denominator
    return str(int(round(framerate)))


# Probers
def _probe_image(path: str) -> str:
    """Read image dimensions without fully decoding the file."""
    log_debug(f"Probe image: {path} (backend={IMAGE_BACKEND or 'none'})")
    if IMAGE_BACKEND == "GExiv2":
        try:
            metadata = GExiv2.Metadata.new()
            metadata.open_path(path)
            width = int(metadata.get_pixel_width() or 0)
            height = int(metadata.get_pixel_height() or 0)
            if width <= 0 or height <= 0:
                return ""
            if metadata.get_orientation() in (
                GExiv2.Orientation.ROT_90,
                GExiv2.Orientation.ROT_270,
                GExiv2.Orientation.ROT_90_HFLIP,
                GExiv2.Orientation.ROT_90_VFLIP,
            ):
                width, height = height, width
                log_debug("Image orientation swap applied")
            return f"{width}x{height}"
        except (GLib.Error, ValueError) as e:
            log_debug(f"GExiv2 probe failed (non-fatal): {type(e).__name__}: {e}")
            return ""

    if IMAGE_BACKEND == "GdkPixbuf":
        try:
            _, width, height = GdkPixbuf.Pixbuf.get_file_info(path)
            return f"{width}x{height}" if width > 0 and height > 0 else ""
        except GLib.Error as e:
            log_debug(f"GdkPixbuf probe failed (non-fatal): {type(e).__name__}: {e}")
            return ""

    return ""


def _get_discoverer():
    """Create and reuse a GStreamer discoverer when needed."""
    global _DISCOVERER
    if _DISCOVERER is None:
        try:
            _DISCOVERER = GstPbutils.Discoverer.new(DISCOVER_TIMEOUT_NS)
            log_info("Created GStreamer discoverer")
        except Exception as e:
            log_warn(f"Failed to create GStreamer discoverer: {type(e).__name__}: {e}")
            _DISCOVERER = None
    return _DISCOVERER


class VideoMetadata(NamedTuple):
    """Container for probed video metadata returned by _probe_video()."""

    duration: str
    dimensions: str
    framerate: str


def _probe_video(path: str) -> VideoMetadata:
    """Read video duration, size, and framerate using GStreamer."""
    log_debug(f"Probe video: {path}")
    uri = Gst.filename_to_uri(path)
    discoverer = _get_discoverer()
    if discoverer is None:
        return VideoMetadata("", "", "")

    start_ns = time.time_ns()
    try:
        info = discoverer.discover_uri(uri)
    except Exception as e:
        # GStreamer GI bindings can raise inconsistent exceptions.
        # Metadata is optional; never fail the file manager.
        log_warn(f"GStreamer discover failed (non-fatal): {type(e).__name__}: {e}")
        return VideoMetadata("", "", "")
    finally:
        # Always record probe timing (runs even if discover_uri fails or returns early)
        elapsed_ms = (time.time_ns() - start_ns) / 1_000_000
        log_debug(f"GStreamer discover time: {elapsed_ms:.1f}ms")

    duration = _fmt_duration_ns(info.get_duration())

    dimensions = ""
    framerate = ""

    try:
        streams = info.get_video_streams()
    except Exception as e:
        # GI/C boundary: GStreamer introspection can vary by version/plugins.
        # Treat missing stream info as "no video streams" and keep Nautilus stable.
        log_debug(f"get_video_streams failed (non-fatal): {type(e).__name__}: {e}")
        streams = []

    for stream in streams:
        try:
            width = stream.get_width()
            height = stream.get_height()
            if width > 0 and height > 0:
                dimensions = f"{width}x{height}"
        except Exception as e:
            # GI/C boundary: stream fields can be unavailable for some demuxers/codecs.
            log_debug(f"Stream dimension read failed (non-fatal): {type(e).__name__}: {e}")

        try:
            framerate_num = stream.get_framerate_num()
            framerate_denom = stream.get_framerate_denom()
            if framerate_num and framerate_denom:
                framerate = _fmt_framerate_ratio(framerate_num, framerate_denom)
        except Exception as e:
            # GI/C boundary: framerate access may raise across plugin/ABI differences.
            log_debug(f"Stream framerate read failed (non-fatal): {type(e).__name__}: {e}")

        break

    log_debug(f"Video probed -> dur='{duration}' dims='{dimensions}' fps='{framerate}'")
    return VideoMetadata(duration, dimensions, framerate)


class MediaColumns(GObject.GObject, Nautilus.ColumnProvider, Nautilus.InfoProvider):
    """Extension: Adds media metadata columns to Nautilus."""

    def __init__(self):
        super().__init__()
        log_info("nautilus-media-columns extension initialized")

    def get_columns(self):
        return [
            Nautilus.Column(
                name="NautilusPython::media_dimensions",
                attribute="media_dimensions",
                label="Dimensions",
                description="Image/video dimensions",
            ),
            Nautilus.Column(
                name="NautilusPython::media_duration",
                attribute="media_duration",
                label="Duration",
                description="Video duration",
            ),
            Nautilus.Column(
                name="NautilusPython::media_framerate",
                attribute="media_framerate",
                label="FPS",
                description="Video framerate (FPS)",
            ),
        ]

    # Called once per file by Nautilus
    def update_file_info(self, file: Nautilus.FileInfo, *_unused) -> None:
        """Fill in media columns for one file."""

        # Only handle local filesystem URIs (skip smb://, trash://, etc.)
        if file.get_uri_scheme() != "file":
            return

        # Resolve Nautilus FileInfo to a Gio.File
        location = file.get_location()
        if location is None:
            return  # No resolvable location (virtual/search results)

        # Convert Gio.File to a POSIX path
        path = location.get_path()
        if not path:
            return  # Not backed by a real local path

        # Only process regular files (excludes directories, symlinks to dirs, devices, etc.)
        if not os.path.isfile(path):
            return

        _, ext = os.path.splitext(path)
        ext = ext.lower()

        if ext not in SUPPORTED_EXTS:
            if ext == "":
                return  # skip extensionless files quietly
            log_debug(f"Skip: unsupported ext '{ext}' ({path})")
            return

        file.add_string_attribute("media_dimensions", "")
        file.add_string_attribute("media_duration", "")
        file.add_string_attribute("media_framerate", "")

        try:
            stat_result = os.stat(path)
            mtime_ns = getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1e9))
            size = int(stat_result.st_size)
        except OSError:
            return

        cached_entry = _cache_get(path, mtime_ns, size)
        if cached_entry is not None:
            dimensions, duration, framerate = cached_entry
            if dimensions:
                file.add_string_attribute("media_dimensions", dimensions)
            if duration:
                file.add_string_attribute("media_duration", duration)
            if framerate:
                file.add_string_attribute("media_framerate", framerate)
            return

        dimensions = ""
        duration = ""
        framerate = ""

        if ext in IMAGE_EXTS:
            dimensions = _probe_image(path)

        if ext in VIDEO_EXTS:
            duration, video_dimensions, framerate = _probe_video(path)
            if video_dimensions:
                dimensions = video_dimensions

        _cache_put(path, mtime_ns, size, dimensions, duration, framerate)

        if dimensions:
            file.add_string_attribute("media_dimensions", dimensions)
        if duration:
            file.add_string_attribute("media_duration", duration)
        if framerate:
            file.add_string_attribute("media_framerate", framerate)
