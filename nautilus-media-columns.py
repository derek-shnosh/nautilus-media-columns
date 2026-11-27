"""Nautilus extension adding Dimensions, Duration, and FPS columns for media files."""

import atexit
import functools
import logging
import pathlib
import sqlite3
import time
from typing import NamedTuple

import gi
from gi.repository import GLib, GObject, Nautilus

# Video backend: optional; without it, only image columns are populated
HAVE_GST = False
try:
    gi.require_version("Gst", "1.0")
    gi.require_version("GstPbutils", "1.0")
    from gi.repository import Gst, GstPbutils

    HAVE_GST = True
except (ValueError, ImportError):
    Gst = GstPbutils = None

# Image backends:
# - Prefer GExiv2: metadata-only, orientation-aware, matches Nautilus internals
# - Fallback to GdkPixbuf header probe to avoid hard dependencies or extension failure
try:
    gi.require_version("GExiv2", "0.10")
    from gi.repository import GExiv2
except (ValueError, ImportError):
    GExiv2 = None

try:
    gi.require_version("GdkPixbuf", "2.0")
    from gi.repository import GdkPixbuf
except (ValueError, ImportError):
    GdkPixbuf = None


# Logging
_LOG_DOMAIN = "nautilus-media-columns"

_debug_env = (GLib.getenv("G_MESSAGES_DEBUG") or "").strip()
_DEBUG_ENABLED = _debug_env == "all" or _LOG_DOMAIN in _debug_env.split(",")

try:
    from systemd.journal import JournalHandler
except ImportError:
    _journal_logger = None
else:
    _journal_logger = logging.getLogger(_LOG_DOMAIN)
    _journal_logger.setLevel(logging.DEBUG)
    _handler = JournalHandler(SYSLOG_IDENTIFIER=_LOG_DOMAIN)
    _handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    _journal_logger.addHandler(_handler)


def _journal(level, msg):
    if _journal_logger:
        _journal_logger.log(level, msg)


def _glib_log(level_flag, msg):
    GLib.log_default_handler(_LOG_DOMAIN, level_flag, msg, None)


def log_debug(msg):
    """Log a debug message (only when G_MESSAGES_DEBUG enables this domain)."""
    if not _DEBUG_ENABLED:
        return
    _glib_log(GLib.LogLevelFlags.LEVEL_DEBUG, msg)
    _journal(logging.DEBUG, msg)


def log_info(msg):
    """Log an informational message."""
    _glib_log(GLib.LogLevelFlags.LEVEL_MESSAGE, msg)
    _journal(logging.INFO, msg)


def log_warn(msg):
    """Log a warning message."""
    _glib_log(GLib.LogLevelFlags.LEVEL_WARNING, msg)
    _journal(logging.WARNING, msg)


def log_error(msg):
    """Log an error message."""
    _glib_log(GLib.LogLevelFlags.LEVEL_CRITICAL, msg)
    _journal(logging.ERROR, msg)


# Initialize GStreamer
if HAVE_GST:
    Gst.init(None)
else:
    log_warn("GstPbutils unavailable; video columns disabled")

# Config
VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".avi", ".webm", ".m4v"}
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}
SUPPORTED_EXTS = IMAGE_EXTS | (VIDEO_EXTS if HAVE_GST else set())

_NS_PER_SEC = 1_000_000_000
DISCOVER_TIMEOUT_NS = 2 * _NS_PER_SEC

DB_TIMEOUT_S = 2.0
DB_BUSY_TIMEOUT_MS = 2000
CACHE_TTL_DAYS = 90
CACHE_MAX_ROWS = 50000
CACHE_DIR = pathlib.Path.home() / ".cache" / "nautilus-media-columns"
CACHE_DB = CACHE_DIR / "media.sqlite3"

# In-process cache to avoid repeated SQLite hits while Nautilus is running
_MEM_CACHE_MAX = 2048

COMMIT_EVERY = 50
COMMIT_DELAY_S = 2

IMAGE_BACKEND = "GExiv2" if GExiv2 else "GdkPixbuf" if GdkPixbuf else None

if IMAGE_BACKEND:
    log_info(f"Image backend selected: {IMAGE_BACKEND}")
else:
    log_warn("No image backend available; image dimensions will be empty")


class MediaInfo(NamedTuple):
    """Probed media metadata; empty strings mean unknown."""

    dimensions: str = ""
    duration: str = ""
    framerate: str = ""


class _Cache:
    """SQLite-backed metadata cache with an in-memory front and batched commits."""

    def __init__(self) -> None:
        self._conn: sqlite3.Connection | None = None
        self._mem: dict[str, tuple[int, int, MediaInfo]] = {}
        self._pending = 0
        self._commit_source_id = 0

    def get(self, path: str, mtime_ns: int, size: int) -> MediaInfo | None:
        """Return cached metadata for a file if its mtime and size still match."""
        cached = self._mem.get(path)
        if cached and cached[0] == mtime_ns and cached[1] == size:
            log_debug(f"Cache hit (mem): {path}")
            return cached[2]

        try:
            conn = self._db()
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
        if row[0] != mtime_ns or row[1] != size:
            log_debug(f"Cache stale: {path}")
            return None

        info = MediaInfo(row[2] or "", row[3] or "", row[4] or "")
        try:
            conn.execute(
                "UPDATE cache SET last_access_ns=? WHERE path=?",
                (time.time_ns(), path),
            )
            self._note_write()
        except sqlite3.Error as e:
            log_debug(f"Cache last_access update failed (non-fatal): {type(e).__name__}: {e}")

        self._remember(path, mtime_ns, size, info)
        log_debug(f"Cache hit (db): {path}")
        return info

    def put(self, path: str, mtime_ns: int, size: int, info: MediaInfo) -> None:
        """Store metadata for a file."""
        self._remember(path, mtime_ns, size, info)
        try:
            self._db().execute(
                "INSERT OR REPLACE INTO cache(path,mtime_ns,size,dims,dur,fps,last_access_ns) VALUES(?,?,?,?,?,?,?)",
                (path, mtime_ns, size, *info, time.time_ns()),
            )
            self._note_write()
        except sqlite3.Error as e:
            log_debug(f"Cache put failed (non-fatal): {type(e).__name__}: {e}")

    def close(self) -> None:
        """Commit pending writes and close the database (best effort)."""
        if self._conn is None:
            return
        self._commit()
        try:
            self._conn.close()
            log_debug("Closed cache DB")
        except sqlite3.Error as e:
            log_warn(f"SQLite close failed (non-fatal): {type(e).__name__}: {e}")
        self._conn = None

    def _db(self) -> sqlite3.Connection:
        """Open the database on first use and return the connection."""
        if self._conn is not None:
            return self._conn

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
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

        self._conn = conn
        self._prune()
        return conn

    def _prune(self) -> None:
        """Remove entries past the TTL, then trim to CACHE_MAX_ROWS by least-recent access."""
        cutoff = time.time_ns() - (CACHE_TTL_DAYS * 24 * 60 * 60 * _NS_PER_SEC)
        deleted = 0
        try:
            cur = self._conn.execute(
                "DELETE FROM cache WHERE last_access_ns < ?",
                (cutoff,),
            )
            deleted += cur.rowcount
            cur = self._conn.execute(
                "DELETE FROM cache WHERE rowid IN ("
                "SELECT rowid FROM cache ORDER BY last_access_ns ASC LIMIT ("
                "SELECT MAX(0, COUNT(*)-?) FROM cache))",
                (CACHE_MAX_ROWS,),
            )
            deleted += cur.rowcount
        except sqlite3.Error as e:
            log_debug(f"Cache prune failed (non-fatal): {type(e).__name__}: {e}")
            return
        if deleted:
            log_info(f"Cache pruned: {deleted} rows removed")
            self._note_write()
        else:
            log_debug("Cache prune: no rows removed")

    def _remember(self, path: str, mtime_ns: int, size: int, info: MediaInfo) -> None:
        """Keep an entry in memory, evicting the oldest past _MEM_CACHE_MAX."""
        self._mem[path] = (mtime_ns, size, info)
        if len(self._mem) > _MEM_CACHE_MAX:
            evicted = next(iter(self._mem))
            del self._mem[evicted]
            log_debug(f"Mem-cache evicted: {evicted}")

    def _note_write(self) -> None:
        """Count a pending write; commit at the threshold, else schedule a delayed commit."""
        self._pending += 1
        if self._pending >= COMMIT_EVERY:
            if self._commit_source_id:
                GLib.source_remove(self._commit_source_id)
            self._commit()
        elif not self._commit_source_id:
            self._commit_source_id = GLib.timeout_add_seconds(COMMIT_DELAY_S, self._commit)

    def _commit(self) -> bool:
        """Commit pending writes; also used as the GLib timeout callback."""
        self._commit_source_id = 0
        if self._conn is not None and self._pending:
            try:
                self._conn.commit()
                log_debug(f"Commit: {self._pending} pending writes")
                self._pending = 0
            except sqlite3.Error as e:
                log_warn(f"SQLite commit failed (non-fatal): {type(e).__name__}: {e}")
        return GLib.SOURCE_REMOVE


_CACHE = _Cache()
atexit.register(_CACHE.close)


# Formatting
def _fmt_duration_ns(duration_ns: int) -> str:
    """Turn a nanosecond duration into a readable time string."""
    if duration_ns <= 0:
        return ""
    total_seconds = duration_ns // _NS_PER_SEC
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _fmt_framerate_ratio(numerator: int, denominator: int) -> str:
    """Turn a framerate ratio into a whole-number FPS string."""
    if denominator == 0 or numerator == 0:
        return ""
    return str(round(numerator / denominator))


# Probers
def _probe_image(path: str) -> str:
    """Read image dimensions without fully decoding the file."""
    log_debug(f"Probe image: {path} (backend={IMAGE_BACKEND or 'none'})")
    if IMAGE_BACKEND == "GExiv2":
        try:
            metadata = GExiv2.Metadata.new()
            metadata.open_path(path)
        except GLib.Error as e:
            log_debug(f"GExiv2 probe failed (non-fatal): {type(e).__name__}: {e}")
            return ""
        width = metadata.get_pixel_width()
        height = metadata.get_pixel_height()
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

    if IMAGE_BACKEND == "GdkPixbuf":
        try:
            _, width, height = GdkPixbuf.Pixbuf.get_file_info(path)
        except GLib.Error as e:
            log_debug(f"GdkPixbuf probe failed (non-fatal): {type(e).__name__}: {e}")
            return ""
        return f"{width}x{height}" if width > 0 and height > 0 else ""

    return ""


@functools.cache
def _get_discoverer():
    """Create the GStreamer discoverer once; None if it can't be created."""
    try:
        discoverer = GstPbutils.Discoverer.new(DISCOVER_TIMEOUT_NS)
    except GLib.Error as e:
        log_warn(f"Failed to create GStreamer discoverer: {type(e).__name__}: {e}")
        return None
    log_info("Created GStreamer discoverer")
    return discoverer


def _probe_video(path: str) -> MediaInfo:
    """Read video duration, size, and framerate using GStreamer."""
    log_debug(f"Probe video: {path}")
    uri = Gst.filename_to_uri(path)
    discoverer = _get_discoverer()
    if discoverer is None:
        return MediaInfo()

    start_ns = time.time_ns()
    try:
        info = discoverer.discover_uri(uri)
    except GLib.Error as e:
        # Metadata is optional; never fail the file manager.
        log_warn(f"GStreamer discover failed (non-fatal): {type(e).__name__}: {e}")
        return MediaInfo()
    finally:
        # Always record probe timing (runs even if discover_uri fails or returns early)
        elapsed_ms = (time.time_ns() - start_ns) / 1_000_000
        log_debug(f"GStreamer discover time: {elapsed_ms:.1f}ms")

    duration = _fmt_duration_ns(info.get_duration())

    dimensions = ""
    framerate = ""

    streams = info.get_video_streams()
    if streams:
        stream = streams[0]
        width, height = stream.get_width(), stream.get_height()
        if width > 0 and height > 0:
            dimensions = f"{width}x{height}"
        num, denom = stream.get_framerate_num(), stream.get_framerate_denom()
        if num and denom:
            framerate = _fmt_framerate_ratio(num, denom)

    log_debug(f"Video probed -> dur='{duration}' dims='{dimensions}' fps='{framerate}'")
    return MediaInfo(dimensions=dimensions, duration=duration, framerate=framerate)


def _local_path(file: Nautilus.FileInfo) -> pathlib.Path | None:
    """Return the file's local path if it is a supported regular media file."""
    if file.get_uri_scheme() != "file":
        return None  # skip smb://, trash://, etc.
    location = file.get_location()
    raw = location.get_path() if location else None
    if not raw:
        return None  # virtual/search results, or no local backing
    path = pathlib.Path(raw)
    if not path.is_file():
        return None
    ext = path.suffix.lower()
    if ext not in SUPPORTED_EXTS:
        if ext:
            log_debug(f"Skip: unsupported ext '{ext}' ({path})")
        return None
    return path


def _probe(path: pathlib.Path) -> MediaInfo:
    """Probe a file for media metadata based on its extension."""
    if path.suffix.lower() in VIDEO_EXTS:
        return _probe_video(str(path))
    return MediaInfo(dimensions=_probe_image(str(path)))


def _apply(file: Nautilus.FileInfo, info: MediaInfo) -> None:
    """Set the media column attributes on a Nautilus file."""
    file.add_string_attribute("media_dimensions", info.dimensions)
    file.add_string_attribute("media_duration", info.duration)
    file.add_string_attribute("media_framerate", info.framerate)


def _fill_columns(file: Nautilus.FileInfo) -> None:
    """Resolve, probe (or read from cache), and apply media columns for one file."""
    path = _local_path(file)
    if path is None:
        return
    try:
        stat_result = path.stat()
    except OSError:
        return
    key = str(path)
    mtime_ns, size = stat_result.st_mtime_ns, stat_result.st_size
    info = _CACHE.get(key, mtime_ns, size)
    if info is None:
        info = _probe(path)
        _CACHE.put(key, mtime_ns, size, info)
    _apply(file, info)


class MediaColumns(GObject.GObject, Nautilus.ColumnProvider, Nautilus.InfoProvider):
    """Extension: Adds media metadata columns to Nautilus."""

    def __init__(self):
        """Initialize the extension."""
        super().__init__()
        log_info("nautilus-media-columns extension initialized")

    def get_columns(self):
        """Return the column definitions shown in Nautilus list view."""
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
        """Fill in media columns for one file; never let an error escape into Nautilus."""
        try:
            _fill_columns(file)
        except Exception as e:  # noqa: BLE001 - extension boundary
            log_error(f"update_file_info failed: {type(e).__name__}: {e}")
