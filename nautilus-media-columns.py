#!/usr/bin/env python3

# ruff: noqa: E402
import gi

gi.require_version("Gst", "1.0")
gi.require_version("GstPbutils", "1.0")

import os
import sqlite3
import pathlib
from urllib.parse import unquote
from typing import NamedTuple
from gi.repository import GObject, Nautilus, Gst, GstPbutils, GdkPixbuf


class VideoMetadata(NamedTuple):
    duration: str
    dimensions: str
    framerate: str

# Initialize GStreamer
Gst.init(None)

# Config
VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".avi", ".webm", ".m4v"}
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tiff"}

SUPPORTED_EXTS = VIDEO_EXTS | IMAGE_EXTS

DISCOVER_TIMEOUT_NANOSEC = 2 * Gst.SECOND

CACHE_DIR = os.path.join(os.path.expanduser("~/.cache"), "nautilus-media-columns")
CACHE_DB = os.path.join(CACHE_DIR, "media.sqlite3")


# Formatters
def _fmt_duration_nanosec(ns: int) -> str:
    if not isinstance(ns, int) or ns <= 0:
        return ""
    total = int(round(ns / 1_000_000_000))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _fmt_framerate_ratio(num: int, den: int) -> str:
    if not num or not den:
        return ""
    v = num / den
    return f"{v:.3f}".rstrip("0").rstrip(".")


# Persistent cache (SQLite)
def _open_cache_db():
    pathlib.Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(CACHE_DB, timeout=0.2)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS cache(
        path TEXT PRIMARY KEY,
        mtime_ns INTEGER NOT NULL,
        size INTEGER NOT NULL,
        dims TEXT,
        dur TEXT,
        fps TEXT
      )
    """)
    return conn


def _cache_get(path: str, mtime_ns: int, size: int):
    try:
        conn = _open_cache_db()
        row = conn.execute(
            "SELECT dims,dur,fps FROM cache WHERE path=? AND mtime_ns=? AND size=?",
            (path, mtime_ns, size),
        ).fetchone()
        conn.close()
        return row  # (dims, dur, fps) or None
    except sqlite3.Error:
        return None


def _cache_put(path: str, mtime_ns: int, size: int, dimensions: str, duration: str, framerate: str) -> None:
    try:
        conn = _open_cache_db()
        conn.execute(
            "INSERT OR REPLACE INTO cache(path,mtime_ns,size,dims,dur,fps) VALUES(?,?,?,?,?,?)",
            (path, mtime_ns, size, dimensions, duration, framerate),
        )
        conn.commit()
        conn.close()
    except sqlite3.Error:
        pass


# Probes
def _probe_image(path: str) -> str:
    # reads headers only (no full decode)
    try:
        _, width, height = GdkPixbuf.Pixbuf.get_file_info(path)
        if width and height:
            return f"{width}x{height}"
    except Exception:
        # GI/C boundary: GdkPixbuf may raise non-deterministic exceptions
        # across versions/codecs; failures must not break Nautilus.
        pass
    return ""


def _probe_video(path: str) -> VideoMetadata:
    """Probe a video file for duration, dimensions, and framerate.

    Uses GStreamer discoverer. Failures are non-fatal and return empty fields
    to avoid breaking Nautilus.
    """
    uri = Gst.filename_to_uri(path)
    discoverer = GstPbutils.Discoverer.new(DISCOVER_TIMEOUT_NANOSEC)

    try:
        info = discoverer.discover_uri(uri)
    except Exception:
        # GStreamer GI bindings can raise inconsistent exceptions.
        # Metadata is optional; never fail the file manager.
        return VideoMetadata("", "", "")

    dur_nanosec = info.get_duration()
    duration = _fmt_duration_nanosec(dur_nanosec)

    dimensions = ""
    framerate = ""

    try:
        streams = info.get_video_streams()
    except Exception:
        # GI/C boundary: GStreamer introspection can vary by version/plugins.
        # Treat missing stream info as "no video streams" and keep Nautilus stable.
        streams = []

    for stream in streams:
        try:
            width = stream.get_width()
            height = stream.get_height()
            if width > 0 and height > 0:
                dimensions = f"{width}x{height}"
        except Exception:
            # GI/C boundary: stream fields can be unavailable for some demuxers/codecs.
            pass

        try:
            framerate_num = stream.get_framerate_num()
            framerate_denom = stream.get_framerate_denom()
            if framerate_num and framerate_denom:
                framerate = _fmt_framerate_ratio(framerate_num, framerate_denom)
        except Exception:
            # GI/C boundary: framerate access may raise across plugin/ABI differences.
            pass

        break

    return VideoMetadata(duration, dimensions, framerate)


# Nautilus extension
class MediaColumns(GObject.GObject, Nautilus.ColumnProvider, Nautilus.InfoProvider):
    def __init__(self):
        super().__init__()

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
                name="NautilusPython::media_fps",
                attribute="media_fps",
                label="FPS",
                description="Video framerate",
            ),
        ]

    def update_file_info(self, file: Nautilus.FileInfo) -> None:
        if file.get_uri_scheme() != "file":
            return

        path = unquote(file.get_uri()[7:])
        _, ext = os.path.splitext(path)
        ext = ext.lower()

        file.add_string_attribute("media_dimensions", "")
        file.add_string_attribute("media_duration", "")
        file.add_string_attribute("media_fps", "")

        if ext not in SUPPORTED_EXTS or not os.path.isfile(path):
            return

        try:
            stat_result = os.stat(path)
            mtime_ns = getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1e9))
            size = int(stat_result.st_size)
        except OSError:
            return

        hit = _cache_get(path, mtime_ns, size)
        if hit is not None:
            dimensions, duration, framerate = hit
            if dimensions:
                file.add_string_attribute("media_dimensions", dimensions)
            if duration:
                file.add_string_attribute("media_duration", duration)
            if framerate:
                file.add_string_attribute("media_fps", framerate)
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
            file.add_string_attribute("media_fps", framerate)
