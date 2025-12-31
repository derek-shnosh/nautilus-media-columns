# Nautilus Media Columns

Adds **Dimensions**, **Duration**, and **FPS** (Framerate) columns to Nautilus (GNOME Files) list view for images and videos — cached for performance and fully GNOME-native. Nautilus already has access to this metadata, but does not expose it in list view.

<p align="center">
  <img src="assets/media-columns.png" width="80%" alt="Media Columns Example">
</p>

***

## Features

- **Dimensions**
  - Images: fast header read (no full decode)
  - Videos: via GStreamer discoverer
- **Duration** (videos)
- **FPS** (videos)
- **Persistent cache** (SQLite, keyed by path + mtime + size)
- **No additional dependencies required** beyond a standard GNOME desktop environment — this extension uses GNOME’s existing GStreamer and GdkPixbuf libraries already shipped with GNOME; there is no `ffmpeg` dependency

---

## Requirements

- Python 3
- GStreamer (installed by default on GNOME desktops)
- Nautilus with **nautilus-python (API 4.0)**

```bash
# If nautilus-python is not installed...
sudo apt update && sudo apt install python3-nautilus -y
```

Tested on:
- GNOME / Nautilus 48
- Ubuntu 25.04

Expected to work on:
- Nautilus versions that ship nautilus-python API 4.0 (GNOME 45+)

---

## Installation (per user)

```bash
mkdir -p ~/.local/share/nautilus-python/extensions
wget https://raw.githubusercontent.com/derek-shnosh/nautilus-media-columns/main/nautilus-media-columns.py \
  -O ~/.local/share/nautilus-python/extensions/nautilus-media-columns.py
nautilus -q
```

Restart Nautilus, switch to **List View**, open **Visible Columns**, and enable:
- Dimensions
- Duration
- FPS

---

## Cache Location

Persistent cache is stored at:

```
~/.cache/nautilus-media-columns/media.sqlite3
```

Safe to delete at any time; it will be recreated automatically.

---

## Supported Formats

**Images**
- PNG, JPG/JPEG, WebP, BMP, TIFF

**Videos**
- MP4, MKV, MOV, AVI, WebM, M4V  
(Actual codec support depends on installed GStreamer plugins.)

---

## Performance Notes

- First visit to a folder may probe uncached files
- Subsequent visits are instant
- Files are only re-parsed if **mtime or size changes**

---

## Compatibility Notes

This extension targets the **nautilus-python API 4.0**.  
If your system loads Python extensions from:

```
~/.local/share/nautilus-python/extensions/
```

... it is compatible.

---

## License

MIT
