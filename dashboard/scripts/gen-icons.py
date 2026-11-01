#!/usr/bin/env python3
"""Generate the dashboard favicon / PWA icon suite into public/.

Requires: rsvg-convert (librsvg) on PATH and Pillow.
Usage: python3 scripts/gen-icons.py
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parent.parent
PUBLIC = ROOT / "public"

# Palette mirrors dashboard/src/styles/tokens.css.
TILE_TOP = "#14203a"
TILE_BOTTOM = "#070b16"
TEAL_TOP = "#8ff5e6"
TEAL_BODY_TOP = "#5beed9"
TEAL_BODY_BOTTOM = "#2bb8a8"
LIVE = "#79f2ad"
INK = "#070b16"

SIZES = (16, 32, 48, 64, 96)


def glyph(scale: float = 1.0) -> str:
    """Filled database cylinder plus the live dot, in a 64-unit space."""
    body = (
        f'<path d="M17 26 V44 A13 5 0 0 0 43 44 V26 Z" fill="url(#teal)"/>'
        f'<ellipse cx="30" cy="26" rx="13" ry="5" fill="{TEAL_TOP}"/>'
        f'<path d="M17 32 A13 5 0 0 0 43 32" fill="none" stroke="{INK}" '
        'stroke-opacity="0.4" stroke-width="1.8"/>'
        f'<path d="M17 38 A13 5 0 0 0 43 38" fill="none" stroke="{INK}" '
        'stroke-opacity="0.4" stroke-width="1.8"/>'
        f'<circle cx="48.5" cy="15.5" r="6" fill="{LIVE}" stroke="{INK}" stroke-width="2.4"/>'
    )
    if scale == 1.0:
        return body
    return f'<g transform="translate(32 32) scale({scale}) translate(-32 -32)">{body}</g>'


def svg_doc(rounded: bool, glyph_scale: float) -> str:
    """The tile mark: navy tile, teal cylinder, green live dot."""
    rx = 14 if rounded else 0
    highlight = (
        f'<rect x="1.5" y="1.5" width="61" height="61" rx="{max(rx - 1.5, 0)}" '
        'fill="none" stroke="#ffffff" stroke-opacity="0.08" stroke-width="1"/>'
        if rounded
        else ""
    )
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64">'
        "<defs>"
        f'<linearGradient id="tile" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0" stop-color="{TILE_TOP}"/>'
        f'<stop offset="1" stop-color="{TILE_BOTTOM}"/></linearGradient>'
        f'<linearGradient id="teal" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0" stop-color="{TEAL_BODY_TOP}"/>'
        f'<stop offset="1" stop-color="{TEAL_BODY_BOTTOM}"/></linearGradient>'
        "</defs>"
        f'<rect width="64" height="64" rx="{rx}" fill="url(#tile)"/>'
        + highlight
        + glyph(glyph_scale)
        + "</svg>"
    )


def rasterize(svg: str, size: int, out: Path) -> None:
    with tempfile.NamedTemporaryFile(suffix=".svg", delete=False) as tmp:
        tmp.write(svg.encode())
        tmp_path = tmp.name
    try:
        subprocess.run(
            ["rsvg-convert", "-w", str(size), "-h", str(size), "-o", str(out), tmp_path],
            check=True,
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def main() -> None:
    icons = PUBLIC / "icons"
    icons.mkdir(parents=True, exist_ok=True)

    rounded = svg_doc(rounded=True, glyph_scale=1.0)
    full_bleed = svg_doc(rounded=False, glyph_scale=0.78)

    (PUBLIC / "favicon.svg").write_text(rounded)
    for size in SIZES:
        rasterize(rounded, size, PUBLIC / f"favicon-{size}x{size}.png")
    # Opaque square background for iOS, content pulled in from the corner mask.
    rasterize(svg_doc(rounded=False, glyph_scale=0.92), 180, PUBLIC / "apple-touch-icon.png")
    rasterize(rounded, 192, icons / "icon-192.png")
    rasterize(rounded, 512, icons / "icon-512.png")
    rasterize(full_bleed, 512, icons / "icon-maskable-512.png")

    tiles = [Image.open(PUBLIC / f"favicon-{s}x{s}.png") for s in (16, 32, 48)]
    tiles[0].save(
        PUBLIC / "favicon.ico",
        format="ICO",
        append_images=tiles[1:],
        sizes=[(16, 16), (32, 32), (48, 48)],
    )
    print(f"wrote icon suite to {PUBLIC}")


if __name__ == "__main__":
    main()
