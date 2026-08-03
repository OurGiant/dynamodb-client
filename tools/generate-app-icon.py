#!/usr/bin/env python3
"""Regenerates all of DynamoDB Browser's app icon assets from one drawn master image.

Unlike a source .xcf/.ai file, this icon has no external art dependency -- the design
(a data table with a header row, plus a magnifying-glass badge) is drawn directly with
Pillow, so it's fully reproducible by re-running this script. Requires Pillow:
`pip install pillow` (or use an existing venv that has it).

Usage: python3 tools/generate-app-icon.py   (run from the repo root)
"""
import math
from pathlib import Path

from PIL import Image, ImageDraw

REPO_ROOT = Path(__file__).resolve().parent.parent

NAVY = (35, 47, 62, 255)
ORANGE = (255, 153, 0, 255)
WHITE = (255, 255, 255, 255)

SIZE = 1024


def draw_master() -> Image.Image:
    img = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    bg_margin = 32
    draw.rounded_rectangle(
        [bg_margin, bg_margin, SIZE - bg_margin, SIZE - bg_margin],
        radius=190, fill=NAVY
    )

    # Three stacked horizontal "table row" bars: orange header + two white data rows,
    # full width, bold, with navy gutters between them so the row structure survives
    # downscaling to a 16px taskbar icon.
    bar_left, bar_right = 150, 874
    gutter = 34
    bar_radius = 40
    header_h, row_h = 190, 150
    row_top_1 = 150
    row_top_2 = row_top_1 + header_h + gutter
    row_top_3 = row_top_2 + row_h + gutter

    draw.rounded_rectangle([bar_left, row_top_1, bar_right, row_top_1 + header_h], radius=bar_radius, fill=ORANGE)
    draw.rounded_rectangle([bar_left, row_top_2, bar_right, row_top_2 + row_h], radius=bar_radius, fill=WHITE)
    draw.rounded_rectangle([bar_left, row_top_3, bar_right, row_top_3 + row_h], radius=bar_radius, fill=WHITE)

    # Magnifying glass badge, bottom-right, overlapping the last row -- signals
    # "browsing/querying" a table rather than just "a spreadsheet."
    glass_cx, glass_cy = 800, 800
    badge_r = 195
    draw.ellipse(
        [glass_cx - badge_r, glass_cy - badge_r, glass_cx + badge_r, glass_cy + badge_r],
        fill=ORANGE, outline=NAVY, width=14
    )
    lens_r = 108
    draw.ellipse(
        [glass_cx - lens_r, glass_cy - lens_r, glass_cx + lens_r, glass_cy + lens_r],
        outline=WHITE, width=42
    )
    handle_len, handle_w = 105, 44
    angle = math.radians(45)
    hx1 = glass_cx + lens_r * math.cos(angle)
    hy1 = glass_cy + lens_r * math.sin(angle)
    hx2 = hx1 + handle_len * math.cos(angle)
    hy2 = hy1 + handle_len * math.sin(angle)
    draw.line([(hx1, hy1), (hx2, hy2)], fill=WHITE, width=handle_w)
    draw.ellipse([hx2 - handle_w / 2, hy2 - handle_w / 2, hx2 + handle_w / 2, hy2 + handle_w / 2], fill=WHITE)
    draw.ellipse([hx1 - handle_w / 2, hy1 - handle_w / 2, hx1 + handle_w / 2, hy1 + handle_w / 2], fill=WHITE)

    return img


def main() -> None:
    master = draw_master()

    png_512 = master.resize((512, 512), Image.LANCZOS)
    png_512.save(REPO_ROOT / "src/main/resources/app-icon.png")
    png_512.save(REPO_ROOT / "src/packaging/linux/app-icon.png")

    ico_sizes = [(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
    master.save(REPO_ROOT / "src/packaging/app-icon.ico", format="ICO", sizes=ico_sizes)
    master.save(REPO_ROOT / "src/packaging/app-icon.icns", format="ICNS")

    print("Wrote src/main/resources/app-icon.png, src/packaging/linux/app-icon.png, "
          "src/packaging/app-icon.ico, src/packaging/app-icon.icns")


if __name__ == "__main__":
    main()
