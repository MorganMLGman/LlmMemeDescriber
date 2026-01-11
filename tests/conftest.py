import os
import sys

from pathlib import Path
from PIL import Image

from _helpers import *

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

def _ensure_test_images():
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    rgb_path = data_dir / "rgb.png"
    if not rgb_path.exists():
        img = Image.new("RGB", (64, 64), (128, 128, 128))
        for x in range(10, 30):
            for y in range(10, 30):
                img.putpixel((x, y), (200, 50, 50))
        img.save(rgb_path, format="PNG")

    var2_path = data_dir / "rgb_variant2.png"
    if not var2_path.exists():
        img = Image.new("RGB", (64, 64), (128, 128, 128))
        for x in range(2, 22):
            for y in range(2, 22):
                img.putpixel((x, y), (50, 200, 50))
        img.save(var2_path, format="PNG")

    gray_path = data_dir / "grayscale.png"
    if not gray_path.exists():
        img = Image.new("L", (64, 64), 128)
        for x in range(10, 30):
            for y in range(10, 30):
                img.putpixel((x, y), 200)
        img.save(gray_path, format="PNG")

    pal_path = data_dir / "paletted.png"
    if not pal_path.exists():
        img = Image.new("RGB", (64, 64), (10, 20, 30))
        p = img.convert("P")
        p.save(pal_path, format="PNG")

    rgba_path = data_dir / "rgba.png"
    if not rgba_path.exists():
        img = Image.new("RGBA", (64, 64), (128, 128, 128, 255))
        for x in range(10, 30):
            for y in range(10, 30):
                img.putpixel((x, y), (200, 50, 50, 128))
        img.save(rgba_path, format="PNG")

    try:
        jpg_path = data_dir / "rgb.jpg"
        if not jpg_path.exists():
            img = Image.open(rgb_path)
            img.save(jpg_path, format="JPEG")
            img.save(data_dir / "rgb.jpeg", format="JPEG")
    except Exception:
        pass

    # GIF (paletted)
    try:
        gif_path = data_dir / "rgb.gif"
        if not gif_path.exists():
            img = Image.open(rgb_path).convert("P")
            img.save(gif_path, format="GIF")
    except Exception:
        pass

    try:
        webp_large = data_dir / "rgb_large.webp"
        if not webp_large.exists():
            img_large = Image.new("RGB", (1024, 1024), (128, 128, 128))
            for x in range(80, 200):
                for y in range(80, 200):
                    img_large.putpixel((x, y), (200, 50, 50))
            img_large.save(webp_large, format="WEBP", lossless=True, quality=100)
    except Exception:
        pass


_ensure_test_images()

