"""
Convert favicon.png to squircle shape and save as .ico format
"""
import sys
from pathlib import Path

try:
    from PIL import Image, ImageDraw
except ImportError:
    print("Error: Pillow is not installed. Install it with: pip install Pillow")
    sys.exit(1)


def create_squircle_mask(size: int, radius: float = 0.15) -> Image.Image:
    """Create a squircle (rounded square) mask with subtle corner rounding"""
    mask = Image.new("L", (size, size), 0)
    draw = ImageDraw.Draw(mask)
    r = int(size * radius)
    draw.rounded_rectangle([(0, 0), (size - 1, size - 1)], radius=r, fill=255)
    
    return mask


def convert_favicon_to_squircle_ico():
    """Convert favicon.png to squircle .ico"""
    static_dir = Path(__file__).parent / "llm_memedescriber" / "static"
    input_file = static_dir / "favicon.png"
    output_file = static_dir / "favicon.ico"
    
    if not input_file.exists():
        print(f"Error: {input_file} not found")
        sys.exit(1)
    
    print(f"Loading {input_file}...")
    img = Image.open(input_file)
    
    if img.mode != "RGBA":
        img = img.convert("RGBA")
    
    sizes = [16, 32, 48, 64, 128, 256]
    ico_images = []
    
    for size in sizes:
        print(f"  Creating {size}x{size} squircle version...")
        
        resized = img.resize((size, size), Image.Resampling.LANCZOS)
        mask = create_squircle_mask(size, radius=0.25)
        resized.putalpha(mask)
        
        ico_images.append(resized)
    
    print(f"Saving as {output_file}...")
    try:
        ico_images[0].save(
            output_file,
            format="ICO",
            save_all=True,
            append_images=ico_images[1:] if len(ico_images) > 1 else [],
            duration=[100] * len(ico_images),
            loop=0
        )
    except Exception as e:
        print(f"  Note: {e}")
        print(f"  Falling back to single 256x256 resolution ICO...")
        ico_images[-1].save(output_file, format="ICO")
    
    print(f"✓ Successfully created {output_file}")
    print(f"  Sizes included: {', '.join(map(str, sizes))}")
    
    png_output = static_dir / "favicon-squircle.png"
    print(f"\nAlso saving high-quality PNG version as {png_output}...")
    
    size = 256
    resized = img.resize((size, size), Image.Resampling.LANCZOS)
    mask = create_squircle_mask(size, radius=0.25)
    resized.putalpha(mask)
    resized.save(png_output)
    
    print(f"✓ Successfully created {png_output}")


if __name__ == "__main__":
    try:
        convert_favicon_to_squircle_ico()
        print("\n✓ Favicon conversion complete!")
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
