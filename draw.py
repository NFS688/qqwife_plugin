import io
import asyncio
import aiohttp
import base64
from typing import Optional, Sequence
from PIL import Image, ImageDraw, ImageFont, ImageFilter


AVATAR_TIMEOUT_SECONDS = 5
DEFAULT_AVATAR_FETCH_CONCURRENCY = 12


def get_font(size: int, bold=False):
    weight_file = "msyhbd.ttc" if bold else "msyh.ttc"
    try:
        return ImageFont.truetype(weight_file, size)
    except Exception:
        try:
            return ImageFont.truetype("simhei.ttf", size)
        except Exception:
            return ImageFont.load_default()


def _fallback_avatar() -> Image.Image:
    return Image.new("RGBA", (140, 140), "#E5E5EA")


async def _fetch_avatar_with_session(
    user_id: str,
    session: aiohttp.ClientSession,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> Image.Image:
    url = f"https://q1.qlogo.cn/g?b=qq&nk={user_id}&s=640"
    if semaphore is None:
        try:
            async with session.get(url, timeout=AVATAR_TIMEOUT_SECONDS) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    return Image.open(io.BytesIO(data)).convert("RGBA")
        except Exception:
            return _fallback_avatar()
        return _fallback_avatar()

    async with semaphore:
        try:
            async with session.get(url, timeout=AVATAR_TIMEOUT_SECONDS) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    return Image.open(io.BytesIO(data)).convert("RGBA")
        except Exception:
            return _fallback_avatar()
    return _fallback_avatar()


async def fetch_avatar(
    user_id: str,
    session: Optional[aiohttp.ClientSession] = None,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> Image.Image:
    if session is not None:
        return await _fetch_avatar_with_session(user_id, session, semaphore)

    connector = aiohttp.TCPConnector(limit=max(8, DEFAULT_AVATAR_FETCH_CONCURRENCY * 2), ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=AVATAR_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as local_session:
        return await _fetch_avatar_with_session(user_id, local_session, semaphore)


async def _fetch_avatars(user_ids: Sequence[str], max_concurrency: int = DEFAULT_AVATAR_FETCH_CONCURRENCY):
    concurrency = max(1, int(max_concurrency))
    semaphore = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=max(8, concurrency * 2), ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=AVATAR_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [fetch_avatar(user_id, session=session, semaphore=semaphore) for user_id in user_ids]
        return await asyncio.gather(*tasks)

async def fetch_avatar_base64(user_id: str) -> str:
    img = await fetch_avatar(user_id)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


def make_circle_avatar(avatar: Image.Image, size: int) -> Image.Image:
    avatar = avatar.resize((size, size), Image.Resampling.LANCZOS)
    mask = Image.new('L', (size, size), 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((0, 0, size, size), fill=255)
    
    result = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    result.paste(avatar, (0, 0), mask)
    return result

def create_rounded_rectangle(w, h, r, fill):
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle((0, 0, w, h), r, fill=fill)
    return img

async def generate_roster_image(records, title="群老婆列表") -> str:
    """Generate roster image and return base64 string"""
    avatar_ids = []
    for r in records:
        avatar_ids.append(str(r.user_id))
        avatar_ids.append(str(r.target_id))
    avatars = await _fetch_avatars(avatar_ids)
    
    width = 900
    row_height = 120
    card_padding = 40
    card_width = width - 80
    header_height = 120
    content_height = max(len(records) * row_height, 100) + card_padding
    card_height = header_height + content_height + card_padding
    
    bg_height = card_height + 120
    
    # Background
    bg = Image.new("RGBA", (width, bg_height), "#F2F2F7")
    
    # Shadow
    shadow = create_rounded_rectangle(card_width, card_height, 30, (0, 0, 0, 20))
    shadow_layer = Image.new("RGBA", (width, bg_height), (0, 0, 0, 0))
    shadow_layer.paste(shadow, (40, 60), shadow)
    shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(12))
    bg.paste(shadow_layer, (0, 0), shadow_layer)
    
    # Card
    card = create_rounded_rectangle(card_width, card_height, 30, "#FFFFFF")
    bg.paste(card, (40, 60), card)
    
    # Draw logic on card
    draw = ImageDraw.Draw(bg)
    title_font = get_font(42, bold=True)
    body_font = get_font(28)
    
    # Draw Title
    title_bbox = draw.textbbox((0, 0), title, font=title_font)
    title_w = title_bbox[2] - title_bbox[0]
    draw.text((40 + (card_width - title_w)//2, 60 + 50), title, fill="#1C1C1E", font=title_font)
    
    y = 60 + header_height + 20
    
    for i, r in enumerate(records):
        left_name = r.username or str(r.user_id)
        right_name = r.target_name or str(r.target_id)
        left_av = make_circle_avatar(avatars[i*2], 80)
        right_av = make_circle_avatar(avatars[i*2 + 1], 80)
        
        # Center block relative to card
        # Structure: [Left AV]    LeftName  <---->  RightName    [Right AV]
        # Text sizes
        arrow = "<---->"
        heart_font = get_font(26)
        ln_w = draw.textbbox((0,0), left_name, font=body_font)[2]
        rn_w = draw.textbbox((0,0), right_name, font=body_font)[2]
        heart_w = draw.textbbox((0,0), arrow, font=heart_font)[2]
        
        # We can align them symmetrically
        center_x = 40 + card_width // 2
        
        # Draw Left Side
        # AV left edge: center_x - 40(spacing) - ln_w - 20 - 80
        curr_x = center_x - heart_w//2 - 20 - ln_w
        draw.text((curr_x, y + 25), left_name, fill="#3A3A3C", font=body_font)
        bg.paste(left_av, (curr_x - 20 - 80, y), left_av)
        
        # Draw Arrow
        draw.text((center_x - heart_w//2, y + 25), arrow, fill="#8E8E93", font=heart_font)
        
        # Draw Right Side
        curr_x = center_x + heart_w//2 + 20
        draw.text((curr_x, y + 25), right_name, fill="#3A3A3C", font=body_font)
        bg.paste(right_av, (curr_x + rn_w + 20, y), right_av)
        
        # Draw Separator
        if i < len(records) - 1:
            draw.line([(80, y + 105), (width - 80, y + 105)], fill="#E5E5EA", width=2)
            
        y += row_height
        
    buf = io.BytesIO()
    bg.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


async def generate_favor_image(pairs, title="好感度列表") -> str:
    """pairs: list of tuples (target_id, favor, target_name)"""
    avatar_ids = [str(p[0]) for p in pairs]
    avatars = await _fetch_avatars(avatar_ids)
    
    width = 800
    row_height = 100
    card_padding = 40
    header_height = 120
    card_width = width - 80
    content_height = max(len(pairs) * row_height, 100) + card_padding
    card_height = header_height + content_height + card_padding
    bg_height = card_height + 120
    
    # Background
    bg = Image.new("RGBA", (width, bg_height), "#F2F2F7")
    
    # Shadow
    shadow = create_rounded_rectangle(card_width, card_height, 30, (0, 0, 0, 20))
    shadow_layer = Image.new("RGBA", (width, bg_height), (0, 0, 0, 0))
    shadow_layer.paste(shadow, (40, 60), shadow)
    shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(12))
    bg.paste(shadow_layer, (0, 0), shadow_layer)
    
    # Card
    card = create_rounded_rectangle(card_width, card_height, 30, "#FFFFFF")
    bg.paste(card, (40, 60), card)
    
    # Draw logic on card
    draw = ImageDraw.Draw(bg)
    title_font = get_font(42, bold=True)
    body_font = get_font(28, bold=True)
    rank_font = get_font(24, bold=True)
    val_font = get_font(28, bold=True)
    
    # Draw Title
    title_bbox = draw.textbbox((0, 0), title, font=title_font)
    title_w = title_bbox[2] - title_bbox[0]
    draw.text((40 + (card_width - title_w)//2, 60 + 50), title, fill="#1C1C1E", font=title_font)
    
    y = 60 + header_height + 20
    
    for i, p in enumerate(pairs):
        target_id, favor, name = p
        av = make_circle_avatar(avatars[i], 70)
        
        # Rank
        draw.text((80, y + 20), f"#{i+1}", fill="#8E8E93", font=rank_font)
        
        # Avatar
        bg.paste(av, (140, y + 5), av)
        
        # Name
        draw.text((230, y + 20), name, fill="#1C1C1E", font=body_font)
        
        # Favor Bar
        bar_x = 500
        bar_w = 180
        # draw background bar
        draw.rounded_rectangle((bar_x, y + 30, bar_x + bar_w, y + 45), 7, fill="#E5E5EA")
        # draw fill
        fill_w = min(max(favor / 100.0, 0.05), 1.0) * bar_w
        # gradient color mapping
        fill_color = "#34C759" if favor > 60 else ("#FF9500" if favor > 30 else "#FF3B30")
        draw.rounded_rectangle((bar_x, y + 30, bar_x + fill_w, y + 45), 7, fill=fill_color)
        
        # Amount
        draw.text((bar_x + bar_w + 15, y + 20), str(favor), fill=fill_color, font=val_font)
        
        # Separator
        if i < len(pairs) - 1:
            draw.line([(80, y + 95), (width - 80, y + 95)], fill="#E5E5EA", width=2)
            
        y += row_height
        
    buf = io.BytesIO()
    bg.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()
