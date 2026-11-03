#!/usr/bin/env python3
"""
Gemini Live TTS Bot (@Clauseclambot) — High-Speed 5-Key Parallel Cloud Edition
Runs 24/7 on GitHub Actions with:
- Intelligent text boundary splitting (paragraphs, sentences, punctuation, formatting)
- Dual Mode: Single vs Parallel (default) toggleable via /mode
- 5-Key Concurrent Generation: 5 Gemini Live streams simultaneously with automatic key failover
- High-fidelity FFmpeg audio concatenation (24kHz s16le PCM -> MP3)
- Graceful 4h45m shift-handoff auto-chaining
"""
import asyncio
import json
import base64
import logging
import os
import re
import subprocess
import time
from datetime import datetime

import requests
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, CommandHandler, filters
from telegram.request import HTTPXRequest

# ============ CONFIG ============
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
MODEL = "models/gemini-2.5-flash-native-audio-latest"

# 5 Gemini API Keys Failover & Parallel Pool
GEMINI_KEYS = [
    os.environ.get("GEMINI_KEY_1", "").strip(),
    os.environ.get("GEMINI_KEY_2", "").strip(),
    os.environ.get("GEMINI_KEY_3", "").strip(),
    os.environ.get("GEMINI_KEY_4", "").strip(),
    os.environ.get("GEMINI_KEY_5", "").strip(),
]
GEMINI_KEYS = [k for k in GEMINI_KEYS if k]
if not GEMINI_KEYS and os.environ.get("GEMINI_API_KEY"):
    GEMINI_KEYS = [os.environ.get("GEMINI_API_KEY").strip()]

DEFAULT_VOICE = "Puck"
VOICES = ["Puck", "Charon", "Kore", "Fenrir", "Aoede"]
DEFAULT_MODE = "parallel"  # parallel or single

user_voice_prefs = {}
user_mode_prefs = {}  # {user_id: "parallel" | "single"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RUN_MINUTES = int(os.environ.get("RUN_MINUTES", "285"))
START_TIME = time.time()

# ============ SMART TEXT CHUNKER ============
def split_text_smart(text: str, max_chunks: int = 5) -> list[str]:
    """
    Intelligently splits long text into 2-5 natural chunks without breaking
    sentences, paragraphs, or thoughts in half.
    """
    text = text.strip()
    if len(text) < 180 or max_chunks <= 1:
        return [text]

    # Split by double newlines (paragraphs/sections) first
    paragraphs = [p.strip() for p in re.split(r'\n\s*\n+', text) if p.strip()]
    if len(paragraphs) >= 2 and len(paragraphs) <= max_chunks:
        return paragraphs

    # If paragraph count is large or text is one big block, split by sentence endings
    # Persian & English sentence terminators: . ! ? ؟ \n
    sentences = re.split(r'(?<=[.!?؟\n])\s+', text)
    sentences = [s.strip() for s in sentences if s.strip()]

    if len(sentences) < 2:
        # Fallback to comma/semicolon/dash boundary
        sentences = re.split(r'(?<=[,،؛;—-])\s+', text)
        sentences = [s.strip() for s in sentences if s.strip()]

    if len(sentences) < 2:
        return [text]

    # Group sentences into roughly equal-sized balanced chunks
    total_len = sum(len(s) for s in sentences)
    target_chunk_len = total_len // max_chunks

    chunks = []
    current_chunk = []
    current_len = 0

    for s in sentences:
        current_chunk.append(s)
        current_len += len(s)
        if current_len >= target_chunk_len and len(chunks) < max_chunks - 1:
            chunks.append(" ".join(current_chunk).strip())
            current_chunk = []
            current_len = 0

    if current_chunk:
        chunks.append(" ".join(current_chunk).strip())

    return [c for c in chunks if c]

# ============ GEMINI LIVE WS STREAMER ============
async def generate_chunk_audio(text: str, voice: str, key: str, chunk_idx: int) -> bytearray:
    """Stream audio for a single text chunk over Gemini Live WebSocket."""
    import websockets
    ws_url = f"wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key={key}"

    async with websockets.connect(ws_url, max_size=200 * 1024 * 1024, open_timeout=20, close_timeout=20) as ws:
        await ws.send(json.dumps({
            "setup": {
                "model": MODEL,
                "generationConfig": {
                    "responseModalities": ["AUDIO"],
                    "speechConfig": {
                        "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": voice}}
                    }
                }
            }
        }))
        ack = json.loads(await ws.recv())
        if "setupComplete" not in ack:
            raise RuntimeError(f"setup failed on chunk {chunk_idx}: {ack}")

        prompt = (
            "SYSTEM DIRECTIVE: You are a strict verbatim audio reader. "
            "Speak ONLY the exact words below, character-for-character, in the "
            "language they are written in (English, Persian, or any language). "
            "Do NOT add any intro, outro, filler, translation, summary, or explanation. "
            "Begin reading immediately.\n\nTEXT:\n" + text
        )
        await ws.send(json.dumps({
            "clientContent": {
                "turns": [{"role": "user", "parts": [{"text": prompt}]}],
                "turnComplete": True
            }
        }))

        pcm = bytearray()
        while True:
            msg = json.loads(await ws.recv())
            if "serverContent" not in msg:
                continue
            sc = msg["serverContent"]
            if sc.get("interrupted"):
                break
            for part in sc.get("modelTurn", {}).get("parts", []):
                if "inlineData" in part:
                    pcm += base64.b64decode(part["inlineData"].get("data", ""))
            if sc.get("turnComplete"):
                break

    if not pcm:
        raise RuntimeError(f"No audio received for chunk {chunk_idx}")
    return pcm

async def generate_chunk_with_failover(text: str, voice: str, preferred_key_idx: int, chunk_idx: int) -> bytearray:
    """Generate audio with automatic failover across all 5 keys if one gets throttled."""
    num_keys = len(GEMINI_KEYS)
    last_err = None

    for attempt in range(num_keys):
        k_idx = (preferred_key_idx + attempt) % num_keys
        k = GEMINI_KEYS[k_idx]
        try:
            return await generate_chunk_audio(text, voice, k, chunk_idx)
        except Exception as e:
            logger.warning(f"[chunk {chunk_idx}] Key #{k_idx+1} failed: {e}. Trying next key...")
            last_err = e
            await asyncio.sleep(0.5)

    raise RuntimeError(f"All {num_keys} keys failed for chunk {chunk_idx}: {last_err}")

# ============ TTS HIGH-LEVEL ENGINE ============
async def tts_smart(text: str, voice: str, mode: str, out_path: str) -> tuple[str, float]:
    """
    Main TTS coordinator:
    - Mode 'single' or short text: 1 key direct stream
    - Mode 'parallel' and long text: Splits into up to 5 smart chunks, streams simultaneously across 5 keys,
      and seamlessly concatenates the 24kHz PCM stream before ffmpeg MP3 encoding.
    """
    start_t = time.time()
    num_keys = min(5, len(GEMINI_KEYS))

    if mode == "single" or len(text) < 180 or num_keys <= 1:
        # Single mode execution
        pcm = await generate_chunk_with_failover(text, voice, 0, 0)
    else:
        # Parallel 5-key execution
        chunks = split_text_smart(text, max_chunks=num_keys)
        logger.info(f"[parallel-tts] Splitting text into {len(chunks)} chunks across {num_keys} keys")

        tasks = []
        for i, chunk in enumerate(chunks):
            # assign distinct preferred key index for each chunk
            tasks.append(generate_chunk_with_failover(chunk, voice, i % num_keys, i))

        pcm_results = await asyncio.gather(*tasks)

        # Concatenate in original sequential order
        pcm = bytearray()
        for c_pcm in pcm_results:
            pcm.extend(c_pcm)

    raw_file = f"raw_{int(time.time()*1000)}.pcm"
    with open(raw_file, "wb") as f:
        f.write(bytes(pcm))

    # Convert 24kHz Mono 16-bit PCM to high-quality MP3
    subprocess.run([
        "ffmpeg", "-y", "-f", "s16le", "-ar", "24000", "-ac", "1",
        "-i", raw_file, out_path
    ], check=True, capture_output=True)

    try:
        os.remove(raw_file)
    except OSError:
        pass

    duration = time.time() - start_t
    return out_path, duration

# ============ TELEGRAM COMMANDS & HANDLERS ============
async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    text = (
        "🎙️ **Gemini Live 5-Key Parallel TTS Bot (@Clauseclambot)**\n\n"
        "Send me any text (English or Persian), and I'll generate ultra-realistic native voice audio for you!\n\n"
        "⚡ **Features:**\n"
        "• **5-Key Turbo Parallel:** Multi-key simultaneous processing for 3x–5x faster generation.\n"
        "• **Auto-Failover:** Zero dropped requests if an API key rate-limits.\n"
        "• **Seamless Concatenation:** Natural paragraph & sentence chunking.\n\n"
        "🛠️ **Commands:**\n"
        "• `/mode parallel` — 5-key turbo speed (Default)\n"
        "• `/mode single` — Standard sequential single-key\n"
        "• `/voice <name>` — Puck, Charon, Kore, Fenrir, Aoede"
    )
    await msg.reply_text(text, parse_mode="Markdown")

async def mode_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    user_id = msg.from_user.id
    if ctx.args and len(ctx.args) > 0:
        chosen = ctx.args[0].lower()
        if chosen in ["parallel", "single"]:
            user_mode_prefs[user_id] = chosen
            label = "⚡ Turbo 5-Key Parallel" if chosen == "parallel" else "🐢 Single Stream"
            await msg.reply_text(f"✅ Mode switched to: **{label}**", parse_mode="Markdown")
            return
    current = user_mode_prefs.get(user_id, DEFAULT_MODE)
    await msg.reply_text(
        f"Current mode: **{current}**\nUsage: `/mode parallel` or `/mode single`",
        parse_mode="Markdown"
    )

async def voice_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    if ctx.args and len(ctx.args) > 0:
        chosen = ctx.args[0].capitalize()
        if chosen in VOICES:
            user_voice_prefs[msg.from_user.id] = chosen
            await msg.reply_text(f"✅ Voice set to: **{chosen}**", parse_mode="Markdown")
            return
    await msg.reply_text(f"Available voices: {', '.join(VOICES)}\nUsage: `/voice Puck`")

async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text:
        return

    text = msg.text.strip()
    if not text or text.startswith("/"):
        return

    user_id = msg.from_user.id
    voice = user_voice_prefs.get(user_id, DEFAULT_VOICE)
    mode = user_mode_prefs.get(user_id, DEFAULT_MODE)

    mode_label = "⚡ Parallel" if mode == "parallel" and len(text) >= 180 else "Single"
    status = await msg.reply_text(f"🎙️ Generating voice ({voice} | {mode_label})...")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"tts_{ts}_{msg.message_id}.mp3"

    try:
        audio_path, duration = await tts_smart(text, voice, mode, out_name)
        with open(audio_path, "rb") as f:
            caption = f"🎧 Voice: {voice} | ⏱️ Generated in {duration:.1f}s ({mode_label})"
            await msg.reply_voice(voice=f, caption=caption)
        logger.info(f"Delivered audio to {user_id} in {duration:.1f}s ({mode_label})")

        try:
            os.remove(audio_path)
        except OSError:
            pass
        await status.delete()

    except Exception as e:
        logger.error(f"TTS failed: {e}")
        try:
            await status.delete()
        except Exception:
            pass
        await msg.reply_text(f"❌ Error generating audio: {e}")

# ============ LIFECYCLE & RUNNER CHAINING ============
def trigger_next_shift():
    repo = os.environ.get("GITHUB_REPOSITORY")
    token = os.environ.get("GITHUB_TOKEN")
    if not repo or not token:
        return

    url = f"https://api.github.com/repos/{repo}/actions/workflows/tts.yml/dispatches"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "TTS-Chain-Pusher"
    }
    try:
        r = requests.post(url, json={"ref": "main"}, headers=headers, timeout=10)
        logger.info(f"Chained next shift: HTTP {r.status_code}")
    except Exception as e:
        logger.warning(f"Failed to chain next shift: {e}")

async def shift_watchdog(app):
    while True:
        await asyncio.sleep(60)
        elapsed = (time.time() - START_TIME) / 60
        if elapsed >= RUN_MINUTES:
            logger.info(f"Shift closing ({elapsed:.1f}m). Dispatching next shift...")
            trigger_next_shift()
            await asyncio.sleep(5)
            app.stop_running()
            break

def main():
    if not TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable not set!")
    if not GEMINI_KEYS:
        raise ValueError("No GEMINI_KEY environment variables found!")

    logger.info(f"Starting Gemini Live TTS Bot with {len(GEMINI_KEYS)} Gemini keys loaded.")

    request = HTTPXRequest(
        read_timeout=60,
        write_timeout=60,
        connect_timeout=30,
        pool_timeout=30,
    )
    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .request(request)
        .build()
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("mode", mode_cmd))
    app.add_handler(CommandHandler("voice", voice_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    loop = asyncio.get_event_loop()
    loop.create_task(shift_watchdog(app))

    app.run_polling()

if __name__ == "__main__":
    main()
