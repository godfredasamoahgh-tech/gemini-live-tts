#!/usr/bin/env python3
"""
Gemini Live TTS Bot (@Clauseclambot) — GitHub Actions 24/7 Edition
Runs on GitHub Actions runner with direct Google Gemini Live WebSocket.
Supports zero-gap shift chaining and graceful shutdown before runner timeout.
"""
import asyncio
import json
import base64
import logging
import os
import subprocess
import time
from datetime import datetime

import requests
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, CommandHandler, filters
from telegram.request import HTTPXRequest

# ============ CONFIG ============
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
API_KEY = os.environ.get("GEMINI_API_KEY")
MODEL = "models/gemini-2.5-flash-native-audio-latest"

# Direct Google WebSocket (GitHub runners have clean international IPs, no relay needed!)
WS_URL = f"wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key={API_KEY}"

MAX_KEEP = 3
DEFAULT_VOICE = "Puck"
VOICES = ["Puck", "Charon", "Kore", "Fenrir", "Aoede"]
user_voice_prefs = {}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Run shift budgeting
RUN_MINUTES = int(os.environ.get("RUN_MINUTES", "285"))  # 4h 45m shift
START_TIME = time.time()

async def tts(text: str, voice: str, out_path: str) -> str:
    """Generate speech for text via Gemini Live WebSocket, returns mp3 path."""
    import websockets
    async with websockets.connect(WS_URL, max_size=200 * 1024 * 1024) as ws:
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
            raise RuntimeError(f"setup failed: {ack}")

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
        raise RuntimeError("No audio received from Gemini Live")

    raw = f"raw_{int(time.time()*1000)}.pcm"
    with open(raw, "wb") as f:
        f.write(bytes(pcm))

    subprocess.run([
        "ffmpeg", "-y", "-f", "s16le", "-ar", "24000", "-ac", "1",
        "-i", raw, out_path
    ], check=True, capture_output=True)

    try:
        os.remove(raw)
    except OSError:
        pass

    return out_path

async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    text = (
        "🎙️ **Gemini Live TTS Bot (@Clauseclambot)**\n\n"
        "Send me any text (English or Persian), and I'll generate ultra-realistic native voice audio for you!\n\n"
        "**Available Voices:**\n"
        "• `/voice Puck` (Default male)\n"
        "• `/voice Charon` (Deep male)\n"
        "• `/voice Kore` (Female)\n"
        "• `/voice Fenrir` (Fast male)\n"
        "• `/voice Aoede` (Smooth female)"
    )
    await msg.reply_text(text, parse_mode="Markdown")

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
    await msg.reply_text(f"Available voices: {', '.join(VOICES)}")

async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text:
        return

    text = msg.text.strip()
    if not text or text.startswith("/"):
        return

    voice = user_voice_prefs.get(msg.from_user.id, DEFAULT_VOICE)
    status = await msg.reply_text(f"🎙️ Generating voice ({voice})...")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"tts_{ts}_{msg.message_id}.mp3"

    try:
        audio_path = await tts(text, voice, out_name)
        with open(audio_path, "rb") as f:
            await msg.reply_voice(voice=f, caption=f"🎧 Voice: {voice}")
        logger.info(f"Sent audio to {msg.from_user.id} ({voice})")

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

def trigger_next_shift():
    """Trigger the next workflow run right before exiting."""
    repo = os.environ.get("GITHUB_REPOSITORY")
    token = os.environ.get("GITHUB_TOKEN")
    if not repo or not token:
        logger.info("Not running in GitHub Actions, skipping chain dispatch.")
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
    """Gracefully stop bot when shift window closes to let next runner take over."""
    while True:
        await asyncio.sleep(60)
        elapsed = (time.time() - START_TIME) / 60
        if elapsed >= RUN_MINUTES:
            logger.info(f"Shift time reached ({elapsed:.1f} mins). Dispatching next runner...")
            trigger_next_shift()
            await asyncio.sleep(5)
            logger.info("Stopping bot for shift handoff...")
            app.stop_running()
            break

def main():
    if not TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable not set!")
    if not API_KEY:
        raise ValueError("GEMINI_API_KEY environment variable not set!")

    request = HTTPXRequest(
        read_timeout=50,
        write_timeout=50,
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
    app.add_handler(CommandHandler("voice", voice_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Add watchdog task
    loop = asyncio.get_event_loop()
    loop.create_task(shift_watchdog(app))

    logger.info("Gemini Live TTS Bot [@Clauseclambot] starting 24/7 runner polling...")
    app.run_polling()

if __name__ == "__main__":
    main()
