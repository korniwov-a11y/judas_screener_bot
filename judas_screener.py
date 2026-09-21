import asyncio
import os
import json
import base64
from datetime import datetime, timedelta
import pandas as pd
import mplfinance as mpf
import ccxt.async_support as ccxt_async
from aiohttp import ClientSession, FormData
import websockets

# --- 1. ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ (GitHub Secrets) ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Проверка наличия всех ключей
missing_keys = [
    name for name, val in [
        ("GEMINI_API_KEY", GEMINI_API_KEY),
        ("GROQ_API_KEY", GROQ_API_KEY),
        ("BOT_TOKEN", TELEGRAM_BOT_TOKEN),
        ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
    ] if not val
]

if missing_keys:
    raise ValueError(f"❌ Ошибка: Не найдены следующие GitHub Secrets: {', '.join(missing_keys)}")

# Время работы скрипта (3 часа = 10800 секунд)
WORK_DURATION_SECONDS = 3 * 3600

# --- 2. ПОЛУЧЕНИЕ ТОП-15 МОНЕТ (COINCAP API) ---
async def get_top_15_symbols(exchange: ccxt_async.bybit = None) -> list:
    """Динамически получает ТОП-15 монет по капитализации и объему через открытый API CoinCap"""
    print("🔍 Динамическая загрузка ТОП-15 монет по рынку (CoinCap)...")
    url = "https://api.coincap.io/v2/assets?limit=30"
    stables_and_wraps = {'USDC', 'USDT', 'FDUSD', 'DAI', 'TUSD', 'WBTC', 'WBETH', 'USDE', 'STETH'}

    try:
        async with ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    top_symbols = []
                    
                    for item in data.get('data', []):
                        symbol = item.get('symbol', '').upper()
                        if symbol in stables_and_wraps:
                            continue
                        
                        top_symbols.append(f"{symbol}USDT")
                        if len(top_symbols) == 15:
                            break
                    
                    if top_symbols:
                        print(f"✅ Динамический ТОП-15 загружен: {', '.join(top_symbols)}")
                        return top_symbols
    except Exception as e:
        print(f"⚠️ Ошибка загрузки рейтинга через CoinCap API: {e}. Используем базовый список.")

    # Резервный список на случай сбоя сети
    return [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
        "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "SUIUSDT", "LINKUSDT",
        "NEARUSDT", "DOTUSDT", "LTCUSDT", "APTUSDT", "PEPEUSDT"
    ]

# --- 3. ГЕНЕРАЦИЯ ГРАФИКА ---
def generate_chart_sync(symbol: str, dataframe: pd.DataFrame) -> str:
    clean_symbol = symbol.replace('/', '_').replace(':', '')
    image_path = f"chart_{clean_symbol}.png"

    mpf.plot(
        dataframe,
        type='candle',
        style='charles',
        savefig=image_path,
        volume=False
    )
    return image_path

async def fetch_and_draw_chart_async(exchange: ccxt_async.bybit, symbol: str) -> str:
    ccxt_symbol = f"{symbol[:-4]}/USDT" if symbol.endswith("USDT") and '/' not in symbol else symbol
    ohlcv = await exchange.fetch_ohlcv(ccxt_symbol, timeframe="15m", limit=40)

    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)

    return await asyncio.to_thread(generate_chart_sync, symbol, df)

# --- 4. AGENT 1: GEMINI FLASH ---
async def analyze_gemini_async(session: ClientSession, image_path: str) -> dict:
    def read_image_b64(path):
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode('utf-8')

    base64_image = await asyncio.to_thread(read_image_b64, image_path)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"

    prompt_text = (
        "Ты — SMC аналитик. Проанализируй свечной график на предмет сетапа Judas Swing. "
        "Определи: 1) Свип ликвидности. 2) Displacement. 3) CHOCH и свежий FVG. "
        "Верни СТРОГО JSON без markdown: "
        '{"sweep_quality": "HIGH", "displacement": true, "choch_detected": true, "fvg_detected": true, "comment": "краткое описание"}'
    )

    payload = {
        "contents": [{
            "parts": [
                {"text": prompt_text},
                {"inline_data": {"mime_type": "image/png", "data": base64_image}}
            ]
        }]
    }

    try:
        async with session.post(url, json=payload, timeout=30) as resp:
            resp_data = await resp.json()
            raw_text = resp_data['candidates'][0]['content']['parts'][0]['text']
            clean_json = raw_text.replace("```json", "").replace("```", "").strip()
            return json.loads(clean_json)
    except Exception as e:
        print(f"❌ Ошибка Gemini: {e}")
        return {"sweep_quality": "LOW", "displacement": False, "comment": f"Ошибка AI: {e}"}

# --- 5. AGENT 2: GROQ LLAMA 3.3 ---
async def analyze_groq_async(session: ClientSession, vision_json: dict) -> dict:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    system_prompt = (
        "Ты — Главный Риск-Менеджер сетапа Judas Swing. "
        "Правила: Требуется R:R >= 1:3, наличие CHOCH и FVG. "
        "Верни СТРОГО JSON: {\"verdict\": \"EXECUTE\" или \"REJECT\", \"reasons\": [\"причина\"]}"
    )

    data = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps({"vision_metrics": vision_json})}
        ],
        "response_format": {"type": "json_object"}
    }

    try:
        async with session.post(url, headers=headers, json=data, timeout=30) as resp:
            res_json = await resp.json()
            return json.loads(res_json['choices'][0]['message']['content'])
    except Exception as e:
        print(f"❌ Ошибка Groq: {e}")
        return {"verdict": "REJECT", "reasons": [f"Ошибка API Groq: {e}"]}

# --- 6. ОТПРАВКА В TELEGRAM ---
async def send_telegram_async(session: ClientSession, symbol: str, vision_data: dict, final_verdict: dict, image_path: str):
    caption = (
        f"🚨 **СИГНАЛ JUDAS SWING (BYBIT): {symbol}**\n\n"
        f"📍 **Вердикт:** `{final_verdict.get('verdict')}`\n"
        f"📊 **Качество свипа:** {vision_data.get('sweep_quality', 'N/A')}\n"
        f"⚡ **CHOCH / FVG:** {vision_data.get('choch_detected')}/{vision_data.get('fvg_detected')}\n"
        f"📝 **Анализ:** {vision_data.get('comment', '')}\n"
        f"🛡 **Причины решения:** {', '.join(final_verdict.get('reasons', []))}\n"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    data = FormData()
    data.add_field('chat_id', TELEGRAM_CHAT_ID)
    data.add_field('caption', caption)
    data.add_field('parse_mode', 'Markdown')

    with open(image_path, 'rb') as f:
        data.add_field('photo', f.read(), filename=os.path.basename(image_path), content_type='image/png')

    try:
        async with session.post(url, data=data, timeout=30) as resp:
            if resp.status == 200:
                print(f"🟢 [{symbol}] Алерт отправлен в Telegram!")
    except Exception as e:
        print(f"❌ Ошибка Telegram: {e}")

# --- 7. ОБРАБОТКА ЗАКРЫТИЯ СВЕЧИ ---
async def process_candle_event(session: ClientSession, exchange: ccxt_async.bybit, symbol: str):
    print(f"⚡ [{datetime.now().strftime('%H:%M:%S')}] Свеча 15m закрылась по {symbol}. Анализируем...")
    image_path = None
    try:
        image_path = await fetch_and_draw_chart_async(exchange, symbol)
        vision_res = await analyze_gemini_async(session, image_path)

        if vision_res.get("sweep_quality") in ["HIGH", "MEDIUM"]:
            final_decision = await analyze_groq_async(session, vision_res)
            if final_decision.get("verdict") == "EXECUTE":
                await send_telegram_async(session, symbol, vision_res, final_decision, image_path)
    except Exception as e:
        print(f"❌ Ошибка обработки {symbol}: {e}")
    finally:
        if image_path and os.path.exists(image_path):
            os.remove(image_path)

# --- 8. WEBSOCKET СЛУШАТЕЛЬ (BYBIT V5) ---
async def send_bybit_ping(ws, end_time):
    """Каждые 20 секунд отправляет ping для поддержания Bybit WebSocket соединения"""
    try:
        while datetime.now() < end_time:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"op": "ping"}))
    except asyncio.CancelledError:
        pass
    except Exception:
        pass

async def bybit_websocket_listener(session: ClientSession, exchange: ccxt_async.bybit):
    symbols = await get_top_15_symbols(exchange)
    ws_url = "wss://stream.bybit.com/v5/public/spot"

    start_time = datetime.now()
    end_time = start_time + timedelta(seconds=WORK_DURATION_SECONDS)

    print(f"⏰ Запуск сканирования Bybit на 3 часа (до {end_time.strftime('%H:%M:%S')})...")

    while datetime.now() < end_time:
        try:
            async with websockets.connect(ws_url) as ws:
                # Подписываемся на 15m свечи для выбранных монет
                args = [f"kline.15.{s}" for s in symbols]
                sub_msg = {"op": "subscribe", "args": args}
                await ws.send(json.dumps(sub_msg))

                print("✅ WebSocket Bybit подключен. Слушаем 15m свечи...")
                
                # Запускаем фоновую задачу ping-понга для Bybit
                ping_task = asyncio.create_task(send_bybit_ping(ws, end_time))

                try:
                    while datetime.now() < end_time:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        data = json.loads(msg)

                        # Проверяем приход kline сообщения и закрытие свечи (confirm == True)
                        if "topic" in data and data["topic"].startswith("kline.15."):
                            symbol = data["topic"].split(".")[-1]
                            kline_list = data.get("data", [])
                            for kline in kline_list:
                                if kline.get("confirm") is True:
                                    asyncio.create_task(process_candle_event(session, exchange, symbol))

                finally:
                    ping_task.cancel()

        except asyncio.TimeoutError:
            continue
        except Exception as e:
            if datetime.now() < end_time:
                print(f"⚠️ Ошибка сети Bybit WS: {e}. Переподключение через 5 секунд...")
                await asyncio.sleep(5)

    print("🏁 3 часа работы истекли. Завершаем работу сессии GitHub Actions.")

# --- 9. ТОЧКА ВХОДА ---
async def main():
    exchange = ccxt_async.bybit({
        'enableRateLimit': True,
        'options': {
            'defaultType': 'spot',
        }
    })
    async with ClientSession() as session:
        try:
            await bybit_websocket_listener(session, exchange)
        finally:
            await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())
