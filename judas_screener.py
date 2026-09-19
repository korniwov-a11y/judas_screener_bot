import asyncio
import os
import json
import base64
from datetime import datetime
import pandas as pd
import mplfinance as mpf
import ccxt.async_support as ccxt_async
from aiohttp import web, ClientSession, FormData
import websockets

# --- 1. ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PORT = int(os.getenv("PORT", 8080))

# --- 2. ДИНАМИЧЕСКИЙ ФИЛЬТР ТОП-15 МОНЕТ ---
async def get_top_15_symbols(exchange: ccxt_async.binance) -> list:
    """
    Получает топ-15 USDT-пар по комбинации Капитализации и 24h Объема торгов.
    Фильтрует стейблкоины и wrap-токены.
    """
    try:
        print("🔍 Обновление списка ТОП-15 монет по капитализации и объему...")
        tickers = await exchange.fetch_tickers()
        
        # Исключаем стейблкоины и обернутые активы
        stables_and_wraps = {'USDC', 'USDT', 'FDUSD', 'DAI', 'TUSD', 'WBTC', 'WBETH', 'USDE'}
        
        candidates = []
        for symbol, ticker in tickers.items():
            if not symbol.endswith('/USDT'):
                continue
            
            base = symbol.split('/')[0]
            if base in stables_and_wraps:
                continue

            quote_volume = ticker.get('quoteVolume', 0) # Объём в USDT
            if quote_volume and quote_volume > 0:
                candidates.append({
                    'symbol': symbol.replace('/', '').replace(':USDT', ''), # Перевод в формат BTCUSDT
                    'ccxt_symbol': symbol,
                    'volume': quote_volume
                })
        
        # Сортировка по объему торгов и выбор TOP-15
        sorted_candidates = sorted(candidates, key=lambda x: x['volume'], reverse=True)
        top_15 = [c['symbol'] for c in sorted_candidates[:15]]
        
        print(f"✅ Выбран ТОП-15 монет: {', '.join(top_15)}")
        return top_15
    except Exception as e:
        print(f"⚠️ Ошибка при получении ТОП монет: {e}. Откат к резервному списку.")
        return [
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
            "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "SUIUSDT", "LINKUSDT",
            "NEARUSDT", "DOTUSDT", "LTCUSDT", "APTUSDT", "PEPEUSDT"
        ]

# --- 3. HEALTH CHECK СЕРВЕР ДЛЯ RENDER ---
async def handle_health_check(request):
    return web.Response(text="Judas Async Screener is Running!")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f"🌐 Health-check сервер запущен на порту {PORT}")

# --- 4. АСИНХРОННАЯ ЗАГРУЗКА ДАННЫХ И ОТРИСОВКА ГРАФИКА ---
def generate_chart_sync(symbol: str, dataframe: pd.DataFrame) -> str:
    """Синхронный рендеринг PNG-графика через mplfinance (CPU-bound)"""
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

async def fetch_and_draw_chart_async(exchange: ccxt_async.binance, symbol: str) -> str:
    """Асинхронно фетчит OHLCV и запускает генерацию графика в отдельном потоке"""
    ccxt_symbol = f"{symbol[:-4]}/USDT" if symbol.endswith("USDT") else symbol
    
    ohlcv = await exchange.fetch_ohlcv(ccxt_symbol, timeframe="15m", limit=40)
    
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)

    image_path = await asyncio.to_thread(generate_chart_sync, symbol, df)
    return image_path

# --- 5. AGENT 1: GEMINI FLASH (VISION) ---
async def analyze_gemini_async(session: ClientSession, image_path: str) -> dict:
    """Асинхронный анализ структуры SMC по графику через Gemini 1.5 Flash"""
    def read_image_b64(path):
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode('utf-8')

    base64_image = await asyncio.to_thread(read_image_b64, image_path)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"

    prompt_text = (
        "Ты — SMC аналитик. Проанализируй свечной график на предмет сетапа Judas Swing. "
        "Определи: 1) Свип ликвидности (тенью или телом). 2) Наличие импульсного движения (Displacement). "
        "3) Наличие CHOCH и свежего FVG. "
        "Верни СТРОГО JSON без markdown символов и без ```json: "
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
        print(f"❌ Ошибка запроса к Gemini Flash: {e}")
        return {"sweep_quality": "LOW", "displacement": False, "comment": f"Ошибка AI: {e}"}

# --- 6. AGENT 2: GROQ LLAMA 3.3 (RISK MANAGER) ---
async def analyze_groq_async(session: ClientSession, vision_json: dict, risk_reward: float = 3.5, candles_closed_outside: int = 0) -> dict:
    """Асинхронная проверка риска и валидация сетапа через Groq Llama 3.3"""
    url = "[https://api.groq.com/openai/v1/chat/completions](https://api.groq.com/openai/v1/chat/completions)"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    system_prompt = (
        "Ты — Главный Риск-Менеджер сетапа Judas Swing. "
        "Правила отбраковки: "
        "1. Если candles_closed_outside >= 2 — REJECT. "
        "2. Требуется R:R >= 1:3. "
        "3. Должны присутствовать CHOCH и FVG. "
        "Верни СТРОГО JSON вида: {\"verdict\": \"EXECUTE\" или \"REJECT\", \"reasons\": [\"причина1\", \"причина2\"]}"
    )

    user_payload = {
        "vision_metrics": vision_json,
        "risk_reward": risk_reward,
        "candles_closed_outside": candles_closed_outside
    }

    data = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload)}
        ],
        "response_format": {"type": "json_object"}
    }

    try:
        async with session.post(url, headers=headers, json=data, timeout=30) as resp:
            res_json = await resp.json()
            content = res_json['choices'][0]['message']['content']
            return json.loads(content)
    except Exception as e:
        print(f"❌ Ошибка запроса к Groq: {e}")
        return {"verdict": "REJECT", "reasons": [f"Ошибка API Groq: {e}"]}

# --- 7. TELEGRAM SENDER ---
async def send_telegram_async(session: ClientSession, symbol: str, vision_data: dict, final_verdict: dict, image_path: str):
    """Асинхронная отправка фото с описанием в Telegram"""
    caption = (
        f"🚨 **СИГНАЛ JUDAS SWING: {symbol}**\n\n"
        f"📍 **Вердикт:** `{final_verdict.get('verdict')}`\n"
        f"📊 **Качество свипа:** {vision_data.get('sweep_quality', 'N/A')}\n"
        f"⚡ **CHOCH / FVG:** {vision_data.get('choch_detected')}/{vision_data.get('fvg_detected')}\n"
        f"📝 **Анализ:** {vision_data.get('comment', '')}\n"
        f"🛡 **Причины решения:** {', '.join(final_verdict.get('reasons', []))}\n"
    )

    url = f"[https://api.telegram.org/bot](https://api.telegram.org/bot){TELEGRAM_BOT_TOKEN}/sendPhoto"

    data = FormData()
    data.add_field('chat_id', TELEGRAM_CHAT_ID)
    data.add_field('caption', caption)
    data.add_field('parse_mode', 'Markdown')

    with open(image_path, 'rb') as f:
        data.add_field('photo', f.read(), filename=os.path.basename(image_path), content_type='image/png')

    try:
        async with session.post(url, data=data, timeout=30) as resp:
            if resp.status == 200:
                print(f"🟢 [{symbol}] Алерт успешно отправлен в Telegram!")
            else:
                err_text = await resp.text()
                print(f"❌ Ошибка отправки в Telegram [{resp.status}]: {err_text}")
    except Exception as e:
        print(f"❌ Исключение при отправке в Telegram: {e}")

# --- 8. ОБРАБОТЧИК СОБЫТИЯ ЗАКРЫТИЯ СВЕЧИ ---
async def process_candle_event(session: ClientSession, exchange: ccxt_async.binance, symbol: str):
    """Параллельно вызываемый пайплайн анализа конкретной монеты"""
    print(f"⚡ [{datetime.now().strftime('%H:%M:%S')}] Свеча 15m закрылась по {symbol}. Старт анализа...")
    
    image_path = None
    try:
        image_path = await fetch_and_draw_chart_async(exchange, symbol)

        vision_res = await analyze_gemini_async(session, image_path)
        print(f"[{symbol}] Gemini Flash: {vision_res}")

        if vision_res.get("sweep_quality") in ["HIGH", "MEDIUM"]:
            final_decision = await analyze_groq_async(session, vision_res)
            print(f"[{symbol}] Groq Llama 3.3: {final_decision}")

            if final_decision.get("verdict") == "EXECUTE":
                await send_telegram_async(session, symbol, vision_res, final_decision, image_path)
            else:
                print(f"⚪ [{symbol}] Сетап отбракован Риск-Менеджером.")
        else:
            print(f"⚪ [{symbol}] Низкое качество свипа ({vision_res.get('sweep_quality')}). Пропуск Groq.")

    except Exception as e:
        print(f"❌ Ошибка при обработке пайплайна {symbol}: {e}")
    finally:
        if image_path and os.path.exists(image_path):
            try:
                os.remove(image_path)
            except OSError:
                pass

# --- 9. WEBSOCKET СЛУШАТЕЛЬ С ДИНАМИЧЕСКИМ ТОП-15 ---
async def binance_websocket_listener(session: ClientSession, exchange: ccxt_async.binance):
    while True:
        # Получаем актуальный ТОП-15 перед подключением/переподключением
        symbols = await get_top_15_symbols(exchange)
        stream_names = "/".join([f"{s.lower()}@kline_15m" for s in symbols])
        ws_url = f"wss://[stream.binance.com:9443/ws/](https://stream.binance.com:9443/ws/){stream_names}"

        try:
            print(f"🔗 Подключение к Binance WebSocket Stream для {len(symbols)} монет...")
            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            ) as ws:
                print("✅ WebSocket подключен. Мониторинг закрытия 15m свечей 24/7...")
                
                # Таймер для перерасчета ТОП-15 монет каждые 6 часов
                last_top_update = datetime.now()

                while True:
                    # Раз в 6 часов переподключаемся, чтобы обновить ТОП-15 пар
                    if (datetime.now() - last_top_update).total_seconds() > 21600:
                        print("🔄 Прошло 6 часов, переподключение для обновления ТОП-15 монет...")
                        break

                    msg = await ws.recv()
                    data = json.loads(msg)

                    if data.get('e') == 'kline':
                        kline = data['k']
                        if kline.get('x'):  # Закрытие 15m свечи
                            symbol = data['s']
                            asyncio.create_task(process_candle_event(session, exchange, symbol))

        except (websockets.ConnectionClosed, websockets.WebSocketException, Exception) as e:
            print(f"⚠️ Ошибка WebSocket: {e}. Переподключение через 5 секунд...")
            await asyncio.sleep(5)

# --- 10. ГЛАВНАЯ ТОЧКА ВХОДА ---
async def main():
    print("🚀 Запуск асинхронного воркера Judas Screener (Динамический ТОП-15, Режим 24/7)...")

    await start_web_server()

    exchange = ccxt_async.binance({'enableRateLimit': True})
    
    async with ClientSession() as session:
        try:
            await binance_websocket_listener(session, exchange)
        finally:
            await exchange.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Воркер остановлен вручную.")
