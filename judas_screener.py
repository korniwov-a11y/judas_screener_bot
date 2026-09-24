import asyncio
import base64
from datetime import datetime, timedelta, timezone
import json
import os
from aiohttp import ClientSession, FormData
import ccxt.async_support as ccxt_async
import mplfinance as mpf
import pandas as pd
import websockets

# --- 1. ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ (GitHub Secrets) ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

missing_keys = [
    name
    for name, val in [
        ("GEMINI_API_KEY", GEMINI_API_KEY),
        ("GROQ_API_KEY", GROQ_API_KEY),
        ("BOT_TOKEN", TELEGRAM_BOT_TOKEN),
        ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
    ]
    if not val
]

if missing_keys:
    raise ValueError(
        f"❌ Ошибка: Не найдены следующие GitHub Secrets: {', '.join(missing_keys)}"
    )

WORK_DURATION_SECONDS = 3 * 3600  # 3 часа


def to_ccxt_symbol(symbol: str) -> str:
    """Приводит тикер к формату CCXT (BTCUSDT -> BTC/USDT)."""
    if "/" in symbol:
        return symbol
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}/USDT"
    return symbol


# --- 2. ПОЛУЧЕНИЕ ТОП-15 МОНЕТ (CRYPTOCOMPARE API) ---
async def get_top_15_symbols(exchange: ccxt_async.bybit = None) -> list:
    """Динамически получает ТОП-15 монет по капитализации/объему."""
    print("🔍 Динамическая загрузка ТОП-15 монет по рынку (CryptoCompare)...")
    url = (
        "https://min-api.cryptocompare.com/data/top/mktcapfull?limit=30&tsym=USD"
    )
    stables_and_wraps = {
        "USDC",
        "USDT",
        "FDUSD",
        "DAI",
        "TUSD",
        "WBTC",
        "WBETH",
        "USDE",
        "STETH",
    }

    try:
        async with ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    top_symbols = []

                    for item in data.get("Data", []):
                        coin_info = item.get("CoinInfo", {})
                        symbol = coin_info.get("Name", "").upper()

                        if not symbol or symbol in stables_and_wraps:
                            continue

                        top_symbols.append(f"{symbol}USDT")
                        if len(top_symbols) == 15:
                            break

                    if top_symbols:
                        print(
                            f"✅ Динамический ТОП-15 загружен: {', '.join(top_symbols)}"
                        )
                        return top_symbols
    except Exception as e:
        print(
            f"⚠️ Ошибка загрузки рейтинга через CryptoCompare API: {e}. Используем базовый список."
        )

    return [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "BNBUSDT",
        "XRPUSDT",
        "DOGEUSDT",
        "ADAUSDT",
        "AVAXUSDT",
        "SUIUSDT",
        "LINKUSDT",
        "NEARUSDT",
        "DOTUSDT",
        "LTCUSDT",
        "APTUSDT",
        "PEPEUSDT",
    ]


# --- 3. ГЕНЕРАЦИЯ ГРАФИКА ---
def generate_chart_sync(symbol: str, df_40: pd.DataFrame) -> str:
    clean_symbol = symbol.replace("/", "_").replace(":", "")
    image_path = f"chart_{clean_symbol}.png"
    df_plot = df_40.copy()

    mpf.plot(
        df_plot,
        type="candle",
        style="charles",
        volume=False,
        savefig=image_path,
    )
    return image_path


# --- 4. МАТЕМАТИЧЕСКИЕ И АЛГОРИТМИЧЕСКИЕ ФИЛЬТРЫ ---
async def check_economic_news_async(
    session: ClientSession,
) -> tuple[bool, str]:
    """Проверяет отсутствие High Impact новостей по USD/EUR в окне ±30 минут."""
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    now_utc = datetime.now(timezone.utc)

    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                return True, "Новостной календарь недоступен (пропущено)"

            events = await resp.json()
            for event in events:
                if (
                    event.get("impact") == "High"
                    and event.get("country") in ["USD", "EUR"]
                ):
                    event_date_str = event.get("date")
                    if not event_date_str:
                        continue
                    event_time = datetime.fromisoformat(
                        event_date_str
                    ).astimezone(timezone.utc)
                    time_diff = (
                        abs((event_time - now_utc).total_seconds()) / 60.0
                    )

                    if time_diff <= 30:
                        return (
                            False,
                            f"Новость '{event.get('title')}' ({event.get('country')}) через {int(time_diff)} мин.",
                        )
            return True, "Чистый макро-фон"
    except Exception as e:
        return True, f"Ошибка новостей: {e}"


async def fetch_htf_context_async(
    exchange: ccxt_async.bybit, symbol: str, current_price: float
) -> dict:
    """Анализирует 1D тренд и ищет зоны 4H POI по закрытым свечам."""
    ccxt_symbol = to_ccxt_symbol(symbol)

    # 1D Тренд по SMA20
    ohlcv_1d = await exchange.fetch_ohlcv(
        ccxt_symbol, timeframe="1d", limit=30
    )
    df_1d = pd.DataFrame(
        ohlcv_1d,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df_1d["sma20"] = df_1d["close"].rolling(20).mean()
    global_trend = (
        "BULLISH"
        if df_1d["close"].iloc[-1] > df_1d["sma20"].iloc[-1]
        else "BEARISH"
    )

    # 4H POI по ЗАКРЫТЫМ свечам
    ohlcv_4h = await exchange.fetch_ohlcv(
        ccxt_symbol, timeframe="4h", limit=40
    )
    df_4h = pd.DataFrame(
        ohlcv_4h,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df_4h_closed = df_4h.iloc[:-1].copy()

    poi_detected, poi_type, poi_details = False, "None", ""

    # Поиск 4H FVG
    for i in range(len(df_4h_closed) - 2, 0, -1):
        if df_4h_closed["low"].iloc[i + 1] > df_4h_closed["high"].iloc[i - 1]:
            fvg_low, fvg_high = (
                df_4h_closed["high"].iloc[i - 1],
                df_4h_closed["low"].iloc[i + 1],
            )
            if fvg_low <= current_price <= fvg_high:
                poi_detected, poi_type, poi_details = (
                    True,
                    "4H Bullish FVG",
                    f"[{fvg_low:.4f} - {fvg_high:.4f}]",
                )
                break
        elif (
            df_4h_closed["high"].iloc[i + 1] < df_4h_closed["low"].iloc[i - 1]
        ):
            fvg_high, fvg_low = (
                df_4h_closed["low"].iloc[i - 1],
                df_4h_closed["high"].iloc[i + 1],
            )
            if fvg_low <= current_price <= fvg_high:
                poi_detected, poi_type, poi_details = (
                    True,
                    "4H Bearish FVG",
                    f"[{fvg_low:.4f} - {fvg_high:.4f}]",
                )
                break

    # Поиск 4H Order Block
    if not poi_detected:
        for i in range(len(df_4h_closed) - 5, len(df_4h_closed)):
            ob_low, ob_high = (
                df_4h_closed["low"].iloc[i],
                df_4h_closed["high"].iloc[i],
            )
            if ob_low <= current_price <= ob_high:
                poi_detected, poi_type, poi_details = (
                    True,
                    "4H Order Block",
                    f"[{ob_low:.4f} - {ob_high:.4f}]",
                )
                break

    return {
        "global_trend": global_trend,
        "poi_detected": poi_detected,
        "poi_type": poi_type,
        "poi_details": poi_details,
    }


def analyze_asian_range_and_disqualification(df_15m: pd.DataFrame) -> dict:
    """Проверяет London Killzone (08:00–11:00 UTC+3), ширину Азиатского боковика (<= 2%) и правила Expansion."""
    df = df_15m.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")

    tz_utc3 = timezone(timedelta(hours=3))
    df["time_utc3"] = df.index.tz_convert(tz_utc3)
    now_utc3 = datetime.now(tz_utc3)

    # 1. London Killzone (08:00–11:00 UTC+3)
    if not (8 <= now_utc3.hour < 11):
        return {
            "valid": False,
            "reason": f"Вне окна London Killzone (сейчас {now_utc3.strftime('%H:%M')} UTC+3)",
        }

    # 2. Asian Range (00:00 - 08:00 UTC+3)
    today = now_utc3.date()
    asian_df = df[
        (df["time_utc3"].dt.date == today)
        & (df["time_utc3"].dt.hour >= 0)
        & (df["time_utc3"].dt.hour < 8)
    ]

    if len(asian_df) < 8:
        return {
            "valid": False,
            "reason": "Недостаточно свечей для Азиатского диапазона",
        }

    asian_high, asian_low = asian_df["high"].max(), asian_df["low"].min()
    range_pct = ((asian_high - asian_low) / ((asian_high + asian_low) / 2.0)) * 100

    if range_pct > 2.0:
        return {
            "valid": False,
            "reason": f"Азиатский диапазон слишком широкий ({range_pct:.2f}%)",
        }

    # 3. Дисквалификация: Expansion (2 полнотелые свечи за диапазоном)
    last_2 = df.tail(2)
    closed_above = all(
        c > asian_high and o > asian_high
        for c, o in zip(last_2["close"], last_2["open"])
    )
    closed_below = all(
        c < asian_low and o < asian_low
        for c, o in zip(last_2["close"], last_2["open"])
    )

    if closed_above or closed_below:
        return {
            "valid": False,
            "reason": "Дисквалификация: 2 полнотелые свечи закрепились за ренджем (Expansion)",
        }

    return {
        "valid": True,
        "asian_high": asian_high,
        "asian_low": asian_low,
        "range_pct": range_pct,
    }


async def check_smt_divergence_async(
    exchange: ccxt_async.bybit, main_symbol: str, swept_side: str
) -> bool:
    """Проверяет SMT дивергенцию относительно BTC/USDT (или ETH/USDT для самого BTC)."""
    try:
        paired_symbol = (
            "ETH/USDT" if "BTC" in main_symbol.upper() else "BTC/USDT"
        )
        ohlcv_pair = await exchange.fetch_ohlcv(
            paired_symbol, timeframe="15m", limit=40
        )
        df_pair = pd.DataFrame(
            ohlcv_pair,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        df_pair["timestamp"] = (
            pd.to_datetime(df_pair["timestamp"], unit="ms")
            .dt.tz_localize("UTC")
            .dt.tz_convert(timezone(timedelta(hours=3)))
        )

        now_utc3 = datetime.now(timezone(timedelta(hours=3)))
        asian_pair = df_pair[
            (df_pair["timestamp"].dt.date == now_utc3.date())
            & (df_pair["timestamp"].dt.hour >= 0)
            & (df_pair["timestamp"].dt.hour < 8)
        ]

        if asian_pair.empty:
            return False

        pair_asian_high, pair_asian_low = (
            asian_pair["high"].max(),
            asian_pair["low"].min(),
        )
        last_pair_candle = df_pair.iloc[-1]

        if swept_side == "LOW":
            return last_pair_candle["low"] > pair_asian_low
        elif swept_side == "HIGH":
            return last_pair_candle["high"] < pair_asian_high

        return False
    except Exception as e:
        print(f"⚠️ Ошибка расчета SMT: {e}")
        return False


# --- 5. AGENT 1: GEMINI FLASH VISION ---
async def analyze_gemini_async(
    session: ClientSession, image_path: str, context: dict
) -> dict:
    def read_image_b64(path):
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    base64_image = await asyncio.to_thread(read_image_b64, image_path)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"

    prompt_text = (
        f"Ты — SMC аналитик. Проанализируй график {context['symbol']}.\n"
        f"КОНТЕКСТ: Снят Asian {context['swept_side']} (Границы: Low={context['asian_low']}, High={context['asian_high']}).\n"
        f"Зона 4H POI: {context['htf_poi']}. SMT Дивергенция: {context['smt_divergence']}.\n"
        "Задачи по графику:\n"
        "1. Оцени качество свипа (тенью или возвратом).\n"
        "2. Проверь наличие Displacement и CHOCH.\n"
        "3. Найди FVG для лимитного ордера.\n"
        "Верни СТРОГО JSON без markdown:\n"
        '{"sweep_quality": "HIGH"|"MEDIUM"|"LOW", "displacement": true, "choch_detected": true, "fvg_detected": true, "comment": "текст"}'
    )

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt_text},
                    {
                        "inline_data": {
                            "mime_type": "image/png",
                            "data": base64_image,
                        }
                    },
                ]
            }
        ]
    }

    try:
        async with session.post(url, json=payload, timeout=30) as resp:
            resp_data = await resp.json()
            raw_text = resp_data["candidates"][0]["content"]["parts"][0]["text"]
            clean_json = raw_text.replace("```json", "").replace("```", "").strip()
            return json.loads(clean_json)
    except Exception as e:
        print(f"❌ Ошибка Gemini: {e}")
        return {
            "sweep_quality": "LOW",
            "displacement": False,
            "comment": f"Ошибка: {e}",
        }


# --- 6. AGENT 2: GROQ LLAMA 3.3 ---
async def analyze_groq_async(
    session: ClientSession, vision_res: dict, context: dict
) -> dict:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }

    prompt = (
        f"Ты — Risk Manager сетапа Judas Swing. Прими решение по {context['symbol']}.\n"
        f"Vision Анализ: {json.dumps(vision_res, ensure_ascii=False)}\n"
        f"Контекст: Trend={context['global_trend_1d']}, POI={context['htf_poi']}, SMT={context['smt_divergence']}.\n"
        "Условия:\n"
        "1. R:R должен быть строго >= 1:3.\n"
        "2. При вердикте EXECUTE обязательно рассчитай:\n"
        "   - entry_range: диапазон цен для лимитного входа (по зоне FVG/OB), например '64200.0 - 64450.0'\n"
        "   - sl: точный уровень Stop Loss (за уровень свипа)\n"
        "   - tp: точный уровень Take Profit\n\n"
        "Верни СТРОГО JSON без markdown:\n"
        '{"verdict": "EXECUTE"|"REJECT", "entry_range": "мин_цена - макс_цена", "sl": 0.0, "tp": 0.0, "rr": "1:3.5", "reasons": "описание"}'
    )

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
    }

    try:
        async with session.post(
            url, headers=headers, json=payload, timeout=20
        ) as resp:
            res_json = await resp.json()
            raw_text = res_json["choices"][0]["message"]["content"]
            clean_json = raw_text.replace("```json", "").replace("```", "").strip()
            return json.loads(clean_json)
    except Exception as e:
        print(f"❌ Ошибка Groq: {e}")
        return {"verdict": "REJECT", "reasons": f"Ошибка API Groq: {e}"}


# --- 7. ОТПРАВКА В TELEGRAM ---
async def send_telegram_async(
    session: ClientSession,
    symbol: str,
    vision_data: dict,
    groq_data: dict,
    image_path: str,
):
    caption = (
        f"🚨 **СИГНАЛ JUDAS SWING: {symbol}**\n\n"
        f"📍 **Вердикт:** `{groq_data.get('verdict')}`\n"
        f"📊 **Качество свипа:** {vision_data.get('sweep_quality')}\n"
        f"⚡ **Displacement / CHOCH / FVG:** {vision_data.get('displacement')} / {vision_data.get('choch_detected')} / {vision_data.get('fvg_detected')}\n\n"
    )

    if groq_data.get("verdict") == "EXECUTE":
        caption += (
            f"🎯 **Диапазон входа (Entry):** `{groq_data.get('entry_range')}`\n"
            f"🛑 **Стоп-лосс (SL):** `{groq_data.get('sl')}`\n"
            f"🏆 **Тейк-профит (TP):** `{groq_data.get('tp')}`\n"
            f"📐 **Соотношение R:R:** `{groq_data.get('rr')}`\n\n"
        )

    caption += (
        f"📝 **Анализ:** {vision_data.get('comment')}\n"
        f"🛡 **Риск-менеджмент:** {groq_data.get('reasons')}"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    data = FormData()
    data.add_field("chat_id", TELEGRAM_CHAT_ID)
    data.add_field("caption", caption)
    data.add_field("parse_mode", "Markdown")

    with open(image_path, "rb") as f:
        data.add_field(
            "photo",
            f.read(),
            filename=os.path.basename(image_path),
            content_type="image/png",
        )

    try:
        async with session.post(url, data=data, timeout=30) as resp:
            if resp.status == 200:
                print(f"🟢 [{symbol}] Алерт успешно отправлен в Telegram!")
    except Exception as e:
        print(f"❌ Ошибка Telegram: {e}")


# --- 8. ОБРАБОТКА ЗАКРЫТИЯ СВЕЧИ СО ВСЕМИ ФИЛЬТРАМИ ---
async def process_candle_event(
    session: ClientSession, exchange: ccxt_async.bybit, symbol: str
):
    print(
        f"⚡ [{datetime.now().strftime('%H:%M:%S')}] Свеча 15m закрылась по {symbol}. Запуск проверок..."
    )
    image_path = None
    try:
        ccxt_symbol = to_ccxt_symbol(symbol)
        ohlcv = await exchange.fetch_ohlcv(
            ccxt_symbol, timeframe="15m", limit=50
        )
        df_15m = pd.DataFrame(
            ohlcv,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        df_15m["timestamp"] = pd.to_datetime(df_15m["timestamp"], unit="ms")
        df_15m.set_index("timestamp", inplace=True)

        # 1. Проверка Killzone, ширины Азии и Expansion
        asian_check = analyze_asian_range_and_disqualification(df_15m)
        if not asian_check["valid"]:
            print(f"ℹ️ [{symbol}] {asian_check['reason']}")
            return

        asian_high, asian_low = (
            asian_check["asian_high"],
            asian_check["asian_low"],
        )
        last_candle = df_15m.iloc[-1]

        # 2. Проверка факта свипа ликвидности Азии
        swept_side = None
        if last_candle["low"] < asian_low:
            swept_side = "LOW"
        elif last_candle["high"] > asian_high:
            swept_side = "HIGH"

        if not swept_side:
            return  # Нет свипа — выходим без затрат ресурсов

        # 3. Новостной фильтр (±30 мин)
        news_ok, news_reason = await check_economic_news_async(session)
        if not news_ok:
            print(f"⛔ [{symbol}] {news_reason}")
            return

        # 4. Контекст HTF (4H POI)
        htf_info = await fetch_htf_context_async(
            exchange, symbol, last_candle["close"]
        )
        if not htf_info["poi_detected"]:
            print(f"⛔ [{symbol}] Пропуск: Свип без 4H POI.")
            return

        # 5. SMT Дивергенция
        smt_present = await check_smt_divergence_async(
            exchange, symbol, swept_side
        )

        print(
            f"🎯 [{symbol}] Все математические условия выполнены! Вызываем ИИ-агенты..."
        )

        # 6. Генерация графика и вызов ИИ-агентов
        image_path = await asyncio.to_thread(
            generate_chart_sync, symbol, df_15m.tail(40)
        )

        prompt_data = {
            "symbol": symbol,
            "swept_side": swept_side,
            "asian_high": asian_high,
            "asian_low": asian_low,
            "global_trend_1d": htf_info["global_trend"],
            "htf_poi": f"{htf_info['poi_type']} {htf_info['poi_details']}",
            "smt_divergence": smt_present,
        }

        vision_res = await analyze_gemini_async(
            session, image_path, prompt_data
        )

        if vision_res.get("sweep_quality") in ["HIGH", "MEDIUM"]:
            final_decision = await analyze_groq_async(
                session, vision_res, prompt_data
            )
            if final_decision.get("verdict") == "EXECUTE":
                await send_telegram_async(
                    session, symbol, vision_res, final_decision, image_path
                )
            else:
                print(
                    f"⛔ [{symbol}] Отклонено Groq: {final_decision.get('reasons')}"
                )

    except Exception as e:
        print(f"❌ Ошибка обработки {symbol}: {e}")
    finally:
        if image_path and os.path.exists(image_path):
            os.remove(image_path)


# --- 9. WEBSOCKET СЛУШАТЕЛЬ (BYBIT V5) ---
async def send_bybit_ping(ws, end_time):
    try:
        while datetime.now() < end_time:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"op": "ping"}))
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


async def bybit_websocket_listener(
    session: ClientSession, exchange: ccxt_async.bybit
):
    symbols = await get_top_15_symbols(exchange)
    ws_url = "wss://stream.bybit.com/v5/public/spot"

    start_time = datetime.now()
    end_time = start_time + timedelta(seconds=WORK_DURATION_SECONDS)

    print(
        f"⏰ Запуск сканирования Bybit на 3 часа (до {end_time.strftime('%H:%M:%S')})..."
    )

    while datetime.now() < end_time:
        try:
            async with websockets.connect(ws_url) as ws:
                args = [f"kline.15.{s}" for s in symbols]
                sub_msg = {"op": "subscribe", "args": args}
                await ws.send(json.dumps(sub_msg))

                print("✅ WebSocket Bybit подключен. Слушаем 15m свечи...")
                ping_task = asyncio.create_task(send_bybit_ping(ws, end_time))

                try:
                    while datetime.now() < end_time:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        data = json.loads(msg)

                        if "topic" in data and data["topic"].startswith(
                            "kline.15."
                        ):
                            symbol = data["topic"].split(".")[-1]
                            kline_list = data.get("data", [])
                            for kline in kline_list:
                                if kline.get("confirm") is True:
                                    asyncio.create_task(
                                        process_candle_event(
                                            session, exchange, symbol
                                        )
                                    )
                finally:
                    ping_task.cancel()

        except asyncio.TimeoutError:
            continue
        except Exception as e:
            if datetime.now() < end_time:
                print(
                    f"⚠️ Ошибка сети Bybit WS: {e}. Переподключение через 5 секунд..."
                )
                await asyncio.sleep(5)

    print("🏁 3 часа работы истекли. Завершаем работу сессии GitHub Actions.")


# --- 10. ТОЧКА ВХОДА С ЗАЩИТОЙ ОТ БЛОКИРОВКИ 403 ---
async def main():
    exchange = ccxt_async.bybit(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "urls": {
                "api": {
                    "spot": "https://api.bytick.com",
                    "public": "https://api.bytick.com",
                }
            },
        }
    )
    async with ClientSession() as session:
        try:
            await bybit_websocket_listener(session, exchange)
        finally:
            await exchange.close()


if __name__ == "__main__":
    asyncio.run(main())
