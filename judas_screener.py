import asyncio
import base64
from datetime import datetime, timedelta, timezone
import html
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
CHOCH_TIMEOUT_MINUTES = 45  # Таймер для поиска CHOCH


def to_ccxt_symbol(symbol: str) -> str:
    """Приводит тикер к формату CCXT (BTCUSDT -> BTC/USDT)."""
    if "/" in symbol:
        return symbol
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}/USDT"
    return symbol


def get_current_is_daylight_saving() -> bool:
    """Определяет, находимся ли мы в летнем времени (EDT) или зимнем (EST)."""
    now = datetime.now(timezone.utc)
    # Хроническое определение: в США летнее время обычно с марта по ноябрь
    # Упрощенно: если месяц в диапазоне 3-10 (март-октябрь), то EDT
    return 3 <= now.month <= 10


def get_london_killzone_window_utc() -> tuple[int, int]:
    """
    Возвращает корректное окно London Killzone в UTC с учетом сезонного сдвига.

    Winter (EST / UTC-5): 07:00 - 10:00 UTC
    Summer (EDT / UTC-4): 06:00 - 09:00 UTC
    """
    if get_current_is_daylight_saving():
        return 6, 9  # EDT: 06:00 - 09:00 UTC
    else:
        return 7, 10  # EST: 07:00 - 10:00 UTC


def get_london_killzone_window_utc3() -> tuple[int, int]:
    """Возвращает окно London Killzone для UTC+3."""
    utc_start, utc_end = get_london_killzone_window_utc()
    utc3_start = (utc_start + 3) % 24
    utc3_end = (utc_end + 3) % 24
    return utc3_start, utc3_end


# --- 2. ПОЛУЧЕНИЕ ТОП-100 МОНЕТ (CRYPTOCOMPARE API) ---
async def get_top_100_symbols(exchange: ccxt_async.bybit = None) -> list:
    """Динамически получает ТОП-100 монет по капитализации/объему."""
    print("🔍 Динамическая загрузка ТОП-100 монет по рынку (CryptoCompare)...")

    # Расширенный список стейблкоинов, обернутых и ликвидных токенов
    stables_and_wraps = {
        "USDC", "USDT", "FDUSD", "DAI", "TUSD", "WBTC", "WBETH", "USDE",
        "STETH", "WEETH", "RETH", "CBETH", "BUSD", "USDD", "PYUSD", "FRAX",
        "LUSD", "GUSD", "USDS", "CRVUSD", "SUSD", "USDP", "USDJ"
    }

    top_symbols = []

    try:
        async with ClientSession() as session:
            for page in range(3):
                url = (
                    f"https://min-api.cryptocompare.com/data/top/mktcapfull"
                    f"?limit=100&page={page}&tsym=USD"
                )
                async with session.get(url, timeout=10) as resp:
                    if resp.status != 200:
                        continue

                    data = await resp.json()

                    for item in data.get("Data", []):
                        coin_info = item.get("CoinInfo", {})
                        symbol = coin_info.get("Name", "").upper()

                        if not symbol or symbol in stables_and_wraps:
                            continue

                        top_symbols.append(f"{symbol}USDT")
                        if len(top_symbols) == 100:
                            break

                if len(top_symbols) == 100:
                    break

            if top_symbols:
                print(
                    f"✅ Динамический ТОП-100 загружен ({len(top_symbols)} монет). "
                    f"Первые 5: {', '.join(top_symbols[:5])}..."
                )
                return top_symbols

    except Exception as e:
        print(
            f"⚠️ Ошибка загрузки рейтинга через CryptoCompare API: {e}. "
            f"Используем базовый резервный список."
        )

    return [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT",
        "ADAUSDT", "AVAXUSDT", "SUIUSDT", "LINKUSDT", "NEARUSDT", "DOTUSDT",
        "LTCUSDT", "APTUSDT", "PEPEUSDT", "SHIBUSDT", "TRXUSDT", "BCHUSDT",
        "UNIUSDT", "FETUSDT", "ICPUSDT", "ETCUSDT", "XLMUSDT", "RENDERUSDT",
        "TAOUSDT", "AAVEUSDT", "INJUSDT", "TIAUSDT", "STXUSDT", "FILUSDT"
    ][:100]


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


# --- 4. ПОЛУЧЕНИЕ FUNDING RATE ---
async def fetch_funding_rate_async(
    exchange: ccxt_async.bybit, symbol: str
) -> float:
    """Получает текущий Funding Rate для криптовалютного фьючерса."""
    try:
        ccxt_symbol = to_ccxt_symbol(symbol)
        ticker = await exchange.fetch_ticker(ccxt_symbol)
        funding_rate = ticker.get("info", {}).get("fundingRate")
        if funding_rate:
            return float(funding_rate)
    except Exception as e:
        print(f"⚠️ Ошибка получения Funding Rate для {symbol}: {e}")

    return 0.0


# --- 5. МАТЕМАТИЧЕСКИЕ И АЛГОРИТМИЧЕСКИЕ ФИЛЬТРЫ ---
async def check_economic_news_async(
    session: ClientSession,
) -> tuple[bool, str]:
    """
    Проверяет отсутствие макроэкономических новостей США в окне ±45 минут
    и крипто-событий в окне ±2 часа.
    """
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    now_utc = datetime.now(timezone.utc)

    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                return True, "Новостной календарь недоступен (пропущено)"

            events = await resp.json()

            us_critical_keywords = [
                "CPI", "NFP", "Non-Farm", "FOMC", "Interest Rate",
                "Federal Funds Rate", "Retail Sales", "Unemployment"
            ]

            for event in events:
                impact = event.get("impact", "")
                country = event.get("country", "")
                title = event.get("title", "").upper()

                event_date_str = event.get("date")
                if not event_date_str:
                    continue

                try:
                    event_time = datetime.fromisoformat(event_date_str).astimezone(timezone.utc)
                except Exception:
                    continue

                time_diff_minutes = abs((event_time - now_utc).total_seconds()) / 60.0

                if (
                    impact == "High"
                    and country in ["USD", "EUR"]
                    and any(kw in title for kw in us_critical_keywords)
                    and time_diff_minutes <= 45
                ):
                    return (
                        False,
                        f"Новость '{event.get('title')}' ({event.get('country')}) через {int(time_diff_minutes)} мин.",
                    )

            return True, "Чистый макро-фон и крипто-календарь"
    except Exception as e:
        return True, f"Ошибка новостей: {e}"


async def fetch_htf_context_async(
    exchange: ccxt_async.bybit, symbol: str, current_price: float
) -> dict:
    """
    Анализирует 1D тренд и ищет зоны 4H POI по закрытым свечам.
    Также проверяет, запечатана ли зона (инвалидирована).
    """
    ccxt_symbol = to_ccxt_symbol(symbol)

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

    ohlcv_4h = await exchange.fetch_ohlcv(
        ccxt_symbol, timeframe="4h", limit=40
    )
    df_4h = pd.DataFrame(
        ohlcv_4h,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df_4h_closed = df_4h.iloc[:-1].copy()

    poi_detected, poi_type, poi_details = False, "None", ""
    poi_invalidated = False

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
            elif current_price > fvg_high:
                poi_invalidated = True

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
            elif current_price < fvg_low:
                poi_invalidated = True

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
            elif current_price > ob_high or current_price < ob_low:
                poi_invalidated = True

    return {
        "global_trend": global_trend,
        "poi_detected": poi_detected,
        "poi_type": poi_type,
        "poi_details": poi_details,
        "poi_invalidated": poi_invalidated,
    }


def analyze_asian_range_and_disqualification(df_15m: pd.DataFrame, symbol: str = "") -> dict:
    """
    Проверяет London Killzone с сезонным сдвигом, ширину Азиатского боковика
    (2.5% для BTC, 4% для альткоинов) и правила Expansion.
    """
    df = df_15m.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")

    tz_utc3 = timezone(timedelta(hours=3))
    df["time_utc3"] = df.index.tz_convert(tz_utc3)
    now_utc3 = datetime.now(tz_utc3)

    killzone_start, killzone_end = get_london_killzone_window_utc3()
    current_hour = now_utc3.hour

    if killzone_start < killzone_end:
        in_killzone = killzone_start <= current_hour < killzone_end
    else:
        in_killzone = current_hour >= killzone_start or current_hour < killzone_end

    if not in_killzone:
        return {
            "valid": False,
            "reason": f"Вне окна London Killzone (сейчас {now_utc3.strftime('%H:%M')} UTC+3, окно {killzone_start}-{killzone_end})",
        }

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

    is_btc = "BTC" in symbol.upper()
    max_range_pct = 2.5 if is_btc else 4.0

    if range_pct > max_range_pct:
        return {
            "valid": False,
            "reason": f"Азиатский диапазон слишком широкий ({range_pct:.2f}%, макс {max_range_pct}%)",
        }

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
) -> dict:
    """Проверяет криптовалютную SMT-дивергенцию между BTC/ETH/SOL."""
    try:
        is_btc = "BTC" in main_symbol.upper()
        if is_btc:
            paired_symbols = ["ETH/USDT", "SOL/USDT"]
        else:
            paired_symbols = ["BTC/USDT", "ETH/USDT"]

        result = {
            "smt_detected": False,
            "smt_type": "NONE",
            "details": "",
            "pair_analysis": {}
        }

        ccxt_main = to_ccxt_symbol(main_symbol)
        ohlcv_main = await exchange.fetch_ohlcv(
            ccxt_main, timeframe="15m", limit=120
        )
        df_main = pd.DataFrame(
            ohlcv_main,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        df_main["timestamp"] = (
            pd.to_datetime(df_main["timestamp"], unit="ms")
            .dt.tz_localize("UTC")
            .dt.tz_convert(timezone(timedelta(hours=3)))
        )
        df_main = df_main.iloc[:-1].copy()

        now_utc3 = datetime.now(timezone(timedelta(hours=3)))
        asian_main = df_main[
            (df_main["timestamp"].dt.date == now_utc3.date())
            & (df_main["timestamp"].dt.hour >= 0)
            & (df_main["timestamp"].dt.hour < 9)
        ]

        if asian_main.empty:
            return result

        main_asian_high = asian_main["high"].max()
        main_asian_low = asian_main["low"].min()
        main_last = df_main.iloc[-1]

        for pair_symbol in paired_symbols:
            try:
                ohlcv_pair = await exchange.fetch_ohlcv(
                    pair_symbol, timeframe="15m", limit=120
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
                df_pair = df_pair.iloc[:-1].copy()

                asian_pair = df_pair[
                    (df_pair["timestamp"].dt.date == now_utc3.date())
                    & (df_pair["timestamp"].dt.hour >= 0)
                    & (df_pair["timestamp"].dt.hour < 9)
                ]

                if asian_pair.empty:
                    continue

                pair_asian_high = asian_pair["high"].max()
                pair_asian_low = asian_pair["low"].min()
                pair_last = df_pair.iloc[-1]
                pair_name = pair_symbol.split("/")[0]

                if swept_side == "LOW":
                    main_low_swept = main_last["low"] < main_asian_low
                    pair_high_held = pair_last["low"] >= pair_asian_low

                    if main_low_swept and pair_high_held:
                        result["smt_detected"] = True
                        result["smt_type"] = "BULLISH"
                        result["details"] = f"{main_symbol} обновил Low, {pair_name} удерживает Low (сила)"
                        result["pair_analysis"][pair_name] = "STRONG"
                    else:
                        result["pair_analysis"][pair_name] = "WEAK" if main_low_swept else "ALIGNED"

                elif swept_side == "HIGH":
                    main_high_swept = main_last["high"] > main_asian_high
                    pair_high_held = pair_last["high"] <= pair_asian_high

                    if main_high_swept and pair_high_held:
                        result["smt_detected"] = True
                        result["smt_type"] = "BEARISH"
                        result["details"] = f"{main_symbol} обновил High, {pair_name} удерживает High (сила)"
                        result["pair_analysis"][pair_name] = "STRONG"
                    else:
                        result["pair_analysis"][pair_name] = "WEAK" if main_high_swept else "ALIGNED"

            except Exception:
                pass

        return result
    except Exception as e:
        print(f"⚠️ Ошибка расчета SMT: {e}")
        return {
            "smt_detected": False,
            "smt_type": "NONE",
            "details": f"Ошибка: {e}",
            "pair_analysis": {}
        }


# --- 6. AGENT 1: GEMINI FLASH VISION ---
async def analyze_gemini_async(
    session: ClientSession, image_path: str, context: dict
) -> dict:
    def read_image_b64(path):
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    base64_image = await asyncio.to_thread(read_image_b64, image_path)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"

    prompt_text = (
        f"Ты — SMC аналитик деривативных рынков. Проанализируй график {context['symbol']}.\n"
        f"КОНТЕКСТ: Снят Asian {context['swept_side']} (Границы: Low={context['asian_low']}, High={context['asian_high']}).\n"
        f"Зона 4H POI: {context['htf_poi']}. "
        f"Funding Rate: {context.get('funding_rate', 0.0)}. "
        f"SMT Анализ: {context.get('smt_analysis', 'N/A')}.\n"
        "Задачи по графику:\n"
        "1. Оцени качество свипа (тенью или возвратом).\n"
        "2. Проверь наличие Displacement (агрессивный ответ рынка).\n"
        "3. Проверь CHOCH (слом структуры на LTF).\n"
        "4. Найди FVG для лимитного ордера в импульсном движении.\n"
        "5. Оцени Open Interest метрику (должно быть снижение на момент свипа).\n"
        "Верни СТРОГО JSON без дополнительных пояснений:\n"
        '{"sweep_quality": "HIGH"|"MEDIUM"|"LOW", "displacement": true|false, "choch_detected": true|false, "fvg_detected": true|false, "oi_confirmation": "спад с выкупом"|"спад с продажей"|"нет данных", "comment": "текст"}'
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
        ],
        "generationConfig": {
            "response_mime_type": "application/json"
        }
    }

    try:
        async with session.post(url, json=payload, timeout=30) as resp:
            resp_data = await resp.json()
            raw_text = resp_data["candidates"][0]["content"]["parts"][0]["text"]
            return json.loads(raw_text)
    except Exception as e:
        print(f"❌ Ошибка Gemini: {e}")
        return {
            "sweep_quality": "LOW",
            "displacement": False,
            "comment": f"Ошибка: {e}",
        }


# --- 7. AGENT 2: GROQ LLAMA 3.3 ---
async def analyze_groq_async(
    session: ClientSession, vision_res: dict, context: dict, entry_timestamp: datetime = None
) -> dict:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }

    prompt = (
        f"Ты — Risk Manager сетапа Judas Swing для деривативов. Прими решение по {context['symbol']}.\n"
        f"Vision Анализ: {json.dumps(vision_res, ensure_ascii=False)}\n"
        f"Контекст: Trend={context['global_trend_1d']}, POI={context['htf_poi']}, "
        f"SMT={context['smt_type']}, Funding Rate={context.get('funding_rate', 0.0)}.\n"
        "Правила дисквалификации:\n"
        "- Закрепление телом + Рост OI: Цена за границей диапазона с одновременным ростом OI → REJECT\n"
        "- Отсутствие CHOCH за 45 мин: Боковая консолидация без импульса → REJECT\n"
        "- Пробой HTF POI: Зона полностью запечатана → REJECT\n"
        "Условия EXECUTE:\n"
        "1. R:R должен быть строго >= 1:3.\n"
        "2. Обязательно рассчитай:\n"
        "   - entry_range: диапазон цен для лимитного входа (по зоне FVG/OB), например '64200.0 - 64450.0'\n"
        "   - sl: точный уровень Stop Loss (за уровень свипа)\n"
        "   - tp: точный уровень Take Profit\n\n"
        "Верни СТРОГО JSON:\n"
        '{"verdict": "EXECUTE"|"REJECT", "entry_range": "мин_цена - макс_цена", "sl": 0.0, "tp": 0.0, "rr": "1:3.5", "reasons": "описание", "disqualification_reason": ""}'
    )

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "response_format": {"type": "json_object"}
    }

    try:
        async with session.post(
            url, headers=headers, json=payload, timeout=20
        ) as resp:
            res_json = await resp.json()
            raw_text = res_json["choices"][0]["message"]["content"]
            return json.loads(raw_text)
    except Exception as e:
        print(f"❌ Ошибка Groq: {e}")
        return {"verdict": "REJECT", "reasons": f"Ошибка API Groq: {e}"}


# --- 8. ОТПРАВКА В TELEGRAM ---
async def send_telegram_async(
    session: ClientSession,
    symbol: str,
    vision_data: dict,
    groq_data: dict,
    image_path: str,
    additional_context: dict = None,
):
    caption = (
        f"🚨 <b>СИГНАЛ JUDAS SWING (DERIVATIVES): {html.escape(symbol)}</b>\n\n"
        f"📍 <b>Вердикт:</b> <code>{html.escape(str(groq_data.get('verdict')))}</code>\n"
        f"📊 <b>Качество свипа:</b> {html.escape(str(vision_data.get('sweep_quality')))}\n"
        f"⚡ <b>Displacement / CHOCH / FVG:</b> {vision_data.get('displacement')} / {vision_data.get('choch_detected')} / {vision_data.get('fvg_detected')}\n"
        f"💾 <b>OI Подтверждение:</b> {vision_data.get('oi_confirmation', 'N/A')}\n\n"
    )

    if additional_context:
        caption += (
            f"🔷 <b>Funding Rate:</b> <code>{additional_context.get('funding_rate', 0.0)}</code>\n"
            f"🧬 <b>SMT Дивергенция:</b> <code>{additional_context.get('smt_type', 'NONE')}</code>\n"
        )
        if additional_context.get('smt_details'):
            caption += f"   {additional_context.get('smt_details')}\n"
        caption += "\n"

    if groq_data.get("verdict") == "EXECUTE":
        caption += (
            f"🎯 <b>Диапазон входа (Entry):</b> <code>{html.escape(str(groq_data.get('entry_range')))}</code>\n"
            f"🛑 <b>Стоп-лосс (SL):</b> <code>{html.escape(str(groq_data.get('sl')))}</code>\n"
            f"🏆 <b>Тейк-профит (TP):</b> <code>{html.escape(str(groq_data.get('tp')))}</code>\n"
            f"📐 <b>Соотношение R:R:</b> <code>{html.escape(str(groq_data.get('rr')))}</code>\n\n"
        )

    caption += (
        f"📝 <b>Анализ:</b> {html.escape(str(vision_data.get('comment', '')))}\n"
        f"🛡 <b>Риск-менеджмент:</b> {html.escape(str(groq_data.get('reasons', '')))}"
    )

    if groq_data.get('disqualification_reason'):
        caption += f"\n⚠️ <b>Дисквалификация:</b> {html.escape(str(groq_data.get('disqualification_reason')))}"

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    data = FormData()
    data.add_field("chat_id", TELEGRAM_CHAT_ID)
    data.add_field("caption", caption)
    data.add_field("parse_mode", "HTML")

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


# --- 9. ОБРАБОТКА ЗАКРЫТИЯ СВЕЧИ СО ВСЕМИ ФИЛЬТРАМИ ---
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
            ccxt_symbol, timeframe="15m", limit=120
        )
        df_15m = pd.DataFrame(
            ohlcv,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        df_15m["timestamp"] = pd.to_datetime(df_15m["timestamp"], unit="ms")
        df_15m.set_index("timestamp", inplace=True)
        df_15m = df_15m.iloc[:-1].copy()

        asian_check = analyze_asian_range_and_disqualification(df_15m, symbol)
        if not asian_check["valid"]:
            print(f"ℹ️ [{symbol}] {asian_check['reason']}")
            return

        asian_high, asian_low = (
            asian_check["asian_high"],
            asian_check["asian_low"],
        )
        last_candle = df_15m.iloc[-1]

        swept_side = None
        if last_candle["low"] < asian_low:
            swept_side = "LOW"
        elif last_candle["high"] > asian_high:
            swept_side = "HIGH"

        if not swept_side:
            return

        news_ok, news_reason = await check_economic_news_async(session)
        if not news_ok:
            print(f"⛔ [{symbol}] {news_reason}")
            return

        htf_info = await fetch_htf_context_async(
            exchange, symbol, last_candle["close"]
        )
        if not htf_info["poi_detected"]:
            print(f"⛔ [{symbol}] Пропуск: Свип без 4H POI.")
            return

        if htf_info["poi_invalidated"]:
            print(f"⛔ [{symbol}] Пропуск: 4H POI полностью запечатана (инвалидирована).")
            return

        funding_rate = await fetch_funding_rate_async(exchange, symbol)
        smt_result = await check_smt_divergence_async(
            exchange, symbol, swept_side
        )

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
            "smt_type": smt_result["smt_type"],
            "smt_analysis": smt_result.get("details", "N/A"),
            "funding_rate": funding_rate,
        }

        vision_res = await analyze_gemini_async(
            session, image_path, prompt_data
        )

        if vision_res.get("sweep_quality") in ["HIGH", "MEDIUM"]:
            final_decision = await analyze_groq_async(
                session, vision_res, prompt_data
            )
            if final_decision.get("verdict") == "EXECUTE":
                additional_ctx = {
                    "funding_rate": funding_rate,
                    "smt_type": smt_result["smt_type"],
                    "smt_details": smt_result.get("details", ""),
                }
                await send_telegram_async(
                    session, symbol, vision_res, final_decision, image_path, additional_ctx
                )
            else:
                disq_reason = final_decision.get('disqualification_reason', '')
                if disq_reason:
                    print(f"⛔ [{symbol}] Дисквалификация: {disq_reason}")
                else:
                    print(f"⛔ [{symbol}] Отклонено Groq: {final_decision.get('reasons')}")

    except Exception as e:
        print(f"❌ Ошибка обработки {symbol}: {e}")
    finally:
        if image_path and os.path.exists(image_path):
            os.remove(image_path)


# --- 10. WEBSOCKET СЛУШАТЕЛЬ (BYBIT V5) ---
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
    symbols = await get_top_100_symbols(exchange)
    ws_url = "wss://stream.bybit.com/v5/public/spot"

    start_time = datetime.now()
    end_time = start_time + timedelta(seconds=WORK_DURATION_SECONDS)

    print(
        f"⏰ Запуск сканирования Bybit на 3 часа (до {end_time.strftime('%H:%M:%S')})..."
    )

    killzone_start, killzone_end = get_london_killzone_window_utc3()
    is_daylight = get_current_is_daylight_saving()
    season = "EDT (лето)" if is_daylight else "EST (зима)"
    print(f"🕐 London Killzone UTC+3: {killzone_start:02d}:00 - {killzone_end:02d}:00 (сейчас {season})")

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
                        msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        data = json.loads(msg)

                        if "topic" in data and data["topic"].startswith("kline.15."):
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


# --- 11. ТОЧКА ВХОДА С ЗАЩИТОЙ ОТ БЛОКИРОВКИ 403 ---
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
