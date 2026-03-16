"""
✈️ Chile Flight Price Monitor
Monitorea precios de vuelos desde SCL y detecta anomalías/errores de precio.
Envía alertas por Telegram cuando encuentra ofertas anómalas.
"""

import os
import json
import time
import random
import logging
import hashlib
import statistics
from datetime import datetime, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
GROQ_API_KEY     = os.environ.get("GROQ_API_KEY", "")   # opcional
DATA_FILE        = Path("data/price_history.json")

# Umbral: alerta si el precio es X% menor al promedio histórico
ANOMALY_THRESHOLD_PCT = float(os.environ.get("ANOMALY_THRESHOLD_PCT", "35"))
REAL_DEAL_THRESHOLD_PCT = float(os.environ.get("REAL_DEAL_THRESHOLD_PCT", "60"))
MIN_PRICE_CLP = 10_000
# Fechas alternativas (días desde el lunes objetivo) para mejorar cobertura por ruta
DATE_OFFSETS_DAYS = [0, 2, 4]
INTERNATIONAL_DATE_OFFSETS_DAYS = [0, 1, 2, 3, 4, 5, 6]

# Rutas a monitorear: (origen, destino, label)
ROUTES = [
    # ── Nacionales ──────────────────────────────────────────────────────────────
    ("SCL", "IQQ", "Santiago → Iquique"),
    ("SCL", "ARI", "Santiago → Arica"),
    ("SCL", "ANF", "Santiago → Antofagasta"),
    ("SCL", "CCP", "Santiago → Concepción"),
    ("SCL", "PMC", "Santiago → Puerto Montt"),
    ("SCL", "PUQ", "Santiago → Punta Arenas"),
    ("SCL", "LSC", "Santiago → La Serena"),
    # ── Internacionales ─────────────────────────────────────────────────────────
    ("SCL", "LIM", "Santiago → Lima"),
    ("SCL", "BOG", "Santiago → Bogotá"),
    ("SCL", "GRU", "Santiago → São Paulo (Guarulhos)"),
    ("SCL", "CGH", "Santiago → São Paulo (Congonhas)"),
    ("SCL", "GIG", "Santiago → Río de Janeiro"),
    ("SCL", "BSB", "Santiago → Brasilia"),
    ("SCL", "FOR", "Santiago → Fortaleza"),
    ("SCL", "SSA", "Santiago → Salvador"),
    ("SCL", "EZE", "Santiago → Buenos Aires"),
    ("SCL", "MIA", "Santiago → Miami"),
    ("SCL", "MAD", "Santiago → Madrid"),
    ("SCL", "CUN", "Santiago → Cancún"),
]

DOMESTIC_DESTS = {
    "IQQ", "ARI", "ANF", "CCP", "PMC", "PUQ", "LSC",
}

# Fechas a consultar: próximas N semanas (lunes de cada semana)
WEEKS_AHEAD = 8

# ── HTTP session con reintentos ────────────────────────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5,
                  status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    })
    return s

SESSION = make_session()


def validate_config() -> bool:
    """
    Verifica variables críticas para evitar fallos por KeyError al iniciar.
    """
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")

    if missing:
        log.error("Faltan variables de entorno requeridas: %s", ", ".join(missing))
        return False
    return True


AIRLINE_NAMES = {
    "LA": "LATAM Airlines",
    "JA": "SKY Airline",
    "H2": "JetSMART",
    "JZ": "JetSMART",
    "AR": "Aerolíneas Argentinas",
    "IB": "Iberia",
    "G3": "GOL",
}


def airline_display_name(airline: str) -> str:
    code = (airline or "?").strip()
    if not code:
        return "Desconocida"
    return AIRLINE_NAMES.get(code[:2], code)


def candidate_depart_dates(base_monday: datetime, dest: str) -> list[str]:
    """
    Genera fechas de salida alternativas por semana para aumentar cobertura.
    Para rutas internacionales, recorre toda la semana.
    """
    offsets = DATE_OFFSETS_DAYS if dest in DOMESTIC_DESTS else INTERNATIONAL_DATE_OFFSETS_DAYS
    dates = []
    for days in offsets:
        dt = base_monday + timedelta(days=days)
        dates.append(dt.strftime("%Y-%m-%d"))
    return dates


# ══════════════════════════════════════════════════════════════════════════════
# 1.  FUENTES DE PRECIOS
# ══════════════════════════════════════════════════════════════════════════════

def search_aviasales(origin: str, dest: str, depart: str) -> list[dict]:
    """
    Aviasales Travel Payouts API — gratuita con registro.
    https://www.travelpayouts.com/developers/api
    Devuelve el precio más bajo encontrado por la combinación.
    """
    token = os.environ.get("AVIASALES_TOKEN", "")
    if not token:
        return []

    url = "https://api.travelpayouts.com/v1/prices/cheap"
    params = {
        "origin":       origin,
        "destination":  dest,
        "depart_date":  depart,  # YYYY-MM
        "currency":     "CLP",
        "token":        token,
        "limit":        5,
    }
    try:
        r = SESSION.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json().get("data", {}).get(dest, {})
        results = []
        for flight in data.values():
            results.append({
                "price":    flight.get("price", 0),
                "airline":  flight.get("airline", "?"),
                "depart":   flight.get("departure_at", ""),
                "return":   flight.get("return_at", ""),
                "link":     f"https://www.aviasales.com/search/{origin}{depart.replace('-','')}{dest}1",
                "source":   "aviasales",
            })
        return results
    except Exception as e:
        log.warning("Aviasales error %s→%s: %s", origin, dest, e)
        return []


def search_google_flights_serpapi(origin: str, dest: str, depart: str) -> list[dict]:
    """
    SerpAPI – Google Flights.  Plan gratuito: 100 búsquedas/mes.
    https://serpapi.com/google-flights-api
    """
    api_key = os.environ.get("SERPAPI_KEY", "")
    if not api_key:
        return []

    url = "https://serpapi.com/search"
    params = {
        "engine":           "google_flights",
        "departure_id":     origin,
        "arrival_id":       dest,
        "outbound_date":    depart,
        "currency":         "CLP",
        "hl":               "es",
        "api_key":          api_key,
        "type":             "2",  # solo ida
    }
    try:
        r = SESSION.get(url, params=params, timeout=20)
        r.raise_for_status()
        best = r.json().get("best_flights", [])
        results = []
        for f in best[:3]:
            results.append({
                "price":   f.get("price", 0),
                "airline": f["flights"][0].get("airline", "?") if f.get("flights") else "?",
                "depart":  depart,
                "link":    f"https://www.google.com/flights?hl=es#flt={origin}.{dest}.{depart}",
                "source":  "google_flights",
            })
        return results
    except Exception as e:
        log.warning("SerpAPI error %s→%s: %s", origin, dest, e)
        return []


def search_kayak_scrape(origin: str, dest: str, depart: str) -> list[dict]:
    """
    Scraping liviano de la API pública de Kayak (no oficial, usar con respeto).
    Solo se usa si las APIs anteriores no devuelven resultados.
    """
    # Kayak usa una URL de exploración con JSON embebido
    url = (
        f"https://www.kayak.cl/flights/{origin}-{dest}"
        f"/{depart}?sort=price_a"
    )
    headers = {
        "User-Agent": SESSION.headers["User-Agent"],
        "Accept": "text/html,application/xhtml+xml",
    }
    try:
        r = SESSION.get(url, headers=headers, timeout=15)
        # Extracción básica: busca patrones de precio en el HTML
        import re
        prices = re.findall(r'"amount":(\d+)', r.text)
        if not prices:
            prices = re.findall(r'CLP\s*([\d\.]+)', r.text)
        if prices:
            p = int(prices[0].replace(".", ""))
            return [{
                "price":   p,
                "airline": "Varios",
                "depart":  depart,
                "link":    url,
                "source":  "kayak",
            }]
    except Exception as e:
        log.warning("Kayak scrape error %s→%s: %s", origin, dest, e)
    return []


def fetch_prices(origin: str, dest: str) -> list[dict]:
    """
    Agrega precios de todas las fuentes disponibles para las próximas semanas.
    Si no hay datos para el lunes, prueba fechas alternativas de la misma semana.
    """
    all_prices = []
    today = datetime.today()

    for week in range(1, WEEKS_AHEAD + 1):
        depart_dt = today + timedelta(weeks=week)
        # Redondea al próximo lunes
        depart_dt += timedelta(days=(7 - depart_dt.weekday()) % 7)

        found_week = []
        for depart_str in candidate_depart_dates(depart_dt, dest):
            month_str = depart_str[:7]

            found = []
            found += search_aviasales(origin, dest, month_str)
            valid_found = [i for i in found if i.get("price", 0) >= MIN_PRICE_CLP]

            # Fallback por resultado válido (no solo por respuesta no vacía).
            if not valid_found:
                serp = search_google_flights_serpapi(origin, dest, depart_str)
                found += serp
                valid_found = [i for i in found if i.get("price", 0) >= MIN_PRICE_CLP]

            if not valid_found:
                found += search_kayak_scrape(origin, dest, depart_str)

            for item in found:
                if item.get("price", 0) >= MIN_PRICE_CLP:
                    item["origin"] = origin
                    item["dest"] = dest
                    item["queried_depart"] = depart_str
                    found_week.append(item)

            # Si ya encontramos precios para esta semana, evitamos gastar más cuota/API.
            if found_week:
                break

            time.sleep(random.uniform(1.0, 2.0))

        all_prices.extend(found_week)
        time.sleep(random.uniform(1.5, 3.5))  # cortesía

    return all_prices


# ══════════════════════════════════════════════════════════════════════════════
# 2.  HISTORIAL DE PRECIOS
# ══════════════════════════════════════════════════════════════════════════════

def load_history() -> dict:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    if DATA_FILE.exists():
        try:
            data = json.loads(DATA_FILE.read_text())
            if isinstance(data, dict):
                return data
            log.warning("%s no tiene estructura dict válida; se reinicia historial.", DATA_FILE)
        except Exception:
            log.warning("No se pudo leer %s; se reinicia historial.", DATA_FILE)
    return {}


def save_history(history: dict) -> None:
    DATA_FILE.write_text(json.dumps(history, ensure_ascii=False, indent=2))


def route_key(origin: str, dest: str) -> str:
    return f"{origin}-{dest}"


def update_history(history: dict, origin: str, dest: str, price: int) -> None:
    key = route_key(origin, dest)
    if key not in history:
        history[key] = {"prices": [], "updated": ""}
    prices = history[key]["prices"]
    # Evita ruido por duplicados consecutivos en una misma ruta.
    if not prices or prices[-1] != price:
        prices.append(price)
    # Mantener solo los últimos 60 registros
    history[key]["prices"] = history[key]["prices"][-60:]
    history[key]["updated"] = datetime.now().isoformat()


def get_stats(history: dict, origin: str, dest: str) -> dict | None:
    key = route_key(origin, dest)
    prices = history.get(key, {}).get("prices", [])
    if len(prices) < 3:
        return None
    return {
        "mean":   statistics.mean(prices),
        "median": statistics.median(prices),
        "stdev":  statistics.stdev(prices) if len(prices) > 1 else 0,
        "min":    min(prices),
        "count":  len(prices),
    }


# ══════════════════════════════════════════════════════════════════════════════
# 3.  DETECCIÓN DE ANOMALÍAS
# ══════════════════════════════════════════════════════════════════════════════

def is_anomaly(price: int, stats: dict) -> tuple[bool, float]:
    """
    Retorna (es_anomalía, pct_descuento).
    Para reducir falsos positivos, se alerta solo con descuento "real".
    """
    mean = stats["mean"]
    pct_below = (mean - price) / mean * 100

    # Filtro de descuento real (configurable, recomendado 60% para alertas muy agresivas)
    if pct_below >= REAL_DEAL_THRESHOLD_PCT:
        return True, pct_below

    return False, pct_below


def groq_analyze(price: int, stats: dict, route_label: str, depart: str) -> str:
    """
    Usa Groq (gratis) para agregar contexto inteligente a la alerta.
    Si GROQ_API_KEY no está disponible, devuelve string vacío.
    """
    if not GROQ_API_KEY:
        return ""

    prompt = (
        f"Eres un experto en tarifas aéreas en Chile. "
        f"Analiza esta oferta en 2-3 oraciones en español:\n"
        f"- Ruta: {route_label}\n"
        f"- Fecha de salida: {depart}\n"
        f"- Precio encontrado: CLP {price:,}\n"
        f"- Precio promedio histórico: CLP {stats['mean']:,.0f}\n"
        f"- Precio mínimo histórico: CLP {stats['min']:,}\n"
        f"- Total de muestras: {stats['count']}\n\n"
        f"¿Es un error de precio o una oferta legítima? ¿Vale la pena comprar?"
    )

    try:
        r = SESSION.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "llama3-8b-8192",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 200,
                "temperature": 0.4,
            },
            timeout=20,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        log.warning("Groq error: %s", e)
        return ""


# ══════════════════════════════════════════════════════════════════════════════
# 4.  TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════

def send_telegram(message: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        r = SESSION.post(url, json=payload, timeout=15)
        r.raise_for_status()
        log.info("✅ Telegram OK")
        return True
    except Exception as e:
        log.error("❌ Telegram error: %s", e)
        return False


def format_alert(item: dict, stats: dict, pct_below: float, ai_comment: str) -> str:
    airline = item.get("airline", "?")
    airline_name = airline_display_name(airline)
    airline_emoji = {"JA": "🟡", "LA": "🔴", "H2": "🟠", "JZ": "🟣"}.get(airline[:2], "✈️")

    lines = [
        f"🚨 <b>ALERTA DE PRECIO ANÓMALO</b> 🚨",
        f"",
        f"{airline_emoji} <b>{item.get('route_label', item['origin'] + ' → ' + item['dest'])}</b>",
        f"🛩 Aerolínea: <b>{airline_name}</b> ({airline})",
        f"📅 Salida: <b>{item.get('queried_depart', item.get('depart', '?'))}</b>",
        f"",
        f"💰 Precio: <b>CLP {item['price']:,}</b>",
        f"📊 Promedio histórico: CLP {stats['mean']:,.0f}",
        f"📉 Descuento estimado: <b>{pct_below:.0f}% bajo el promedio</b>",
        f"🏆 Mínimo histórico: CLP {stats['min']:,}",
        f"",
    ]

    if ai_comment:
        lines += [f"🤖 <i>{ai_comment}</i>", ""]

    lines += [
        f"🔗 <a href='{item.get('link', '#')}'>Ver y comprar aquí</a>",
        f"",
        f"⏰ Detectado: {datetime.now().strftime('%d/%m/%Y %H:%M')} (hora Chile)",
        f"⚡ Fuente: {item.get('source', '?')}",
    ]
    return "\n".join(lines)


def format_summary(
    alerts_sent: int,
    routes_checked: int,
    prices_found: int,
    best_deals: list[dict],
    routes_without_data: list[str],
) -> str:
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    status = "✅ Sin anomalías detectadas" if alerts_sent == 0 else f"🚨 {alerts_sent} alerta(s) enviada(s)"

    lines = [
        f"📋 <b>Resumen del monitoreo</b>",
        f"🕐 {now} (hora Chile)",
        f"🛫 Rutas revisadas: {routes_checked}",
        f"💲 Precios recopilados: {prices_found}",
        f"Estado: {status}",
        f"",
    ]

    # Top 5 precios más baratos encontrados hoy
    if best_deals:
        lines.append(f"🏷 <b>Mejores precios de hoy:</b>")
        for d in best_deals[:5]:
            pct_str = f" (-{d['pct']:.0f}%)" if d.get("pct") else ""
            lines.append(
                f"  • {d['label']}: <b>CLP {d['price']:,}</b>{pct_str} — {d['depart']}"
            )
        lines.append("")

    # Rutas sin datos (posible problema de cobertura)
    if routes_without_data:
        lines.append(f"⚠️ Sin datos: {', '.join(routes_without_data)}")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# 5.  DEDUPLICACIÓN (evitar spam de alertas repetidas)
# ══════════════════════════════════════════════════════════════════════════════

ALERTS_FILE = Path("data/sent_alerts.json")


def load_sent_alerts() -> set:
    if ALERTS_FILE.exists():
        try:
            data = json.loads(ALERTS_FILE.read_text())
            if isinstance(data, list):
                return set(data)
            log.warning("%s no tiene formato lista válido; se reinicia deduplicación.", ALERTS_FILE)
        except Exception:
            log.warning("No se pudo leer %s; se reinicia deduplicación.", ALERTS_FILE)
    return set()


def save_sent_alerts(sent: set) -> None:
    ALERTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ALERTS_FILE.write_text(json.dumps(list(sent)))


def alert_id(item: dict) -> str:
    raw = f"{item['origin']}-{item['dest']}-{item['price']}-{item.get('queried_depart','')}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


# ══════════════════════════════════════════════════════════════════════════════
# 6.  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    if not validate_config():
        return

    log.info("🛫 Iniciando monitoreo de vuelos desde SCL")

    history     = load_history()
    sent_alerts = load_sent_alerts()

    alerts_sent        = 0
    prices_found       = 0
    best_deals         = []   # top precios del día para el resumen
    routes_without_data = []  # rutas sin resultados

    label_map = {(o, d): lbl for o, d, lbl in ROUTES}

    for origin, dest, label in ROUTES:
        log.info("Consultando %s...", label)

        prices = fetch_prices(origin, dest)
        log.info("  -> %d precio(s) encontrado(s)", len(prices))

        if not prices:
            routes_without_data.append(label.split("→")[-1].strip())

        for item in prices:
            price = item["price"]
            if price <= 0:
                continue

            prices_found += 1
            item["route_label"] = label_map.get((origin, dest), label)

            log.info(
                "  CLP %s | Aerolinea: %s | Fuente: %s | Fecha: %s",
                f"{price:,}",
                item.get("airline", "?"),
                item.get("source", "?"),
                item.get("queried_depart", "?"),
            )

            stats_before = get_stats(history, origin, dest)
            update_history(history, origin, dest, price)

            if stats_before is None:
                log.info("  Acumulando historial para %s (precio: %s)", label, price)
                # Igual guardamos para el resumen aunque no tengamos historial
                best_deals.append({
                    "label":  label,
                    "price":  price,
                    "depart": item.get("queried_depart", "?"),
                    "pct":    None,
                })
                continue

            anomaly, pct = is_anomaly(price, stats_before)

            # Guardar para resumen (todos los precios, no solo anomalías)
            best_deals.append({
                "label":  label,
                "price":  price,
                "depart": item.get("queried_depart", "?"),
                "pct":    pct if pct > 0 else None,
            })

            if not anomaly:
                log.info("  Precio normal: CLP %s (%.0f%% bajo media)", price, pct)
                continue

            aid = alert_id(item)
            if aid in sent_alerts:
                log.info("  Alerta ya enviada antes (%s), omitiendo.", aid)
                continue

            log.info("  ANOMALIA detectada! CLP %s (%.0f%% bajo media)", price, pct)

            ai_comment = groq_analyze(price, stats_before, label, item.get("queried_depart", ""))

            msg = format_alert(item, stats_before, pct, ai_comment)
            if send_telegram(msg):
                sent_alerts.add(aid)
                alerts_sent += 1
                time.sleep(2)

    save_history(history)
    save_sent_alerts(sent_alerts)

    # Ordenar best_deals por precio ascendente y deduplicar por ruta
    seen_labels = set()
    unique_deals = []
    for d in sorted(best_deals, key=lambda x: x["price"]):
        if d["label"] not in seen_labels:
            seen_labels.add(d["label"])
            unique_deals.append(d)

    # Resumen siempre se envía (3 veces al día = info útil)
    send_telegram(format_summary(
        alerts_sent,
        len(ROUTES),
        prices_found,
        unique_deals,
        routes_without_data,
    ))

    log.info("Monitoreo completo. Alertas enviadas: %d", alerts_sent)


if __name__ == "__main__":
    main()
