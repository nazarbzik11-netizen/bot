import discord
import aiohttp
import asyncio
import json
import os
import logging
import re
import random
import io
import time
import shutil
import base64
from pathlib import Path
from itertools import cycle
from datetime import datetime, timezone, timedelta
from aiohttp import web
from discord.ext import tasks

# ---------- НАЛАШТУВАННЯ ----------
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID")) if os.getenv("CHANNEL_ID") else 0
NEWSKY_API_KEY = os.getenv("NEWSKY_API_KEY")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
NEWSKY_SID = os.getenv("NEWSKY_SID")
GITHUB_REPO = "kazuar-avia/kazuar-avia.github.io"
GITHUB_FILE_PATH = "newsky-airports.txt"

ADMIN_IDS = [
    598767470140063744,
]

START_TIME = datetime.now(timezone.utc)

STATE_FILE = Path("/app/data/sent.json")
STATUS_FILE = Path("/app/data/statuses.json")
WEEKLY_STATS_FILE = Path("/app/data/weekly_stats.json")
CHECK_INTERVAL = 30
BASE_URL = "https://newsky.app/api/airline-api"
AIRPORTS_DB_URL = "https://raw.githubusercontent.com/mwgg/Airports/master/airports.json"
HEADERS = {"Authorization": f"Bearer {NEWSKY_API_KEY}"}

logging.basicConfig(level=logging.INFO)
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

AIRPORTS_DB = {}
BANNED_WOW_MESSAGES = set()
MONITORING_STARTED = False
LAST_TRAFFIC_TIME = 0.0
TAXIING_FLIGHTS = set()
last_sent_message = None

# ---------- ДОПОМІЖНІ ФУНКЦІЇ ----------
def load_state():
    if not STATE_FILE.exists(): return {}
    try: return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except: return {}

def save_state(state):
    try:
        if len(state) > 100: state = dict(list(state.items())[-50:])
        STATE_FILE.write_text(json.dumps(state), encoding="utf-8")
    except: pass

# 🔥 БЛОК ФУНКЦІЙ ДЛЯ СТАТИСТИКИ 🔥
def load_weekly_stats():
    if not WEEKLY_STATS_FILE.exists(): return {}
    try: return json.loads(WEEKLY_STATS_FILE.read_text(encoding="utf-8"))
    except: return {}

def save_weekly_stats(stats):
    try: WEEKLY_STATS_FILE.write_text(json.dumps(stats, indent=4), encoding="utf-8")
    except: pass

def get_iso_week(dt_str=None):
    if dt_str:
        try:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except:
            dt = datetime.now(timezone.utc)
    else:
        dt = datetime.now(timezone.utc)
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"

def get_week_dates_string(week_tag):
    try:
        y, w = map(int, week_tag.split("-W"))
        first_day = datetime.strptime(f'{y}-W{w}-1', "%G-W%V-%u")
        last_day = datetime.strptime(f'{y}-W{w}-7', "%G-W%V-%u")
        return f"{first_day.strftime('%d.%m.%Y')} - {last_day.strftime('%d.%m.%Y')}"
    except: return week_tag

def init_week_stats():
    return {
        "flights": 0, "earnings": 0, "pax": 0, "cargo": 0,
        "rating_sum": 0.0, "fpm_sum": 0, "g_sum": 0.0,
        "pilots": {}, "airports": {}, "aircrafts": {},
        "records": {
            "butter": {"fpm": -99999, "g": 0.0, "pilot": "None"},
            "hardest": {"fpm": 0, "g": 0.0, "pilot": "None"},
            "longest": [],
            "shortest": []
        }
    }

def update_weekly_stats(f, week_tag):
    stats = load_weekly_stats()
    if week_tag not in stats:
        stats[week_tag] = init_week_stats()
        
    s = stats[week_tag]
    
    t = f.get("result", {}).get("totals", {})
    balance = int(t.get("balance", 0))
    
    raw_pax = t.get("payload", {}).get("pax", 0)
    if raw_pax == 0 and f.get("type") != "cargo":
        raw_pax = int(f.get("payload", {}).get("pax", 0))
        
    cargo_kg = int(f.get("payload", {}).get("weights", {}).get("cargo", 0))
    rating = float(f.get("rating", 0.0))
    ftime = int(t.get("time", 0))
    
    pilot = f.get("pilot", {}).get("fullname", "Unknown Pilot")
    dep = f.get("dep", {}).get("icao", "???")
    arr = f.get("arr", {}).get("icao", "???")
    
    ac_data = f.get("aircraft", {})
    ac_icao = "Unknown"
    if isinstance(ac_data, dict):
        ac_icao = ac_data.get("icao") or ac_data.get("airframe", {}).get("icao") or "Unknown"
    check_g, check_fpm = 0.0, 0
    has_crash_title = False

    if "result" in f and "violations" in f["result"]:
        for v in f["result"]["violations"]:
            if "Crashed on landing" in v.get("title", ""):
                has_crash_title = True
                
            entry = v.get("entry", {}).get("payload", {}).get("touchDown", {})
            if entry and check_g == 0.0:
                check_g = float(entry.get("gForce", 0))
                check_fpm = int(entry.get("rate", 0))

    if check_g == 0 and "landing" in f:
        check_g = float(f.get("landing", {}).get("gForce", 0))
        check_fpm = int(f.get("landing", {}).get("rate", 0) or f.get("landing", {}).get("touchDownRate", 0))
        
    fpm_val = -abs(check_fpm) if check_fpm != 0 else 0
    
    is_hard_crash = abs(check_g) > 3.0 or abs(check_fpm) > 2000 or has_crash_title
    
    s["flights"] += 1
    s["earnings"] += balance
    s["pax"] += raw_pax
    s["cargo"] += cargo_kg
    s["rating_sum"] += rating
    s["fpm_sum"] += fpm_val
    s["g_sum"] += check_g
    
    s["pilots"][pilot] = s["pilots"].get(pilot, 0) + 1
    s["airports"][dep] = s["airports"].get(dep, 0) + 1
    s["airports"][arr] = s["airports"].get(arr, 0) + 1
    
    if "aircrafts" not in s: s["aircrafts"] = {}
    if ac_icao != "Unknown":
        s["aircrafts"][ac_icao] = s["aircrafts"].get(ac_icao, 0) + 1
    
    is_accelerated = False
    accel_scalar = t.get("prices", {}).get("costScalars", {}).get("timeAcceleration", 1.0)
    if accel_scalar < 1.0:
        is_accelerated = True
    elif "result" in f and "violations" in f["result"]:
        for v in f["result"]["violations"]:
            if "Time acceleration" in v.get("title", ""):
                is_accelerated = True
                break

    if not is_hard_crash:
        if fpm_val < s["records"]["hardest"]["fpm"]:
            s["records"]["hardest"] = {"fpm": fpm_val, "g": check_g, "pilot": pilot}
        if fpm_val < 0 and fpm_val > s["records"]["butter"]["fpm"]:
            s["records"]["butter"] = {"fpm": fpm_val, "g": check_g, "pilot": pilot}
            
        if ftime > 0 and not is_accelerated:
            flight_rec = {"time": ftime, "pilot": pilot, "dep": dep, "arr": arr}
            
            s["records"]["longest"].append(flight_rec)
            s["records"]["longest"] = sorted(s["records"]["longest"], key=lambda x: x["time"], reverse=True)[:3]
            
            s["records"]["shortest"].append(flight_rec)
            s["records"]["shortest"] = sorted(s["records"]["shortest"], key=lambda x: x["time"])[:3]
            
    save_weekly_stats(stats)

async def check_and_publish_weekly_stats(channel, state, ongoing_ids):
    stats = load_weekly_stats()
    if not stats: return
    
    current_week = get_iso_week()
    weeks_to_delete = []
    
    for week_tag, s in stats.items():
        if week_tag >= current_week:
            continue
            
        active_flight_exists = False
        for fid, fstate in state.items():
            if isinstance(fstate, dict) and not fstate.get("completed") and fstate.get("week") == week_tag:
                if fid in ongoing_ids:
                    active_flight_exists = True
                    break
                else:
                    print(f"👻 Ігноруємо завислий рейс (привид): {fid}")
                    
        if not active_flight_exists:
            try:
                pinned_msgs = await channel.pins()
                for p_msg in pinned_msgs:
                    if p_msg.author == client.user and p_msg.embeds:
                        if p_msg.embeds[0].title and "Weekly Summary" in p_msg.embeds[0].title:
                            await p_msg.unpin()
            except Exception as e:
                print(f"Error unpinning old real report: {e}")

            new_msg = await publish_weekly_embed(channel, week_tag, s)
            
            if new_msg:
                try:
                    await new_msg.pin()
                except Exception as e:
                    print(f"Error pinning message: {e}")

            try:
                owner = await client.fetch_user(ADMIN_IDS[0])
                dates_str = get_week_dates_string(week_tag)
                file_bin = io.BytesIO(json.dumps({week_tag: s}, indent=4).encode('utf-8'))
                
                await owner.send(
                    content=f"📁 **Архів тижня: {week_tag}** ({dates_str})", 
                    file=discord.File(file_bin, filename=f"weekly_stats_{week_tag}.json")
                )
            except Exception as e:
                print(f"Error sending stats DM to admin: {e}")

            weeks_to_delete.append(week_tag)
            
    if weeks_to_delete:
        for w in weeks_to_delete:
            del stats[w]
        save_weekly_stats(stats)

async def publish_weekly_embed(channel, week_tag, s):
    dates_str = get_week_dates_string(week_tag)
    fl = s["flights"]
    if fl == 0: return None
    
    avg_rating = round(s["rating_sum"] / fl, 1)
    avg_fpm = int(s["fpm_sum"] / fl)
    avg_g = round(s["g_sum"] / fl, 2)
    
    medals = ["🥇", "🥈", "🥉"]
    
    # --- ТОП 3 ПІЛОТИ ---
    sorted_pilots = sorted(s.get("pilots", {}).items(), key=lambda x: x[1], reverse=True)[:3]
    pilots_str = ""
    if not sorted_pilots:
        pilots_str = "╰ None\n"
    else:
        for i, (p_name, p_flights) in enumerate(sorted_pilots):
            pilots_str += f"╰ {medals[i]} **{p_name}** ({p_flights} flights)\n"
            
    # --- ТОП 3 АЕРОПОРТИ ---
    sorted_apts = sorted(s.get("airports", {}).items(), key=lambda x: x[1], reverse=True)[:3]
    apts_str = ""
    if not sorted_apts:
        apts_str = "╰ None\n"
    else:
        for i, (apt_icao, apt_ops) in enumerate(sorted_apts):
            apt_flag = get_flag(AIRPORTS_DB.get(apt_icao.upper(), {}).get("country", "XX"))
            apts_str += f"╰ {medals[i]} {apt_flag} **{apt_icao}** ({apt_ops} ops)\n"
            
    # --- ТОП 3 ЛІТАКИ ---
    sorted_acs = sorted(s.get("aircrafts", {}).items(), key=lambda x: x[1], reverse=True)[:3]
    acs_str = ""
    if not sorted_acs:
        acs_str = "╰ None\n"
    else:
        for i, (ac_icao, ac_flights) in enumerate(sorted_acs):
            acs_str += f"╰ {medals[i]} **{ac_icao}** ({ac_flights} flights)\n"
            
    rec = s["records"]
    
    def format_duration(minutes):
        return f"{int(minutes // 60):02d} hrs {int(minutes % 60):02d} mins"

    earn_val = s['earnings']
    sign = "+" if earn_val >= 0 else "-" 
    
    # --- БЛОК ДЛЯ ТОП-3 НАЙДОВШИХ ---
    longest_str = ""
    if not rec['longest']:
        longest_str = "╰ None\n"
    else:
        for i, r in enumerate(rec['longest']):
            l_dep = r.get('dep', '???')
            l_arr = r.get('arr', '???')
            l_route = f" ({get_flag(AIRPORTS_DB.get(l_dep, {}).get('country', 'XX'))} {l_dep} ➔ {get_flag(AIRPORTS_DB.get(l_arr, {}).get('country', 'XX'))} {l_arr})" if l_dep != "???" else ""
            longest_str += f"╰ {medals[i] if i < 3 else '🔹'} {format_duration(r['time'])} — **{r.get('pilot', 'Unknown')}**{l_route}\n"

    # --- БЛОК ДЛЯ ТОП-3 НАЙКОРОТШИХ ---
    shortest_str = ""
    if not rec['shortest']:
        shortest_str = "╰ None\n"
    else:
        for i, r in enumerate(rec['shortest']):
            s_dep = r.get('dep', '???')
            s_arr = r.get('arr', '???')
            s_route = f" ({get_flag(AIRPORTS_DB.get(s_dep, {}).get('country', 'XX'))} {s_dep} ➔ {get_flag(AIRPORTS_DB.get(s_arr, {}).get('country', 'XX'))} {s_arr})" if s_dep != "???" else ""
            shortest_str += f"╰ {medals[i] if i < 3 else '🔹'} {format_duration(r['time'])} — **{r.get('pilot', 'Unknown')}**{s_route}\n"
    
    desc = (
        f"### 📈 General Statistics\n"
        f"🛫 **Flights Completed:** {fl}\n"
        f"💰 **Airline Earnings:** {sign}{abs(earn_val):,} $\n"
        f"👫 **Passengers Carried:** {s['pax']:,} Pax\n"
        f"📦 **Cargo Carried:** {s['cargo']:,} kg\n\n"
        
        f"### 🏆 Weekly Records\n"
        f"👨‍✈️ **Most Active Pilots:**\n"
        f"{pilots_str}\n"
        
        f"🧈 **Butter Landing:**\n"
        f"╰ {rec['butter']['pilot']} ({rec['butter']['fpm']} fpm, {rec['butter']['g']} G)\n\n"
        
        f"🛬 **Hardest Landing:**\n"
        f"╰ {rec['hardest']['pilot']} ({rec['hardest']['fpm']} fpm, {rec['hardest']['g']} G)\n\n"
        
        f"🐢 **Longest Flights:**\n"
        f"{longest_str}\n"
        
        f"🚀 **Shortest Flights:**\n"
        f"{shortest_str}\n"
        
        f"### ⭐ Company Averages\n"
        f"📊 **Average Rating:** {avg_rating}\n"
        f"📉 **Average FPM:** {avg_fpm} fpm | {avg_g} G\n\n"
        
        f"📍 **Most Popular Airports:**\n"
        f"{apts_str}\n"
        
        f"✈️ **Most Popular Aircraft:**\n"
        f"{acs_str.rstrip()}"
    )
    
    embed = discord.Embed(
        title=f"📊 Weekly Summary for {dates_str}",
        description=desc,
        color=0x3498db
    )
    
    try: 
        msg = await channel.send(embed=embed)
        return msg 
    except Exception as e: 
        print(f"Error sending weekly embed: {e}")
        return None
# 🔥 КІНЕЦЬ БЛОКУ СТАТИСТИКИ 🔥

# --- 🎭 СТАНДАРТНІ СТАТУСИ ---
DEFAULT_STATUSES = [
    {"type": "play", "name": "🕹️Tracking with Newsky.app"},
    {"type": "play", "name": "🕹️Playing AirportSim"},
    {"type": "play", "name": "✈️Playing Microsoft Flight Simulator 2024"},
    {"type": "listen", "name": "🎧LiveATC @ KBP"},
    {"type": "watch", "name": "🔴Watching Youtube KAZUAR AVIA"}
]

def load_statuses():
    if not STATUS_FILE.exists():
        return list(DEFAULT_STATUSES)
    try:
        data = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
        if not data: return list(DEFAULT_STATUSES)
        return data
    except:
        return list(DEFAULT_STATUSES)

def save_statuses():
    try:
        STATUS_FILE.write_text(json.dumps(status_list, indent=4), encoding="utf-8")
    except Exception as e:
        print(f"⚠️ Failed to save statuses: {e}")

status_list = load_statuses()
status_cycle = cycle(status_list)

def clean_text(text):
    if not text: return ""
    text = re.sub(r"\(.*?\)", "", text)
    removals = ["International", "Regional", "Airport", "Aerodrome", "Air Base", "Intl"]
    for word in removals:
        pattern = re.compile(re.escape(word), re.IGNORECASE)
        text = pattern.sub("", text)
    return text.strip().strip(",").strip()

# --- 🌍 ЗАВАНТАЖЕННЯ БАЗИ ---
async def update_airports_db():
    global AIRPORTS_DB
    print("🌍 Downloading airports database...")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(AIRPORTS_DB_URL) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    AIRPORTS_DB = {}
                    for k, v in data.items():
                        AIRPORTS_DB[k.upper()] = {
                            "country": v.get("country", "XX"),
                            "city": v.get("city", ""),
                            "name": v.get("name", "")
                        }
                    print(f"✅ Airports DB loaded! ({len(AIRPORTS_DB)} airports)")
                else:
                    print(f"⚠️ Failed to load airports DB: Status {resp.status}")
        except Exception as e:
            print(f"⚠️ Error loading DB: {e}")

def get_flag(country_code):
    if not country_code or country_code == "XX": return "🏳️"
    try:
        return "".join([chr(ord(c) + 127397) for c in country_code.upper()])
    except:
        return "🏳️"

# --- 🧠 РОЗУМНЕ ФОРМУВАННЯ НАЗВИ ---
def format_airport_string(icao, api_name):
    icao = icao.upper()
    db_data = AIRPORTS_DB.get(icao)
    
    if db_data:
        city = db_data.get("city", "") or ""
        name = db_data.get("name", "") or ""
        country = db_data.get("country", "XX")
        
        # ВИПРАВЛЕННЯ НАЗВ МІСТ
        CITY_FIXES = {
            "Kiev": "Kyiv",
            "Dnipropetrovsk": "Dnipro",
            "Kirovograd": "Kropyvnytskyi",
            "Nikolayev": "Mykolaiv",
            "Odessa": "Odesa",
            "Vinnitsa": "Vinnytsia",
            "Zaporizhia": "Zaporizhzhia",
            "Larnarca": "Larnaca",
			"Frankfurt-am-Main": "Frankfurt am Main",
            "Sharm el-Sheikh": "Sharm El Sheikh"
        }
        
        for old, new in CITY_FIXES.items():
            if city.lower() == old.lower(): 
                city = new
            name = name.replace(old, new)
        
        clean_name = clean_text(name)
        display_text = ""
        
        if city and clean_name:
            if city.lower() in clean_name.lower():
                display_text = clean_name
            else:
                display_text = f"{city} {clean_name}"
        elif clean_name:
            display_text = clean_name
        elif city:
            display_text = city
        else:
            display_text = clean_text(api_name)

        return f"{get_flag(country)} **{icao}** ({display_text})"
    
    flag = "🏳️"
    if len(icao) >= 2:
        prefix = icao[:2]
        manual_map = {'UK': 'UA', 'KJ': 'US', 'K': 'US', 'EG': 'GB', 'LF': 'FR', 'ED': 'DE', 'LP': 'PT', 'LE': 'ES', 'LI': 'IT', 'U': 'RU'}
        code = manual_map.get(prefix, "XX")
        if code != "XX": flag = get_flag(code)

    return f"{flag} **{icao}** ({clean_text(api_name)})"

def get_timing(delay):
    try:
        d = float(delay)
        if d > 15: return f"🔴 **Delay** (+{int(d)} min)"
        if d < -15: return f"🟡 **Early** ({int(d)} min)"
        return "🟢 **On time**"
    except: return "⏱️ **N/A**"

def format_time(minutes):
    if not minutes: return "00:00"
    return f"{int(minutes // 60):02d}:{int(minutes % 60):02d}"

def get_rating_square(rating):
    try:
        r = float(rating)
        if r >= 8.0: return "🟩"
        if r >= 6.0: return "🟨" 
        if r >= 4.0: return "🟧"
        return "🟥"
    except: return "⬜"

# --- FPM + G-Force + Wind Search ---
def get_landing_data(f, details_type):
    if details_type == "test":
        fpm = -random.randint(50, 400)
        g = round(random.uniform(0.9, 1.8), 2)
        return f"📉 **{fpm} fpm**, **{g} G**\n<:wind:1482073151071326229> **220° | 8 kt** (Crosswind: 3 kt | Tailwind: 5 kt)"

    fpm, g_force, found = 0, 0.0, False
    weather = {}

    if "result" in f and "violations" in f["result"]:
        for v in f["result"]["violations"]:
            payload = v.get("entry", {}).get("payload", {})
            td = payload.get("touchDown", {})
            if td:
                fpm = int(td.get("rate", 0))
                g_force = float(td.get("gForce", 0))
                weather = payload.get("weather", {})
                found = True
                break

    if not found and "landing" in f and f["landing"]:
        payload = f["landing"]
        fpm = int(payload.get("rate", 0) or payload.get("touchDownRate", 0))
        g_force = float(payload.get("gForce", 0))
        weather = payload.get("weather", {})
        found = True

    if not found:
        val = f.get("lastState", {}).get("speed", {}).get("touchDownRate")
        if val: 
            fpm = int(val)
            found = True

    if found and fpm != 0:
        fpm_val = -abs(fpm)
        g_str = f", **{g_force} G**" if g_force > 0 else ""
        
        wind_str = ""
        if weather and "windDir" in weather:
            w_dir = int(round(weather.get("windDir", 0)))
            w_spd = int(round(weather.get("windSpd", 0)))
            w_x = int(round(abs(weather.get("windX", 0))))
            w_z = weather.get("windZ", 0)
            
            if w_dir == 0 and w_spd > 0: 
                w_dir = 360
                
            comp = []
            if w_x > 0:
                comp.append(f"Crosswind: {w_x} kt")
            if w_z > 0.5:
                comp.append(f"Tailwind: {int(round(w_z))} kt")
                
            extra_str = f" ({' | '.join(comp)})" if comp else ""
            wind_str = f"\n<:wind:1482073151071326229> **{w_dir}° | {w_spd} kt**{extra_str}"
            
        return f"📉 **{fpm_val} fpm**{g_str}{wind_str}"
    
    return "📉 **N/A**"

API_LOCK = None
REQUEST_TIMES = []

async def fetch_api(session, path, method="GET", body=None):
    global REQUEST_TIMES, API_LOCK
    
    if API_LOCK is None:
        API_LOCK = asyncio.Lock()
    
    # 1. СТАЄМО В ЧЕРГУ
    async with API_LOCK:
        now = time.time()
        
        # Очищаємо історію від старих запитів (ті, що були понад 10 секунд тому)
        REQUEST_TIMES = [t for t in REQUEST_TIMES if now - t < 10.0]
        
        # Якщо в нашій пам'яті вже є 5 запитів за останні 10 сек — вмикаємо гальма
        if len(REQUEST_TIMES) >= 5:
            oldest_request = REQUEST_TIMES[0]
            wait_time = 10.0 - (now - oldest_request)
            
            if wait_time > 0:
                print(f"🚦 API Limit: Черга чекає {wait_time:.2f} сек... (Запит: {path})")
                await asyncio.sleep(wait_time)
        
        # Перед виходом з черги записуємо свій точний час
        REQUEST_TIMES.append(time.time())

    # 2. РОБИМО ЗАПИТ
    try:
        async with session.request(method, f"{BASE_URL}{path}", headers=HEADERS, json=body, timeout=10) as r:
            if r.status == 200:
                return await r.json()
            elif r.status == 429:
                print(f"⚠️ Зловили 429 Too Many Requests на {path}! Сервер просить пригальмувати.")
                return None
            else:
                return None
    except Exception as e:
        print(f"⚠️ API Error ({path}): {e}")
        return None

# ---------- MESSAGE GENERATOR ----------
async def send_flight_message(channel, status, f, details_type="ongoing", reply_to_id=None):
    fid = f.get("_id") or f.get("id") or "test_id"
    if status == "Completed" or status == "Cancelled":
        flight_url = f"https://newsky.app/flight/{fid}"
    else:
        flight_url = f"https://newsky.app/map/{fid}"

    # ---  ВИЗНАЧЕННЯ ТИПУ РЕЙСУ (СМАЙЛИК) ---
    if f.get("schedule"):
        type_emoji = "<:sch:1496947924154847242>"
    else:
        type_emoji = "<:free:1496947871604539392>"

    # --- 🌐 ВИЗНАЧЕННЯ МЕРЕЖІ (VATSIM/IVAO/OFFLINE) ---
    net_data = f.get("network")
    # Якщо network немає або ім'я null, то OFFLINE
    net = (net_data.get("name") if isinstance(net_data, dict) else str(net_data)) or "OFFLINE"

    cs = f.get("flightNumber") or f.get("callsign") or "N/A"
    airline = f.get("airline", {}).get("icao", "")
    full_cs = f"{airline} {cs}" if airline else cs
    
    dep_str = format_airport_string(f.get("dep", {}).get("icao", ""), f.get("dep", {}).get("name", ""))
    arr_str = format_airport_string(f.get("arr", {}).get("icao", ""), f.get("arr", {}).get("name", ""))
    
    ac = f.get("aircraft", {}).get("airframe", {}).get("name", "A/C")
    pilot = f.get("pilot", {}).get("fullname", "Pilot")
    
    raw_pax = 0
    if details_type == "result":
        raw_pax = f.get("result", {}).get("totals", {}).get("payload", {}).get("pax", 0)
    
    if raw_pax == 0 and f.get("type") != "cargo":
        raw_pax = f.get("payload", {}).get("pax", 0)
    
    flight_type = f.get("type", "pax")
    
    # --- ВИПРАВЛЕННЯ ВАГИ ВАНТАЖУ ---
    cargo_kg = int(f.get("payload", {}).get("weights", {}).get("cargo", 0))

    if flight_type == "cargo":
        payload_str = f"📦 **{cargo_kg}** kg"
    else:
        payload_str = f"👫 **{raw_pax}** Pax  |  📦 **{cargo_kg}** kg"

    embed = None
    arrow = " \u2003➡️\u2003 "

    if status == "Departed":
        delay = f.get("delay", 0)
        
        # --- РОЗРАХУНОК TAXI TIME ---
        taxi_str = ""
        try:
            t_gate_str = f.get("depTimeAct")
            t_air_str = f.get("takeoffTimeAct")
            
            if t_gate_str and t_air_str:
                t_gate = datetime.fromisoformat(t_gate_str.replace("Z", "+00:00"))
                t_air = datetime.fromisoformat(t_air_str.replace("Z", "+00:00"))
                diff = t_air - t_gate
                taxi_min = int(diff.total_seconds() // 60)
                taxi_str = f"🚕 **Taxi:** {taxi_min} min\n\n"
        except Exception as e:
            print(f"Taxi Calc Error: {e}")

        desc = (
            f"{dep_str}{arrow}{arr_str}\n\n"
            f"✈️ **{ac}**\n\n"
            f"{get_timing(delay)}\n" 
            f"{taxi_str}"            
            f"👨‍✈️ **{pilot}**\n\n"
            f"🌐 **{net.upper()}**\n\n"
            f"{payload_str}"
        )
        embed = discord.Embed(title=f"{type_emoji} 🛫 {full_cs} departed", url=flight_url, description=desc, color=0x3498db)

    elif status == "Completed":
        t = f.get("result", {}).get("totals", {})
        dist = t.get("distance", 0)
        ftime = t.get("time", 0)
        
        raw_balance = int(t.get("balance", 0))
        formatted_balance = f"{raw_balance:,}".replace(",", ".")
        rating = f.get("rating", 0.0)
        delay = f.get("delay", 0)
        
        check_g = 0.0
        check_fpm = 0
        has_crash_title = False # 🔴 НОВА ЗМІННА: Шукаємо краш у тексті
        
        if "result" in f and "violations" in f["result"]:
            for v in f["result"]["violations"]:
                # Перевіряємо наявність базової фрази крашу
                if "Crashed on landing" in v.get("title", ""):
                    has_crash_title = True
                    
                entry = v.get("entry", {}).get("payload", {}).get("touchDown", {})
                if entry and check_g == 0.0:
                    check_g = float(entry.get("gForce", 0))
                    check_fpm = int(entry.get("rate", 0))
                    
        if check_g == 0 and "landing" in f:
            check_g = float(f["landing"].get("gForce", 0))
            check_fpm = int(f["landing"].get("rate", 0) or f["landing"].get("touchDownRate", 0))

        title_text = f"{type_emoji} 😎 {full_cs} completed"
        color_code = 0x2ecc71
        rating_str = f"{get_rating_square(rating)} **{rating}**"

        # 🔥 Перевірка на краш (3G, 2000fpm АБО наявність фрази Crashed on landing) 🔥
        is_hard_crash = abs(check_g) > 3.0 or abs(check_fpm) > 2000 or has_crash_title
        
        time_info_str = f"{get_timing(delay)}\n\n"

        if is_hard_crash: 
            title_text = f"{type_emoji} 💥 {full_cs} CRASHED"
            color_code = 0x992d22 
            rating_str = "💀 **CRASH**"
            time_info_str = "" 
        
        elif f.get("emergency") is True:
            title_text = f"{type_emoji} ⚠️ {full_cs} EMERGENCY"
            color_code = 0xe67e22 
            rating_str = "🟥 **EMEG**"
            
        landing_info = get_landing_data(f, details_type)

        desc = (
            f"{dep_str}{arrow}{arr_str}\n\n"
            f"✈️ **{ac}**\n\n"
            f"{time_info_str}" 
            f"👨‍✈️ **{pilot}**\n\n"
            f"🌐 **{net.upper()}**\n\n"
            f"{landing_info}\n\n" 
            f"{payload_str}\n\n"
            f"📏 **{dist}** nm  |  ⏱️ **{format_time(ftime)}**\n\n"
            f"💰 **{formatted_balance} $**\n\n"
            f"{rating_str}"
        )
        embed = discord.Embed(title=title_text, url=flight_url, description=desc, color=color_code)

    elif status == "Cancelled":
        flight_duration = 0
        if f.get("durationAct"):
            flight_duration = f.get("durationAct")
        elif f.get("takeoffTimeAct") and f.get("lastState", {}).get("timestamp"):
            try:
                takeoff = datetime.fromisoformat(f.get("takeoffTimeAct").replace("Z", "+00:00"))
                last_ping = datetime.fromtimestamp(f["lastState"]["timestamp"] / 1000, tz=timezone.utc)
                flight_duration = int((last_ping - takeoff).total_seconds() // 60)
            except: pass

        desc = (
            f"{dep_str}{arrow}{arr_str}\n\n"
            f"✈️ **{ac}**\n\n"
            f"📍 **Status:** Flight Cancelled / Connection Lost\n"
            f"⏱️ **Flight time:** ~{flight_duration} min\n\n"
            f"👨‍✈️ **{pilot}**\n\n"
            f"🌐 **{net.upper()}**\n\n"
            f"{payload_str}"
        )
        embed = discord.Embed(title=f"⚫ {full_cs} flight cancelled", url=flight_url, description=desc, color=0x2b2d31)

    if embed:
        try:
            if reply_to_id:
                sent_msg = await channel.send(embed=embed, reference=discord.MessageReference(message_id=reply_to_id, channel_id=CHANNEL_ID, fail_if_not_exists=False))
            else:
                sent_msg = await channel.send(embed=embed)
            return sent_msg.id
        except Exception as e:
            print(f"Send error: {e}")
            return None

async def change_status():
    current_status = next(status_cycle)
    activity_type = discord.ActivityType.playing
    if current_status["type"] == "watch":
        activity_type = discord.ActivityType.watching
    elif current_status["type"] == "listen":
        activity_type = discord.ActivityType.listening
    await client.change_presence(activity=discord.Activity(type=activity_type, name=current_status["name"]))

async def status_loop():
    await client.wait_until_ready()
    while not client.is_closed():
        await change_status()
        await asyncio.sleep(3600)

# --- 🔍 ФУНКЦІЯ: Універсальний пошук повідомлень (DRY принцип) ---
async def find_discord_message(target_id, command_message):
    found_message = None
    main_channel = client.get_channel(CHANNEL_ID)
    
    if main_channel:
        try:
            found_message = await main_channel.fetch_message(target_id)
        except:
            pass
    
    if not found_message:
        await command_message.channel.send("🔍 **Searching for message...**")
        for guild in client.guilds:
            for channel in guild.text_channels:
                if channel.id == CHANNEL_ID: continue
                try:
                    found_message = await channel.fetch_message(target_id)
                    if found_message: break
                except:
                    continue
            if found_message: break
            
    return found_message
# -----------------------------------------------------------------

@client.event
async def on_message(message):
    global last_sent_message
    
    if message.author == client.user: return

    # --- 🕵️ ПЕРЕХОПЛЕННЯ ПП ---
    if isinstance(message.channel, discord.DMChannel):
        if message.author.id in ADMIN_IDS:
            pass
        else:
            try:
                owner = await client.fetch_user(ADMIN_IDS[0])
                await owner.send(f"🕵️ **Intercepted DM from {message.author.mention} ({message.author.name}):**\n{message.content}")
            except Exception as e:
                print(f"DM Intercept Error: {e}")

    is_admin = False
    if message.author.id in ADMIN_IDS:
        is_admin = True
    else:
        # 1. Якщо пишуть прямо на сервері
        if message.guild and message.author.guild_permissions.administrator:
            is_admin = True
        # 2. Якщо пишуть у ПП, перевіряємо їхні права на головному сервері
        elif not message.guild:
            main_channel = client.get_channel(CHANNEL_ID)
            if main_channel and main_channel.guild:
                try:
                    # Шукаємо цього користувача на сервері
                    member = main_channel.guild.get_member(message.author.id)
                    if not member: # Якщо його немає в кеші, робимо запит
                        member = await main_channel.guild.fetch_member(message.author.id)
                        
                    if member and member.guild_permissions.administrator:
                        is_admin = True
                except:
                    pass
    
    # --- 📥 КОМАНДА: !cache (СКАЧАТИ ФАЙЛ ПАМ'ЯТІ) ---
    if message.content == "!cache":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        if not STATE_FILE.exists() or os.path.getsize(STATE_FILE) == 0:
            return await message.channel.send("⚠️ **Cache file (sent.json) is empty or does not exist yet.**")
            
        await message.channel.send(
            content="📂 **Bot Memory File (sent.json):**", 
            file=discord.File(STATE_FILE)
        )
        return
    # --------------------------------------------------------

	# --- 📥 КОМАНДА: !syncweek (СИНХРОНІЗАЦІЯ ЗА ЧАСОМ ЗАКРИТТЯ РЕЙСІВ) ---
    if message.content == "!syncweek":
        if not is_admin: 
            return await message.channel.send("🚫 **Access Denied**")
        
        # 1. Визначаємо початок поточного тижня (Понеділок, 00:00:00 UTC)
        now = datetime.now(timezone.utc)
        monday = now - timedelta(days=now.weekday())
        monday_start = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        start_iso = monday_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        current_week_tag = get_iso_week()
        
        status_msg = await message.channel.send(f"⏳ **Рахую рейси з початку тижня ({monday_start.strftime('%d.%m.%Y')})...**")
        
        try:
            async with aiohttp.ClientSession() as session:
                body = {
                    "count": 100, 
                    "start": start_iso
                }
                recent = await fetch_api(session, "/flights/recent", method="POST", body=body)
                
                if not recent or "results" not in recent:
                    return await status_msg.edit(content="❌ **Помилка:** Не вдалося отримати список рейсів від Newsky.")
                
                flights_list = recent["results"]
                total_found = len(flights_list)
                added_count = 0
                
                if total_found == 0:
                    return await status_msg.edit(content="✅ **За цей тиждень ще немає завершених рейсів.**")
                
                # 🔥 МАГІЯ ТУТ: Сортуємо строго за часом ЗАКРИТТЯ рейсу (updatedAt)
                # Від тих, що закрилися першими, до тих, що закрилися щойно.
                # Це повністю імітує роботу живого бота!
                flights_list.sort(key=lambda x: x.get("updatedAt", ""))
                
                await status_msg.edit(content=f"⏳ **Знайдено {total_found} рейсів. Додаю в порядку їх закриття...**")
                
                for raw_f in flights_list:
                    # Ігноруємо видалені та незакриті
                    if raw_f.get("deleted") or not raw_f.get("close"):
                        continue
                        
                    fid = str(raw_f.get("_id") or raw_f.get("id"))
                    
                    det = await fetch_api(session, f"/flight/{fid}")
                    if not det or "flight" not in det: 
                        continue
                        
                    f = det["flight"]
                    t = f.get("result", {}).get("totals", {})
                    
                    if t.get("distance", 0) == 0 and t.get("time", 0) == 0:
                        continue
                        
                    sched_time = f.get("depTimeSched") or f.get("creationDate")
                    week_tag = get_iso_week(sched_time)
                    
                    if week_tag != current_week_tag:
                        continue
                    
                    update_weekly_stats(f, week_tag)
                    added_count += 1
                    
                    if added_count % 5 == 0:
                        await status_msg.edit(content=f"⏳ **Синхронізація: додано {added_count} / {total_found} рейсів...**")
                        
                await status_msg.edit(content=f"✅ **Синхронізація завершена!**\nДодано **{added_count}** рейсів у статистику так, якби бот був онлайн весь тиждень.")
                
        except Exception as e:
            await status_msg.edit(content=f"❌ **Сталася помилка:** {e}")
        return
    # -------------------------------------------------------------

	# --- 🕵️‍♂️ КОМАНДА: !check (ПЕРЕВІРКА ФАЙЛУ З ОБХОДОМ 1МБ) ---
    if message.content == "!check":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        msg = await message.channel.send("⏳ **Стягую файл прямо з GitHub (Blob API)...**")
        
        github_api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
        gh_headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(github_api_url, headers=gh_headers) as resp:
                    if resp.status != 200:
                        return await msg.edit(content=f"❌ **Помилка API:** HTTP {resp.status}")
                    
                    gh_data = await resp.json()
                    if gh_data.get('content'):
                        text = base64.b64decode(gh_data['content']).decode('utf-8')
                    else:
                        # Якщо контент пустий, робимо другий запит до Blob
                        async with session.get(gh_data['git_url'], headers=gh_headers) as b_resp:
                            b_data = await b_resp.json()
                            text = base64.b64decode(b_data['content']).decode('utf-8')
                    
                    preview = text[:1000] # Перші 1000 символів
                    await msg.edit(content=f"✅ **Розмір файлу: {gh_data.get('size')} байт**\n**Бот бачить текст:**\n```json\n{preview}...\n```")
        except Exception as e:
            await msg.edit(content=f"❌ **Помилка:** {e}")
        return
#---------------------#

	# --- 🔄 КОМАНДА: !updatedemand (ПРИМУСОВЕ ОНОВЛЕННЯ GITHUB) ---
    if message.content == "!updatedemand":
        if not is_admin: 
            return await message.channel.send("🚫 **Access Denied**")

        status_msg = await message.channel.send("⏳ **Launching a forced update of `newsky-airports.txt` on GitHub...**")

        try:
            # Викликаємо базову асинхронну функцію таски напряму
            await update_github_demand_task.coro()
            await status_msg.edit(content="✅ **Forced update completed!**")
        except Exception as e:
            await status_msg.edit(content=f"❌ **An error occurred during the forced update:** {e}")
        return
    # -------------------------------------------------------------

# --- 🧪 КОМАНДА 1: !teststatspin (ЗІ ЗАКРІПЛЕННЯМ ТА ВІДКРІПЛЕННЯМ) ---
    if message.content == "!teststatspin":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        stats = load_weekly_stats()
        if not stats: return await message.channel.send("⚠️ **Stats file is empty.**")
        
        await message.channel.send("🛠️ **Generating real report (WITH pins)...**")
        try:
            pinned_msgs = await message.channel.pins()
            for p_msg in pinned_msgs:
                if p_msg.author == client.user and p_msg.embeds and p_msg.embeds[0].title and "Weekly Summary" in p_msg.embeds[0].title:
                    await p_msg.unpin()
        except Exception as e:
            print(f"Error unpinning: {e}")
            
        for week_tag, s in stats.items():
            new_msg = await publish_weekly_embed(message.channel, week_tag, s)
            if new_msg:
                try: await new_msg.pin()
                except Exception as e: print(f"Error pinning: {e}")
        return

    # --- 📌 КОМАНДА 2: !teststats (БЕЗ ЗАКРІПЛЕННЯ) ---
    if message.content == "!teststats":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        stats = load_weekly_stats()
        if not stats: return await message.channel.send("⚠️ **Stats file is empty.**")
        
        await message.channel.send("🛠️ **Generating real report (NO pins)...**")
        for week_tag, s in stats.items():
            await publish_weekly_embed(message.channel, week_tag, s)
        return

    # --- 🤡 КОМАНДА 3: !teststatstest (ФЕЙКОВІ ДАНІ ДЛЯ ПРЕЗЕНТАЦІЇ) ---
    if message.content == "!teststatstest":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        await message.channel.send("🛠️ **Generating presentation report (Randomized Fake Data)...**")
        
        # Рандомні базові значення
        f_count = random.randint(80, 200)
        
        # Допоміжна функція для генерації випадкових рейсів (ТОП-3)
        def gen_random_flights(time_min, time_max):
            apts = ["UKBB", "UKLL", "UKOO", "UKLU", "EPWA", "LOWW", "KJFK", "LTFM", "EDDF"]
            return [
                {
                    "time": random.randint(time_min, time_max),
                    "pilot": f"Pilot {random.choice(['Alpha', 'Bravo', 'Charlie', 'Delta', 'Echo'])}",
                    "dep": random.choice(apts),
                    "arr": random.choice(apts)
                } for _ in range(3)
            ]

        # Генеруємо красиві штучні дані
        dummy_s = {
            "flights": f_count, 
            "earnings": f_count * random.randint(8000, 20000), 
            "pax": f_count * random.randint(120, 250), 
            "cargo": f_count * random.randint(1500, 5000),
            
            "rating_sum": f_count * random.uniform(9.0, 9.9), 
            "fpm_sum": f_count * random.randint(-350, -150),    
            "g_sum": f_count * random.uniform(1.05, 1.25),      
            
            "pilots": {f"Capt. {name}": random.randint(10, 40) for name in ["Shevchenko", "Kovalenko", "Boyko"]},
            "airports": {apt: random.randint(15, 60) for apt in ["UKBB", "UKLL", "LOWW"]},
            "aircrafts": {ac: random.randint(20, 70) for ac in ["B738", "A320", "B77W"]},
            
            "records": {
                "butter": {"fpm": random.randint(-60, -10), "g": round(random.uniform(1.0, 1.05), 2), "pilot": "Capt. Silk"},
                "hardest": {"fpm": random.randint(-800, -500), "g": round(random.uniform(1.6, 2.2), 2), "pilot": "Capt. Ryan"},
                
                # Рандомні списки для ТОП-3 (відсортовані)
                "longest": sorted(gen_random_flights(400, 700), key=lambda x: x["time"], reverse=True),
                "shortest": sorted(gen_random_flights(25, 60), key=lambda x: x["time"])
            }
        }
        
        current_week = get_iso_week()
        await publish_weekly_embed(message.channel, current_week, dummy_s)
        return

    # --- 🧹 КОМАНДА: !clearstats (ОЧИСТИТИ ВСЮ СТАТИСТИКУ З ПЕРЕВІРКОЮ) ---
    if message.content == "!clearstats":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        # Генеруємо два числа від 3 до 9
        num1 = random.randint(3, 9)
        num2 = random.randint(3, 9)
        correct_answer = num1 * num2
        
        await message.channel.send(f"⚠️ **WARNING!** You are about to completely wipe all weekly statistics.\nTo confirm this action, please solve this math problem:\n**What is {num1} x {num2}?**\n*(Just type the number in the chat, you have 10 seconds)*")
        
        # Фільтр: бот прийме відповідь тільки від того, хто запустив команду, і тільки в цьому ж каналі
        def check(m):
            return m.author == message.author and m.channel == message.channel
            
        try:
            # Бот чекає на повідомлення 10 секунд
            reply = await client.wait_for('message', check=check, timeout=10.0)
            
            # Перевіряємо, чи зійшлася математика
            if reply.content.strip() == str(correct_answer):
                save_weekly_stats({})
                await message.channel.send("🗑️ **Stats file (`weekly_stats.json`) completely wiped!**")
            else:
                await message.channel.send(f"❌ **Wrong answer (it should be {correct_answer}).** Protection triggered, deletion cancelled! 😅")
                
        except asyncio.TimeoutError:
            # Якщо за 20 секунд ніхто нічого не написав
            await message.channel.send("⏳ **Time's up.** Deletion operation cancelled.")
            
        return
    # -------------------------------------------------------------

    # --- ➕ КОМАНДА: !addflight <ID> (ДОДАТИ ПРОПУЩЕНИЙ РЕЙС) ---
    if message.content.startswith("!addflight"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        parts = message.content.split()
        if len(parts) < 2:
            return await message.channel.send("⚠️ Usage: `!addflight <Flight_ID>`")
        
        fid = parts[1]
        msg = await message.channel.send(f"⏳ **Searching for flight `{fid}` in Newsky DB...**")
        
        try:
            async with aiohttp.ClientSession() as session:
                det = await fetch_api(session, f"/flight/{fid}")
                
                if not det or "flight" not in det:
                    return await msg.edit(content=f"❌ **Error:** Flight `{fid}` not found in API.")
                
                f = det["flight"]
                
                sched_time = f.get("depTimeSched") or f.get("creationDate")
                week_tag = get_iso_week(sched_time)
                
                update_weekly_stats(f, week_tag)
                
                cs = f.get("flightNumber") or f.get("callsign") or "Unknown"
                await msg.edit(content=f"✅ **Flight `{cs}` successfully found and added to week `{week_tag}`!**")
                
        except Exception as e:
            await msg.edit(content=f"❌ **Internal error occurred:** {e}")
        return
    # -------------------------------------------------------------

	# --- ➖ КОМАНДА: !delflight <ID> (ВІДНЯТИ РЕЙС ЗІ СТАТИСТИКИ) ---
    if message.content.startswith("!delflight"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        parts = message.content.split()
        if len(parts) < 2:
            return await message.channel.send("⚠️ Usage: `!delflight <Flight_ID>`")
        
        fid = parts[1]
        msg = await message.channel.send(f"⏳ **Removing flight `{fid}` from stats...**")
        
        try:
            async with aiohttp.ClientSession() as session:
                det = await fetch_api(session, f"/flight/{fid}")
                
                if not det or "flight" not in det:
                    return await msg.edit(content=f"❌ **Error:** Flight `{fid}` not found in API.")
                
                f = det["flight"]
                sched_time = f.get("depTimeSched") or f.get("creationDate")
                week_tag = get_iso_week(sched_time)
                
                stats = load_weekly_stats()
                if week_tag not in stats:
                    return await msg.edit(content=f"⚠️ Week `{week_tag}` not found in stats.")
                
                s = stats[week_tag]
                
                t = f.get("result", {}).get("totals", {})
                balance = int(t.get("balance", 0))
                
                raw_pax = t.get("payload", {}).get("pax", 0)
                if raw_pax == 0 and f.get("type") != "cargo":
                    raw_pax = int(f.get("payload", {}).get("pax", 0))
                    
                cargo_kg = int(f.get("payload", {}).get("weights", {}).get("cargo", 0))
                rating = float(f.get("rating", 0.0))
                
                pilot = f.get("pilot", {}).get("fullname", "Unknown Pilot")
                
                check_g, check_fpm = 0.0, 0
                if "result" in f and "violations" in f["result"]:
                    for v in f["result"]["violations"]:
                        entry = v.get("entry", {}).get("payload", {}).get("touchDown", {})
                        if entry:
                            check_g = float(entry.get("gForce", 0))
                            check_fpm = int(entry.get("rate", 0))
                            break 
                if check_g == 0 and "landing" in f:
                    check_g = float(f.get("landing", {}).get("gForce", 0))
                    check_fpm = int(f.get("landing", {}).get("rate", 0) or f.get("landing", {}).get("touchDownRate", 0))
                    
                fpm_val = -abs(check_fpm) if check_fpm != 0 else 0
                
                if s["flights"] > 0:
                    s["flights"] -= 1
                    s["earnings"] -= balance
                    s["pax"] -= raw_pax
                    s["cargo"] -= cargo_kg
                    s["rating_sum"] -= rating
                    s["fpm_sum"] -= fpm_val
                    s["g_sum"] -= check_g
                
                if pilot in s.get("pilots", {}):
                    s["pilots"][pilot] -= 1
                    if s["pilots"][pilot] <= 0: del s["pilots"][pilot]
                
                save_weekly_stats(stats)
                
                cs = f.get("flightNumber") or f.get("callsign") or "Unknown"
                await msg.edit(content=f"✅ **Flight `{cs}` successfully REMOVED from week `{week_tag}`!**\n*(Note: Totals and averages are fixed. Weekly records like 'Hardest Landing' are not changed).*")
                
        except Exception as e:
            await msg.edit(content=f"❌ **Internal error occurred:** {e}")
        return
    # -------------------------------------------------------------

        # --- 📊 КОМАНДА: !stats (СКАЧАТИ ФАЙЛ СТАТИСТИКИ) ---
    if message.content == "!stats":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        try:
            if not WEEKLY_STATS_FILE.exists() or os.path.getsize(WEEKLY_STATS_FILE) == 0:
                return await message.channel.send("⚠️ **Stats file (weekly_stats.json) is empty or does not exist yet.**")
                
            with open(WEEKLY_STATS_FILE, "rb") as fp:
                await message.channel.send(
                    content="📊 **Weekly Stats File:**", 
                    file=discord.File(fp, filename="weekly_stats.json")
                )
        except Exception as e:
            await message.channel.send(f"❌ **Error sending file:** {e}")
        return
    # --------------------------------------------------------

    # --- 📜 КОМАНДА: !audit [all/кількість] (СКАЧАТИ ЖУРНАЛ АУДИТУ) ---
    if message.content.startswith("!audit"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        parts = message.content.split()
        
        fetch_limit = 50
        is_all = False
        
        if len(parts) > 1:
            if parts[1].lower() == "all":
                fetch_limit = None
                is_all = True
            elif parts[1].isdigit():
                fetch_limit = int(parts[1])

        main_channel = client.get_channel(CHANNEL_ID)
        if not main_channel:
            return await message.channel.send("❌ **Error:** Cannot find the main server. Check CHANNEL_ID.")
        
        guild = main_channel.guild
        
        if is_all:
            await message.channel.send(f"⏳ **Gathering ALL available logs (up to 90 days) from '{guild.name}'... This may take a minute.**")
        else:
            await message.channel.send(f"⏳ **Gathering the last {fetch_limit} audit logs from '{guild.name}'...**")
        
        try:
            audit_text = f"=== Audit Log: {guild.name} ===\n"
            audit_text += f"Limit: {'All available (up to 90 days)' if is_all else fetch_limit}\n" + "="*50 + "\n\n"
            
            count = 0
            async for entry in guild.audit_logs(limit=fetch_limit):
                time_str = entry.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
                user = entry.user
                action = entry.action.name
                target = entry.target
                reason = entry.reason or "Not specified"
                
                audit_text += f"[{time_str}] {user} -> ACTION: {action}\n"
                audit_text += f"   Target: {target}\n"
                audit_text += f"   Reason: {reason}\n"
                audit_text += "-"*50 + "\n"
                count += 1
                
            audit_text += f"\nTotal logs gathered: {count}"
                
            file_bin = io.BytesIO(audit_text.encode('utf-8'))
            
            await message.channel.send(
                content=f"✅ **Done! Found {count} logs.** Here is your report:", 
                file=discord.File(file_bin, filename=f"audit_log_{'all' if is_all else fetch_limit}.txt")
            )
            
        except discord.Forbidden:
            await message.channel.send("❌ **Error:** I don't have the 'View Audit Log' (Перегляд журналу аудиту) permission.")
        except Exception as e:
            await message.channel.send(f"❌ **Error:** {e}")
        return
    # -------------------------------------------------------------

    # --- 🗑️ КОМАНДА: !del (РОЗУМНЕ ВИДАЛЕННЯ ТІЛЬКИ ЗА ID) ---
    if message.content.startswith("!del "):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        parts = message.content.split()
        if len(parts) != 2:
            return await message.channel.send("⚠️ **Format:** `!del <Message_ID>`")
            
        try:
            msg_id = int(parts[1])
        except ValueError:
            return await message.channel.send("❌ **Error:** Message ID must be a number.")
            
        status_msg = await message.channel.send("⏳ **Searching for message across all channels...**")
        
        found_msg = None
        
        for guild in client.guilds:
            for channel in guild.text_channels:
                try:
                    found_msg = await channel.fetch_message(msg_id)
                    break
                except:
                    continue
            if found_msg:
                break
                
        if found_msg:
            try:
                await found_msg.delete()
                await status_msg.edit(content=f"✅ **Success!** Message silently deleted in channel `#{found_msg.channel.name}` 🥷")
            except discord.Forbidden:
                await status_msg.edit(content="❌ **Error:** Found the message, but lack permission to delete it (check Manage Messages).")
        else:
            await status_msg.edit(content="❌ **Error:** Message with this ID not found on the server (or already deleted).")
            
        return
    # -------------------------------------------------------------------------

    # --- 🧹 КОМАНДА: !clearwow <ID> (ОЧИСТИТИ ВСІ РЕАКЦІЇ) ---
    if message.content.startswith("!clearwow"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split()
        if len(parts) < 2:
            return await message.channel.send("⚠️ Usage: `!clearwow <Message_ID>`")
        
        target_id = parts[1]
        if not target_id.isdigit():
             return await message.channel.send("⚠️ ID must be a number.")

        found_message = await find_discord_message(int(target_id), message)
        
        if found_message:
            try:
                await found_message.clear_reactions()
                channel_mention = found_message.channel.mention if hasattr(found_message.channel, 'mention') else "Direct Messages"
                await message.channel.send(f"✅ **Cleared all reactions from message in {channel_mention}**")
            except discord.Forbidden:
                await message.channel.send("❌ **Error:** I don't have 'Manage Messages' permission to clear reactions. Please check role settings.")
            except Exception as e:
                await message.channel.send(f"❌ **Error clearing reactions:** {e}")
        else:
            await message.channel.send("❌ **Message not found.** (Check ID or bot permissions)")
        return
    # -------------------------------------------------------------

    # --- 🛡️ КОМАНДА: !banwow <ID> (ЗАБОРОНИТИ СМАЙЛИ) ---
    if message.content.startswith("!banwow"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split()
        if len(parts) < 2 or not parts[1].isdigit():
            return await message.channel.send("⚠️ Usage: `!banwow <Message_ID>`")
        
        msg_id = int(parts[1])
        BANNED_WOW_MESSAGES.add(msg_id)
        await message.channel.send(f"🛡️ **Message {msg_id} is now protected!**\nAny new reactions will be instantly deleted.")
        return

    # --- 🟢 КОМАНДА: !unbanwow <ID> (ДОЗВОЛИТИ СМАЙЛИ) ---
    if message.content.startswith("!unbanwow"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split()
        if len(parts) < 2 or not parts[1].isdigit():
            return await message.channel.send("⚠️ Usage: `!unbanwow <Message_ID>`")
        
        msg_id = int(parts[1])
        if msg_id in BANNED_WOW_MESSAGES:
            BANNED_WOW_MESSAGES.remove(msg_id)
            await message.channel.send(f"✅ **Protection removed.** You can now react to message {msg_id} again.")
        else:
            await message.channel.send("⚠️ This message is not currently blacklisted.")
        return
    
    # --- 👹 КОМАНДА: !wow <ID> <EMOJI> (СТАВИТИ РЕАКЦІЮ) ---
    if message.content.startswith("!wow"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split()
        if len(parts) < 3:
            return await message.channel.send("⚠️ Usage: `!wow <Message_ID> <Emoji>`")
        
        target_id = parts[1]
        emoji = parts[2]
        if not target_id.isdigit():
             return await message.channel.send("⚠️ ID must be a number.")

        found_message = await find_discord_message(int(target_id), message)
        
        if found_message:
            try:
                await found_message.add_reaction(emoji)
                await message.channel.send(f"✅ **Reacted {emoji} to message in {found_message.channel.mention}**")
            except Exception as e:
                await message.channel.send(f"❌ **Error adding reaction:** {e}")
        else:
            await message.channel.send("❌ **Message not found.** (Check ID or bot permissions)")
        return
    # -------------------------------------------------------------

    # --- 💽 КОМАНДА: !disk (РЕАЛЬНА ПАМ'ЯТЬ БОТА) ---
    if message.content == "!disk":
        def get_size(path="."):
            total = 0
            for dirpath, _, filenames in os.walk(path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    if not os.path.islink(fp):
                        total += os.path.getsize(fp)
            return total / (1024 * 1024)

        app_size_mb = get_size("/app") 
        volume_size_mb = get_size("/app/data") if os.path.exists("/app/data") else 0
        
        temp_used_mb = app_size_mb - volume_size_mb
        
        LIMIT_MB = 1024.0
        free_mb = LIMIT_MB - temp_used_mb
        
        text = (
            f"💽 **Bot's Real Memory Stats:**\n"
            f"**Available Limit:** {LIMIT_MB} MB (1 GB)\n"
            f"**Currently Used:** {temp_used_mb:.2f} MB\n"
            f"**FREE TO WRITE:** {free_mb:.2f} MB\n"
            f"*(Reference: Volume persistent memory takes {volume_size_mb:.2f} MB)*"
        )
        return await message.channel.send(text)
    # -------------------------------------------------------------

	# --- 🔍 КОМАНДА: !idemoji <назва> (ДІЗНАТИСЯ КОД ЕМОДЗІ) ---
    if message.content.startswith("!idemoji"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split()
        if len(parts) < 2:
            return await message.channel.send("⚠️ Usage: `!idemoji <name>` (without colons)")
        
        if not message.guild:
            return await message.channel.send("⚠️ Please use this command in a server channel, not in DM.")

        emoji_name = parts[1].replace(":", "") # Прибираємо двокрапки, якщо випадково написав
        
        # Шукаємо емодзі на сервері
        found_emoji = discord.utils.get(message.guild.emojis, name=emoji_name)
        
        if found_emoji:
            # Формуємо правильний код (з 'a', якщо анімований)
            emoji_code = f"<{'a' if found_emoji.animated else ''}:{found_emoji.name}:{found_emoji.id}>"
            await message.channel.send(f"✅ **Found it!**\nCopy this code:\n`{emoji_code}`\n\nPreview: {emoji_code}")
        else:
            await message.channel.send(f"❌ **Error:** Emoji named `{emoji_name}` not found on this server.")
        return
    # -------------------------------------------------------------

    # --- 📂 КОМАНДА: !files (ВМІСТ ПОСТІЙНОЇ ПАМ'ЯТІ / VOLUME) ---
    if message.content == "!files":
        folder_path = "/app/data"
        
        if os.path.exists(folder_path):
            files = os.listdir(folder_path)
            
            if len(files) > 0:
                file_list = "\n".join([f"📄 {file}" for file in files])
                await message.channel.send(f"📂 **Contents of persistent folder `{folder_path}`:**\n```text\n{file_list}\n```")
            else:
                await message.channel.send(f"📂 Folder `{folder_path}` is currently completely empty.")
        else:
            await message.channel.send(f"❌ Error: Folder `{folder_path}` does not exist! Volume is not attached or the path is incorrect.")
        return

    # --- 🗑️ КОМАНДА: !unwow <ID> <EMOJI> (ПРИБРАТИ РЕАКЦІЮ) ---
    if message.content.startswith("!unwow"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split()
        if len(parts) < 3:
            return await message.channel.send("⚠️ Usage: `!unwow <Message_ID> <Emoji>`")
        
        target_id = parts[1]
        emoji = parts[2]
        if not target_id.isdigit():
             return await message.channel.send("⚠️ ID must be a number.")

        found_message = await find_discord_message(int(target_id), message)
        
        if found_message:
            try:
                await found_message.remove_reaction(emoji, client.user)
                await message.channel.send(f"✅ **Removed {emoji} from message in {found_message.channel.mention}**")
            except Exception as e:
                await message.channel.send(f"❌ **Error removing reaction:** {e}")
        else:
            await message.channel.send("❌ **Message not found.** (Check ID or bot permissions)")
        return
    # -------------------------------------------------------------

    # --- 💬 НОВА КОМАНДА: !reply <ID> <text> (ВІДПОВІСТИ НА ПОВІДОМЛЕННЯ) ---
    if message.content.startswith("!reply"):
        if not is_admin: 
            return await message.channel.send("🚫 **Access Denied**")
        
        parts = message.content.split()
        if len(parts) < 3:
            return await message.channel.send("⚠️ Usage: `!reply <Message_ID> <text>`")
        
        target_id = parts[1]
        if not target_id.isdigit():
             return await message.channel.send("⚠️ ID must be a number.")

        content = " ".join(parts[2:])
        found_message = await find_discord_message(int(target_id), message)
        
        if found_message:
            try:
                sent_msg = await found_message.reply(content)
                last_sent_message = sent_msg # 🔥 Зберігаємо для команди !undo
                await message.channel.send(f"✅ **Replied to message in {found_message.channel.mention}:**\n{content}")
            except Exception as e:
                await message.channel.send(f"❌ **Error replying:** {e}")
        else:
            await message.channel.send("❌ **Message not found.** (Check ID or bot permissions)")
        return
    # -------------------------------------------------------------

    # --- 🔨 КОМАНДА: !ban <User_ID> [reason] (БАН КОРИСТУВАЧА НА СЕРВЕРІ) ---
    if message.content.startswith("!ban ") or message.content == "!ban":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split()
        if len(parts) < 2:
            return await message.channel.send("⚠️ Usage: `!ban <User_ID> [reason]`")
        
        target_id_str = parts[1]
        if not target_id_str.isdigit():
             return await message.channel.send("⚠️ User ID must be a number.")
        
        target_user_id = int(target_id_str)

        ban_reason = " ".join(parts[2:]) if len(parts) > 2 else None

        main_channel = client.get_channel(CHANNEL_ID)
        if not main_channel:
            return await message.channel.send("❌ **Error:** Cannot find the main server. Check CHANNEL_ID.")
        
        guild = main_channel.guild
        
        try:
            user_to_ban = discord.Object(id=target_user_id)
            await guild.ban(user_to_ban, reason=ban_reason, delete_message_seconds=0)
            
            reason_text = f" for: {ban_reason}" if ban_reason else " (No reason provided)"
            await message.channel.send(f"✅ **User {target_user_id} has been banned{reason_text}.** (Messages kept)")
        except discord.Forbidden:
            await message.channel.send("❌ **Error:** I don't have the 'Ban Members' permission, or my role is lower than the target's role.")
        except Exception as e:
            await message.channel.send(f"❌ **Error banning user:** {e}")
        return
    # -------------------------------------------------------------

    # --- 🕊️ КОМАНДА: !unban <User_ID> (РОЗБАН КОРИСТУВАЧА) ---
    if message.content.startswith("!unban"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split()
        if len(parts) < 2:
            return await message.channel.send("⚠️ Usage: `!unban <User_ID>`")
        
        target_id_str = parts[1]
        if not target_id_str.isdigit():
             return await message.channel.send("⚠️ User ID must be a number.")
        
        target_user_id = int(target_id_str)
        main_channel = client.get_channel(CHANNEL_ID)
        if not main_channel:
            return await message.channel.send("❌ **Error:** Cannot find the main server.")
        
        guild = main_channel.guild
        
        try:
            user_to_unban = discord.Object(id=target_user_id)
            await guild.unban(user_to_unban, reason="Unbanned via bot.")
            
            await message.channel.send(f"✅ **User {target_user_id} has been unbanned in '{guild.name}'.**")
        except discord.NotFound:
            await message.channel.send(f"⚠️ **User {target_user_id} is not banned on this server.**")
        except discord.Forbidden:
            await message.channel.send("❌ **Error:** I don't have the 'Ban Members' permission.")
        except Exception as e:
            await message.channel.send(f"❌ **Error unbanning user:** {e}")
        return
    # -------------------------------------------------------------
    
    # --- 📋 КОМАНДА: !banlist (ПОКАЗАТИ СПИСОК ЗАБАНЕНИХ) ---
    if message.content == "!banlist":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        main_channel = client.get_channel(CHANNEL_ID)
        if not main_channel:
            return await message.channel.send("❌ **Error:** Cannot find the main server.")
        
        guild = main_channel.guild
        
        try:
            await message.channel.send(f"⏳ **Fetching ban list for server '{guild.name}'...**")
            
            ban_list_text = f"=== Ban List for Server: {guild.name} ===\n\n"
            count = 0
            
            async for ban_entry in guild.bans():
                user = ban_entry.user
                reason = ban_entry.reason or "Not specified"
                ban_list_text += f"User: {user} (ID: {user.id})\nReason: {reason}\n{'-'*40}\n"
                count += 1
                
            if count == 0:
                return await message.channel.send("✅ **The ban list is empty!** No banned users on this server.")
                
            ban_list_text += f"\nTotal banned users: {count}."
            
            file_bin = io.BytesIO(ban_list_text.encode('utf-8'))
            await message.channel.send(
                content=f"📜 **Done! Found {count} banned users.** Here is the file:", 
                file=discord.File(file_bin, filename="banlist.txt")
            )
            
        except discord.Forbidden:
            await message.channel.send("❌ **Error:** I don't have the 'Ban Members' permission to view the ban list.")
        except Exception as e:
            await message.channel.send(f"❌ **Error fetching ban list:** {e}")
        return
    # -------------------------------------------------------------

	# --- 📝 КОМАНДА: !rename <ID> <текст> (ДОДАТИ ТЕКСТ В КАРТКУ ЧЕРЕЗ ПП) ---
    if message.content.startswith("!rename"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        parts = message.content.split(maxsplit=2)
        if len(parts) < 3:
            return await message.channel.send("⚠️ Usage: `!rename <Message_ID> <text>`")
            
        target_id_str = parts[1]
        if not target_id_str.isdigit():
             return await message.channel.send("⚠️ ID must be a number.")
             
        new_text = parts[2]
        
        found_message = await find_discord_message(int(target_id_str), message)
        
        if found_message:
            if found_message.author == client.user and found_message.embeds:
                try:
                    embed = found_message.embeds[0]
                    
                    old_desc = embed.description or ""
                    embed.description = f"{old_desc}\n\n{new_text}"
                    
                    await found_message.edit(embed=embed)
                    
                    channel_mention = found_message.channel.mention if hasattr(found_message.channel, 'mention') else "Direct Messages"
                    await message.channel.send(f"✅ **Success!** Text added to message in {channel_mention}.")
                    
                    if not isinstance(message.channel, discord.DMChannel):
                        try:
                            await message.delete()
                        except discord.Forbidden:
                            pass
                            
                except Exception as e:
                    await message.channel.send(f"❌ **Error updating message:** {e}")
            else:
                await message.channel.send("❌ **Error:** Message must be sent by the bot and contain an embed.")
        else:
            await message.channel.send("❌ **Message not found.** (Check ID or bot permissions)")
        return
    # -------------------------------------------------------------

   # --- ✉️ КОМАНДА: !msg <ID_каналу> <текст> (+ МОЖНА ПРИКРІПЛЯТИ КАРТИНКИ) ---
    if message.content.startswith("!msg"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        parts = message.content.split(" ", 2)
        if len(parts) < 2 and not message.attachments:
            return await message.channel.send("⚠️ **Format:** `!msg <Channel_ID> <text>` (and/or attach an image)")
            
        try:
            channel_id = int(parts[1])
            target_channel = client.get_channel(channel_id) 
            if not target_channel:
                return await message.channel.send("❌ **Error:** Channel with this ID not found.")
                
            text_to_send = parts[2] if len(parts) > 2 else ""
            
            files_to_send = []
            for attachment in message.attachments:
                files_to_send.append(await attachment.to_file())
                
            if not text_to_send and not files_to_send:
                return await message.channel.send("⚠️ **Error:** No text or image to send.")
                
            sent_msg = await target_channel.send(content=text_to_send, files=files_to_send)
            
            last_sent_message = sent_msg
            
            await message.add_reaction("✅") 
            
        except ValueError:
            await message.channel.send("❌ **Error:** Channel ID must be a number.")
        except Exception as e:
            await message.channel.send(f"❌ **Error sending message:** {e}")
        return
    # -------------------------------------------------------------------------
    
# --- 📡 КОМАНДА: !traffic (ПОКАЗАТИ АКТИВНІ РЕЙСИ) ---
    if message.content == "!traffic":
        global LAST_TRAFFIC_TIME
        current_time = time.time()

        if current_time - LAST_TRAFFIC_TIME < 10:
            remaining = int(10 - (current_time - LAST_TRAFFIC_TIME))
            warning_msg = await message.channel.send(f"⏳ **Please wait {remaining} seconds** before requesting traffic again.")
            await asyncio.sleep(3)
            try:
                await warning_msg.delete()
            except:
                pass
            return
            
        LAST_TRAFFIC_TIME = current_time
        
        msg = await message.channel.send("🔄 **Fetching live traffic data...**")
        
        async with aiohttp.ClientSession() as session:
            ongoing = await fetch_api(session, "/flights/ongoing")
            
            # 1. ОБРОБКА СИТУАЦІЇ, КОЛИ НЕМАЄ РЕЙСІВ
            if not ongoing or "results" not in ongoing or len(ongoing["results"]) == 0:
                embed = discord.Embed(title="📡 Live Traffic - Ukraine Classic Air Alliance", description="😴 **No active flights.**", color=0xffff00)
                current_utc_time = datetime.now(timezone.utc).strftime('%H:%M')
                return await msg.edit(content=None, embed=embed)
            
            # 2. ЯКЩО РЕЙСИ Є, ОБРОБЛЯЄМО ЇХ
            desc_lines = []
            for raw_f in ongoing["results"]:
                fid = str(raw_f.get("_id") or raw_f.get("id"))
                
                det = await fetch_api(session, f"/flight/{fid}")
                
                alt_str, gs_str = "---", "---"
                phase_str = "⏳ Unknown"
                
                if det and "flight" in det:
                    f = det["flight"]
                    last_state = f.get("lastState", {})
                    
                    loc = last_state.get("location", {})
                    spd = last_state.get("speed", {})
                    
                    # alt_ft - стандартна висота (MSL) для ешелону, agl_ft - радіовисотомір
                    alt_ft = int(loc.get("alt", 0))
                    alt_str = f"{alt_ft:,}".replace(",", ".") + " ft"
                    
                    gs_kts = int(spd.get("gs", 0))
                    gs_str = f"{gs_kts} kts"
                    
                    agl_ft = int(loc.get("agl", alt_ft))
                    vs_fpm = int(spd.get("vs", 0))
                    
                    dep_time = f.get("depTimeAct")
                    takeoff_time = f.get("takeoffTimeAct")
                    arr_time = f.get("arrTimeAct") or f.get("landing")
                    
                    # --- ЛОГІКА ФАЗ ПОЛЬОТУ ---
                    if not takeoff_time:
                        if dep_time:
                            phase_str = "🚜 Taxiing"
                        else:
                            phase_str = "🧳 Boarding"
                    else:
                        if arr_time or (agl_ft < 200 and gs_kts < 50):
                            if agl_ft > 200:
                                phase_str = "⤴️ Go Around"
                            else:
                                phase_str = "🏁 Arrived"
                        else:
                            # Аналіз історії для Cruise (перевірка MSL висоти 60 сек тому)
                            current_ts = last_state.get("timestamp", time.time() * 1000) / 1000.0
                            found_old = False
                            old_alt = alt_ft
                            
                            for p in reversed(f.get("path", [])):
                                p_time = p.get("createdAt")
                                if p_time:
                                    p_ts = datetime.fromisoformat(p_time.replace("Z", "+00:00")).timestamp()
                                    if (current_ts - p_ts) >= 60:
                                        old_alt = int(p.get("alt", alt_ft))
                                        found_old = True
                                        break
                                        
                            is_cruising = False
                            if found_old and abs(alt_ft - old_alt) <= 250:
                                is_cruising = True
                            elif not found_old and abs(vs_fpm) <= 250:
                                is_cruising = True
                                
                            if is_cruising:
                                phase_str = "✈️ Cruise"
                            elif vs_fpm < -250 or (found_old and (old_alt - alt_ft) > 250):
                                if agl_ft < 10000:
                                    phase_str = "🛬 Approach"
                                else:
                                    phase_str = "↘️ Descent"
                            else:
                                if agl_ft < 10000:
                                    phase_str = "🛫 Departed"
                                else:
                                    phase_str = "↗️ Climb"
                else:
                    f = raw_f

                cs = f.get("flightNumber") or f.get("callsign") or "N/A"
                airline = f.get("airline", {}).get("icao", "")
                full_cs = f"{airline} {cs}".strip() if airline else cs
                
                pilot_data = f.get("pilot", {})
                pilot = pilot_data.get("fullname", "Unknown Pilot") if isinstance(pilot_data, dict) else "Unknown Pilot"
                
                ac_data = f.get("aircraft", {})
                ac = "A/C"
                if isinstance(ac_data, dict):
                    ac = ac_data.get("icao") or ac_data.get("airframe", {}).get("icao") or ac_data.get("airframe", {}).get("name") or "A/C"
                
                dep = f.get("dep", {}).get("icao", "???") if isinstance(f.get("dep"), dict) else "???"
                arr = f.get("arr", {}).get("icao", "???") if isinstance(f.get("arr"), dict) else "???"
                
                desc_lines.append(f"**{full_cs}** • {pilot} • {ac} • {dep} ➔ {arr}\n╰ ⛰️ {alt_str}  |  🛰️ {gs_str}  |  {phase_str}")
            
            embed = discord.Embed(title="📡 Live Traffic - Ukraine Classic Air Alliance", description="\n\n".join(desc_lines), color=0x3498db)
            
            current_utc_time = datetime.now(timezone.utc).strftime('%H:%M')
            total_flights = len(ongoing["results"])
            embed.set_footer(text=f"✈️ Active flights: {total_flights}  |  🔄 Updated: {current_utc_time} UTC  |  Newsky API")
            
            await msg.edit(content=None, embed=embed)
        return
    # -------------------------------------------------------------
	
	# --- 🎤 КОМАНДА: !enter <Channel_ID> (Зайти в голосовий канал) ---
    if message.content.startswith("!enter"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split()
        if len(parts) < 2 or not parts[1].isdigit():
            return await message.channel.send("⚠️ Usage: `!enter <Voice_Channel_ID>`")

        vc_id = int(parts[1])
        target_vc = client.get_channel(vc_id)

        if not target_vc or not isinstance(target_vc, discord.VoiceChannel):
            return await message.channel.send("❌ **Error:** Voice channel with this ID not found.")

        guild = target_vc.guild

        if guild.voice_client:
            await guild.voice_client.disconnect()

        try:
            await target_vc.connect()
            await message.channel.send(f"✅ **Bot joined channel:** {target_vc.name}")
        except Exception as e:
            await message.channel.send(f"❌ **Error:** {e}\n*(Make sure `PyNaCl` is in your requirements.txt)*")
        return
    # -------------------------------------------------------------

    # --- 🔇 КОМАНДА: !mute (Замутити/Розмутити бота) ---
    if message.content == "!mute":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")

        main_channel = client.get_channel(CHANNEL_ID)
        guild = message.guild if message.guild else (main_channel.guild if main_channel else None)
        
        if not guild or not guild.voice_client:
            return await message.channel.send("⚠️ **Bot is not currently in any voice channel.**")

        bot_voice_state = guild.me.voice
        current_mute = bot_voice_state.self_mute if bot_voice_state else False
        new_mute_state = not current_mute

        try:
            await guild.change_voice_state(channel=guild.voice_client.channel, self_mute=new_mute_state)
            status_text = "🔇 **Bot microphone MUTED.**" if new_mute_state else "🔊 **Bot microphone UNMUTED.**"
            await message.channel.send(f"✅ {status_text}")
        except Exception as e:
            await message.channel.send(f"❌ **Error:** {e}")
        return
    # -------------------------------------------------------------

    # --- 🚪 КОМАНДА: !leave (Вийти з голосового) ---
    if message.content == "!leave":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")

        main_channel = client.get_channel(CHANNEL_ID)
        guild = message.guild if message.guild else (main_channel.guild if main_channel else None)

        if guild and guild.voice_client:
            await guild.voice_client.disconnect()
            await message.channel.send("✅ **Bot left the voice channel.**")
        else:
            await message.channel.send("⚠️ **Bot is already not in a voice channel.**")
        return
    # -------------------------------------------------------------

	# --- 📌 КОМАНДА: !pin <ID> (ЗАКРІПИТИ ПОВІДОМЛЕННЯ) ---
    if message.content.startswith("!pin"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        parts = message.content.split()
        if len(parts) < 2:
            return await message.channel.send("⚠️ Usage: `!pin <Message_ID>`")
            
        target_id = parts[1]
        if not target_id.isdigit():
             return await message.channel.send("⚠️ ID must be a number.")

        found_message = await find_discord_message(int(target_id), message)
        
        if found_message:
            try:
                await found_message.pin()
                await message.channel.send(f"📌 **Message successfully pinned in {found_message.channel.mention}!**")
            except discord.Forbidden:
                await message.channel.send("❌ **Error:** I don't have 'Manage Messages' permission to pin this.")
            except Exception as e:
                await message.channel.send(f"❌ **Error pinning message:** {e}")
        else:
            await message.channel.send("❌ **Message not found.** (Check ID or bot permissions)")
        return
    # -------------------------------------------------------------

    # --- 🧲 КОМАНДА: !unpin <ID> (ВІДКРІПИТИ ПОВІДОМЛЕННЯ) ---
    if message.content.startswith("!unpin"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        
        parts = message.content.split()
        if len(parts) < 2:
            return await message.channel.send("⚠️ Usage: `!unpin <Message_ID>`")
            
        target_id = parts[1]
        if not target_id.isdigit():
             return await message.channel.send("⚠️ ID must be a number.")

        found_message = await find_discord_message(int(target_id), message)
        
        if found_message:
            try:
                await found_message.unpin()
                await message.channel.send(f"🧲 **Message successfully unpinned in {found_message.channel.mention}!**")
            except discord.Forbidden:
                await message.channel.send("❌ **Error:** I don't have 'Manage Messages' permission to unpin this.")
            except Exception as e:
                await message.channel.send(f"❌ **Error unpinning message:** {e}")
        else:
            await message.channel.send("❌ **Message not found.** (Check ID or bot permissions)")
        return
    # -------------------------------------------------------------

# --- 📚 КОМАНДА: !help (ДИНАМІЧНА ДЛЯ КОРИСТУВАЧІВ, АДМІНІВ ТА ВЛАСНИКА) ---
    if message.content == "!help":
        is_owner = message.author.id in ADMIN_IDS
        
        embed = discord.Embed(title="📚 Bot Commands", color=0x3498db)
        
        # 1. Це бачать УСІ користувачі
        desc = "**🔹 User Commands:**\n"
        desc += "**`!help`** — Show command list\n"
        desc += "**`!traffic`** — Show active flights\n\n"
        
        # 2. Це бачать АДМІНІСТРАТОРИ сервера (і ти також)
        if is_admin:
            desc += "**🔒 Admin Commands:**\n"
            desc += "**`!status`** — System status\n"
            desc += "**`!test [min]`** — Run test scenarios\n"
            desc += "**`!syncweek`** — Sync flights for current week\n"
            desc += "**`!updatedemand`** — Force GitHub demand update\n"
            desc += "**`!delflight <ID>`** — Remove specific flight from stats\n"
            desc += "**`!ban <ID> [reason]`** — Ban user on server\n"
            desc += "**`!unban <ID>`** — Unban user on server\n"
            desc += "**`!audit [all/num]`** — Download server audit log\n"
            desc += "**`!cache`** — Download bot memory (sent.json)\n"
            desc += "**`!spy <ID>`** — Dump raw flight JSON data\n"
            desc += "**`!stats`** — Download weekly_stats.json\n"
            desc += "**`!banlist`** — Download banned users list\n"
            desc += "**`!disk`** — Show server disk/memory usage\n\n"
            
            desc += "**💬 Message & UI Management:**\n"
            desc += "**`!msg [ID] <text/pic>`** — Send text or image message\n"
            desc += "**`!reply <ID> <text>`** — Reply to a message\n"
            desc += "**`!rename <ID> <text>`** — Append text to an existing embed\n"
            desc += "**`!del <msg_ID>`** — Delete any message globally\n"
            desc += "**`!pin <ID>`** — Pin a message\n"
            desc += "**`!unpin <ID>`** — Unpin a message\n"
            desc += "**`!wow <ID> <emoji>`** — Add reaction to a message\n"
            desc += "**`!unwow <ID> <emoji>`** — Remove reaction\n"
            desc += "**`!teststatspin`** — Generate weekly report (PINS message)\n"
            desc += "**`!teststats`** — Generate weekly report (NO pin)\n"
            desc += "**`!teststatstest`** — Generate presentation report (Fake Data)\n"
            desc += "**`!addflight <ID>`** — Force add missed flight to stats\n"
            desc += "**`!clearstats`** — Wipe all weekly stats data\n"
            desc += "**`!idemoji <name>`** — Get code for a custom server emoji\n\n"
            
            desc += "**🎭 Status Management:**\n"
            desc += "**`!next`** — Force next bot status\n"
            desc += "**`!addstatus <type> <text>`** — Save & Add status\n"
            desc += "**`!delstatus [num]`** — Delete status\n\n"
            
        # 3. Це бачиш ТІЛЬКИ ТИ (ID з ADMIN_IDS)
        if is_owner:
            desc += "**👑 Owner Commands (Super Secret):**\n"
            desc += "**`!clearwow <ID>`** — Clear ALL reactions from a msg\n"
            desc += "**`!banwow <ID>`** — Protect msg from new reactions\n"
            desc += "**`!unbanwow <ID>`** — Remove reaction protection\n"
            desc += "**`!enter <ID>`** — Enter voice channel\n"
            desc += "**`!leave`** — Leave voice channel\n"
            desc += "**`!mute`** — Toggle bot microphone mute\n"
            desc += "**`!files`** — List files in local storage (/app/data)\n\n"
            
        embed.description = desc
        await message.channel.send(embed=embed)
        return
    # ----------------------------------------------------------------
    
    if message.content == "!next":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        await change_status()
        await message.channel.send("✅ **Status switched!**")
        return

    if message.content.startswith("!addstatus"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split(maxsplit=2)
        if len(parts) < 3: return await message.channel.send("⚠️ Usage: `!addstatus <watch/play> <text>`")
        sType = parts[1].lower()
        if sType not in ["watch", "play", "listen"]: return await message.channel.send("⚠️ Use: `watch`, `play`, `listen`")
        status_list.append({"type": sType, "name": parts[2]})
        save_statuses()
        global status_cycle
        status_cycle = cycle(status_list)
        await message.channel.send(f"✅ Saved & Added: **{parts[2]}**")
        return

    if message.content.startswith("!delstatus"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split()
        if len(parts) == 1:
            list_str = "\n".join([f"`{i+1}.` {s['type'].upper()}: {s['name']}" for i, s in enumerate(status_list)])
            embed = discord.Embed(title="🗑️ Delete Status", description=f"Type `!delstatus <number>` to delete.\n\n{list_str}", color=0xe74c3c)
            return await message.channel.send(embed=embed)
        try:
            idx = int(parts[1]) - 1
            if 0 <= idx < len(status_list):
                if len(status_list) <= 1: return await message.channel.send("⚠️ Cannot delete the last status!")
                removed = status_list.pop(idx)
                save_statuses()
                status_cycle = cycle(status_list) 
                await message.channel.send(f"🗑️ Deleted & Saved: **{removed['name']}**")
            else:
                await message.channel.send("⚠️ Invalid number.")
        except ValueError:
            await message.channel.send("⚠️ Please enter a number.")
        return

    if message.content == "!status":
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        msg = await message.channel.send("🔄 **Checking Systems...**")
        api_status = "❌ API Error"
        flights_count = 0 
        
        async with aiohttp.ClientSession() as session:
            test = await fetch_api(session, "/flights/ongoing")
            if test is not None: 
                api_status = "✅ Connected to Newsky"
                if "results" in test:
                    flights_count = len(test["results"])
        
        launch_str = START_TIME.strftime("%d-%m-%Y %H:%M:%S UTC")

        embed = discord.Embed(title="🤖 Bot System Status", color=0x2ecc71)
        embed.add_field(name="📡 Newsky API", value=api_status, inline=False)
        embed.add_field(name="✈️ Active Flights", value=f"**{flights_count}** tracking", inline=False)
        embed.add_field(name="🌍 Airports DB", value=f"✅ Loaded ({len(AIRPORTS_DB)} airports)", inline=False)
        embed.add_field(name="📶 Discord Ping", value=f"**{round(client.latency * 1000)}ms**", inline=False)
        embed.add_field(name="🚀 Launched at", value=f"`{launch_str}`", inline=False)
        await msg.edit(content=None, embed=embed)
        return

    if message.content.startswith("!spy"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        try:
            parts = message.content.split()
            if len(parts) < 2: return await message.channel.send("⚠️ Usage: `!spy <ID>`")
            fid = parts[1]
            await message.channel.send(f"🕵️ **Analyzing {fid}...**")
            async with aiohttp.ClientSession() as session:
                data = await fetch_api(session, f"/flight/{fid}")
                if not data: return await message.channel.send("❌ API Error")
                file_bin = io.BytesIO(json.dumps(data, indent=4).encode())
                await message.channel.send(content=f"📂 **Dump {fid}:**", file=discord.File(file_bin, filename=f"flight_{fid}.json"))
        except Exception as e: await message.channel.send(f"Error: {e}")
        return

    if message.content.startswith("!test"):
        if not is_admin: return await message.channel.send("🚫 **Access Denied**")
        parts = message.content.split()
        if len(parts) == 2:
            try:
                custom_delay = int(parts[1])
                await message.channel.send(f"🛠️ **Custom Test (Delay: {custom_delay} min)...**")
                mock_custom = {"_id": "test_custom", "flightNumber": "TEST1", "airline": {"icao": "OSA"}, "dep": {"icao": "UKBB", "name": "Boryspil"}, "arr": {"icao": "LPMA", "name": "Madeira"}, "aircraft": {"airframe": {"name": "B738"}}, "pilot": {"fullname": "Capt. Test"}, "payload": {"pax": 140, "cargo": 35}, "network": "VATSIM", "rating": 9.9, "landing": {"rate": -120, "gForce": 1.05}, "delay": custom_delay, "result": {"totals": {"distance": 350, "time": 55, "balance": 12500, "payload": {"pax": 140, "cargo": 35}}}}
                await send_flight_message(message.channel, "Completed", mock_custom, "test")
                return
            except ValueError: pass

        await message.channel.send("🛠️ **Running Full Test Suite...**")
        mock_dep = {"_id": "test_dep", "flightNumber": "TEST1", "airline": {"icao": "OSA"}, "dep": {"icao": "UKBB", "name": "Boryspil"}, "arr": {"icao": "LPMA", "name": "Madeira"}, "aircraft": {"airframe": {"name": "B738"}}, "pilot": {"fullname": "Capt. Test"}, "payload": {"pax": 145, "cargo": 35}, "delay": 2}
        await send_flight_message(message.channel, "Departed", mock_dep, "test")
        mock_norm = {"_id": "test_norm", "schedule": True, "flightNumber": "TEST1", "airline": {"icao": "OSA"}, "dep": {"icao": "UKBB", "name": "Boryspil"}, "arr": {"icao": "LPMA", "name": "Madeira"}, "aircraft": {"airframe": {"name": "B738"}}, "pilot": {"fullname": "Capt. Test"}, "payload": {"pax": 100, "cargo": 40}, "network": "VATSIM", "rating": 9.9, "landing": {"rate": -150, "gForce": 1.1}, "delay": -10, "result": {"totals": {"distance": 350, "time": 55, "balance": 12500, "payload": {"pax": 100, "cargo": 40}}}}
        await send_flight_message(message.channel, "Completed", mock_norm, "test")
        mock_emerg = mock_norm.copy(); mock_emerg["_id"] = "test_emerg"; mock_emerg["emergency"] = True; mock_emerg["delay"] = 45; mock_emerg["result"] = {"totals": {"distance": 350, "time": 55, "balance": 0, "payload": {"pax": 100, "cargo": 40}}}
        await send_flight_message(message.channel, "Completed", mock_emerg, "test")
        mock_crash = mock_norm.copy(); mock_crash["_id"] = "test_crash"; mock_crash["landing"] = {"rate": -2500, "gForce": 4.5}; mock_crash["rating"] = 0.0; mock_crash["delay"] = 0; mock_crash["result"] = {"totals": {"distance": 350, "time": 55, "balance": -1150000, "payload": {"pax": 100, "cargo": 40}}}
        await send_flight_message(message.channel, "Completed", mock_crash, "test")
        return

async def main_loop():
    await client.wait_until_ready()
    await update_airports_db()
    
    channel = client.get_channel(CHANNEL_ID)
    state = load_state()
    first_run = True

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                ongoing_ids = None
                ongoing = await fetch_api(session, "/flights/ongoing")
                if ongoing and "results" in ongoing:
                    ongoing_ids = set()
                    print(f"📡 Tracking {len(ongoing['results'])} flights...", end='\r')
                    for raw_f in ongoing["results"]:
                        fid = str(raw_f.get("_id") or raw_f.get("id"))
                        ongoing_ids.add(fid)
                        
                        state.setdefault(fid, {})
                        
                        # --- 1. ЛІНИВА ПЕРЕВІРКА ---
                        if state[fid].get("takeoff"):
                            continue
                            
                        det = await fetch_api(session, f"/flight/{fid}")
                        if not det or "flight" not in det: continue
                        f = det["flight"]
                        cs = f.get("flightNumber") or f.get("callsign") or "N/A"
                        if cs == "N/A": continue
                        
                        if f.get("takeoffTimeAct") and not state[fid].get("takeoff"):
                            msg_id = await send_flight_message(channel, "Departed", f, "ongoing")
                            state[fid]["takeoff"] = True
                            
                            # 🔥 НОВЕ: Генерація мітки тижня під час зльоту 🔥
                            sched_time = f.get("depTimeSched") or f.get("creationDate")
                            state[fid]["week"] = get_iso_week(sched_time)
                            
                            if msg_id:
                                state[fid]["msg_id"] = msg_id

                recent = await fetch_api(session, "/flights/recent", method="POST", body={"count": 5})
                if recent and "results" in recent:
                    for raw_f in recent["results"]:
                        fid = str(raw_f.get("_id") or raw_f.get("id"))
                        if first_run:
                            state.setdefault(fid, {})["completed"] = True
                            continue
                        if fid in state and state[fid].get("completed"): continue
                        
                        # --- ЛОГІКА ДЛЯ ЗАКРИТИХ ТА ВИДАЛЕНИХ РЕЙСІВ ---
                        if raw_f.get("close"):
                            print(f"⏳ Waiting for calculation: {fid}")
							
                            await asyncio.sleep(3)
                            
                            det = await fetch_api(session, f"/flight/{fid}")
                            if not det or "flight" not in det: continue
                            f = det["flight"]
                            
                            # 🔥 ФІЛЬТР ПОКИНУТИХ РЕЙСІВ (ABANDONED) 🔥
                            t = f.get("result", {}).get("totals", {})
                            if t.get("distance", 0) == 0 and t.get("time", 0) == 0:
                                print(f"🙈 Ignored abandoned flight: {fid}")
                                state.setdefault(fid, {})["completed"] = True
                                continue

                            cs = f.get("flightNumber") or f.get("callsign") or "N/A"
                            if cs == "N/A": continue

                            reply_id = state.get(fid, {}).get("msg_id")
                            await send_flight_message(channel, "Completed", f, "result", reply_to_id=reply_id)
                            
                            # 🔥 НОВЕ: Збір статистики після посадки 🔥
                            week_tag = state.get(fid, {}).get("week") or get_iso_week()
                            update_weekly_stats(f, week_tag)
                            
                            state.setdefault(fid, {})["completed"] = True
                            print(f"✅ Report Sent: {cs}")
                        
                        elif raw_f.get("deleted"):
                            
                            det = await fetch_api(session, f"/flight/{fid}")
                            if not det or "flight" not in det: continue
                            f = det["flight"]
                            cs = f.get("flightNumber") or f.get("callsign") or "N/A"
                            if cs == "N/A": continue

                            reply_id = state.get(fid, {}).get("msg_id")
                            await send_flight_message(channel, "Cancelled", f, "ongoing", reply_to_id=reply_id)
                            state.setdefault(fid, {})["completed"] = True
                            print(f"⚫ Cancel Report Sent: {cs}")

                if first_run:
                    print("🔕 First run sync complete. No spam.")
                    first_run = False

                save_state(state)
                
                # 🔥 ОНОВЛЕНИЙ ВИКЛИК: Передаємо список живих рейсів (ongoing_ids)
                if ongoing_ids is not None:
                    await check_and_publish_weekly_stats(channel, state, ongoing_ids)
                
            except Exception as e: 
                print(f"Loop Error: {e}")
            
            await asyncio.sleep(CHECK_INTERVAL)

# --- ⚡ РАДАР РЕАКЦІЙ (МИТТЄВЕ ВИДАЛЕННЯ) ---
@client.event
async def on_raw_reaction_add(payload):
    if payload.user_id == client.user.id:
        return

    if payload.message_id in BANNED_WOW_MESSAGES:
        try:
            channel = client.get_channel(payload.channel_id)
            if not channel: return
            
            message = await channel.fetch_message(payload.message_id)
            
            user = payload.member
            if not user:
                user = await client.fetch_user(payload.user_id)

            await message.remove_reaction(payload.emoji, user)
        except discord.Forbidden:
            print("⚠️ Error: Bot lacks 'Manage Messages' permission on the server!")
        except Exception as e:
            print(f"Error removing reaction: {e}")

# ====================================================================
# 🌐 WEBHOOK СЕРВЕР ДЛЯ NEWSKY (ПРИЙОМ ЗАЯВОК)
# ====================================================================
async def newsky_webhook(request):
    try:
        data = await request.json()
        print(f"📥 [WEBHOOK DATA]: {data}") # Лог для перевірки ключів!

        channel = client.get_channel(CHANNEL_ID) # Тут має бути ID твого каналу
        if not channel:
            return web.Response(text="Channel not found", status=500)

        # 1. Якщо Newsky надсилає вже готовий дизайн Discord
        if "embeds" in data and len(data["embeds"]) > 0:
            embed = discord.Embed.from_dict(data["embeds"][0])
            await channel.send(embed=embed)
            return web.Response(text="OK", status=200)

        # 2. Якщо потрібно зібрати дизайн самостійно
        pilot_data = data.get("pilot", {})
        pilot_name = pilot_data.get("fullname") or pilot_data.get("username") or "Unknown Pilot"
        
        embed = discord.Embed(
            title=f"👋 Pilot application: {pilot_name}",
            color=0x2b5293 
        )

        flights = pilot_data.get("flights", 0)
        hours = pilot_data.get("hours", 0.0)
        rating = pilot_data.get("rating", 0.0)
        embed.description = f"🔢 **{flights} flights** ㅤ ⏱️ **{hours}h** ㅤ 🟩 **{rating}**\n"

        # Літаки (динамічно)
        aircraft_list = pilot_data.get("most_active_aircraft", []) 
        if aircraft_list:
            embed.add_field(name="\u200b\n✈️ Most active on type:", value="", inline=False)
            for ac in aircraft_list[:3]:
                ac_type = ac.get("type", "Unknown")
                ac_flights = ac.get("flights", 0)
                ac_rating = ac.get("rating", 0.0)
                embed.add_field(name=ac_type, value=f"*flights:* {ac_flights}\n*rating:* {ac_rating}", inline=True)

        # Компанії (динамічно)
        airlines_list = pilot_data.get("previous_employment", [])
        if airlines_list:
            embed.add_field(name="\u200b\n💺 Previous employment:", value="", inline=False)
            for airline in airlines_list[:3]:
                al_name = airline.get("name", airline.get("airline", "Unknown Airline"))
                al_flights = airline.get("flights", 0)
                al_rating = airline.get("rating", 0.0)
                embed.add_field(name=al_name, value=f"*flights:* {al_flights}\n*rating:* {al_rating}", inline=False)

        await channel.send(embed=embed)
        return web.Response(text="OK", status=200)

    except Exception as e:
        print(f"❌ Webhook Error: {e}")
        return web.Response(text="Error", status=500)

async def start_web_server():
    app = web.Application()
    app.add_routes([web.post('/webhook', newsky_webhook)])
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    import os
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🌐 Web-сервер успішно запущено на порту {port}! (Шлях: /webhook)")
# ====================================================================

@tasks.loop(minutes=60)
async def update_github_demand_task():
    if not GITHUB_TOKEN or not NEWSKY_SID:
        print("⚠️ Немає токенів для GitHub або Newsky. Пропускаю оновлення файлу.")
        return

    print("🚀 Починаємо оновлення файлу newsky-airports.txt через GitHub API...")
    github_api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    gh_headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    async with aiohttp.ClientSession() as session:
        # 1. Завантажуємо метадані файлу з GitHub (SHA та перевірка розміру)
        async with session.get(github_api_url, headers=gh_headers) as resp:
            if resp.status != 200:
                print(f"❌ Помилка завантаження файлу з GitHub: {resp.status}")
                return
            gh_data = await resp.json()
            file_sha = gh_data['sha'] 
            
            # 🔥 ОБХІД ЛІМІТУ GITHUB 1 МБ 🔥
            if gh_data.get('content'):
                # Якщо файл < 1 МБ, контент є у відповіді
                old_text = base64.b64decode(gh_data['content']).decode('utf-8')
            else:
                # Якщо файл > 1 МБ, тягнемо контент через Blob API
                print(f"📦 Файл великий ({gh_data.get('size')} байт). Використовую Blob API...")
                blob_url = gh_data['git_url']
                async with session.get(blob_url, headers=gh_headers) as b_resp:
                    b_data = await b_resp.json()
                    old_text = base64.b64decode(b_data['content']).decode('utf-8')

        # 2. Розумний парсинг ICAO (через розділювачі ---)
        icaos = []
        blocks = old_text.split("---")
        for block in blocks:
            block = block.strip()
            # Шукаємо JSON усередині блоку (навіть якщо він розбитий на рядки)
            start = block.find("{")
            end = block.rfind("}")
            if start != -1 and end != -1:
                try:
                    data = json.loads(block[start:end+1])
                    if "icao" in data:
                        icaos.append(data["icao"])
                except:
                    continue

        if not icaos:
            print("❌ Не знайдено ICAO у файлі (список icaos пустий)!")
            return
        
        print(f"✅ Успішно знайдено {len(icaos)} ICAO для оновлення.")

        # 3. Збираємо свіжі дані з Newsky
        fresh_data = []
        ns_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Cookie": f"sid={NEWSKY_SID}" if not NEWSKY_SID.startswith("sid=") else NEWSKY_SID,
            "Accept": "application/json"
        }

        for icao in icaos:
            ns_url = f"https://newsky.app/api/airport/{icao}"
            try:
                async with session.get(ns_url, headers=ns_headers, timeout=10) as ns_resp:
                    if ns_resp.status == 200:
                        data = await ns_resp.json()
                        # Форматування з пробілами (separators), рівно як у тебе
                        formatted_line = json.dumps(data, ensure_ascii=False, separators=(', ', ': '))
                        fresh_data.append(formatted_line)
                    else:
                        print(f"❌ {icao} - Помилка {ns_resp.status}")
            except Exception as e:
                print(f"❌ {icao} - Помилка з'єднання: {e}")
            
            # Асинхронна пауза, щоб не блокувати бота
            await asyncio.sleep(0.5)

        # 4. Формуємо новий текст і пушимо на GitHub
        if fresh_data:
            now_str = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
            new_content = f"UPDATED: {now_str}\n\n" + "\n\n---\n\n".join(fresh_data)
            
            # GitHub API вимагає текст у форматі Base64
            new_content_b64 = base64.b64encode(new_content.encode('utf-8')).decode('utf-8')

            push_payload = {
                "message": f"🤖 Auto update demand data",
                "content": new_content_b64,
                "sha": file_sha  # ВАЖЛИВО: Це має бути sha з ПЕРШОГО запиту (/contents/), а не з Blob!
            }
            
            async with session.put(github_api_url, headers=gh_headers, json=push_payload) as put_resp:
                if put_resp.status in [200, 201]:
                    print(f"🎉 Файл на GitHub успішно оновлено!")
                else:
                    err_msg = await put_resp.text()
                    print(f"❌ Помилка запису на GitHub: {put_resp.status} - {err_msg}")

# --- 🚀 ЗАПУСК ГОЛОВНОГО ЦИКЛУ ---
@client.event
async def on_ready():
    global MONITORING_STARTED
    if MONITORING_STARTED: return
    MONITORING_STARTED = True
    
    if not update_github_demand_task.is_running():
        update_github_demand_task.start()
    
    print(f"✅ Bot online: {client.user}")
    print("🚀 MONITORING STARTED")
    client.loop.create_task(status_loop())
    client.loop.create_task(main_loop())
    client.loop.create_task(start_web_server())

client.run(DISCORD_TOKEN)
