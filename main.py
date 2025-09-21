#!/usr/bin/env python3
# main.py — TeleBot marketplace (sqlite3) — full flow
# - telebot (pyTelegramBotAPI)
# - APScheduler for timeouts (accept confirmation and auto-release)
# - SQLite with migrations including order_key
#
# Install:
# pip install pyTelegramBotAPI apscheduler python-dotenv

import os
import math
import time
import secrets
import logging
import threading
import sqlite3
from datetime import datetime, timedelta

from dotenv import load_dotenv
import telebot
from telebot.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
)

import html
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

# ---------------- Config & Logging ----------------

load_dotenv()
GEMINI_API_KEY = os.getenv("Paste_here_GEMINI_AI_API_KEY", "Paste_here_GEMINI_AI_API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "marketplace.db")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

INITIAL_RADIUS_KM = float(os.getenv("INITIAL_RADIUS_KM", "3.5"))
EXPANSION_STEP_KM = float(os.getenv("EXPANSION_STEP_KM", "1"))
EXPANSION_INTERVAL_SEC = int(os.getenv("EXPANSION_INTERVAL_SEC", str(5 * 60)))
MAX_RADIUS_KM = float(os.getenv("MAX_RADIUS_KM", "30"))
MAX_NOTIFY_EXECUTORS = int(os.getenv("MAX_NOTIFY_EXECUTORS", "50"))
CONFIRMATION_TIMEOUT_SEC = int(os.getenv("CONFIRMATION_TIMEOUT_SEC", "60"))
AUTO_RELEASE_DELAY_SEC = int(os.getenv("AUTO_RELEASE_DELAY_SEC", str(24 * 3600)))
UPLOADS_DIR = os.getenv("UPLOADS_DIR", "uploads")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("marketplace_bot")

if not BOT_TOKEN:
    logger.error("BOT_TOKEN not set in environment. Exiting.")
    raise SystemExit("BOT_TOKEN required")

bot = telebot.TeleBot(BOT_TOKEN)
scheduler = BackgroundScheduler()
scheduler.start()

# ---------------- Concurrency helpers ----------------

order_locks = {}         # order_id -> threading.Lock
order_locks_lock = threading.Lock()

def get_order_lock(order_id: int):
    with order_locks_lock:
        lk = order_locks.get(order_id)
        if lk is None:
            lk = threading.Lock()
            order_locks[order_id] = lk
        return lk

# ---------------- Utils ----------------

def ensure_dir(path):
    if not path:
        return
    os.makedirs(path, exist_ok=True)

def get_conn():
    # allow multithreaded use
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def gen_order_key():
    # short hex key (8 chars)
    return secrets.token_hex(4)


def now_iso():
    return datetime.now(timezone.utc).isoformat(sep=' ', timespec='seconds')
# ---------------- Haversine ----------------

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# ---------------- DB init & migrations ----------------

def init_db():
    ensure_dir(os.path.dirname(DB_PATH) or ".")
    conn = get_conn()
    try:
        cur = conn.cursor()
        # создаём таблицы, включая ratings
        cur.executescript(f"""
        BEGIN;
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE NOT NULL,
            username TEXT,
            full_name TEXT,
            age INTEGER,
            phone TEXT,
            role TEXT DEFAULT 'user',
            balance_coins INTEGER DEFAULT 0,
            frozen_total_coins INTEGER DEFAULT 0,
            status TEXT DEFAULT 'offline',
            lat REAL,
            lon REAL,
            available_since TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
            -- rating_sum и rating_count добавятся миграцией ниже
        );
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_key TEXT UNIQUE,
            creator_tg INTEGER NOT NULL,
            description TEXT,
            price_coins INTEGER NOT NULL,
            lat REAL,
            lon REAL,
            radius_km REAL DEFAULT {INITIAL_RADIUS_KM},
            status TEXT DEFAULT 'PUBLISHED',
            frozen_amount INTEGER DEFAULT 0,
            accepted_by INTEGER,
            accept_ts TEXT,
            requires_photo INTEGER DEFAULT 0,
            auto_release_at TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            executor_tg INTEGER,
            sent_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            executor_tg INTEGER,
            file_path TEXT,
            uploaded_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS disputes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            claimant_tg INTEGER,
            reason TEXT,
            status TEXT DEFAULT 'OPEN',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS admin_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_tg INTEGER,
            action TEXT,
            details TEXT,
            ts TEXT DEFAULT (datetime('now'))
        );
        -- Рейтинги и комментарии (one row per (order_id, from_tg))
        CREATE TABLE IF NOT EXISTS ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            from_tg INTEGER,
            to_tg INTEGER,
            stars INTEGER,
            comment TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(order_id, from_tg)
        );
        COMMIT;
        """)
        conn.commit()

        # --- protective migrations for orders
        cur.execute("PRAGMA table_info(orders);")
        cols_orders = {r["name"] for r in cur.fetchall()}
        if 'order_key' not in cols_orders:
            try:
                cur.execute("ALTER TABLE orders ADD COLUMN order_key TEXT UNIQUE;")
                conn.commit()
            except Exception:
                logger.exception("Failed to add order_key column (non-fatal).")

        # --- protective migrations for users
        cur.execute("PRAGMA table_info(users);")
        cols_users = {r["name"] for r in cur.fetchall()}

        if 'age' not in cols_users:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN age INTEGER;")
                conn.commit()
            except Exception:
                logger.exception("Failed to add age column (non-fatal).")

        if 'rating_sum' not in cols_users:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN rating_sum INTEGER DEFAULT 5;")
                conn.commit()
                # инициализация для существующих пользователей
                cur.execute("UPDATE users SET rating_sum = 5 WHERE rating_sum IS NULL;")
                conn.commit()
            except Exception:
                logger.exception("Failed to add rating_sum column (non-fatal).")

        if 'rating_count' not in cols_users:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN rating_count INTEGER DEFAULT 1;")
                conn.commit()
                # инициализация для существующих пользователей
                cur.execute("UPDATE users SET rating_count = 1 WHERE rating_count IS NULL;")
                conn.commit()
            except Exception:
                logger.exception("Failed to add rating_count column (non-fatal).")

        # ensure ratings table exists (protective)
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ratings';")
        if not cur.fetchone():
            try:
                cur.executescript("""
                    CREATE TABLE IF NOT EXISTS ratings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        order_id INTEGER,
                        from_tg INTEGER,
                        to_tg INTEGER,
                        stars INTEGER,
                        comment TEXT,
                        created_at TEXT DEFAULT (datetime('now')),
                        UNIQUE(order_id, from_tg)
                    );
                """)
                conn.commit()
            except Exception:
                logger.exception("Failed to create ratings table (non-fatal).")

        logger.info("DB initialized (%s).", DB_PATH)
    finally:
        conn.close()


# ---------------- Basic user functions ----------------

def ensure_user(tg_id: int, username: str = None, full_name: str = None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users (tg_id, username, full_name)
                VALUES (?, ?, ?)
                ON CONFLICT(tg_id) DO UPDATE SET
                  username=COALESCE(excluded.username, username),
                  full_name=COALESCE(excluded.full_name, full_name),
                  updated_at=CURRENT_TIMESTAMP;
            """, (tg_id, username, full_name))
        except sqlite3.OperationalError:
            # older sqlite variant: fallback
            cur.execute("SELECT id FROM users WHERE tg_id=?", (tg_id,))
            if cur.fetchone():
                cur.execute("UPDATE users SET username=?, full_name=?, updated_at=CURRENT_TIMESTAMP WHERE tg_id=?", (username, full_name, tg_id))
            else:
                cur.execute("INSERT INTO users (tg_id, username, full_name) VALUES (?, ?, ?)", (tg_id, username, full_name))
        conn.commit()
    except Exception:
        logger.exception("ensure_user failed")
    finally:
        conn.close()

def get_user(tg_id: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT tg_id, username, full_name, phone, role, status, balance_coins, frozen_total_coins, lat, lon FROM users WHERE tg_id=?", (tg_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "tg_id": row["tg_id"], "username": row["username"], "full_name": row["full_name"],
            "phone": row["phone"], "role": row["role"], "status": row["status"],
            "balance_coins": row["balance_coins"], "frozen_total_coins": row["frozen_total_coins"],
            "lat": row["lat"], "lon": row["lon"]
        }
    except Exception:
        logger.exception("get_user error")
        return None
    finally:
        conn.close()

def set_user_location(tg_id: int, lat: float, lon: float, status='available'):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE users SET lat=?, lon=?, status=?, available_since=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE tg_id=?", (lat, lon, status, tg_id))
        conn.commit()
    except Exception:
        logger.exception("set_user_location error")
    finally:
        conn.close()

def set_user_offline(tg_id: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE users SET status='offline', updated_at=CURRENT_TIMESTAMP WHERE tg_id=?", (tg_id,))
        conn.commit()
    except Exception:
        logger.exception("set_user_offline error")
    finally:
        conn.close()

def reset_user_location(tg_id: int):
    """Сбросить координаты пользователя и пометить оффлайн.
    Используется после завершения заказа, чтобы фрилансер заново отправил локацию.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET lat=NULL, lon=NULL, status='offline', updated_at=CURRENT_TIMESTAMP WHERE tg_id=?",
            (tg_id,)
        )
        conn.commit()
    except Exception:
        logger.exception("reset_user_location error for %s", tg_id)
    finally:
        conn.close()

def set_user_role(tg_id: int, role: str):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO users (tg_id, role) VALUES (?,?)", (tg_id, role))
        cur.execute("UPDATE users SET role=? WHERE tg_id=?", (role, tg_id))
        conn.commit()
    except Exception:
        logger.exception("set_user_role error")
    finally:
        conn.close()

def add_coins(tg_id: int, amount: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE users SET balance_coins = COALESCE(balance_coins,0) + ? WHERE tg_id=?", (amount, tg_id))
        conn.commit()
    except Exception:
        logger.exception("add_coins error")
    finally:
        conn.close()

# ---------------- Orders ----------------

def create_order_and_reserve(creator_tg: int, description: str, price: int, lat=None, lon=None, requires_photo: bool = False):
    conn = get_conn()

    # normalize pending lat/lon coming from user_state_data
    def _norm_pending_coords(v):
        try:
            if v is None: return None
            if isinstance(v, (int, float)): return float(v)
            s = str(v).strip()
            if s == "" or s.lower() in ("none", "null"): return None
            f = float(s)
            return f if abs(f) > 1e-9 else None
        except Exception:
            return None

    lat = _norm_pending_coords(lat)
    lon = _norm_pending_coords(lon)

    try:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        cur.execute("SELECT balance_coins FROM users WHERE tg_id=?", (creator_tg,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None, None, "user_not_found"
        balance = row["balance_coins"] or 0
        if balance < price:
            conn.rollback()
            return None, None, "insufficient_funds"
        # reserve funds
        cur.execute("UPDATE users SET balance_coins=balance_coins-?, frozen_total_coins=COALESCE(frozen_total_coins,0)+? WHERE tg_id=?", (price, price, creator_tg))
        order_key = gen_order_key()
        cur.execute(
            "INSERT INTO orders (order_key, creator_tg, description, price_coins, lat, lon, radius_km, frozen_amount, requires_photo) VALUES (?,?,?,?,?,?,?,?,?)",
            (order_key, creator_tg, description, price, lat, lon, INITIAL_RADIUS_KM, price, int(requires_photo))
        )
        order_id = cur.lastrowid
        conn.commit()
        return order_id, order_key, None
    except Exception:
        conn.rollback()
        logger.exception("create_order_and_reserve error")
        return None, None, "db_error"
    finally:
        conn.close()

def get_order_by_id(order_id: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE id=?", (order_id,))
        return cur.fetchone()
    finally:
        conn.close()

def get_order_by_key(key: str):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE order_key=?", (key,))
        return cur.fetchone()
    finally:
        conn.close()

def resolve_order_identifier(param: str):
    p = (param or "").strip()
    if not p:
        return None
    if p.lower().startswith("order:"):
        p = p.split(":",1)[1].strip()
    # if numeric -> try id
    if p.isdigit():
        r = get_order_by_id(int(p))
        if r:
            return r
    # try by key
    return get_order_by_key(p)

# ---------------- Geo & notifications ----------------

def find_executors_within(lat: float, lon: float, radius_km: float, limit: int = MAX_NOTIFY_EXECUTORS):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT tg_id, lat, lon FROM users WHERE role='executor' AND status='available' AND lat IS NOT NULL AND lon IS NOT NULL")
        rows = cur.fetchall()
        candidates = []
        for r in rows:
            try:
                tg = r["tg_id"]
                lat2 = float(r["lat"]); lon2 = float(r["lon"])
                d = haversine_km(lat, lon, lat2, lon2)
                if d <= radius_km:
                    candidates.append((tg, d))
            except Exception:
                continue
        candidates.sort(key=lambda x: x[1])
        return candidates[:limit]
    except Exception:
        logger.exception("find_executors_within error")
        return []
    finally:
        conn.close()

# ---------- HTML-escape helper ----------
def _escape_html(s: str) -> str:
    if s is None:
        return ""
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;"))

# ---------- Notify executors about a new order ----------

def notify_executors_of_order(order_id: int):
    """
    Send notifications to executors about order.
    Uses callback_data "accept:{order_key}" so executor can accept using order_key or numeric id.
    Message uses HTML parse mode (escaped) to avoid Markdown entity parsing errors.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT o.order_key, o.creator_tg, o.description, o.price_coins,
                   o.lat, o.lon, o.radius_km, u.full_name AS creator_full, u.age AS creator_age, u.username AS creator_username
            FROM orders o
            LEFT JOIN users u ON o.creator_tg = u.tg_id
            WHERE o.id = ?
        """, (order_id,))
        row = cur.fetchone()
        if not row:
            return

        order_key = row["order_key"] or str(order_id)
        creator_tg = row["creator_tg"]
        description = row["description"] or "—"
        price = row["price_coins"] or 0
        lat, lon = row["lat"], row["lon"]
        radius_km = row["radius_km"] or INITIAL_RADIUS_KM

        # creator display name fallback: full_name -> @username -> tg:<id>
        creator_full = row["creator_full"] if row and row["creator_full"] else None
        creator_username = row["creator_username"] if row and row["creator_username"] else None
        creator_age = str(row["creator_age"]) if row and row["creator_age"] is not None else "—"

        if creator_full:
            creator_name = creator_full
        elif creator_username:
            creator_name = f"@{creator_username}"
        else:
            creator_name = f"tg:{creator_tg}"

        # choose targets: online (no lat/lon) or location-based
        if lat is None or lon is None:
            conn2 = get_conn()
            try:
                cur2 = conn2.cursor()
                cur2.execute(
                    "SELECT tg_id FROM users WHERE role='executor' AND status='available' LIMIT ?",
                    (MAX_NOTIFY_EXECUTORS,)
                )
                rows2 = cur2.fetchall()
                targets = [(r["tg_id"], None) for r in rows2]
            finally:
                conn2.close()
        else:
            # expected to return list of (tg_id, distance_km)
            targets = find_executors_within(lat, lon, radius_km, limit=MAX_NOTIFY_EXECUTORS)

        notified_count = 0
        safe_order_key = _escape_html(order_key)
        safe_description = _escape_html(description)
        safe_creator_name = _escape_html(creator_name)
        safe_creator_age = _escape_html(creator_age)

        for tg_id, dist in targets:
            try:
                # record notification (avoid duplicates)
                conn_n = get_conn()
                try:
                    cur_n = conn_n.cursor()
                    cur_n.execute("SELECT 1 FROM notifications WHERE order_id=? AND executor_tg=? LIMIT 1", (order_id, tg_id))
                    if not cur_n.fetchone():
                        cur_n.execute("INSERT INTO notifications (order_id, executor_tg) VALUES (?,?)", (order_id, tg_id))
                        conn_n.commit()
                finally:
                    conn_n.close()

                # prepare keyboard & message
                kb = InlineKeyboardMarkup()
                kb.add(
                    InlineKeyboardButton("Принять", callback_data=f"accept:{safe_order_key}"),
                    InlineKeyboardButton("Пропустить", callback_data=f"skip:{safe_order_key}")
                )

                dist_text = f"{dist:.2f} км" if (dist is not None) else "—"

                msg = (
                    f"🆕 Новый заказ от @{creator_username}\n <b>{safe_creator_name}</b> ({safe_creator_age} лет)\n"
                    f"{safe_description}\n"
                    f"💰 Цена: {price} сум\n"
                    f"📍 Расстояние: {dist_text}\n"
                    f"🆔 ID Заказа: <code>{safe_order_key}</code>\n"
                    f"/order:{_escape_html(str(order_id))}"
                )

                try:
                    bot.send_message(tg_id, msg, reply_markup=kb, parse_mode="HTML")
                    # if offline/offline order had lat/lon — send location after message
                    if lat is not None and lon is not None:
                        try:
                            bot.send_location(tg_id, latitude=lat, longitude=lon)
                        except Exception:
                            logger.exception("send_location failed")
                    notified_count += 1
                except Exception:
                    logger.exception("send message to executor failed")
            except Exception:
                logger.exception("notify_executors_of_order loop error")

        # notify creator how many executors were notified
        try:
            bot.send_message(creator_tg, f"Ваш заказ отправлен {notified_count} исполнителям.")
        except Exception:
            logger.exception("failed to notify creator about notify count")

    except Exception:
        logger.exception("notify_executors_of_order error")
    finally:
        conn.close()



# ---------------- Scheduler tasks ----------------

# ---------------- Scheduler tasks (fixed) ----------------
from datetime import datetime, timedelta, timezone

# Assumes `scheduler` is a BackgroundScheduler created with timezone=timezone.utc:
#   scheduler = BackgroundScheduler(timezone=timezone.utc)
# and `get_conn`, `find_executors_within`, `bot`, `logger`, and config constants exist.

def expansion_job():
    """
    Expand radius for published offline orders and notify newly-included executors.
    Robust to DB errors and logs exceptions per-order so the loop continues.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM orders WHERE status='PUBLISHED' AND lat IS NOT NULL AND lon IS NOT NULL")
        rows = cur.fetchall()
        for r in rows:
            try:
                order_id = r["id"]
                cur.execute(
                    "SELECT lat, lon, radius_km, description, price_coins, order_key "
                    "FROM orders WHERE id=?",
                    (order_id,)
                )
                orow = cur.fetchone()
                if not orow:
                    continue

                lat = orow["lat"]; lon = orow["lon"]
                radius_km = orow["radius_km"] or INITIAL_RADIUS_KM
                new_radius = radius_km + EXPANSION_STEP_KM
                if new_radius > MAX_RADIUS_KM:
                    # optionally archive or mark, but skip expanding further
                    continue

                # find already-notified executors for this order
                cur2 = conn.cursor()
                cur2.execute("SELECT executor_tg FROM notifications WHERE order_id=?", (order_id,))
                already = {x["executor_tg"] for x in cur2.fetchall() if x["executor_tg"]}

                # find executors within new radius
                candidates = find_executors_within(lat, lon, new_radius, limit=MAX_NOTIFY_EXECUTORS)
                newly = [(tg, d) for tg, d in candidates if tg not in already]

                for tg, d in newly:
                    try:
                        kb = InlineKeyboardMarkup()
                        kb.add(
                            InlineKeyboardButton("Принять", callback_data=f"accept_id:{order_id}"),
                            InlineKeyboardButton("Пропустить", callback_data=f"skip_id:{order_id}")
                        )
                        dist_text = f"{d:.2f} км" if d is not None else "—"
                        order_key = orow["order_key"] or str(order_id)
                        desc = orow["description"] or "—"
                        price = orow["price_coins"] or 0

                        bot.send_message(
                            tg,
                            f"🆕 Новый заказ (расширение радиуса)\n"
                            f"{desc}\n"
                            f"💰 Цена: {price} сум\n"
                            f"📍 Расстояние: {dist_text}\n"
                            f"🆔 ID Заказа: `{order_key}`\n"
                            f"/order:{order_id}",
                            reply_markup=kb,
                            parse_mode="Markdown"
                        )

                        # record notification (separate connection to avoid locks)
                        conn3 = get_conn()
                        try:
                            cur3 = conn3.cursor()
                            cur3.execute(
                                "INSERT INTO notifications (order_id, executor_tg) VALUES (?,?)",
                                (order_id, tg)
                            )
                            conn3.commit()
                        finally:
                            conn3.close()
                    except Exception:
                        logger.exception("expansion notify failed for order %s -> tg %s", order_id, tg)

                # update radius in main orders table
                cur.execute("UPDATE orders SET radius_km=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (new_radius, order_id))
                conn.commit()

            except Exception:
                logger.exception("expansion_job loop error for order id=%s", r.get("id"))
    except Exception:
        logger.exception("expansion_job error")
    finally:
        conn.close()


def schedule_accept_timeout(order_id: int):
    """
    Schedule a single-run job that fires after CONFIRMATION_TIMEOUT_SEC.
    Uses timezone-aware datetime (UTC) so APScheduler won't mark the job as 'missed'
    if scheduler timezone is set to UTC as recommended.
    """
    job_id = f"confirm_timeout_{order_id}"

    def task():
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status, accepted_by, creator_tg FROM orders WHERE id=?", (order_id,))
            row = cur.fetchone()
            if not row:
                return
            if row["status"] == 'AWAITING_CUSTOMER_CONFIRM' and row["accepted_by"]:
                accepted_by = row["accepted_by"]
                creator_tg = row["creator_tg"]
                cur.execute("SELECT phone FROM users WHERE tg_id=?", (creator_tg,))
                pr = cur.fetchone()
                phone = pr["phone"] if pr else None
                try:
                    if phone:
                        bot.send_message(
                            accepted_by,
                            f"Заказчик не подтвердил в течение {CONFIRMATION_TIMEOUT_SEC} сек. Номер заказчика: {phone}"
                        )
                    else:
                        bot.send_message(
                            accepted_by,
                            f"Заказчик не подтвердил в течение {CONFIRMATION_TIMEOUT_SEC} сек. Номер не указан — попробуйте связаться через профиль."
                        )
                except Exception:
                    logger.exception("notify accepted_by on timeout failed for order %s", order_id)
        except Exception:
            logger.exception("accept_timeout task error for order %s", order_id)
        finally:
            conn.close()

    # timezone-aware run_at (UTC)
    run_at = datetime.now(timezone.utc) + timedelta(seconds=CONFIRMATION_TIMEOUT_SEC)
    try:
        # replace_existing=True ensures rescheduling works if job exists
        scheduler.add_job(task, 'date', run_date=run_at, id=job_id, replace_existing=True, misfire_grace_time=120)
        logger.info("Scheduled accept timeout job %s at %s (UTC)", job_id, run_at.isoformat())
    except Exception:
        logger.exception("schedule_accept_timeout failed for order %s", order_id)


def schedule_auto_release(order_id: int):
    """
    Schedule auto-release of frozen funds after AUTO_RELEASE_DELAY_SEC.
    Uses timezone-aware run time and transactional DB update with rollback on failure.
    """
    job_id = f"auto_release_{order_id}"

    def task():
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT status, frozen_amount, accepted_by, creator_tg "
                "FROM orders WHERE id=?", (order_id,)
            )
            row = cur.fetchone()
            if not row:
                return

            if row["status"] == "AWAITING_CLIENT_APPROVAL" and row["frozen_amount"] and row["accepted_by"]:
                frozen_amount = row["frozen_amount"] or 0
                accepted_by = row["accepted_by"]
                creator_tg = row["creator_tg"]

                try:
                    # --- транзакция ---
                    cur.execute("BEGIN IMMEDIATE;")
                    # начисляем исполнителю
                    cur.execute(
                        "UPDATE users SET balance_coins = COALESCE(balance_coins, 0) + ? WHERE tg_id=?",
                        (frozen_amount, accepted_by),
                    )
                    # списываем у заказчика
                    cur.execute(
                        "UPDATE users SET frozen_total_coins = COALESCE(frozen_total_coins, 0) - ? WHERE tg_id=?",
                        (frozen_amount, creator_tg),
                    )
                    # финализируем заказ
                    cur.execute(
                        "UPDATE orders "
                        "SET status='COMPLETED', frozen_amount=0, updated_at=CURRENT_TIMESTAMP "
                        "WHERE id=?",
                        (order_id,),
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    logger.exception("auto_release transfer failed for order %s", order_id)
                    return

                # --- уведомления + сброс локации ---
                try:
                    reset_user_location(accepted_by)
                    bot.send_message(
                        accepted_by,
                        "✅ Заказ завершён. Ваша локация сброшена. Чтобы снова получать заказы, отправьте вашу локацию 📍",
                    )
                except Exception:
                    logger.exception("notify executor about reset location failed for order %s", order_id)

                # сообщения о переводе
                try:
                    bot.send_message(
                        accepted_by,
                        f"Деньги автоматически переведены: {frozen_amount} сум (по истечении срока).",
                    )
                    bot.send_message(
                        creator_tg,
                        f"Деньги по заказу переведены автоматически через {AUTO_RELEASE_DELAY_SEC} секунд.",
                    )
                except Exception:
                    logger.exception("notify users after auto_release failed for order %s", order_id)

        except Exception:
            logger.exception("auto_release task error for order %s", order_id)
        finally:
            conn.close()

    run_at = datetime.now(timezone.utc) + timedelta(seconds=AUTO_RELEASE_DELAY_SEC)
    try:
        scheduler.add_job(task, 'date', run_date=run_at, id=job_id, replace_existing=True, misfire_grace_time=3600)
        logger.info("Scheduled auto_release job %s at %s (UTC)", job_id, run_at.isoformat())
    except Exception:
        logger.exception("schedule_auto_release failed for order %s", order_id)

# ---------------- Callback handlers ----------------
# ---------------- Callback handlers ----------------
def _escape_html(s: str) -> str:
    if s is None:
        return ""
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;"))

# ---------- Callback: executor accepts a job ----------
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("accept:"))
def callback_accept(call):
    raw = call.data.split(":", 1)[1]
    # resolve_order_identifier should accept either order_key or numeric id
    order_row = resolve_order_identifier(raw)
    if not order_row:
        bot.answer_callback_query(call.id, "Заказ не найден.")
        return

    order_id = order_row["id"]
    executor_tg = call.from_user.id
    lk = get_order_lock(order_id)
    if not lk.acquire(timeout=5):
        bot.answer_callback_query(call.id, "Попробуйте снова — занято.")
        return

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # check current order status
        cur.execute("SELECT status FROM orders WHERE id=?", (order_id,))
        r = cur.fetchone()
        if not r:
            bot.answer_callback_query(call.id, "Заказ не найден.")
            return
        if r["status"] != 'PUBLISHED':
            bot.answer_callback_query(call.id, "Извините — заказ уже взят или недоступен.")
            return

        # update order -> awaiting customer confirm
        cur.execute("""
            UPDATE orders
            SET status='AWAITING_CUSTOMER_CONFIRM',
                accepted_by=?,
                accept_ts=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, (executor_tg, order_id))
        conn.commit()

        # fetch creator and order_key for notifying creator
        cur.execute("SELECT creator_tg, order_key FROM orders WHERE id=?", (order_id,))
        rr = cur.fetchone()
        if rr:
            creator_tg = rr["creator_tg"]
            order_key = rr["order_key"] or str(order_id)

            # fetch executor profile (full_name, age, username) if present
            cur.execute("SELECT full_name, age, username FROM users WHERE tg_id=?", (executor_tg,))
            ex = cur.fetchone()
            ex_full = ex["full_name"] if ex and ex["full_name"] else None
            ex_age = str(ex["age"]) if ex and ex["age"] is not None else None
            ex_username = ex["username"] if ex and ex["username"] else None

            if ex_full:
                display = ex_full
            elif ex_username:
                display = f"@{ex_username}"
            else:
                display = call.from_user.first_name or f"tg:{executor_tg}"

            if ex_age:
                display_with_age = f"{display} ({_escape_html(ex_age)} лет)"
            else:
                display_with_age = _escape_html(display)

            safe_display = _escape_html(display_with_age)
            safe_key = _escape_html(order_key)

            # keyboard for creator
            kb = InlineKeyboardMarkup()
            kb.add(
                InlineKeyboardButton("Подтвердить", callback_data=f"customer_confirm:{order_id}"),
                InlineKeyboardButton("Отменить", callback_data=f"customer_cancel:{order_id}")
            )

            # notify creator (use HTML parse mode)
            try:
                msg = (
                    f"Исполнитель @{ex_username}\n <b>{safe_display}</b> принял ваш заказ "
                    f"<code>{safe_key}</code>.\n\n"
                    f"Подтвердите в течение {CONFIRMATION_TIMEOUT_SEC} сек."
                )
                bot.send_message(creator_tg, msg, reply_markup=kb, parse_mode="HTML")
            except Exception:
                logger.exception("notify creator failed")

        # inform executor who accepted
        bot.answer_callback_query(call.id, "✅ Вы приняли заказ. Ожидайте подтверждения заказчика.")
        try:
            bot.send_message(executor_tg, "Вы приняли заказ. Ожидайте подтверждения заказчика.")
        except Exception:
            logger.exception("notify executor after accept failed")

        # schedule confirmation timeout (will reveal contact or take action if not confirmed)
        schedule_accept_timeout(order_id)

    except Exception:
        logger.exception("callback_accept error")
        try:
            bot.answer_callback_query(call.id, "Ошибка при попытке принять заказ.")
        except Exception:
            pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        lk.release()


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("skip:"))
def callback_skip(call):
    bot.answer_callback_query(call.id, "Пропущено.")

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("customer_confirm:"))
def callback_customer_confirm(call):
    try:
        order_id = int(call.data.split(":",1)[1])
    except Exception:
        bot.answer_callback_query(call.id, "Неверный заказ.")
        return
    lk = get_order_lock(order_id)
    with lk:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status, accepted_by FROM orders WHERE id=?", (order_id,))
            row = cur.fetchone()
            if not row:
                bot.answer_callback_query(call.id, "Заказ не найден.")
                return
            if row["status"] != 'AWAITING_CUSTOMER_CONFIRM':
                bot.answer_callback_query(call.id, "Этот заказ нельзя подтвердить.")
                return
            cur.execute("UPDATE orders SET status='IN_PROGRESS', updated_at=CURRENT_TIMESTAMP WHERE id=?", (order_id,))
            conn.commit()
            bot.answer_callback_query(call.id, "Вы подтвердили исполнителя. Заказ в работе.")
            accepted_by = row["accepted_by"]
            if accepted_by:
                try:
                    kb = InlineKeyboardMarkup()
                    kb.add(InlineKeyboardButton("Я завершил", callback_data=f"complete:{order_id}"))
                    bot.send_message(accepted_by, f"Заказчик подтвердил. Можете приступать к работе. Когда закончите — нажмите 'Я завершил'.", reply_markup=kb)
                except Exception:
                    logger.exception("notify executor on confirm failed")
        finally:
            conn.close()

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("customer_cancel:"))
def callback_customer_cancel(call):
    try:
        order_id = int(call.data.split(":",1)[1])
    except Exception:
        bot.answer_callback_query(call.id, "Неверный заказ.")
        return
    lk = get_order_lock(order_id)
    with lk:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status, frozen_amount, creator_tg FROM orders WHERE id=?", (order_id,))
            row = cur.fetchone()
            if not row:
                bot.answer_callback_query(call.id, "Заказ не найден.")
                return
            if row["status"] != 'AWAITING_CUSTOMER_CONFIRM':
                bot.answer_callback_query(call.id, "Нельзя отменить этот заказ.")
                return
            frozen_amount = row["frozen_amount"] or 0
            creator_tg = row["creator_tg"]
            # return funds and reset
            cur.execute("BEGIN IMMEDIATE;")
            cur.execute("UPDATE users SET balance_coins = COALESCE(balance_coins,0) + ?, frozen_total_coins = COALESCE(frozen_total_coins,0) - ? WHERE tg_id=?", (frozen_amount, frozen_amount, creator_tg))
            cur.execute("UPDATE orders SET status='PUBLISHED', accepted_by=NULL, accept_ts=NULL, updated_at=CURRENT_TIMESTAMP WHERE id=?", (order_id,))
            conn.commit()
            bot.answer_callback_query(call.id, "Вы отменили подтверждение. Средства возвращены, заказ снова публикуется.")
        except Exception:
            conn.rollback()
            logger.exception("customer_cancel error")
        finally:
            conn.close()

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("complete:"))
def callback_complete(call):
    try:
        order_id = int(call.data.split(":",1)[1])
    except Exception:
        bot.answer_callback_query(call.id, "Неверный заказ.")
        return
    executor_tg = call.from_user.id
    lk = get_order_lock(order_id)
    with lk:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status, accepted_by, creator_tg FROM orders WHERE id=?", (order_id,))
            row = cur.fetchone()
            if not row:
                bot.answer_callback_query(call.id, "Заказ не найден.")
                return
            status = row["status"]
            accepted_by = row["accepted_by"]
            creator_tg = row["creator_tg"]
            if status != 'IN_PROGRESS' or accepted_by != executor_tg:
                bot.answer_callback_query(call.id, "Вы не можете завершить этот заказ.")
                return
            cur.execute("UPDATE orders SET status='AWAITING_CLIENT_APPROVAL', updated_at=CURRENT_TIMESTAMP WHERE id=?", (order_id,))
            conn.commit()
            order_row = get_order_by_id(order_id)
            order_key = order_row["order_key"] or str(order_id)
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Открыть спор", callback_data=f"open_dispute:{order_id}"))
            try:
                bot.send_message(executor_tg, f"Загрузите фото-отчёт (чек/результат) и подпишите его в подписи: order:{order_key}.", reply_markup=kb)
            except Exception:
                logger.exception("notify executor to upload photo failed")
            schedule_auto_release(order_id)
            bot.answer_callback_query(call.id, "Инструкция отправлена — загрузите фото-отчёт.")
        finally:
            conn.close()



@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("open_dispute:"))
def callback_open_dispute(call):
    try:
        order_id = int(call.data.split(":", 1)[1])
    except Exception:
        bot.answer_callback_query(call.id, "Неверный заказ.")
        return

    claimant = call.from_user.id
    conn = get_conn()
    try:
        cur = conn.cursor()
        # Получаем данные по заказу
        cur.execute("SELECT id, creator_tg, accepted_by FROM orders WHERE id=?", (order_id,))
        row = cur.fetchone()
        if not row:
            bot.answer_callback_query(call.id, "Заказ не найден.")
            return
        _, customer_tg, executor_tg = row

        # Проверяем, есть ли уже спор
        cur.execute("SELECT id FROM disputes WHERE order_id=?", (order_id,))
        if cur.fetchone():
            bot.answer_callback_query(call.id, "Спор уже открыт.")
            return

        # Создаём спор
        cur.execute(
            "INSERT INTO disputes (order_id, claimant_tg, reason) VALUES (?,?,?)",
            (order_id, claimant, "Opened via bot")
        )
        # Обновляем статус заказа
        cur.execute(
            "UPDATE orders SET status='DISPUTE', updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (order_id,)
        )
        conn.commit()

        # Сообщение и кнопки
        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton("👨‍💼 Связаться с админом", url="https://t.me/Komronbek_Urinboev")
        )
        kb.add(
            InlineKeyboardButton("📖 Часто задаваемые вопросы", url="https://telegra.ph/Primery-spora-i-kak-ih-reshit-09-14")
        )

        msg_text = (
            "⚖️ <b>Спор открыт</b>\n"
            "Администраторы уведомлены."
        )

        # Уведомляем заказчика
        if customer_tg:
            try:
                bot.send_message(customer_tg, msg_text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                logger.exception(f"Не удалось уведомить заказчика {customer_tg}")

        # Уведомляем исполнителя
        if executor_tg:
            try:
                bot.send_message(executor_tg, msg_text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                logger.exception(f"Не удалось уведомить исполнителя {executor_tg}")

        # Уведомляем админов
        for admin in ADMIN_IDS:
            try:
                bot.send_message(
                    admin,
                    f"⚠️ Новый спор по заказу #{order_id}\n"
                    f"Открыл: {claimant}\n"
                    f"Заказчик: {customer_tg}\n"
                    f"Исполнитель: {executor_tg}"
                )
            except Exception:
                logger.exception(f"Не удалось уведомить админа {admin}")

        bot.answer_callback_query(call.id, "Спор создан. Администраторы уведомлены.")

    except Exception:
        conn.rollback()
        logger.exception("open_dispute error")
        bot.answer_callback_query(call.id, "Ошибка открытия спора.")
    finally:
        conn.close()

# Helpers: работа с рейтингами / вывод пользователя
# ------------------------------------------------------------
def user_display_name(conn, tg_id: int) -> str:
    """Возвращает удобное отображение пользователя: ФИО или @username или tg_id"""
    cur = conn.cursor()
    cur.execute("SELECT full_name, username FROM users WHERE tg_id=?", (tg_id,))
    u = cur.fetchone()
    if not u:
        return str(tg_id)
    if u["full_name"]:
        return u["full_name"]
    if u["username"]:
        return "@" + u["username"]
    return str(tg_id)

def save_rating(order_id: int, from_tg: int, to_tg: int, stars: Optional[int]=None, comment: Optional[str]=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        # если уже есть запись для (order_id, from_tg) — обновим
        cur.execute("SELECT id FROM ratings WHERE order_id=? AND from_tg=? LIMIT 1", (order_id, from_tg))
        if cur.fetchone():
            # обновляем существующую запись (stars и/или comment)
            if stars is not None and comment is not None:
                cur.execute("UPDATE ratings SET stars=?, comment=?, created_at=CURRENT_TIMESTAMP WHERE order_id=? AND from_tg=?", (stars, comment, order_id, from_tg))
            elif stars is not None:
                cur.execute("UPDATE ratings SET stars=?, created_at=CURRENT_TIMESTAMP WHERE order_id=? AND from_tg=?", (stars, order_id, from_tg))
            elif comment is not None:
                cur.execute("UPDATE ratings SET comment=?, created_at=CURRENT_TIMESTAMP WHERE order_id=? AND from_tg=?", (comment, order_id, from_tg))
        else:
            cur.execute("INSERT INTO ratings (order_id, from_tg, to_tg, stars, comment) VALUES (?,?,?,?,?)", (order_id, from_tg, to_tg, stars, comment))
        conn.commit()
    except Exception:
        logger.exception("save_rating failed")
    finally:
        conn.close()

def compute_avg_rating(to_tg: int) -> Optional[float]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT AVG(stars) as avg_stars FROM ratings WHERE to_tg=? AND stars IS NOT NULL", (to_tg,))
        r = cur.fetchone()
        if r and r["avg_stars"] is not None:
            return float(r["avg_stars"])
        return None
    except Exception:
        logger.exception("compute_avg_rating failed")
        return None
    finally:
        conn.close()

def get_comments_for_user(to_tg: int, limit: int=50):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT r.order_id, r.from_tg, r.stars, r.comment, r.created_at, u.full_name, u.username
            FROM ratings r
            LEFT JOIN users u ON u.tg_id = r.from_tg
            WHERE r.to_tg=?
            ORDER BY r.created_at DESC
            LIMIT ?
        """, (to_tg, limit))
        return cur.fetchall()
    except Exception:
        logger.exception("get_comments_for_user failed")
        return []
    finally:
        conn.close()


# ------------------------------------------------------------
# Функция отправки запроса оценки одной стороне
# ------------------------------------------------------------
def send_rating_prompt(order_id: int, from_tg: int, to_tg: int):
    """
    Отправляет пользователю from_tg запрос оценить пользователя to_tg по заказу order_id.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        # check order exists
        cur.execute("SELECT order_key FROM orders WHERE id=?", (order_id,))
        orow = cur.fetchone()
        if not orow:
            return
        order_key = orow["order_key"] or str(order_id)

        # --- NEW: гарантируем, что у to_tg есть стартовый рейтинг 5 ⭐️ ---
        cur.execute("SELECT rating_sum, rating_count FROM users WHERE tg_id=?", (to_tg,))
        urow = cur.fetchone()
        if urow:
            rating_sum = urow["rating_sum"] or 0
            rating_count = urow["rating_count"] or 0
            if rating_count == 0:  # если ещё нет оценок
                cur.execute(
                    "UPDATE users SET rating_sum=5, rating_count=1 WHERE tg_id=?",
                    (to_tg,)
                )
                conn.commit()

        # имя того, кого оценивают
        to_name = user_display_name(conn, to_tg)

        # клавиатура: 1..5 звёзд + кнопка Комментарий
        kb = InlineKeyboardMarkup()
        # Изменил формат callback_data здесь: убрал to_tg из данных
        kb.row(
            InlineKeyboardButton("⭐️", callback_data=f"rate:{order_id}:1"),
            InlineKeyboardButton("⭐️⭐️", callback_data=f"rate:{order_id}:2"),
            InlineKeyboardButton("⭐️⭐️⭐️", callback_data=f"rate:{order_id}:3"),
            InlineKeyboardButton("⭐️⭐️⭐️⭐️", callback_data=f"rate:{order_id}:4"),
            InlineKeyboardButton("⭐️⭐️⭐️⭐️⭐️", callback_data=f"rate:{order_id}:5"),
        )
        kb.add(InlineKeyboardButton("Оставить комментарий", callback_data=f"comment:{order_id}"))

        text = (
            f"Пожалуйста, оцените <b>{html.escape(to_name)}</b> по заказу <code>{html.escape(order_key)}</code>\n\n"
            f"Нажмите звёзды — это займёт 1 клик. После — при желании оставьте комментарий."
        )
        try:
            bot.send_message(from_tg, text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            logger.exception("send_rating_prompt failed")
    finally:
        conn.close()


# ------------------------------------------------------------
# Callback: обработка нажатия на рейтинги
# data format: rate:<order_id>:<stars>
# ------------------------------------------------------------
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("rate:"))
def callback_rate(call):
    try:
        parts = call.data.split(":")
        if len(parts) != 3:
            bot.answer_callback_query(call.id, "Неверный формат.")
            return
        order_id = int(parts[1])
        stars = int(parts[2])
    except Exception:
        bot.answer_callback_query(call.id, "Неверный формат.")
        return

    rater = call.from_user.id
    # determine target (the other party) based on order
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT creator_tg, accepted_by FROM orders WHERE id=?", (order_id,))
        orow = cur.fetchone()
        if not orow:
            bot.answer_callback_query(call.id, "Заказ не найден.")
            return
        creator_tg = orow["creator_tg"]
        accepted_by = orow["accepted_by"]
        if rater == creator_tg:
            to_tg = accepted_by
        elif rater == accepted_by:
            to_tg = creator_tg
        else:
            bot.answer_callback_query(call.id, "Вы не участник этого заказа.")
            return
        if not to_tg:
            bot.answer_callback_query(call.id, "Пользователь ещё не назначен.")
            return

        # save rating
        save_rating(order_id, rater, to_tg, stars=stars, comment=None)
        bot.answer_callback_query(call.id, f"Вы поставили {stars} ⭐️. Спасибо!")

        # вычислим средний рейтинг и при необходимости уведомим администрацию
        avg = compute_avg_rating(to_tg)
        avg_text = f"{avg:.2f}" if avg is not None else "—"
        try:
            bot.send_message(to_tg, f"Вам поставили {stars} ⭐️ за заказ #{order_id}. Средний рейтинг: {avg_text}")
        except Exception:
            # если отправка провалилась, просто логируем
            logger.exception("notify rated user failed")

        # если рейтинг ниже порога, оповестим админов
        if stars < 3:
            admins_kb = InlineKeyboardMarkup()
            admins_kb.add(InlineKeyboardButton("Посмотреть комментарии", callback_data=f"admin_view_comments:{to_tg}"))
            for admin in ADMIN_IDS:
                try:
                    bot.send_message(admin, f"⚠️ Пользователь {to_tg} получил низкую оценку ({stars}⭐). Посмотреть: /comments {to_tg}", reply_markup=admins_kb)
                except Exception:
                    logger.exception("notify admin about low rating failed")
    finally:
        conn.close()


# ------------------------------------------------------------
# Callback: нажали "Оставить комментарий"
# data format: comment:<order_id>
# Затем ожидаем текстовое сообщение от пользователя (FSM)
# ------------------------------------------------------------
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("comment:"))
def callback_comment(call):
    try:
        order_id = int(call.data.split(":",1)[1])
    except Exception:
        bot.answer_callback_query(call.id, "Неверный заказ.")
        return

    rater = call.from_user.id
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT creator_tg, accepted_by FROM orders WHERE id=?", (order_id,))
        orow = cur.fetchone()
        if not orow:
            bot.answer_callback_query(call.id, "Заказ не найден.")
            return
        creator_tg = orow["creator_tg"]; accepted_by = orow["accepted_by"]
        if rater == creator_tg:
            to_tg = accepted_by
        elif rater == accepted_by:
            to_tg = creator_tg
        else:
            bot.answer_callback_query(call.id, "Вы не участник этого заказа.")
            return
        if not to_tg:
            bot.answer_callback_query(call.id, "Пользователь ещё не назначен.")
            return

        # Ставим FSM - ожидаем текст комментария
        user_states[rater] = "awaiting_comment"
        user_state_data[rater] = {"pending_comment": {"order_id": order_id, "to_tg": to_tg}}
        bot.answer_callback_query(call.id, "Напишите комментарий в ответном сообщении. Он будет прикреплён к оценке.")
        bot.send_message(rater, "Напишите, пожалуйста, ваш комментарий (коротко):")
    finally:
        conn.close()


# ------------------------------------------------------------
# Handler: ожидание текста комментария (FSM)
# ------------------------------------------------------------
@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "awaiting_comment", content_types=['text'])
def handle_comment_text(message):
    data = user_state_data.get(message.from_user.id, {}).get("pending_comment")
    if not data:
        bot.send_message(message.chat.id, "Нет ожидаемого комментария. Повторите.")
        user_states.pop(message.from_user.id, None)
        user_state_data.pop(message.from_user.id, None)
        return
    order_id = data["order_id"]
    to_tg = data["to_tg"]
    comment_text = (message.text or "").strip()
    if not comment_text:
        bot.send_message(message.chat.id, "Комментарий не может быть пустым.")
        return

    # Сохраняем комментарий (если рейтинг есть — обновляем, иначе создаём строку)
    rater = message.from_user.id
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM ratings WHERE order_id=? AND from_tg=? LIMIT 1", (order_id, rater))
        if cur.fetchone():
            cur.execute("UPDATE ratings SET comment=?, created_at=CURRENT_TIMESTAMP WHERE order_id=? AND from_tg=?", (comment_text, order_id, rater))
        else:
            cur.execute("INSERT INTO ratings (order_id, from_tg, to_tg, stars, comment) VALUES (?,?,?,?,?)", (order_id, rater, to_tg, None, comment_text))
        conn.commit()
        bot.send_message(message.chat.id, "Спасибо! Комментарий сохранён.")
    except Exception:
        conn.rollback()
        logger.exception("handle_comment_text failed")
        bot.send_message(message.chat.id, "Ошибка при сохранении комментария.")
    finally:
        conn.close()

    user_states.pop(message.from_user.id, None)
    user_state_data.pop(message.from_user.id, None)

# ------------------------------------------------------------
# Команда /comments [id] — показать комментарии пользователя (по умолчанию свои)
# ------------------------------------------------------------
@bot.message_handler(commands=['comments'])
def cmd_comments(message):
    parts = (message.text or "").strip().split()
    if len(parts) > 1:
        try:
            target = int(parts[1])
        except Exception:
            bot.reply_to(message, "Использование: /comments [tg_id]")
            return
    else:
        target = message.from_user.id

    rows = get_comments_for_user(target, limit=100)
    if not rows:
        bot.reply_to(message, "Комментариев не найдено.")
        return

    texts = []
    for r in rows:
        from_name = r["full_name"] or (("@"+r["username"]) if r["username"] else str(r["from_tg"]))
        stars = r["stars"] if r["stars"] is not None else "—"
        comment = r["comment"] or ""
        created = r["created_at"] or ""
        texts.append(f"От: <b>{html.escape(str(from_name))}</b> ({r['from_tg']})\nОценка: {stars}\n{html.escape(comment)}\n{created}\n---")

    # отправляем частями если слишком длинный
    full = "\n".join(texts)
    for chunk in split_long_message(full):
        bot.send_message(message.chat.id, chunk, parse_mode="HTML")


# ------------------------------------------------------------
# Callback: админ просматривает комментарии пользователя
# data: admin_view_comments:<tg_id>
# ------------------------------------------------------------
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("admin_view_comments:"))
def callback_admin_view_comments(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "Только админы.")
        return
    try:
        target = int(call.data.split(":",1)[1])
    except Exception:
        bot.answer_callback_query(call.id, "Неверный ID.")
        return
    rows = get_comments_for_user(target, limit=200)
    if not rows:
        bot.answer_callback_query(call.id, "Комментариев не найдено.")
        return
    texts = []
    for r in rows:
        from_name = r["full_name"] or (("@"+r["username"]) if r["username"] else str(r["from_tg"]))
        stars = r["stars"] if r["stars"] is not None else "—"
        comment = r["comment"] or ""
        created = r["created_at"] or ""
        texts.append(f"От: <b>{html.escape(str(from_name))}</b> ({r['from_tg']})\nОценка: {stars}\n{html.escape(comment)}\n{created}\n---")
    full = "\n".join(texts)
    for chunk in split_long_message(full):
        bot.send_message(call.from_user.id, chunk, parse_mode="HTML")
    bot.answer_callback_query(call.id, "Готово — комментарии отправлены в личку.")


# ------------------------------------------------------------
# Вспомогательные функции
# ------------------------------------------------------------
def split_long_message(s: str, limit: int = 4000):
    """Разбивает длинный текст на части для Telegram (простая реализация)."""
    parts = []
    while s:
        if len(s) <= limit:
            parts.append(s)
            break
        # пытаемся на последнем переносе строки
        idx = s.rfind("\n", 0, limit)
        if idx == -1:
            idx = limit
        parts.append(s[:idx])
        s = s[idx:].lstrip("\n")
    return parts


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("approve:"))
def callback_approve(call):
    raw = call.data.split(":", 1)[1]
    order_row = resolve_order_identifier(raw)
    if not order_row:
        bot.answer_callback_query(call.id, "Заказ не найден.")
        return
    order_id = order_row["id"]
    lk = get_order_lock(order_id)
    with lk:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status, frozen_amount, accepted_by, creator_tg FROM orders WHERE id=?", (order_id,))
            row = cur.fetchone()
            if not row:
                bot.answer_callback_query(call.id, "Заказ не найден.")
                return

            status = row["status"]
            frozen_amount = row["frozen_amount"] or 0
            accepted_by = row["accepted_by"]
            creator_tg = row["creator_tg"]

            if status != 'AWAITING_CLIENT_APPROVAL':
                bot.answer_callback_query(call.id, "Нельзя подтвердить этот заказ.")
                return
            if not frozen_amount or not accepted_by:
                bot.answer_callback_query(call.id, "Нет средств для перевода.")
                return

            # -------- транзакция: переводы и финализация заказа --------
            try:
                cur.execute("BEGIN IMMEDIATE;")
                cur.execute(
                    "UPDATE users SET balance_coins = COALESCE(balance_coins,0) + ? WHERE tg_id=?",
                    (frozen_amount, accepted_by)
                )
                cur.execute(
                    "UPDATE users SET frozen_total_coins = COALESCE(frozen_total_coins,0) - ? WHERE tg_id=?",
                    (frozen_amount, creator_tg)
                )
                cur.execute(
                    "UPDATE orders SET status='COMPLETED', frozen_amount=0, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (order_id,)
                )
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception("approve transfer failed for order %s", order_id)
                bot.answer_callback_query(call.id, "Ошибка при переводе.")
                return

            # ------------ уведомление на клиентский callback (быстро) ------------
            bot.answer_callback_query(call.id, "Вы подтвердили выполнение. Средства переведены.")

            # ------------ уведомления сторонам (не ломают транзакцию) ------------
            try:
                bot.send_message(accepted_by, f"🎉 Заказ #{order_id} подтверждён. Вам перечислено {frozen_amount} сум.")
            except Exception:
                logger.exception("notify accepted_by after approve failed for order %s", order_id)
            try:
                bot.send_message(creator_tg, f"Спасибо! Вы подтвердили выполнение заказа #{order_id}.")
            except Exception:
                logger.exception("notify creator after approve failed for order %s", order_id)

            # ------------ сброс локации исполнителя (после commit) ------------
            try:
                reset_user_location(accepted_by)
                try:
                    bot.send_message(accepted_by, "✅ Заказ подтверждён. Ваша локация сброшена. Чтобы снова получать заказы, отправьте вашу локацию 📍")
                except Exception:
                    logger.exception("notify executor about reset location failed for order %s", order_id)
            except Exception:
                logger.exception("reset_user_location failed for executor %s on order %s", accepted_by, order_id)

            # ------------ запуск промптов для оценок (если есть обе стороны) ------------
            try:
                if accepted_by and creator_tg:
                    # сначала заказчик оценивает исполнителя
                    send_rating_prompt(order_id, creator_tg, accepted_by)
                    # затем исполнитель оценивает заказчика
                    send_rating_prompt(order_id, accepted_by, creator_tg)
            except Exception:
                logger.exception("send_rating_prompt failed for order %s", order_id)

        finally:
            conn.close()

from html import escape

@bot.message_handler(commands=['order'])
def cmd_order(message):
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        bot.reply_to(message, "Использование: /order <ID>")
        return

    order_id = int(parts[1])
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT o.id, o.description, o.price_coins, o.status, o.lat, o.lon, o.order_key,
                   o.creator_tg, o.accepted_by, 
                   cu.full_name as creator_name, cu.age as creator_age, cu.username as creator_username,
                   eu.full_name as executor_name, eu.age as executor_age, eu.username as executor_username
            FROM orders o
            LEFT JOIN users cu ON o.creator_tg = cu.tg_id
            LEFT JOIN users eu ON o.accepted_by = eu.tg_id
            WHERE o.id = ?
        """, (order_id,))
        row = cur.fetchone()
        if not row:
            bot.reply_to(message, "Заказ не найден.")
            return

        text = (
            f"📋 <b>Заказ #{row['id']} ({escape(str(row['order_key']))})</b>\n\n"
            f"👤 <b>Работодатель:</b> {escape(row['creator_name'] or '—')} "
            f"({row['creator_age'] or '—'} лет)\n"
            f"@{escape(row['creator_username'] or '—')}\n"
            f"🆔 tg_id: <code>{row['creator_tg']}</code>\n\n"
            f"🛠 <b>Фрилансер:</b> {escape(row['executor_name'] or '—')} "
            f"({row['executor_age'] or '—'} лет)\n"
            f"@{escape(row['executor_username'] or '—')}\n"
            f"🆔 tg_id: <code>{row['accepted_by'] or '—'}</code>\n\n"
            f"📄 <b>Описание:</b> {escape(row['description'] or '')}\n"
            f"💰 <b>Цена:</b> {row['price_coins']} сум\n"
            f"📍 <b>Локация:</b> {row['lat']},{row['lon']}\n"
            f"⚙️ <b>Статус:</b> {row['status']}"
        )

        bot.reply_to(message, text, parse_mode="HTML")

    except Exception:
        logger.exception("cmd_order error")
        bot.reply_to(message, "Ошибка при получении информации.")
    finally:
        conn.close()


# ---------------- Photo handler ----------------

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    caption = (message.caption or "").strip()
    if not caption:
        bot.send_message(message.chat.id, "Для фото-отчёта укажите ID заказа (например: order:abc123 или abc123 или просто 1).")
        return

    # Accept formats: "order:KEY", "KEY", or numeric id
    text = caption.strip()
    if text.lower().startswith("order:"):
        candidate = text.split(":",1)[1].strip()
    else:
        candidate = text.strip()

    order_row = resolve_order_identifier(candidate)
    if not order_row:
        bot.send_message(message.chat.id, "❌ Заказ не найден. Подпишите фото: order:<ключ> или просто <ключ> или <id>.")
        return

    order_id = order_row["id"]
    order_key = order_row["order_key"] or str(order_id)

    ensure_dir(UPLOADS_DIR)
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        ext = os.path.splitext(file_info.file_path)[1].lower() or ".jpg"
        local_name = os.path.join(UPLOADS_DIR, f"order_{order_id}_{int(time.time())}{ext}")
        downloaded = bot.download_file(file_info.file_path)
        with open(local_name, "wb") as f:
            f.write(downloaded)
    except Exception:
        logger.exception("download file failed")
        bot.send_message(message.chat.id, "Ошибка загрузки файла.")
        return

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO media (order_id, executor_tg, file_path) VALUES (?,?,?)", (order_id, message.from_user.id, local_name))
        conn.commit()
        cur.execute("SELECT creator_tg FROM orders WHERE id=?", (order_id,))
        row = cur.fetchone()
        if row:
            creator_tg = row["creator_tg"]
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("✅ Подтвердить", callback_data=f"approve:{order_key}"),
                   InlineKeyboardButton("⚠️ Открыть спор", callback_data=f"open_dispute:{order_id}"))
            try:
                bot.send_photo(creator_tg, message.photo[-1].file_id, caption=f"📷 Фото-отчёт по заказу `{order_key}`", reply_markup=kb, parse_mode="Markdown")
            except Exception:
                logger.exception("send photo to creator failed")
        bot.send_message(message.chat.id, f"✅ Фото принято и отправлено заказчику (заказ `{order_key}`).", parse_mode="Markdown")
    except Exception:
        conn.rollback()
        logger.exception("handle_photo db error")
        bot.send_message(message.chat.id, "Ошибка сохранения фото.")
    finally:
        conn.close()

# ---------------- Simple commands & flows ----------------

user_states = {}      # ephemeral FSM
user_state_data = {}

# ---------------- Registration flow ----------------

@bot.message_handler(commands=['start'])
def cmd_start(message):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE tg_id=?", (message.from_user.id,))
        user = cur.fetchone()
    finally:
        conn.close()

    if user:
        # если юзер есть, просто показываем меню
        send_main_menu(message.chat.id, message.from_user.id)
    else:
        # запускаем регистрацию
        user_states[message.from_user.id] = "register_name"
        user_state_data[message.from_user.id] = {}
        bot.send_message(message.chat.id, "Добро пожаловать! Давайте зарегистрируемся.\n\nВведите ваше ФИО:")


@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "register_name")
def handle_register_name(message):
    fio = (message.text or "").strip()
    if not fio:
        bot.send_message(message.chat.id, "ФИО не может быть пустым. Введите снова:")
        return
    user_state_data[message.from_user.id]["full_name"] = fio
    user_states[message.from_user.id] = "register_age"
    bot.send_message(message.chat.id, "Введите ваш возраст (число):")


@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "register_age")
def handle_register_age(message):
    try:
        age = int(message.text.strip())
        if age <= 0 or age > 120:
            bot.send_message(message.chat.id, "Возраст указан некорректно. Попробуйте снова:")
            return

        user_state_data[message.from_user.id]["age"] = age
        user_states[message.from_user.id] = "register_phone"

        kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        kb.add(KeyboardButton("📞 Отправить телефон", request_contact=True))
        bot.send_message(
            message.chat.id,
            "Отправьте ваш номер телефона кнопкой ниже 👇\nВнимание! Бот будет игнорировать номера телефонов написанных вручную",
            reply_markup=kb
        )
    except ValueError:
        bot.send_message(message.chat.id, "Возраст должен быть числом.")


@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "register_phone", content_types=['contact'])
def handle_register_phone(message):
    if not message.contact or message.contact.user_id != message.from_user.id:
        bot.send_message(message.chat.id, "Пожалуйста, используйте кнопку для отправки вашего номера 📞")
        return

    phone = message.contact.phone_number
    data = user_state_data.get(message.from_user.id, {})

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (tg_id, username, full_name, age, phone, created_at, updated_at, balance_coins)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0)
        """, (
            message.from_user.id,
            message.from_user.username,
            data.get("full_name"),
            data.get("age"),
            phone
        ))
        conn.commit()
    except Exception:
        conn.rollback()
        bot.send_message(message.chat.id, "Ошибка регистрации. Попробуйте ещё раз.")
        return
    finally:
        conn.close()

    # убираем клавиатуру и показываем меню
    remove_kb = ReplyKeyboardRemove()
    bot.send_message(message.chat.id, "Регистрация завершена ✅", reply_markup=remove_kb)
    send_main_menu(message.chat.id, message.from_user.id)  # твоя функция для показа меню


# ---------------- Helper: главное меню ----------------

def send_main_menu(chat_id, tg_id):
    is_admin = tg_id in ADMIN_IDS
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("➕ Новый заказ"))
    kb.add(KeyboardButton("📦 Мои заказы"), KeyboardButton("💰 Баланс"))
    kb.add(KeyboardButton("🔄 Войти в онлайн / Выйти"), KeyboardButton("📞 Указать номер"))
    if is_admin:
        kb.add(KeyboardButton("🛠️ Панель администратора"))
    bot.send_message(chat_id, "Главное меню:", reply_markup=kb)

@bot.message_handler(content_types=['contact'])
def handle_contact(message):
    contact = message.contact
    if contact and contact.user_id == message.from_user.id:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("UPDATE users SET phone=?, updated_at=CURRENT_TIMESTAMP WHERE tg_id=?", (contact.phone_number, message.from_user.id))
            conn.commit()
            bot.send_message(message.chat.id, "Спасибо, номер сохранён.")
        except Exception:
            conn.rollback()
            logger.exception("handle_contact error")
            bot.send_message(message.chat.id, "Ошибка сохранения номера.")
        finally:
            conn.close()
    else:
        bot.send_message(message.chat.id, "Пожалуйста, пришлите ваш контакт (через кнопку).")

@bot.message_handler(func=lambda m: m.text == "🔄 Войти в онлайн / Выйти")
def handle_online_toggle(message):
    user = get_user(message.from_user.id)
    if user and user.get('status') == 'available':
        set_user_offline(message.from_user.id)
        bot.send_message(message.chat.id, "Вы вышли из сети.")
    else:
        kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        kb.add(KeyboardButton("Отправить локацию", request_location=True))
        bot.send_message(message.chat.id, "Отправьте вашу текущую локацию, чтобы войти в онлайн.", reply_markup=kb)

@bot.message_handler(content_types=['location'])
def handle_location(message):
    # if creating order, treat specially
    if user_states.get(message.from_user.id) == "creating_order_type":
        return handle_new_order_type(message)
    loc = message.location
    if not loc:
        bot.send_message(message.chat.id, "Локация не получена.")
        return
    ensure_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    set_user_location(message.from_user.id, loc.latitude, loc.longitude, status='available')
    bot.send_message(message.chat.id, "Вы в сети. Локация сохранена.")

@bot.message_handler(func=lambda m: m.text == "💰 Баланс")
def handle_balance(message):
    u = get_user(message.from_user.id)
    if not u:
        bot.send_message(message.chat.id, "Пользователь не найден.")
        return
    bal = u.get('balance_coins') or 0
    frozen = u.get('frozen_total_coins') or 0
    bot.send_message(message.chat.id, f"Баланс: {bal} сум\nЗаморожено: {frozen} сум")

@bot.message_handler(func=lambda m: m.text == "📦 Мои заказы")
def handle_my_orders(message):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, order_key, status, description, price_coins FROM orders WHERE creator_tg=? ORDER BY created_at DESC LIMIT 20", (message.from_user.id,))
        rows = cur.fetchall()
        if not rows:
            bot.send_message(message.chat.id, "У вас нет заказов.")
            return
        for r in rows:
            key = r["order_key"] or str(r["id"])
            bot.send_message(message.chat.id, f"#{r['id']} | {r['status']}\n{r['description']}\n{r['price_coins']} сум\nID: `{key}`", parse_mode="Markdown")
    except Exception:
        logger.exception("handle_my_orders error")
        bot.send_message(message.chat.id, "Ошибка получения заказов.")
    finally:
        conn.close()

# ---- BEGIN: расширенный New order flow с Gemini и валидацией веса ----
import os
import re

# Конфиги
MAX_ORDER_KG = float(os.getenv("MAX_ORDER_KG", "16.0"))        # предел в кг (по умолчанию чуть >15kg)
MAX_ORDER_LITERS = float(os.getenv("MAX_ORDER_LITERS", "20.0"))  # предел для литров (вес воды ~= литры)
# Берём ключ из env; при желании можно временно подставить ключ как второй аргумент

# Попытка подключить библиотеку google.generativeai (интерфейс genai.configure)
GENAI_READY = False
try:
    import google.generativeai as genai
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        GENAI_READY = True
    except Exception:
        GENAI_READY = False
        try:
            logger.warning("genai.configure failed — Gemini disabled")
        except NameError:
            pass
except Exception:
    genai = None
    GENAI_READY = False
    try:
        logger.warning("google.generativeai not installed — Gemini disabled")
    except NameError:
        pass

# Вспомогательные функции
PURCHASE_KEYWORDS = [
    "купит", "купить", "покупа", "покупк", "заказ", "приобр", "хочу купить", "куплю", "приобрести"
]

def looks_like_purchase(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    for k in PURCHASE_KEYWORDS:
        if k in t:
            return True
    return False

def parse_number_from_text(text: str):
    """Возвращает float или None. Ищет первое вхождение числа (разрешает запятую и пробелы как разделители тысяч)."""
    if not text:
        return None
    m = re.search(r"(\d+[ \d]*[.,]?\d*)", text.replace("\u202f", " "))
    if not m:
        return None
    raw = m.group(1).replace(" ", "").replace(",", ".")
    try:
        # если целое — вернём int, иначе float
        if "." in raw:
            return float(raw)
        else:
            return int(raw)
    except Exception:
        try:
            return float(raw)
        except Exception:
            return None

def estimate_weight_with_gemini(text: str):
    """
    Попытка получить оценку веса от Gemini.
    Использует API через google.generativeai как:
        genai.configure(api_key=...)
        model = genai.GenerativeModel("gemini-2.0-flash")
        response = model.generate_content(prompt)
        ответ берём из response.text или str(response)
    Возвращает float (кг) или None (если не удалось).
    """
    if not GENAI_READY:
        return None

    # Строгий, но дружелюбный промпт — просим модель вернуть только число или 'UNKNOWN'
    prompt = (
        "Вы — маленький извлекатель. По короткому описанию задачи покупки/доставки "
        "выпишите ТОЛЬКО оценочный вес в килограммах как число (может быть десятичное). "
        "Ни в коем случае не добавляйте слова, единицы измерения или пояснения. "
        "Если оценить нельзя — верните 'UNKNOWN'.\n\n"
        f"Описание: {text}\n\nВывод:"
    )

    try:
        # создаём модель и генерируем
        model = genai.GenerativeModel("gemini-2.0-flash")
        response = model.generate_content(prompt)

        # Попробуем достать текст из разных возможных полей
        resp_text = ""
        if hasattr(response, "text") and response.text is not None:
            resp_text = response.text
        else:
            # иногда объект возвращается как dict-like или с .candidates
            try:
                resp_text = str(response)
            except Exception:
                resp_text = ""

        resp_text = (resp_text or "").strip()
        if not resp_text:
            return None

        # Если модель честно ответила UNKNOWN — считаем неудачей
        if re.search(r"\bUNKNOWN\b", resp_text, re.IGNORECASE):
            return None

        # Ищем первое число в ответе (учитываем запятую как десятичный разделитель)
        m = re.search(r"(\d+[.,]?\d*)", resp_text)
        if not m:
            return None
        val = float(m.group(1).replace(",", "."))
        return val
    except Exception as e:
        try:
            logger.exception("Gemini weight estimation failed")
        except NameError:
            pass
        return None

# --- FSM handlers (заменяют / расширяют ваш оригинал) ---

# Минимальная цена для обычного задания
MIN_TASK_PRICE = 4000

# --- Вспомогательные функции ---
def looks_like_delivery(text: str) -> bool:
    """Эвристика для определения доставки."""
    if not text:
        return False
    t = text.lower()
    keywords = ["достав", "привез", "принес", "курьер", "подвез", "доставка", "доставить", "купить", "магазин"]
    return any(k in t for k in keywords)

def ask_price_message(uid, chat_id):
    """
    Отправляет пользователю подходящее сообщение с просьбой ввести цену:
    - если покупка или доставка -> "цена с учётом вознаграждения/доставки"
    - иначе -> "цена за задание (минимум MIN_TASK_PRICE)"
    """
    desc = (user_state_data.get(uid, {}).get('description') or "")
    checklist = (user_state_data.get(uid, {}).get('checklist') or "")
    combined = (desc + " " + checklist).strip()

    if looks_like_purchase(combined) or looks_like_delivery(combined):
        msg = "Укажите цену с учётом вознаграждения/доставки (числом, например: 150000):"
    else:
        msg = f"Укажите цену за задание (минимум {MIN_TASK_PRICE} сум):"

    bot.send_message(chat_id, msg)

def parse_number_from_text(text: str):
    """Возвращает int/float или None. Ищет первое вхождение числа."""
    if not text:
        return None
    m = re.search(r"(\d+[ \d]*[.,]?\d*)", text.replace("\u202f", " "))
    if not m:
        return None
    raw = m.group(1).replace(" ", "").replace(",", ".")
    try:
        if "." in raw:
            return float(raw)
        else:
            return int(raw)
    except Exception:
        try:
            return float(raw)
        except Exception:
            return None

# --- FSM: создание заказа ---

# Начало создания заказа — остаётся прежним
@bot.message_handler(func=lambda m: m.text == "➕ Новый заказ")
def handle_new_order_start(message):
    user_states[message.from_user.id] = "creating_order_desc"
    user_state_data[message.from_user.id] = {}
    bot.send_message(message.chat.id, "Опишите задачу (коротко):")


# Описание заказа
@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "creating_order_desc")
def handle_new_order_desc(message):
    text = (message.text or "").strip()
    if not text:
        bot.send_message(message.chat.id, "Описание не может быть пустым.")
        return

    uid = message.from_user.id
    user_state_data.setdefault(uid, {})
    user_state_data[uid]['description'] = text
    user_state_data[uid]['created_at'] = now_iso()

    # Если похоже на покупку — запрашиваем чек-лист
    if looks_like_purchase(text):
        user_states[uid] = "creating_order_checklist"
        bot.send_message(message.chat.id,
            "Похоже, это покупка. Напишите краткий чек-лист: магазин/бренд, количество/упаковка и (если есть) ориентировочная цена в магазине.\n\n"
            "Пример: «Oriental Mart, 2 шт, ~12000 сум за 1» или напишите 'не знаю'."
        )
        return

    # Иначе пробуем автоматически оценить вес
    est = estimate_weight_with_gemini(text)
    if est is not None:
        user_state_data[uid]['estimated_kg'] = float(est)
        is_liters = bool(re.search(r"\b(литр|л\b|л\.)", text.lower()))
        limit = MAX_ORDER_LITERS if is_liters else MAX_ORDER_KG
        if est > limit:
            bot.send_message(message.chat.id,
                             f"⚠️ Оценочный вес — {est:.2f} кг, что превышает допустимый лимит ({limit} кг). "
                             "К сожалению, бот не может принять такой заказ. Попробуйте уменьшить объём или разбить заказ.")
            user_states.pop(uid, None)
            user_state_data.pop(uid, None)
            return
        # переходим к запросу цены — используем умное формирование текста
        user_states[uid] = "creating_order_price"
        ask_price_message(uid, message.chat.id)
        return
    else:
        # не удалось: спросим вес вручную
        user_states[uid] = "creating_order_weight"
        bot.send_message(message.chat.id,
                         "Не удалось автоматически определить массу. Укажите, пожалуйста, вес в килограммах (пример: 2.5) или напишите 'не знаю'.")


# Если заказ — покупка: обработка чек-листа
@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "creating_order_checklist")
def handle_new_order_checklist(message):
    uid = message.from_user.id
    checklist = (message.text or "").strip()
    if not checklist:
        bot.send_message(message.chat.id, "Пожалуйста, дайте краткий чек-лист или напишите 'не знаю'.")
        return
    user_state_data.setdefault(uid, {})
    user_state_data[uid]['checklist'] = checklist

    # Комбинируем описание + чеклист и пробуем оценить вес
    combined = user_state_data[uid].get('description', '') + " " + checklist
    est = estimate_weight_with_gemini(combined)
    if est is not None:
        user_state_data[uid]['estimated_kg'] = float(est)
        is_liters = bool(re.search(r"\b(литр|л\b|л\.)", (combined).lower()))
        limit = MAX_ORDER_LITERS if is_liters else MAX_ORDER_KG
        if est > limit:
            bot.send_message(message.chat.id,
                             f"⚠️ Оценочный вес — {est:.2f} кг, что превышает допустимый лимит ({limit} кг). "
                             "К сожалению, бот не может принять такой заказ. Попробуйте уменьшить объём или разбить заказ.")
            user_states.pop(uid, None)
            user_state_data.pop(uid, None)
            return
        # спрашиваем цену — умно
        user_states[uid] = "creating_order_price"
        ask_price_message(uid, message.chat.id)
        return
    else:
        # попросим вес вручную
        user_states[uid] = "creating_order_weight"
        bot.send_message(message.chat.id,
                         "Не удалось автоматически определить вес. Укажите, пожалуйста, вес в килограммах (пример: 2.5) или напишите 'не знаю'.")




# Ручной ввод веса (если Gemini не помог)
@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "creating_order_weight")
def handle_new_order_weight(message):
    uid = message.from_user.id
    txt = (message.text or "").strip().lower()
    pending = user_state_data.get(uid)
    if not pending:
        bot.send_message(message.chat.id, "Сессия заказа не найдена. Запустите /order заново.")
        user_states.pop(uid, None)
        return

    if txt in ("не знаю", "неизвестно"):
        pending['estimated_kg'] = None
    else:
        val = parse_number_from_text(txt)
        if val is None:
            bot.send_message(message.chat.id, "Не понял количество. Введите, пожалуйста, число (в кг) или 'не знаю'.")
            return
        pending['estimated_kg'] = float(val)
        # проверка лимита
        combined_text = pending.get('description', '') + " " + (pending.get('checklist') or '')
        is_liters = bool(re.search(r"\b(литр|л\b|л\.)", combined_text.lower()))
        limit = MAX_ORDER_LITERS if is_liters else MAX_ORDER_KG
        if val > limit:
            bot.send_message(message.chat.id,
                             f"⚠️ Указанный вес — {val:.2f} кг, превышает лимит ({limit} кг). "
                             "К сожалению, бот не может принять такой заказ.")
            user_states.pop(uid, None)
            user_state_data.pop(uid, None)
            return

    # Просим цену — умно
    user_states[uid] = "creating_order_price"
    ask_price_message(uid, message.chat.id)


# Цена: обработчик ввода цены
@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "creating_order_price")
def handle_new_order_price(message):
    uid = message.from_user.id
    if uid not in user_state_data:
        bot.send_message(message.chat.id, "Сессия заказа не найдена. Запустите /order заново.")
        user_states.pop(uid, None)
        return

    text = (message.text or "").strip()
    price_num = parse_number_from_text(text)
    if price_num is None:
        bot.send_message(message.chat.id, "Пожалуйста, введите корректную числовую сумму (например: 150000).")
        return

    # Запрашиваем целое число сум — если пришёл float с дробной частью, просим ввести целое
    if isinstance(price_num, float) and not price_num.is_integer():
        bot.send_message(message.chat.id, "Пожалуйста, укажите сумму целым числом (без копеек), например: 150000.")
        return

    price_val = int(price_num)

    # Проверяем минимальную цену для обычного задания
    desc = (user_state_data.get(uid, {}).get('description') or "")
    checklist = (user_state_data.get(uid, {}).get('checklist') or "")
    combined = (desc + " " + checklist).strip()
    is_purchase_or_delivery = looks_like_purchase(combined) or looks_like_delivery(combined)

    if (not is_purchase_or_delivery) and price_val < MIN_TASK_PRICE:
        bot.send_message(message.chat.id, f"Минимальная цена для обычных заданий — {MIN_TASK_PRICE} сум. Пожалуйста, укажите сумму, не менее {MIN_TASK_PRICE}.")
        return

    # Сохраняем цену и переходим к выбору типа (онлайн/локация)
    user_state_data[uid]['price'] = price_val
    user_states[uid] = "creating_order_type"

    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Онлайн (без локации)"))
    kb.add(KeyboardButton("Оффлайн: отправить локацию", request_location=True))
    bot.send_message(message.chat.id, "Если задача оффлайн — отправьте локацию, иначе нажмите 'Онлайн'.", reply_markup=kb)

# Тип заказа (онлайн/локация) -> превью -> inline confirm/cancel
@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "creating_order_type", content_types=['text', 'location'])
def handle_new_order_type(message):
    uid = message.from_user.id
    data = user_state_data.get(uid, {})
    if not data:
        bot.send_message(message.chat.id, "Сессия заказа не найдена. Запустите /order заново.")
        user_states.pop(uid, None)
        return

    # Скрываем клавиатуру, чтобы не висел reply keyboard
    try:
        bot.send_message(message.chat.id, "Хорошо, формирую превью...", reply_markup=ReplyKeyboardRemove())
    except Exception:
        pass

    if message.content_type == 'location':
        lat, lon = message.location.latitude, message.location.longitude
        data['lat'] = lat; data['lon'] = lon
    else:
        text = (message.text or "").strip().lower()
        if text.startswith("онлайн"):
            data['lat'] = None; data['lon'] = None
        else:
            bot.send_message(message.chat.id, "Непонятный ввод. Отправьте локацию или нажмите 'Онлайн'.")
            return

    desc = data.get('description'); price = data.get('price'); lat = data.get('lat'); lon = data.get('lon')
    est = data.get('estimated_kg')
    est_text = f"Оценочный вес: {est:.2f} кг\n" if est is not None else ""
    preview = f"📌 Предпросмотр заказа\n\nОписание: {desc}\nЦена: {price} сум\n{est_text}"
    if lat is not None and lon is not None:
        preview += f"Адрес: lat={lat}, lon={lon}\n"
    else:
        preview += "Адрес: Онлайн\n"

    tmp_key = f"tmp_order_{uid}"
    user_state_data[tmp_key] = {
        "description": desc,
        "price": price,
        "lat": lat,
        "lon": lon,
        "estimated_kg": est,
        "checklist": data.get('checklist'),
        "created_at": data.get('created_at', now_iso())
    }

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_create:{uid}"),
        InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_create:{uid}")
    )
    bot.send_message(message.chat.id, preview, reply_markup=kb)

    # Завершаем временное основное состояние
    user_states.pop(uid, None)
    # Удаляем основную временную запись (не tmp) — оставляем tmp_order_... до подтверждения
    user_state_data.pop(uid, None)

# Callback: подтверждение создания заказа
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("cancel_create:"))
def callback_cancel_create(call):
    try:
        creator_id = int(call.data.split(":", 1)[1])
    except Exception:
        bot.answer_callback_query(call.id, "Некорректный вызов.")
        return

    tmp_key = f"tmp_order_{creator_id}"
    # только автор может отменить
    if call.from_user.id != creator_id:
        bot.answer_callback_query(call.id, "Только автор может отменить заказ.")
        return

    user_state_data.pop(tmp_key, None)
    bot.answer_callback_query(call.id, "Создание заказа отменено.")
    try:
        bot.send_message(creator_id, "Создание заказа отменено.")
    except Exception:
        pass

# ---- END: расширенный New order flow ----

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("confirm_create:"))
def callback_confirm_create(call):
    try:
        creator_tg = int(call.data.split(":",1)[1])
    except Exception:
        bot.answer_callback_query(call.id, "Неверный callback.")
        return
    tmp_key = f"tmp_order_{creator_tg}"
    data = user_state_data.pop(tmp_key, None)
    if not data:
        bot.answer_callback_query(call.id, "Данные заказа истекли. Повторите.")
        return
    description = data.get('description'); price = data.get('price'); lat = data.get('lat'); lon = data.get('lon')
    order_id, order_key, err = create_order_and_reserve(creator_tg, description, price, lat, lon, requires_photo=False)
    if not order_id:
        if err == 'insufficient_funds':
            bot.send_message(creator_tg, "Недостаточно средств. Пополните баланс.")
        else:
            bot.send_message(creator_tg, "Ошибка создания заказа.")
        bot.answer_callback_query(call.id, "Не удалось создать заказ.")
        return
    bot.answer_callback_query(call.id, f"Заказ #{order_id} создан и средства заморожены. ID Заказа: `{order_key}`")
    # notify executors asynchronously
    threading.Thread(target=notify_executors_of_order, args=(order_id,), daemon=True).start()

import html
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# Вспомогательные функции для безопасной работы с sqlite3.Row и профилями
def _row_val(row, name, default=None):
    """Безопасный доступ к sqlite3.Row: возвращает default если колонки нет или значение is None."""
    try:
        if name in row.keys():
            return row[name]
    except Exception:
        pass
    return default

def _normalize_coord(v):
    """
    Преобразует значение из БД в float координаты или None.
    Поддерживает int/float/str. Всё остальное (включая '', 'None', 'null', '0') -> None.
    (0 координату считаем невалидной — часто это placeholder в БД)
    """
    if v is None:
        return None
    # числовой тип
    if isinstance(v, (int, float)):
        try:
            f = float(v)
        except Exception:
            return None
        # 0.0 трактуем как невалидную координату
        return f if abs(f) > 1e-9 else None
    # строка
    try:
        s = str(v).strip()
        if s == "" or s.lower() in ("none", "null"):
            return None
        f = float(s)
        return f if abs(f) > 1e-9 else None
    except Exception:
        return None

def _has_coords(lat, lon):
    """True если обе координаты валидны (float и не нулевые)."""
    return (lat is not None) and (lon is not None)

def _safe_profile_val(obj, key):
    """
    Получить значение профиля безопасно.
    obj может быть dict (get_user возвращает dict), или sqlite3.Row, или None.
    """
    if not obj:
        return None
    try:
        # dict-like
        if isinstance(obj, dict):
            return obj.get(key)
        # sqlite3.Row supports keys()
        try:
            if key in obj.keys():
                return obj[key]
        except Exception:
            pass
        # fallback getattr
        return getattr(obj, key, None)
    except Exception:
        return None




import html
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- Вспомогательные функции (копировать вместе с обработчиком) ---

def _row_val(row, name, default=None):
    """
    Безопасный доступ к sqlite3.Row: возвращает default если колонки нет.
    Используйте для чтения строк из cur.fetchall() (sqlite3.Row не имеет .get()).
    """
    try:
        if name in row.keys():
            return row[name]
    except Exception:
        pass
    return default

def _normalize_coord(v):
    """
    Преобразует значение из БД в float координаты или None.
    Поддерживает int/float/str. Всё остальное (включая '', 'None', 'null', '0', 0) -> None.
    (0 считается невалидным placeholder-значением; если у вас реальные 0 координаты — уберите это правило)
    """
    if v is None:
        return None
    # Если уже число
    if isinstance(v, (int, float)):
        try:
            f = float(v)
        except Exception:
            return None
        # Фильтруем нулевые placeholder-значения
        if abs(f) < 1e-9:
            return None
        return f
    # Строки
    try:
        s = str(v).strip()
        if s == "" or s.lower() in ("none", "null"):
            return None
        f = float(s)
        if abs(f) < 1e-9:
            return None
        return f
    except Exception:
        return None

def _has_coords(lat, lon):
    """True если обе координаты валидны (float и не нулевые)."""
    return (lat is not None) and (lon is not None)

def _safe_profile_val(obj, key):
    """
    Получить значение профиля безопасно.
    obj может быть dict (get_user возвращает dict) или sqlite3.Row или None.
    """
    if not obj:
        return None
    try:
        if isinstance(obj, dict):
            return obj.get(key)
        # sqlite3.Row: поддерживает keys()
        try:
            if key in obj.keys():
                return obj[key]
        except Exception:
            pass
        # fallback getattr
        return getattr(obj, key, None)
    except Exception:
        return None


# --- Исправленный обработчик /jobs ---
@bot.message_handler(commands=['jobs'])
def list_jobs(message):
    # Получаем профиль исполнителя из БД и нормализуем его координаты
    user_profile = get_user(message.from_user.id)
    user_lat_raw = _safe_profile_val(user_profile, "lat")
    user_lon_raw = _safe_profile_val(user_profile, "lon")
    user_lat = _normalize_coord(user_lat_raw)
    user_lon = _normalize_coord(user_lon_raw)

    # Логируем координаты исполнителя (DEBUG) — это поможет в диагностике
    try:
        logger.debug("list_jobs: executor=%s raw=(%r,%r) norm=(%r,%r)",
                     message.from_user.id, user_lat_raw, user_lon_raw, user_lat, user_lon)
    except Exception:
        pass

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, order_key, description, price_coins, lat, lon, radius_km, creator_tg, created_at "
            "FROM orders WHERE status='PUBLISHED' ORDER BY created_at DESC LIMIT 100"
        )
        rows = cur.fetchall()

        found = 0
        for r in rows:
            # безопасный доступ к полям sqlite3.Row
            order_id = _row_val(r, "id")
            order_key = _row_val(r, "order_key") or str(order_id)
            desc = _row_val(r, "description") or ""
            price = _row_val(r, "price_coins") or 0
            raw_order_lat = _row_val(r, "lat")
            raw_order_lon = _row_val(r, "lon")
            radius_km = _row_val(r, "radius_km")
            creator_tg = _row_val(r, "creator_tg")
            created_at = _row_val(r, "created_at")

            # нормализуем координаты заказа
            order_lat = _normalize_coord(raw_order_lat)
            order_lon = _normalize_coord(raw_order_lon)

            # DEBUG лог: покажем, что реально лежит в БД и что получилось после нормализации
            try:
                logger.debug("list_jobs: order=%s raw_lat=%r raw_lon=%r -> norm=(%r,%r)",
                             order_key, raw_order_lat, raw_order_lon, order_lat, order_lon)
            except Exception:
                pass

            # вычисление расстояния и фильтр по радиусу (если у обоих есть корректные координаты)
            include = True
            dist_text = None
            try:
                if _has_coords(order_lat, order_lon) and _has_coords(user_lat, user_lon):
                    d = haversine_km(float(user_lat), float(user_lon), float(order_lat), float(order_lon))
                    radius = float(radius_km) if (radius_km is not None) else INITIAL_RADIUS_KM
                    if d > radius:
                        include = False
                    else:
                        dist_text = f"{d:.2f} км"
            except Exception:
                # при ошибке расчёта расстояния — не фильтруем, но не показываем расстояние
                dist_text = None

            if not include:
                continue

            found += 1

            # Получаем данные заказчика (безопасно)
            customer = None
            try:
                if creator_tg is not None:
                    customer = get_user(creator_tg)
            except Exception:
                customer = None

            full_name = _safe_profile_val(customer, "full_name")
            username = _safe_profile_val(customer, "username")
            phone = _safe_profile_val(customer, "phone")

            # Попытаемся получить estimated_kg: сначала из row (если есть)
            estimated = None
            try:
                if "estimated_kg" in r.keys():
                    estimated = r["estimated_kg"]
                else:
                    # fallback: отдельный селект (на случай, если столбца в схеме нет)
                    cur2 = conn.cursor()
                    cur2.execute("SELECT estimated_kg FROM orders WHERE id=?", (order_id,))
                    row2 = cur2.fetchone()
                    if row2:
                        try:
                            estimated = row2["estimated_kg"]
                        except Exception:
                            estimated = row2[0] if len(row2) > 0 else None
            except Exception:
                estimated = None

            # Формируем безопасный HTML-текст
            parts = []
            parts.append("🆕 <b>Заказ</b>")
            parts.append(html.escape(desc) if desc else "—")
            parts.append(f"<b>Цена:</b> {html.escape(str(price))} сум")
            parts.append(f"<b>ID:</b> <code>{html.escape(str(order_key))}</code>")

            if full_name:
                parts.append(f"<b>Заказчик (ФИО):</b> {html.escape(str(full_name))}")
            if username:
                parts.append(f"<b>Username:</b> @{html.escape(str(username))}")
            elif creator_tg:
                parts.append(f"<b>User id:</b> <code>{html.escape(str(creator_tg))}</code>")

            if phone:
                parts.append(f"<b>Телефон:</b> {html.escape(str(phone))}")

            if estimated is not None:
                try:
                    est_f = float(estimated)
                    parts.append(f"<b>Оценочный вес:</b> {est_f:.2f} кг")
                except Exception:
                    pass

            # Расстояние / адрес
            if dist_text:
                parts.append(f"<b>Расстояние:</b> {html.escape(dist_text)}")
            else:
                if _has_coords(order_lat, order_lon):
                    parts.append("<b>Адрес:</b> Оффлайн (координаты отправлены ниже)")
                else:
                    parts.append("<b>Адрес:</b> Онлайн")

            if created_at:
                parts.append(f"<b>Создан:</b> {html.escape(str(created_at))}")

            text = "\n".join(parts)

            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Принять заказ", callback_data=f"accept:{order_key}"))

            # Отправляем сообщение (HTML-parse)
            try:
                bot.send_message(message.chat.id, text, reply_markup=kb, parse_mode="HTML")
            except Exception:
                try:
                    bot.send_message(message.chat.id, "\n".join(parts), reply_markup=kb)
                except Exception:
                    logger.exception("Failed to send job message")

            # Если у заказа есть корректные координаты — отправляем геолокацию
            if _has_coords(order_lat, order_lon):
                try:
                    bot.send_location(message.chat.id, float(order_lat), float(order_lon))
                except Exception:
                    logger.exception("Failed to send location for order %s", order_key)

        if found == 0:
            bot.send_message(message.chat.id, "Нет доступных заказов поблизости.")
    except Exception:
        logger.exception("list_jobs error")
        bot.send_message(message.chat.id, "Ошибка получения заказов.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------- Admin handlers ----------------
# ---------------- Admin handlers ----------------
@bot.message_handler(commands=['panel'])
def admin_panel(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "Панель.")
        return
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("➕ Добавить фрилансера"), KeyboardButton("💰 Начислить сум"))
    kb.add(KeyboardButton("🔍 Найти пользователя"), KeyboardButton("📋 Все заказы"))
    kb.add(KeyboardButton("✏️ Изменить ФИО и Возраст"), KeyboardButton("📍 Конвертировать координаты"))
    bot.send_message(message.chat.id, "Панель администратора:", reply_markup=kb)


# --- изменить ФИО и возраст ---
@bot.message_handler(func=lambda m: m.text == "✏️ Изменить ФИО и Возраст")
def admin_edit_user_prompt(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    bot.send_message(message.chat.id, "Отправьте: <tg_id> <ФИО> <возраст>\nПример: 123456789 Иван Иванов 25")
    user_states[message.from_user.id] = "admin_expect_edit_user"


@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "admin_expect_edit_user")
def admin_edit_user_execute(message):
    if message.from_user.id not in ADMIN_IDS:
        user_states.pop(message.from_user.id, None)
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    parts = (message.text or "").strip().split()
    if len(parts) < 3:
        bot.send_message(message.chat.id, "Неверный формат. Используйте: <tg_id> <ФИО> <возраст>")
        user_states.pop(message.from_user.id, None)
        return
    try:
        tg = int(parts[0])
        age = int(parts[-1])
        full_name = " ".join(parts[1:-1])
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("UPDATE users SET full_name=?, age=? WHERE tg_id=?", (full_name, age, tg))
            conn.commit()
            bot.send_message(message.chat.id, f"✅ Данные обновлены: {tg} → {full_name}, {age} лет")
        finally:
            conn.close()
    except Exception:
        logger.exception("admin_edit_user error")
        bot.send_message(message.chat.id, "Ошибка. Проверьте ввод.")
    finally:
        user_states.pop(message.from_user.id, None)


# --- конвертация координат ---
@bot.message_handler(func=lambda m: m.text == "📍 Конвертировать координаты")
def admin_geo_prompt(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    bot.send_message(message.chat.id, "Отправьте координаты: <lat>,<lon>\nПример: 41.311296, 69.279892")
    user_states[message.from_user.id] = "admin_expect_geo"


@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "admin_expect_geo")
def admin_geo_execute(message):
    if message.from_user.id not in ADMIN_IDS:
        user_states.pop(message.from_user.id, None)
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    try:
        coords = (message.text or "").replace(" ", "").split(",")
        if len(coords) != 2:
            bot.send_message(message.chat.id, "Неверный формат. Используйте: lat,lon")
            return
        lat, lon = float(coords[0]), float(coords[1])
        gmaps_url = f"https://maps.google.com/?q={lat},{lon}"
        bot.send_message(message.chat.id, f"🌍 Геолокация:\n<a href='{gmaps_url}'>Открыть в Google Maps</a>", parse_mode="HTML")
    except Exception:
        logger.exception("admin_geo error")
        bot.send_message(message.chat.id, "Ошибка обработки координат.")
    finally:
        user_states.pop(message.from_user.id, None)


@bot.message_handler(func=lambda m: m.text == "➕ Добавить фрилансера")
def admin_add_executor_prompt(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    bot.send_message(message.chat.id, "Отправьте tg_id пользователя:")
    user_states[message.from_user.id] = "admin_expect_add_executor"

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "admin_expect_add_executor")
def admin_add_executor_execute(message):
    if message.from_user.id not in ADMIN_IDS:
        user_states.pop(message.from_user.id, None)
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    try:
        tg = int(message.text.strip())
        ensure_user(tg, None, None)
        set_user_role(tg, "executor")
        bot.send_message(message.chat.id, f"Пользователь {tg} назначен исполнителем.")
    except Exception:
        bot.send_message(message.chat.id, "tg_id неверен. Попробуйте снова.")
    finally:
        user_states.pop(message.from_user.id, None)

@bot.message_handler(func=lambda m: m.text == "💰 Начислить сум")
def admin_add_coins_prompt(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    bot.send_message(message.chat.id, "Отправьте: <tg_id> <amount>")
    user_states[message.from_user.id] = "admin_expect_add_coins"

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "admin_expect_add_coins")
def admin_add_coins_execute(message):
    if message.from_user.id not in ADMIN_IDS:
        user_states.pop(message.from_user.id, None)
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    parts = (message.text or "").strip().split()
    if len(parts) != 2:
        bot.send_message(message.chat.id, "Неверный формат. Используйте: <tg_id> <amount>")
        user_states.pop(message.from_user.id, None)
        return
    try:
        tg = int(parts[0]); amount = int(parts[1])
        add_coins(tg, amount)
        bot.send_message(message.chat.id, f"Начислено {amount} сум пользователю {tg}.")
    except Exception:
        bot.send_message(message.chat.id, "Ошибка. Проверьте ввод.")
    finally:
        user_states.pop(message.from_user.id, None)

@bot.message_handler(func=lambda m: m.text == "🔍 Найти пользователя")
def admin_search_user_prompt(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    bot.send_message(message.chat.id, "Введите tg_id/username/full_name для поиска:")
    user_states[message.from_user.id] = "admin_expect_search_user"

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "admin_expect_search_user")
def admin_search_user_execute(message):
    if message.from_user.id not in ADMIN_IDS:
        user_states.pop(message.from_user.id, None)
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    query = (message.text or "").strip()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT tg_id, username, full_name, role, status, balance_coins, frozen_total_coins FROM users WHERE tg_id = ? OR username LIKE ? OR full_name LIKE ? LIMIT 10", (query, f"%{query}%", f"%{query}%"))
        rows = cur.fetchall()
        if not rows:
            bot.send_message(message.chat.id, "Пользователь не найден.")
            return
        for r in rows:
            bot.send_message(message.chat.id, f"tg:{r['tg_id']} | @{r['username']} | {r['full_name']} | role:{r['role']} | status:{r['status']} | balance:{r['balance_coins']} | frozen:{r['frozen_total_coins']}")
    except Exception:
        logger.exception("admin_search_user error")
        bot.send_message(message.chat.id, "Ошибка поиска.")
    finally:
        conn.close()
        user_states.pop(message.from_user.id, None)

@bot.message_handler(func=lambda m: m.text == "📋 Все заказы")
def admin_all_orders(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, status, description, price_coins, creator_tg FROM orders ORDER BY created_at DESC LIMIT 30")
        rows = cur.fetchall()
        if not rows:
            bot.send_message(message.chat.id, "Нет заказов.")
            return
        s = "Последние заказы:\n"
        for r in rows:
            s += f"#{r['id']} | {r['status']} | {r['price_coins']} сум | creator:{r['creator_tg']}\n{r['description']}\n\n"
        bot.send_message(message.chat.id, s)
    except Exception:
        logger.exception("admin_all_orders error")
        bot.send_message(message.chat.id, "Ошибка получения заказов.")
    finally:
        conn.close()

@bot.message_handler(commands=['id'])
def cmd_id(message):
    parts = (message.text or "").strip().split()
    conn = get_conn()
    try:
        cur = conn.cursor()
        if len(parts) == 1:
            # просто /id → показываем ID отправителя
            tg_id = message.from_user.id
            bot.reply_to(message, f"Ваш Telegram ID: <code>{tg_id}</code>", parse_mode="HTML")
        else:
            target = parts[1]
            if target.startswith("@"):
                username = target[1:]
                cur.execute("SELECT tg_id FROM users WHERE username=?", (username,))
                row = cur.fetchone()
                if row:
                    bot.reply_to(message, f"Пользователь @{username} → ID: <code>{row['tg_id']}</code>", parse_mode="HTML")
                else:
                    bot.reply_to(message, f"Пользователь @{username} не найден.")
            else:
                try:
                    tg_id = int(target)
                    cur.execute("SELECT username, full_name FROM users WHERE tg_id=?", (tg_id,))
                    row = cur.fetchone()
                    if row:
                        name = row["full_name"] or ("@" + row["username"] if row["username"] else str(tg_id))
                        bot.reply_to(message, f"{name} → ID: <code>{tg_id}</code>", parse_mode="HTML")
                    else:
                        bot.reply_to(message, f"Пользователь с ID {tg_id} не найден в базе.")
                except Exception:
                    bot.reply_to(message, "Использование: /id [@username | tg_id]")
    finally:
        conn.close()

# ---------------- Scheduler start ----------------

def start_scheduler_jobs():
    try:
        scheduler.add_job(expansion_job, 'interval', seconds=EXPANSION_INTERVAL_SEC, id="expansion_job", replace_existing=True)
        logger.info("Scheduler jobs started (expansion_job).")
    except Exception:
        logger.exception("Failed to start scheduler jobs")

# ---------------- Main ----------------

def main():
    init_db()
    ensure_dir(UPLOADS_DIR)
    start_scheduler_jobs()
    logger.info("Bot polling started.")
    try:
        bot.infinity_polling(timeout=60, long_polling_timeout=60)
    except Exception:
        logger.exception("Polling stopped unexpectedly")

if __name__ == "__main__":
    main()
