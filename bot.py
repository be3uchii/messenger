import telebot
import sqlite3
import os
import sys
import time
import signal
import threading
import datetime
import random
from collections import defaultdict
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand

TOKEN = "8657084284:AAGKC7Y8ns5utDXL69jW6hVSTctipzz_C9o"
OWNER_ID = 8444252541
BASE_DIR = "/storage/emulated/0/ChatBot"
DB_PATH = os.path.join(BASE_DIR, "activity.db")
LOG_PATH = os.path.join(BASE_DIR, "bot.log")

os.makedirs(BASE_DIR, exist_ok=True)

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=4, parse_mode="HTML")
_stop_event = threading.Event()
spam_tracker = defaultdict(list)
spam_lock = threading.Lock()

POINTS = {
    "text": 1, "reply": 2, "photo": 3, "sticker": 2,
    "gif": 2, "video": 3, "voice": 3, "video_note": 3,
}

LEVELS = [
    (0,     "🌱 Новичок",    "newbie"),
    (100,   "🔰 Начинающий", "beginner"),
    (300,   "⚡ Активный",   "active"),
    (600,   "🔥 Огонь",      "fire"),
    (1000,  "💎 Алмаз",      "diamond"),
    (1500,  "🌟 Звезда",     "star"),
    (2500,  "🚀 Ракета",     "rocket"),
    (4000,  "👑 Король",     "king"),
    (6000,  "🏆 Чемпион",    "champion"),
    (9000,  "🌌 Легенда",    "legend"),
    (15000, "☄️ Абсолют",    "absolute"),
]

def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER,
            chat_id INTEGER,
            username TEXT,
            full_name TEXT,
            points INTEGER DEFAULT 0,
            messages INTEGER DEFAULT 0,
            replies INTEGER DEFAULT 0,
            photos INTEGER DEFAULT 0,
            stickers INTEGER DEFAULT 0,
            gifs INTEGER DEFAULT 0,
            videos INTEGER DEFAULT 0,
            voices INTEGER DEFAULT 0,
            video_notes INTEGER DEFAULT 0,
            joined_date TEXT,
            last_active TEXT,
            streak INTEGER DEFAULT 0,
            last_streak_date TEXT,
            level INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, chat_id)
        );
        CREATE TABLE IF NOT EXISTS chat_settings (
            chat_id INTEGER PRIMARY KEY,
            welcome_enabled INTEGER DEFAULT 1,
            antispam_enabled INTEGER DEFAULT 1,
            digest_enabled INTEGER DEFAULT 1,
            chat_title TEXT,
            created_date TEXT
        );
        CREATE TABLE IF NOT EXISTS message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            chat_id INTEGER,
            msg_type TEXT,
            hour INTEGER,
            day TEXT,
            timestamp INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_users_chat ON users(chat_id, points DESC);
        CREATE INDEX IF NOT EXISTS idx_msg_log ON message_log(chat_id, timestamp);
    """)
    conn.commit()
    conn.close()

init_db()
db_lock = threading.Lock()

def user_link(user_id, full_name, username):
    name = (full_name or "").strip()
    if not name:
        name = ("@" + username) if username else "Участник"
    return f'<a href="tg://user?id={user_id}">{name}</a>'

def get_full_name(user):
    return ((user.first_name or "") + (" " + user.last_name if user.last_name else "")).strip() or "Участник"

def is_owner(user_id):
    return user_id == OWNER_ID

def is_real_user(user):
    if user is None:
        return False
    if user.is_bot:
        return False
    return True

def only_in_group(message):
    return message.chat.type in ("group", "supergroup")

def safe_send(chat_id, text, reply_markup=None):
    try:
        return bot.send_message(chat_id, text, reply_markup=reply_markup, disable_web_page_preview=True)
    except Exception as e:
        log(f"safe_send error {chat_id}: {e}")
        return None

def safe_reply(message, text, reply_markup=None):
    try:
        return bot.reply_to(message, text, reply_markup=reply_markup)
    except Exception:
        try:
            return bot.send_message(message.chat.id, text, reply_markup=reply_markup, disable_web_page_preview=True)
        except Exception as e:
            log(f"safe_reply error: {e}")
            return None

def upsert_user(user_id, chat_id, username, full_name):
    with db_lock:
        conn = get_db()
        c = conn.cursor()
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        today = datetime.date.today().isoformat()
        c.execute("""
            INSERT INTO users (user_id, chat_id, username, full_name, joined_date, last_active, last_streak_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, chat_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                last_active=?
        """, (user_id, chat_id, username or "", full_name, now, now, today, now))
        conn.commit()
        conn.close()

def add_activity(user_id, chat_id, msg_type, username, full_name):
    with db_lock:
        conn = get_db()
        c = conn.cursor()
        now = datetime.datetime.now()
        today = now.date().isoformat()
        pts = POINTS.get(msg_type, 1)

        c.execute("SELECT last_streak_date, streak FROM users WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        row = c.fetchone()
        streak = 0
        if row:
            last_d = row[0]
            streak = row[1] or 0
            yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
            if last_d == today:
                pass
            elif last_d == yesterday:
                streak += 1
            else:
                streak = 1

        bonus = 0
        if streak in (3, 7, 14, 30):
            bonus = streak * 2

        col_map = {
            "text": "messages", "reply": "replies", "photo": "photos",
            "sticker": "stickers", "gif": "gifs", "video": "videos",
            "voice": "voices", "video_note": "video_notes",
        }
        col = col_map.get(msg_type, "messages")

        c.execute(f"""
            UPDATE users SET
                points = points + ?,
                messages = messages + 1,
                {col} = {col} + 1,
                last_active = ?,
                streak = ?,
                last_streak_date = ?
            WHERE user_id=? AND chat_id=?
        """, (pts + bonus, now.strftime("%Y-%m-%d %H:%M:%S"), streak, today, user_id, chat_id))

        c.execute("""
            INSERT INTO message_log (user_id, chat_id, msg_type, hour, day, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, chat_id, msg_type, now.hour, today, int(now.timestamp())))

        conn.commit()
        conn.close()

    update_level(user_id, chat_id)

def get_user(user_id, chat_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE user_id=? AND chat_id=?", (user_id, chat_id))
    row = c.fetchone()
    conn.close()
    return row

def get_top(chat_id, limit=10):
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT user_id, username, full_name, points, messages, level
        FROM users WHERE chat_id=? ORDER BY points DESC LIMIT ?
    """, (chat_id, limit))
    rows = c.fetchall()
    conn.close()
    return rows

def get_chat_settings(chat_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM chat_settings WHERE chat_id=?", (chat_id,))
    row = c.fetchone()
    conn.close()
    return row

def ensure_chat_settings(chat_id, title=""):
    with db_lock:
        conn = get_db()
        c = conn.cursor()
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""
            INSERT OR IGNORE INTO chat_settings (chat_id, chat_title, created_date)
            VALUES (?, ?, ?)
        """, (chat_id, title, now))
        conn.commit()
        conn.close()

def update_level(user_id, chat_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT points, level, full_name, username FROM users WHERE user_id=? AND chat_id=?", (user_id, chat_id))
    row = c.fetchone()
    conn.close()
    if not row:
        return
    pts, cur_level, full_name, username = row
    new_level = 0
    for i, (threshold, _, _) in enumerate(LEVELS):
        if pts >= threshold:
            new_level = i
    if new_level != cur_level:
        with db_lock:
            conn = get_db()
            c = conn.cursor()
            c.execute("UPDATE users SET level=? WHERE user_id=? AND chat_id=?", (new_level, user_id, chat_id))
            conn.commit()
            conn.close()
        if new_level > cur_level:
            lvl_name = LEVELS[new_level][1]
            link = user_link(user_id, full_name, username)
            try:
                bot.send_message(chat_id, f"🎊 {link} повысил уровень до <b>{lvl_name}</b>!")
            except Exception:
                pass

def get_level_info(points):
    current = (0, 0, LEVELS[0][1], LEVELS[0][2])
    next_lvl = None
    for i, (threshold, name, key) in enumerate(LEVELS):
        if points >= threshold:
            current = (i, threshold, name, key)
        else:
            next_lvl = (i, threshold, name, key)
            break
    return current, next_lvl

def is_spam(user_id, chat_id):
    settings = get_chat_settings(chat_id)
    if settings and settings[2] == 0:
        return False
    key = f"{user_id}:{chat_id}"
    now = time.time()
    with spam_lock:
        spam_tracker[key] = [t for t in spam_tracker[key] if now - t < 10]
        spam_tracker[key].append(now)
        return len(spam_tracker[key]) > 15

def process_message(message, msg_type):
    if not only_in_group(message):
        return
    u = message.from_user
    if not is_real_user(u):
        return
    chat_id = message.chat.id
    user_id = u.id
    full_name = get_full_name(u)
    username = u.username or ""
    ensure_chat_settings(chat_id, message.chat.title or "")
    upsert_user(user_id, chat_id, username, full_name)
    if is_spam(user_id, chat_id):
        return
    add_activity(user_id, chat_id, msg_type, username, full_name)

USER_COLS = ["user_id","chat_id","username","full_name","points","messages","replies",
             "photos","stickers","gifs","videos","voices","video_notes",
             "joined_date","last_active","streak","last_streak_date","level"]

@bot.message_handler(content_types=["new_chat_members"])
def on_new_member(message):
    chat_id = message.chat.id
    ensure_chat_settings(chat_id, message.chat.title or "")
    try:
        bot_info = bot.get_me()
    except Exception:
        bot_info = None

    for member in message.new_chat_members:
        if bot_info and member.id == bot_info.id:
            kb = InlineKeyboardMarkup()
            kb.add(
                InlineKeyboardButton("🏆 Топ чата", callback_data=f"top_chat:{chat_id}"),
                InlineKeyboardButton("Помощь", callback_data=f"help_btn:0:{chat_id}"),
            )
            safe_send(chat_id,
                f"👋 Привет! Я <b>ActivityBot</b>, слежу за активностью и веду рейтинги.\n\n"
                f"📊 <b>Очки за сообщения:</b>\n"
                f"Текст: 1 очко\n"
                f"Ответ на сообщение: 2 очка\n"
                f"Стикер / GIF: 2 очка\n"
                f"Фото / видео / голосовое: 3 очка\n\n"
                f"🔥 Будьте активны каждый день, стрик даёт бонусные очки!\n\n"
                f"Используйте /help чтобы увидеть все команды.",
                reply_markup=kb
            )
            continue

        if not is_real_user(member):
            continue

        full_name = get_full_name(member)
        upsert_user(member.id, chat_id, member.username or "", full_name)
        link = user_link(member.id, full_name, member.username)

        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton("📊 Моя статистика", callback_data=f"my_stats:{member.id}:{chat_id}"),
            InlineKeyboardButton("🏆 Топ чата", callback_data=f"top_chat:{chat_id}"),
        )
        kb.add(InlineKeyboardButton("Помощь", callback_data=f"help_btn:{member.id}:{chat_id}"))
        safe_send(chat_id,
            f"👋 Привет, {link}! Добро пожаловать.\n\n"
            f"В этом чате идёт соревнование активности - пиши сообщения, "
            f"получай очки и занимай место в топе.\n"
            f"Используй /help чтобы узнать все команды.",
            reply_markup=kb
        )

@bot.callback_query_handler(func=lambda c: c.data.startswith("my_stats:"))
def cb_my_stats(call):
    parts = call.data.split(":")
    user_id = int(parts[1])
    chat_id = int(parts[2])
    if call.from_user.id != user_id:
        bot.answer_callback_query(call.id, "Это не твоя статистика!", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    show_user_stats(chat_id, user_id, call.message.chat.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("top_chat:"))
def cb_top(call):
    chat_id = int(call.data.split(":")[1])
    bot.answer_callback_query(call.id)
    show_top(chat_id, limit=10)

@bot.callback_query_handler(func=lambda c: c.data.startswith("help_btn:"))
def cb_help(call):
    parts = call.data.split(":")
    user_id = int(parts[1])
    chat_id = int(parts[2])
    if user_id != 0 and call.from_user.id != user_id:
        bot.answer_callback_query(call.id, "Это не для тебя!", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    send_help(chat_id, is_owner(call.from_user.id))

@bot.message_handler(commands=["start"])
def cmd_start(message):
    u = message.from_user
    if not is_real_user(u):
        return
    if only_in_group(message):
        full_name = get_full_name(u)
        ensure_chat_settings(message.chat.id, message.chat.title or "")
        upsert_user(u.id, message.chat.id, u.username or "", full_name)
        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton("📊 Статистика", callback_data=f"my_stats:{u.id}:{message.chat.id}"),
            InlineKeyboardButton("🏆 Топ", callback_data=f"top_chat:{message.chat.id}"),
        )
        kb.add(InlineKeyboardButton("Помощь", callback_data=f"help_btn:{u.id}:{message.chat.id}"))
        safe_send(message.chat.id,
            f"🤖 <b>ActivityBot активен!</b>\n\n"
            f"Слежу за активностью, считаю очки и веду рейтинги.\n"
            f"Пиши, общайся, побеждай!\n\n"
            f"/help - все команды",
            reply_markup=kb
        )
    else:
        safe_reply(message,
            f"👋 Привет! Я <b>ActivityBot</b>.\n\n"
            f"Добавь меня в группу и назначь администратором, "
            f"я начну отслеживать активность и вести рейтинги!\n\n"
            f"/help - все команды"
        )

@bot.message_handler(commands=["help"])
def cmd_help(message):
    u = message.from_user
    if not is_real_user(u):
        return
    send_help(message.chat.id, is_owner(u.id))

def send_help(chat_id, owner=False):
    text = (
        "📖 <b>Команды ActivityBot</b>\n\n"
        "👤 <b>Личные:</b>\n"
        "/stats - твоя статистика\n"
        "/level - все уровни\n"
        "/streak - дни активности подряд\n"
        "/rank - твоё место в рейтинге\n\n"
        "🏆 <b>Рейтинги:</b>\n"
        "/top - топ 10 активных\n"
        "/top20 - топ 20 активных\n"
        "/today - активность за сегодня\n"
        "/week - топ за неделю\n"
        "/leaders - лидеры по категориям\n\n"
        "📊 <b>Аналитика:</b>\n"
        "/chatstats - статистика чата\n"
        "/types - мои типы сообщений\n"
        "/compare - сравнить (ответь на сообщение)\n\n"
        "<i>За каждый тип сообщения свои очки.\n"
        "Стрик активности даёт бонусные очки каждый день!</i>"
    )
    if owner:
        text += (
            "\n\n🔐 <b>Только для тебя:</b>\n"
            "/givepoints N - выдать очки (ответь на сообщение)\n"
            "/addpoints @user N - добавить очки по юзернейму\n"
            "/resetuser - сбросить статистику (ответь)\n"
            "/chatinfo - подробная инфо о чате"
        )
    safe_send(chat_id, text)

@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    u = message.from_user
    if not is_real_user(u):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    show_user_stats(message.chat.id, u.id, message.chat.id)

def show_user_stats(chat_id, user_id, send_to_chat):
    row = get_user(user_id, chat_id)
    if not row:
        safe_send(send_to_chat, "Статистика не найдена. Напиши хоть одно сообщение!")
        return
    ud = dict(zip(USER_COLS, row))
    lvl_idx = min(ud["level"], len(LEVELS) - 1)
    lvl_name = LEVELS[lvl_idx][1]

    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT COUNT(*) + 1 FROM users
        WHERE chat_id=? AND points > (SELECT points FROM users WHERE user_id=? AND chat_id=?)
    """, (chat_id, user_id, chat_id))
    rank = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM users WHERE chat_id=?", (chat_id,))
    total = c.fetchone()[0]
    conn.close()

    link = user_link(user_id, ud["full_name"], ud["username"])
    text = (
        f"📊 <b>Статистика {link}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⭐ Уровень: <b>{lvl_name}</b>\n"
        f"💰 Очки: <b>{ud['points']}</b>\n"
        f"🏅 Место: <b>#{rank}</b> из {total}\n"
        f"🔥 Стрик: <b>{ud['streak']} дн.</b>\n\n"
        f"💬 Сообщения: <b>{ud['messages']}</b>\n"
        f"↩️ Реплаи: <b>{ud['replies']}</b>\n"
        f"📸 Фото: <b>{ud['photos']}</b>\n"
        f"😊 Стикеры: <b>{ud['stickers']}</b>\n"
        f"🎞 GIF: <b>{ud['gifs']}</b>\n"
        f"🎬 Видео: <b>{ud['videos']}</b>\n"
        f"🎙 Голосовые: <b>{ud['voices']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 В чате с: {(ud['joined_date'] or '')[:10]}\n"
        f"⏱ Последняя активность: {(ud['last_active'] or '')[:16]}"
    )
    safe_send(send_to_chat, text)

@bot.message_handler(commands=["top"])
def cmd_top(message):
    if not is_real_user(message.from_user):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    show_top(message.chat.id, limit=10)

@bot.message_handler(commands=["top20"])
def cmd_top20(message):
    if not is_real_user(message.from_user):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    show_top(message.chat.id, limit=20)

def show_top(chat_id, limit=10):
    rows = get_top(chat_id, limit)
    if not rows:
        safe_send(chat_id, "Пока нет данных. Начните общаться!")
        return
    medals = ["🥇","🥈","🥉","4.","5.","6.","7.","8.","9.","10."] + [f"{i}." for i in range(11, 25)]
    lines = [f"🏆 <b>Топ {limit} активных участников</b>\n━━━━━━━━━━━━━━━━━━━━"]
    for i, r in enumerate(rows):
        uid, uname_db, fn, pts, msgs, lvl = r
        lvl_name = LEVELS[min(lvl, len(LEVELS) - 1)][1]
        link = user_link(uid, fn, uname_db)
        medal = medals[i] if i < len(medals) else f"{i+1}."
        lines.append(f"{medal} {link}\n    {lvl_name}  •  {pts} очков  •  {msgs} сообщ.")
    safe_send(chat_id, "\n".join(lines))

@bot.message_handler(commands=["level"])
def cmd_level(message):
    if not is_real_user(message.from_user):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    u = message.from_user
    row = get_user(u.id, message.chat.id)
    if not row:
        safe_reply(message, "Напиши хоть одно сообщение сначала!")
        return
    pts = row[4]
    lvl_idx = row[17]

    lines = ["🎯 <b>Все уровни</b>\n━━━━━━━━━━━━━━━━━━━━"]
    for i, (thr, name, _) in enumerate(LEVELS):
        if i == lvl_idx:
            mark = "▶️"
        elif pts >= thr:
            mark = "✅"
        else:
            mark = "🔒"
        lines.append(f"{mark} {name}  •  от {thr} очков")

    safe_send(message.chat.id, "\n".join(lines))

@bot.message_handler(commands=["streak"])
def cmd_streak(message):
    if not is_real_user(message.from_user):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    u = message.from_user
    row = get_user(u.id, message.chat.id)
    if not row:
        safe_reply(message, "Напиши хоть одно сообщение сначала!")
        return
    ud = dict(zip(USER_COLS, row))
    streak = ud["streak"]
    last_d = ud["last_streak_date"] or "нет данных"
    bonus_info = ""
    for days, mult in [(30, 60), (14, 28), (7, 14), (3, 6)]:
        if streak >= days:
            bonus_info = f"\n💎 Активен бонус за {days} дней подряд (+{mult} очков в день)"
            break
    safe_send(message.chat.id,
        f"🔥 <b>Стрик активности</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Текущий стрик: <b>{streak} дней</b>\n"
        f"Последняя активность: <b>{last_d[:10]}</b>"
        f"{bonus_info}\n\n"
        f"📌 Бонусы за стрик:\n"
        f"  3 дня  +6 очков\n"
        f"  7 дней  +14 очков\n"
        f"  14 дней  +28 очков\n"
        f"  30 дней  +60 очков"
    )

@bot.message_handler(commands=["rank"])
def cmd_rank(message):
    if not is_real_user(message.from_user):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    u = message.from_user
    row = get_user(u.id, message.chat.id)
    if not row:
        safe_reply(message, "Напиши хоть одно сообщение сначала!")
        return
    pts = row[4]
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) + 1 FROM users WHERE chat_id=? AND points > ?", (message.chat.id, pts))
    rank = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM users WHERE chat_id=?", (message.chat.id,))
    total = c.fetchone()[0]
    conn.close()
    pct = int((1 - (rank - 1) / max(total, 1)) * 100)
    link = user_link(u.id, get_full_name(u), u.username)
    safe_send(message.chat.id,
        f"📍 <b>Рейтинг {link}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏅 Место: <b>#{rank}</b> из {total}\n"
        f"💰 Очков: <b>{pts}</b>\n"
        f"📊 Лучше {pct}% участников"
    )

@bot.message_handler(commands=["compare"])
def cmd_compare(message):
    if not is_real_user(message.from_user):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    if not message.reply_to_message:
        safe_reply(message, "Ответь на сообщение участника для сравнения.")
        return
    u2 = message.reply_to_message.from_user
    if not is_real_user(u2):
        safe_reply(message, "Нельзя сравниваться с ботом.")
        return
    u1 = message.from_user
    if u1.id == u2.id:
        safe_reply(message, "Нельзя сравнивать себя с собой.")
        return
    r1 = get_user(u1.id, message.chat.id)
    r2 = get_user(u2.id, message.chat.id)
    if not r1 or not r2:
        safe_reply(message, "У одного из участников нет статистики!")
        return
    d1 = dict(zip(USER_COLS, r1))
    d2 = dict(zip(USER_COLS, r2))
    l1 = user_link(u1.id, d1["full_name"], d1["username"])
    l2 = user_link(u2.id, d2["full_name"], d2["username"])

    def cmp(v1, v2):
        if v1 > v2: return "⬆️", "⬇️"
        if v1 < v2: return "⬇️", "⬆️"
        return "=", "="

    a1, a2 = cmp(d1["points"], d2["points"])
    b1, b2 = cmp(d1["messages"], d2["messages"])
    c1, c2 = cmp(d1["streak"], d2["streak"])
    text = (
        f"⚔️ <b>Сравнение</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{l1} vs {l2}\n\n"
        f"💰 Очки: {a1} {d1['points']} | {d2['points']} {a2}\n"
        f"💬 Сообщений: {b1} {d1['messages']} | {d2['messages']} {b2}\n"
        f"📸 Фото: {d1['photos']} | {d2['photos']}\n"
        f"😊 Стикеры: {d1['stickers']} | {d2['stickers']}\n"
        f"🎙 Голосовые: {d1['voices']} | {d2['voices']}\n"
        f"🔥 Стрик: {c1} {d1['streak']}д | {d2['streak']}д {c2}\n"
        f"⭐ Уровень: {LEVELS[min(d1['level'],len(LEVELS)-1)][1]} | {LEVELS[min(d2['level'],len(LEVELS)-1)][1]}"
    )
    safe_send(message.chat.id, text)

@bot.message_handler(commands=["chatstats"])
def cmd_chatstats(message):
    if not is_real_user(message.from_user):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    chat_id = message.chat.id
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users WHERE chat_id=?", (chat_id,))
    total_users = c.fetchone()[0]
    c.execute("SELECT SUM(messages), SUM(points), SUM(photos), SUM(stickers), SUM(voices), SUM(gifs) FROM users WHERE chat_id=?", (chat_id,))
    sums = c.fetchone()
    c.execute("SELECT COUNT(*) FROM message_log WHERE chat_id=? AND day=?", (chat_id, datetime.date.today().isoformat()))
    today_msgs = c.fetchone()[0]
    c.execute("SELECT user_id, username, full_name, points FROM users WHERE chat_id=? ORDER BY points DESC LIMIT 1", (chat_id,))
    top1 = c.fetchone()
    c.execute("SELECT MAX(streak) FROM users WHERE chat_id=?", (chat_id,))
    max_streak = c.fetchone()[0] or 0
    conn.close()

    total_msgs = sums[0] or 0
    total_pts = sums[1] or 0
    total_photos = sums[2] or 0
    total_stickers = sums[3] or 0
    total_voices = sums[4] or 0
    total_gifs = sums[5] or 0

    if top1:
        top1_link = user_link(top1[0], top1[2], top1[1])
        top1_pts = top1[3]
    else:
        top1_link = "нет данных"
        top1_pts = 0

    safe_send(message.chat.id,
        f"📊 <b>Статистика чата {message.chat.title}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Участников: <b>{total_users}</b>\n"
        f"💬 Всего сообщений: <b>{total_msgs}</b>\n"
        f"💰 Всего очков: <b>{total_pts}</b>\n"
        f"📸 Фотографий: <b>{total_photos}</b>\n"
        f"😊 Стикеров: <b>{total_stickers}</b>\n"
        f"🎙 Голосовых: <b>{total_voices}</b>\n"
        f"🎞 GIF: <b>{total_gifs}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 Сегодня: <b>{today_msgs}</b> сообщений\n"
        f"👑 Лидер: {top1_link} ({top1_pts} очков)\n"
        f"🔥 Макс. стрик: <b>{max_streak} дн.</b>"
    )

@bot.message_handler(commands=["today"])
def cmd_today(message):
    if not is_real_user(message.from_user):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    chat_id = message.chat.id
    today = datetime.date.today().isoformat()
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT ml.user_id, u.full_name, u.username, COUNT(*) as cnt
        FROM message_log ml
        LEFT JOIN users u ON ml.user_id=u.user_id AND ml.chat_id=u.chat_id
        WHERE ml.chat_id=? AND ml.day=?
        GROUP BY ml.user_id ORDER BY cnt DESC LIMIT 10
    """, (chat_id, today))
    rows = c.fetchall()
    conn.close()
    if not rows:
        safe_send(chat_id, "Сегодня ещё никто не писал.")
        return
    medals = ["🥇","🥈","🥉","4.","5.","6.","7.","8.","9.","10."]
    lines = [f"📅 <b>Активность за сегодня ({today})</b>\n━━━━━━━━━━━━━━━━━━━━"]
    for i, (uid, fn, un, cnt) in enumerate(rows):
        link = user_link(uid, fn, un)
        medal = medals[i] if i < len(medals) else f"{i+1}."
        lines.append(f"{medal} {link}  •  {cnt} сообщ.")
    safe_send(chat_id, "\n".join(lines))

@bot.message_handler(commands=["week"])
def cmd_week(message):
    if not is_real_user(message.from_user):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    chat_id = message.chat.id
    week_ago = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT ml.user_id, u.full_name, u.username, COUNT(*) as cnt
        FROM message_log ml
        LEFT JOIN users u ON ml.user_id=u.user_id AND ml.chat_id=u.chat_id
        WHERE ml.chat_id=? AND ml.day>=?
        GROUP BY ml.user_id ORDER BY cnt DESC LIMIT 10
    """, (chat_id, week_ago))
    rows = c.fetchall()
    conn.close()
    if not rows:
        safe_send(chat_id, "За эту неделю нет данных.")
        return
    medals = ["🥇","🥈","🥉","4.","5.","6.","7.","8.","9.","10."]
    lines = [f"📆 <b>Топ за последние 7 дней</b>\n━━━━━━━━━━━━━━━━━━━━"]
    for i, (uid, fn, un, cnt) in enumerate(rows):
        link = user_link(uid, fn, un)
        medal = medals[i] if i < len(medals) else f"{i+1}."
        lines.append(f"{medal} {link}  •  {cnt} сообщ.")
    safe_send(chat_id, "\n".join(lines))

@bot.message_handler(commands=["types"])
def cmd_types(message):
    if not is_real_user(message.from_user):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    u = message.from_user
    row = get_user(u.id, message.chat.id)
    if not row:
        safe_reply(message, "Нет данных. Напиши хоть одно сообщение!")
        return
    ud = dict(zip(USER_COLS, row))
    total = max(ud["messages"], 1)
    def pct(v): return int(v / total * 100)
    safe_send(message.chat.id,
        f"📋 <b>Мои типы сообщений</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💬 Текст: <b>{ud['messages']}</b>\n"
        f"↩️ Реплаи: <b>{ud['replies']}</b> ({pct(ud['replies'])}%)\n"
        f"📸 Фото: <b>{ud['photos']}</b> ({pct(ud['photos'])}%)\n"
        f"😊 Стикеры: <b>{ud['stickers']}</b> ({pct(ud['stickers'])}%)\n"
        f"🎞 GIF: <b>{ud['gifs']}</b> ({pct(ud['gifs'])}%)\n"
        f"🎬 Видео: <b>{ud['videos']}</b> ({pct(ud['videos'])}%)\n"
        f"🎙 Голосовые: <b>{ud['voices']}</b> ({pct(ud['voices'])}%)"
    )

@bot.message_handler(commands=["leaders"])
def cmd_leaders(message):
    if not is_real_user(message.from_user):
        return
    if not only_in_group(message):
        safe_reply(message, "Эта команда работает только в группах.")
        return
    chat_id = message.chat.id
    conn = get_db()
    c = conn.cursor()
    def get_leader(col):
        c.execute(f"SELECT user_id, full_name, username, {col} FROM users WHERE chat_id=? ORDER BY {col} DESC LIMIT 1", (chat_id,))
        r = c.fetchone()
        if r and r[3]:
            return (user_link(r[0], r[1], r[2]), r[3])
        return ("нет данных", 0)
    lpts = get_leader("points")
    lmsg = get_leader("messages")
    lpht = get_leader("photos")
    lstk = get_leader("stickers")
    lvoc = get_leader("voices")
    lstr = get_leader("streak")
    lrpl = get_leader("replies")
    conn.close()
    safe_send(message.chat.id,
        f"👑 <b>Лидеры чата по категориям</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Очки: {lpts[0]} ({lpts[1]})\n"
        f"💬 Сообщений: {lmsg[0]} ({lmsg[1]})\n"
        f"↩️ Реплаев: {lrpl[0]} ({lrpl[1]})\n"
        f"📸 Фото: {lpht[0]} ({lpht[1]})\n"
        f"😊 Стикеры: {lstk[0]} ({lstk[1]})\n"
        f"🎙 Голосовые: {lvoc[0]} ({lvoc[1]})\n"
        f"🔥 Стрик: {lstr[0]} ({lstr[1]} дн.)"
    )

@bot.message_handler(commands=["givepoints"])
def cmd_givepoints(message):
    if not is_owner(message.from_user.id):
        return
    if not only_in_group(message):
        safe_reply(message, "Только в группах.")
        return
    if not message.reply_to_message:
        safe_reply(message, "Ответь на сообщение участника: /givepoints 100")
        return
    target = message.reply_to_message.from_user
    if not is_real_user(target):
        safe_reply(message, "Нельзя выдавать очки ботам.")
        return
    args = message.text.split()
    if len(args) < 2:
        safe_reply(message, "Укажи количество очков: /givepoints 100")
        return
    try:
        pts = int(args[1])
    except ValueError:
        safe_reply(message, "Укажи число!")
        return
    with db_lock:
        conn = get_db()
        conn.execute("UPDATE users SET points=points+? WHERE user_id=? AND chat_id=?",
                     (pts, target.id, message.chat.id))
        conn.commit()
        conn.close()
    full_name = get_full_name(target)
    link = user_link(target.id, full_name, target.username)
    safe_send(message.chat.id, f"💰 Выдано <b>{pts}</b> очков - {link}.")
    update_level(target.id, message.chat.id)

@bot.message_handler(commands=["addpoints"])
def cmd_addpoints(message):
    if not is_owner(message.from_user.id):
        return
    if not only_in_group(message):
        safe_reply(message, "Только в группах.")
        return
    args = message.text.split()
    if len(args) < 3:
        safe_reply(message, "Использование: /addpoints @username 100")
        return
    username = args[1].lstrip("@")
    try:
        pts = int(args[2])
    except ValueError:
        safe_reply(message, "Укажи число очков!")
        return
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id, chat_id, full_name, username FROM users WHERE username=? AND chat_id=? LIMIT 1",
              (username, message.chat.id))
    row = c.fetchone()
    conn.close()
    if not row:
        safe_reply(message, f"Участник @{username} не найден в базе этого чата.")
        return
    uid, cid, fname, uname = row
    with db_lock:
        conn2 = get_db()
        conn2.execute("UPDATE users SET points=points+? WHERE user_id=? AND chat_id=?", (pts, uid, cid))
        conn2.commit()
        conn2.close()
    link = user_link(uid, fname, uname)
    safe_send(message.chat.id, f"💰 Добавлено <b>{pts}</b> очков - {link}.")
    update_level(uid, cid)

@bot.message_handler(commands=["resetuser"])
def cmd_resetuser(message):
    if not is_owner(message.from_user.id):
        return
    if not only_in_group(message):
        safe_reply(message, "Только в группах.")
        return
    if not message.reply_to_message:
        safe_reply(message, "Ответь на сообщение участника для сброса.")
        return
    target = message.reply_to_message.from_user
    if not is_real_user(target):
        safe_reply(message, "Нельзя сбрасывать статистику бота.")
        return
    with db_lock:
        conn = get_db()
        conn.execute("""
            UPDATE users SET points=0, messages=0, replies=0, photos=0, stickers=0,
            gifs=0, videos=0, voices=0, video_notes=0,
            streak=0, level=0 WHERE user_id=? AND chat_id=?
        """, (target.id, message.chat.id))
        conn.execute("DELETE FROM message_log WHERE user_id=? AND chat_id=?", (target.id, message.chat.id))
        conn.commit()
        conn.close()
    full_name = get_full_name(target)
    link = user_link(target.id, full_name, target.username)
    safe_send(message.chat.id, f"🗑 Статистика {link} сброшена.")

@bot.message_handler(commands=["chatinfo"])
def cmd_chatinfo(message):
    if not is_owner(message.from_user.id):
        return
    chat = message.chat
    if not only_in_group(message):
        safe_reply(message, "Только в группах.")
        return
    settings = get_chat_settings(chat.id)
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users WHERE chat_id=?", (chat.id,))
    total = c.fetchone()[0]
    c.execute("SELECT SUM(messages) FROM users WHERE chat_id=?", (chat.id,))
    total_msgs = c.fetchone()[0] or 0
    c.execute("SELECT COUNT(*) FROM message_log WHERE chat_id=? AND day=?", (chat.id, datetime.date.today().isoformat()))
    today_msgs = c.fetchone()[0]
    conn.close()
    created = settings[5][:10] if settings and settings[5] else "нет данных"
    safe_send(message.chat.id,
        f"ℹ️ <b>Информация о чате</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📛 Название: <b>{chat.title}</b>\n"
        f"🆔 ID: <code>{chat.id}</code>\n"
        f"👥 Участников в базе: <b>{total}</b>\n"
        f"💬 Всего сообщений: <b>{total_msgs}</b>\n"
        f"📅 Сегодня: <b>{today_msgs}</b> сообщений\n"
        f"📆 Бот добавлен: <b>{created}</b>\n"
        f"💾 База: <code>{DB_PATH}</code>"
    )

@bot.message_handler(content_types=["text"])
def on_text(message):
    if not only_in_group(message):
        return
    u = message.from_user
    if not is_real_user(u):
        return
    if message.text and message.text.startswith("/"):
        return
    msg_type = "reply" if message.reply_to_message else "text"
    process_message(message, msg_type)

@bot.message_handler(content_types=["photo"])
def on_photo(message):
    if not is_real_user(message.from_user): return
    process_message(message, "photo")

@bot.message_handler(content_types=["sticker"])
def on_sticker(message):
    if not is_real_user(message.from_user): return
    process_message(message, "sticker")

@bot.message_handler(content_types=["animation"])
def on_animation(message):
    if not is_real_user(message.from_user): return
    process_message(message, "gif")

@bot.message_handler(content_types=["video"])
def on_video(message):
    if not is_real_user(message.from_user): return
    process_message(message, "video")

@bot.message_handler(content_types=["voice"])
def on_voice(message):
    if not is_real_user(message.from_user): return
    process_message(message, "voice")

@bot.message_handler(content_types=["video_note"])
def on_video_note(message):
    if not is_real_user(message.from_user): return
    process_message(message, "video_note")

def daily_digest():
    while not _stop_event.is_set():
        try:
            now = datetime.datetime.now()
            target = now.replace(hour=20, minute=0, second=0, microsecond=0)
            if now >= target:
                target += datetime.timedelta(days=1)
            wait = (target - now).total_seconds()
            _stop_event.wait(timeout=wait)
            if _stop_event.is_set():
                break
            today = datetime.date.today().isoformat()
            conn = get_db()
            c = conn.cursor()
            c.execute("SELECT chat_id, digest_enabled FROM chat_settings")
            chats = c.fetchall()
            conn.close()
            for (chat_id, digest_en) in chats:
                if not digest_en:
                    continue
                try:
                    conn2 = get_db()
                    c2 = conn2.cursor()
                    c2.execute("""
                        SELECT ml.user_id, u.full_name, u.username, COUNT(*) as cnt
                        FROM message_log ml
                        LEFT JOIN users u ON ml.user_id=u.user_id AND ml.chat_id=u.chat_id
                        WHERE ml.chat_id=? AND ml.day=?
                        GROUP BY ml.user_id ORDER BY cnt DESC LIMIT 5
                    """, (chat_id, today))
                    top_today = c2.fetchall()
                    c2.execute("SELECT COUNT(*) FROM message_log WHERE chat_id=? AND day=?", (chat_id, today))
                    day_total = c2.fetchone()[0]
                    c2.execute("SELECT user_id, username, full_name, points FROM users WHERE chat_id=? ORDER BY points DESC LIMIT 1", (chat_id,))
                    overall_top = c2.fetchone()
                    conn2.close()
                    if not top_today:
                        continue
                    medals = ["🥇","🥈","🥉","4.","5."]
                    lines = [f"📰 <b>Дайджест дня {today}</b>\n━━━━━━━━━━━━━━━━━━━━"]
                    lines.append(f"💬 Сообщений за день: <b>{day_total}</b>\n")
                    lines.append("🏆 <b>Топ активных сегодня:</b>")
                    for i, (uid, fn, un, cnt) in enumerate(top_today):
                        link = user_link(uid, fn, un)
                        medal = medals[i] if i < len(medals) else f"{i+1}."
                        lines.append(f"{medal} {link}  •  {cnt} сообщ.")
                    if overall_top:
                        top_link = user_link(overall_top[0], overall_top[2], overall_top[1])
                        lines.append(f"\n👑 Общий лидер: {top_link} ({overall_top[3]} очков)")
                    lines.append("\n<i>До завтра! Поддерживай стрик 🔥</i>")
                    bot.send_message(chat_id, "\n".join(lines))
                except Exception as e:
                    log(f"Digest error {chat_id}: {e}")
        except Exception as e:
            log(f"Digest thread error: {e}")
            _stop_event.wait(timeout=60)

def set_commands():
    try:
        commands = [
            BotCommand("start", "Запуск бота"),
            BotCommand("help", "Все команды"),
            BotCommand("stats", "Твоя статистика"),
            BotCommand("top", "Топ 10 активных"),
            BotCommand("top20", "Топ 20 активных"),
            BotCommand("level", "Все уровни"),
            BotCommand("streak", "Стрик активности"),
            BotCommand("rank", "Твоё место в рейтинге"),
            BotCommand("compare", "Сравнить с участником"),
            BotCommand("chatstats", "Статистика чата"),
            BotCommand("today", "Активность за сегодня"),
            BotCommand("week", "Топ за неделю"),
            BotCommand("types", "Мои типы сообщений"),
            BotCommand("leaders", "Лидеры по категориям"),
        ]
        bot.set_my_commands(commands)
        log("Команды зарегистрированы")
    except Exception as e:
        log(f"set_commands error: {e}")

def stop_handler(signum, frame):
    _stop_event.set()
    print("\nБот остановлен.")
    try:
        bot.stop_polling()
    except Exception:
        pass
    sys.exit(0)

signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)

if __name__ == "__main__":
    log("ActivityBot запущен")

    print("╔══════════════════════════════════════╗")
    print("║         ActivityBot  v4.0            ║")
    print("╠══════════════════════════════════════╣")
    print(f"║  База:   activity.db                 ║")
    print(f"║  Логи:   bot.log                     ║")
    print("╠══════════════════════════════════════╣")
    print("║  Ctrl+C для остановки                ║")
    print("╚══════════════════════════════════════╝")
    print()

    threading.Thread(target=daily_digest, daemon=True).start()
    threading.Thread(target=set_commands, daemon=True).start()

    while not _stop_event.is_set():
        try:
            bot.polling(none_stop=False, interval=1, timeout=25, long_polling_timeout=20)
        except Exception as e:
            if _stop_event.is_set():
                break
            log(f"Polling error: {e}")
            _stop_event.wait(timeout=5)
