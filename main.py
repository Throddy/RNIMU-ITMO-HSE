"""
Telegram bot для конкурса "Староста года" на aiogram (v3).

Особенности:
- Назначение куратора из CSV в круговом порядке (round-robin)
- 13 заданий; 13-е доступно только после зачёта >=3 заданий
- При отправке решения: проверка типа (photo/video/text), отправка куратору
- Куратор может принять/отклонить — пользователю приходит уведомление и начисляются баллы
- Пока задание на проверке — нельзя отправить повторное решение
- При отклонении: пользователь может отправить новое решение
- Формирование рейтинга (экспорт в Google Sheets) — заготовка интеграции

Файлы/настройки:
- curators.csv: CSV с колонками: fio, telegram_id
- credentials.json: Google API credentials (для экспорта в Google Sheets) — опционально
- Настройте переменные окружения BOT_TOKEN и ADMIN_ID

Зависимости:
- aiogram==3.*
- aiosqlite
- python-dotenv
- gspread (опционально для Google Sheets)

Запуск: python telegram_bot_aiogram.py
"""

import asyncio
import os
from datetime import datetime
from typing import List, Optional
import logging
import json

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import BotCommand
from aiogram import F
import aiosqlite
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from aiogram.exceptions import TelegramBadRequest
import secrets
import zipfile

logging.basicConfig(level=logging.INFO)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split(","))) if os.getenv("ADMIN_IDS") else []
CURATORS_CSV = os.getenv("CURATORS_CSV", "curators.csv")
DB_PATH = os.getenv("DB_PATH", "bot.db")
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
SPREADSHEET_NAME = "Староста года"  # название таблицы
SHEET_NAME = "Рейтинг"  # лист для рейтинга
TASKS_JSON_PATH = "tasks_data.json"
BACKUP_DIR = "backups"
BACKUP_INTERVAL_HOURS = 24
LOG_FILE = "all_logs.txt"

if not os.path.exists(TASKS_JSON_PATH):
    raise FileNotFoundError(f"Файл {TASKS_JSON_PATH} не найден")

with open(TASKS_JSON_PATH, "r", encoding="utf-8") as f:
    TASKS_DETAILS = {task["id"]: task for task in json.load(f)}

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не установлен в переменных окружения")

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ---- Конфигурация заданий (соответствует документу пользователя) ----
TASKS = [
    {"id": 1, "title": "Знакомство", "type": "photo_text", "points": 1},
    {"id": 2, "title": "Важные сведения", "type": "text", "points": 1},
    {"id": 3, "title": "Документы и не только", "type": "text", "points": 1},
    {"id": 4, "title": "Дни варенья", "type": "text", "points": 1},
    {"id": 5, "title": "Памятный кадр", "type": "photo", "points": 2},
    {"id": 6, "title": "Фото со звездой", "type": "photo", "points": 2},
    {"id": 7, "title": "Нетворкинг", "type": "photo_multi", "points": 2},
    {"id": 8, "title": "Красные дни календаря", "type": "photo", "points": 2},
    {"id": 9, "title": "Часть команды", "type": "photo", "points": 3},
    {"id": 10, "title": "Свети другим!", "type": "video", "points": 3},
    {"id": 11, "title": "Моё любимое!", "type": "video", "points": 3},
    {"id": 12, "title": "Расширь кругозор!", "type": "video", "points": 3},
    {"id": 13, "title": "Проложи маршрут!", "type": "video", "points": 3},
    {"id": 14, "title": "Суперзадание", "type": "photo_video", "points": 10},
]


class BroadcastState(StatesGroup):
    waiting_for_message = State()


# === Настраиваем логирование (чтобы точно писалось в файл) ===
if not os.path.exists(LOG_FILE):
    open(LOG_FILE, "a").close()
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
file_handler.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
if not any(isinstance(h, logging.FileHandler) for h in root_logger.handlers):
    root_logger.addHandler(file_handler)


async def create_backup() -> str:
    """Создает zip-бэкап базы данных, логов и конфигов."""
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)

    # Принудительно сбрасываем буфер логов перед архивированием
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"backup_{timestamp}.zip"
    backup_path = os.path.join(BACKUP_DIR, backup_name)

    with zipfile.ZipFile(backup_path, "w", zipfile.ZIP_DEFLATED) as zf:
        if os.path.exists(DB_PATH):
            zf.write(DB_PATH, arcname="bot.db")
        if os.path.exists(LOG_FILE):
            zf.write(LOG_FILE, arcname="all_logs.txt")
        if os.path.exists("curators.csv"):
            zf.write("curators.csv", arcname="curators.csv")
        if os.path.exists("credentials.json"):
            zf.write("credentials.json", arcname="credentials.json")

    logging.info(f"📦 Бэкап создан: {backup_path}")
    return backup_path


async def send_backup_to_admin():
    """Отправляет бэкап админу и удаляет файл после отправки."""
    if not ADMIN_IDS:
        logging.warning("⚠️ Нет ADMIN_IDS — некуда отправлять бэкап.")
        return

    admin_id = ADMIN_IDS[0]
    backup_path = await create_backup()

    try:
        await bot.send_document(admin_id, types.FSInputFile(backup_path),
                                caption=f"📦 Автоматический бэкап ({datetime.now():%d.%m %H:%M})")
        logging.info(f"✅ Бэкап отправлен админу {admin_id}")
    except Exception as e:
        logging.error(f"❌ Не удалось отправить бэкап админу: {e}")
    finally:
        try:
            os.remove(backup_path)
            logging.info(f"🗑 Бэкап удалён: {backup_path}")
        except Exception as e:
            logging.warning(f"⚠️ Не удалось удалить бэкап: {e}")


async def backup_scheduler():
    """Фоновая задача: создаёт и отправляет бэкапы каждые BACKUP_INTERVAL_HOURS."""
    while True:
        try:
            await send_backup_to_admin()
        except Exception as e:
            logging.error(f"Ошибка при создании/отправке бэкапа: {e}")
        await asyncio.sleep(BACKUP_INTERVAL_HOURS * 3600)


@dp.message(Command("logs"))
async def cmd_logs(message: types.Message):
    """Создает свежий бэкап (включая логи) и отправляет админу."""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Команда доступна только главным администраторам.")
        return

    await message.answer("⏳ Создаю и отправляю свежий бэкап с логами...")

    backup_path = None
    try:
        backup_path = await create_backup()
        await bot.send_document(message.from_user.id, types.FSInputFile(backup_path),
                                caption=f"📦 Бэкап + логи ({datetime.now():%d.%m %H:%M})")
        await message.answer("✅ Бэкап с логами отправлен и удалён.")
    except Exception as e:
        await message.answer(f"❌ Ошибка при отправке бэкапа: {e}")
    finally:
        if backup_path and os.path.exists(backup_path):
            try:
                os.remove(backup_path)
                logging.info(f"🗑 Бэкап {backup_path} удалён после ручной отправки.")
            except Exception as e:
                logging.warning(f"⚠️ Не удалось удалить бэкап: {e}")


async def tasks_keyboard_for_user(user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    async with aiosqlite.connect(DB_PATH) as db:
        # Получаем id задач, которые уже выполнены или на проверке
        cur = await db.execute(
            "SELECT task_id, status FROM submissions WHERE user_id=? AND status IN ('pending', 'accepted')",
            (user_id,)
        )
        done_or_pending = [r[0] for r in await cur.fetchall()]

    # Формируем кнопки только для доступных заданий
    for t in TASKS:
        if t['id'] in done_or_pending:
            continue  # скрываем выполненные и отправленные
        builder.button(
            text=f"{t['id']}. {t['title']}",
            callback_data=f"task_{t['id']}"
        )

    if not builder.buttons:
        builder.button(text="🎉 Все задания выполнены!", callback_data="no_tasks")

    builder.adjust(2)
    return builder.as_markup()


# ---- FSM States ----
class StartStates(StatesGroup):
    waiting_for_fio = State()
    waiting_for_group = State()


class SubmitStates(StatesGroup):
    waiting_for_answer = State()


class AnswerFSM(StatesGroup):
    waiting_for_photo = State()
    waiting_for_text = State()


# ---- Database helpers ----
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS curators (
            idx INTEGER PRIMARY KEY AUTOINCREMENT,
            fio TEXT,
            telegram_id INTEGER
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            tg_id INTEGER PRIMARY KEY,
            fio TEXT,
            acad_group TEXT,
            curator_idx INTEGER,
            points INTEGER DEFAULT 0
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            task_id INTEGER,
            status TEXT,
            content_type TEXT,
            content TEXT,
            curator_comment TEXT,
            created_at TEXT,
            updated_at TEXT
        )""")
        await db.commit()


async def update_google_sheet():
    # подключаемся к Google Sheets
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    sheet = spreadsheet.sheet1

    # собираем данные из базы
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT tg_id, fio, acad_group, points FROM users")
        users = await cur.fetchall()
        rows = []
        for u in users:
            tg_id, fio, acad_group, points = u
            cur2 = await db.execute(
                "SELECT task_id FROM submissions WHERE user_id=? AND status='accepted'",
                (tg_id,)
            )
            tasks = await cur2.fetchall()
            task_list = ",".join(str(t[0]) for t in tasks)
            rows.append([fio, acad_group, points, task_list])

    # формируем таблицу
    headers = ["ФИО", "Группа", "Баллы", "Выполненные задания"]
    data = [headers] + rows

    # очищаем и обновляем
    sheet.clear()
    sheet.update("A1", data)


async def get_next_curator() -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM meta WHERE key='next_curator_idx'")
        row = await cur.fetchone()
        if not row:
            return None
        next_idx = int(row[0])
        cur2 = await db.execute("SELECT idx, fio, telegram_id FROM curators ORDER BY idx")
        all_cur = await cur2.fetchall()
        if not all_cur:
            return None
        # find curator with idx = next_idx, if not exists, wrap
        chosen = None
        for r in all_cur:
            if r[0] == next_idx:
                chosen = r
                break
        if not chosen:
            chosen = all_cur[0]
            next_idx = chosen[0]
        # compute new next
        # next index is next in list; if last, go to first
        idxs = [r[0] for r in all_cur]
        pos = idxs.index(chosen[0])
        new_pos = (pos + 1) % len(idxs)
        new_next = idxs[new_pos]
        await db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('next_curator_idx', ?)", (str(new_next),))
        await db.commit()
        return {"idx": chosen[0], "fio": chosen[1], "telegram_id": chosen[2]}


# ---- Utility functions ----
def task_by_id(tid: int) -> dict:
    for t in TASKS:
        if t["id"] == tid:
            return t
    raise ValueError("task not found")


async def user_has_accepted_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM submissions WHERE user_id=? AND status='accepted'", (user_id,))
        r = await cur.fetchone()
        return r[0]


async def submission_pending_for_task(user_id: int, task_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM submissions WHERE user_id=? AND task_id=? AND status='pending'",
                               (user_id, task_id))
        r = await cur.fetchone()
        return r[0] > 0


async def submission_accepted_for_task(user_id: int, task_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM submissions WHERE user_id=? AND task_id=? AND status='accepted'",
                               (user_id, task_id))
        r = await cur.fetchone()
        return r[0] > 0


def task_action_keyboard(task_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Отправить ответ", callback_data=f"send_{task_id}")
    builder.button(text="Подсказка", callback_data=f"hint_{task_id}")
    builder.button(text="Вернуться к списку заданий", callback_data="back_to_tasks")
    builder.adjust(1)  # все в колонку
    return builder.as_markup()


# клавиатура для куратора при проверке
def curator_check_kb(submission_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Зачесть ✅", callback_data=f"cur_accept_{submission_id}")
    builder.button(text="Не зачесть ❌", callback_data=f"cur_reject_{submission_id}")
    builder.adjust(2)
    return builder.as_markup()


# ---- Handlers ----
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    args = message.text.split(maxsplit=1)
    if len(args) > 1 and args[1].startswith("curator_invite_"):
        token = args[1].replace("curator_invite_", "")
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT value FROM meta WHERE key=?", (f"curator_token_{token}",))
            row = await cur.fetchone()
            if not row or row[0] != "valid":
                await message.answer("❌ Ссылка недействительна или уже использована.")
                return

            # Добавляем пользователя как куратора, если его ещё нет
            cur2 = await db.execute("SELECT COUNT(*) FROM curators WHERE telegram_id=?", (message.from_user.id,))
            count = (await cur2.fetchone())[0]
            if count == 0:
                fio = message.from_user.full_name
                await db.execute("INSERT INTO curators (fio, telegram_id) VALUES (?, ?)", (fio, message.from_user.id))
                await db.commit()
                await message.answer(f"✅ Вы успешно добавлены в список кураторов, {fio}!")
            else:
                await message.answer("✅ Вы уже есть в списке кураторов.")

            # Можно сделать, чтобы ссылка была одноразовой:
            await db.execute("DELETE FROM meta WHERE key=?", (f"curator_token_{token}",))
            await db.commit()
        return

    # Если не приглашение — обычная регистрация
    tg_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT fio FROM users WHERE tg_id=?", (tg_id,))
        r = await cur.fetchone()
        if r:
            await message.answer(
                "Вы уже зарегистрированы. Вот список заданий:",
                reply_markup=await tasks_keyboard_for_user(tg_id)
            )
            return
    await message.answer(
        "Привет! Добро пожаловать в бот конкурса «Староста года»! Для начала напишите, пожалуйста, свое ФИО:"
    )
    await state.set_state(StartStates.waiting_for_fio)


# ===== Временное хранилище подтверждений =====
pending_deletions = {}


def confirm_keyboard(entity_type: str, entity_id: int) -> InlineKeyboardMarkup:
    """Создаёт клавиатуру подтверждения удаления."""
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Подтвердить", callback_data=f"confirm_delete_{entity_type}_{entity_id}")
    kb.button(text="❌ Отмена", callback_data="cancel_delete")
    kb.adjust(2)
    return kb.as_markup()


async def is_submissions_closed() -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM meta WHERE key='submissions_closed'")
        row = await cur.fetchone()
        return row and row[0] == "true"


# === Команда админа: включить/выключить приём заданий ===
@dp.message(Command("stop_submissions"))
async def cmd_stop_submissions(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Команда доступна только администратору.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM meta WHERE key='submissions_closed'")
        row = await cur.fetchone()
        closed = row and row[0] == "true"

        new_state = "false" if closed else "true"
        await db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('submissions_closed', ?)", (new_state,))
        await db.commit()

    if new_state == "true":
        await message.answer("🚫 Приём заданий остановлен. Бот больше не принимает ответы от участников.")
        logging.info("⚠️ Приём заданий остановлен администратором.")
    else:
        await message.answer("✅ Приём заданий возобновлён.")
        logging.info("✅ Приём заданий возобновлён администратором.")


@dp.message(Command("check_unlinked"))
async def cmd_check_unlinked(message: types.Message):
    """Команда для проверки ответов от незарегистрированных пользователей или без куратора"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Команда доступна только администраторам.")
        return

    await message.answer("🔍 Проверяю базу на наличие неподключённых пользователей...")

    async with aiosqlite.connect(DB_PATH) as db:
        # --- 1️⃣ Пользователи, которых нет в таблице users, но есть submissions ---
        cur = await db.execute("""
            SELECT DISTINCT s.user_id 
            FROM submissions s
            LEFT JOIN users u ON s.user_id = u.tg_id
            WHERE u.tg_id IS NULL
        """)
        not_registered = [r[0] for r in await cur.fetchall()]

        # --- 2️⃣ Пользователи, у которых нет куратора ---
        cur = await db.execute("""
            SELECT tg_id 
            FROM users 
            WHERE curator_idx IS NULL OR curator_idx = ''
        """)
        no_curator = [r[0] for r in await cur.fetchall()]

    total_notified = 0
    failed = 0

    # --- Отправляем уведомления пользователям ---
    for uid in set(not_registered + no_curator):
        try:
            await bot.send_message(
                uid,
                "⚠️ Вы ещё не зарегистрированы или у вас не назначен куратор.\n\n"
                "Пожалуйста, обратитесь в техподдержку: @SG_RNIMU_tech"
            )
            total_notified += 1
            await asyncio.sleep(0.3)
        except Exception as e:
            logging.warning(f"Не удалось отправить уведомление пользователю {uid}: {e}")
            failed += 1

    summary = (
        f"📊 Проверка завершена!\n\n"
        f"👤 Не зарегистрированы: {len(not_registered)}\n"
        f"👥 Без куратора: {len(no_curator)}\n"
        f"📩 Уведомлено: {total_notified}\n"
        f"⚠️ Ошибок при отправке: {failed}"
    )

    await message.answer(summary)
    logging.info(summary)


# === Команда: удалить пользователя ===
@dp.message(Command("delete_user"))
async def cmd_delete_user(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Команда доступна только администратору.")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("📋 Использование: `/delete_user <telegram_id>`", parse_mode="Markdown")
        return

    try:
        user_id = int(args[1])
    except ValueError:
        await message.answer("⚠️ Укажите корректный Telegram ID пользователя.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT fio, acad_group FROM users WHERE tg_id=?", (user_id,))
        user = await cur.fetchone()

    if not user:
        await message.answer("❌ Пользователь с таким ID не найден.")
        return

    fio, group = user
    pending_deletions[message.from_user.id] = {"type": "user", "id": user_id}

    await message.answer(
        f"Вы уверены, что хотите удалить пользователя *{fio} ({group})*?",
        parse_mode="Markdown",
        reply_markup=confirm_keyboard("user", user_id)
    )


# === Команда: удалить куратора ===
@dp.message(Command("delete_curator"))
async def cmd_delete_curator(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Команда доступна только администратору.")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("📋 Использование: `/delete_curator <telegram_id>`", parse_mode="Markdown")
        return

    try:
        curator_tg = int(args[1])
    except ValueError:
        await message.answer("⚠️ Укажите корректный Telegram ID куратора.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT fio FROM curators WHERE telegram_id=?", (curator_tg,))
        row = await cur.fetchone()

    if not row:
        await message.answer("❌ Куратор с таким ID не найден.")
        return

    fio = row[0]
    pending_deletions[message.from_user.id] = {"type": "curator", "id": curator_tg}

    await message.answer(
        f"Вы уверены, что хотите удалить куратора *{fio}* (ID {curator_tg})?\n"
        f"Все его участники будут переназначены другим кураторам.",
        parse_mode="Markdown",
        reply_markup=confirm_keyboard("curator", curator_tg)
    )


# === Обработка подтверждения удаления ===
@dp.callback_query(lambda c: c.data.startswith("confirm_delete_"))
async def confirm_deletion(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("Недостаточно прав.", show_alert=True)
        return

    parts = cb.data.split("_")
    entity_type = parts[2]
    entity_id = int(parts[3])

    if entity_type == "user":
        async with aiosqlite.connect(DB_PATH) as db:
            # Удаляем данные пользователя
            await db.execute("DELETE FROM submissions WHERE user_id=?", (entity_id,))
            await db.execute("DELETE FROM users WHERE tg_id=?", (entity_id,))
            await db.commit()
        await cb.message.edit_text(f"🗑 Данные пользователя (ID {entity_id}) успешно удалены.")

    elif entity_type == "curator":
        async with aiosqlite.connect(DB_PATH) as db:
            # Получаем индекс удаляемого куратора
            cur = await db.execute("SELECT idx, fio FROM curators WHERE telegram_id=?", (entity_id,))
            row = await cur.fetchone()
            if not row:
                await cb.message.edit_text("❌ Куратор не найден.")
                return
            idx, fio = row

            # Получаем список всех кураторов
            cur = await db.execute("SELECT idx, telegram_id FROM curators ORDER BY idx")
            all_cur = await cur.fetchall()

            if len(all_cur) <= 1:
                await cb.message.edit_text("⚠️ Нельзя удалить последнего куратора.")
                return

            # Определяем нового куратора для переназначения
            ids = [c[0] for c in all_cur]
            pos = ids.index(idx)
            new_curator = all_cur[(pos + 1) % len(ids)]  # следующий по кругу
            new_idx, new_tg = new_curator

            # Обновляем пользователей
            await db.execute("UPDATE users SET curator_idx=? WHERE curator_idx=?", (new_idx, idx))
            await db.execute("DELETE FROM curators WHERE idx=?", (idx,))
            await db.commit()

        await cb.message.edit_text(
            f"🗑 Куратор *{fio}* удалён.\n"
            f"👥 Его участники переназначены куратору с ID {new_tg}.",
            parse_mode="Markdown"
        )

    pending_deletions.pop(cb.from_user.id, None)
    await cb.answer("Удаление выполнено ✅")


# === Отмена удаления ===
@dp.callback_query(lambda c: c.data == "cancel_delete")
async def cancel_delete(cb: types.CallbackQuery):
    pending_deletions.pop(cb.from_user.id, None)
    await cb.message.edit_text("❎ Удаление отменено.")
    await cb.answer("Действие отменено.")


@dp.message(Command("gen_curator_link"))
async def gen_curator_link(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Эта команда доступна только администратору.")
        return

    token = secrets.token_urlsafe(8)
    link = f"https://t.me/{(await bot.me()).username}?start=curator_invite_{token}"

    # можно сохранить токен в БД, если хочешь ограничить срок действия
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            (f"curator_token_{token}", "valid"),
        )
        await db.commit()

    await message.answer(f"🔗 Ссылка для добавления куратора:\n{link}")


@dp.message(Command("export"))
async def cmd_export(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Команда доступна только администратору.")
        return
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, lambda: asyncio.run(export_to_google_sheets()))
        await message.answer("✅ Рейтинг успешно экспортирован в Google Sheets.")
    except Exception as e:
        await message.answer(f"❌ Ошибка при экспорте: {e}")


# список заданий
@dp.message(Command("tasks"))
async def cmd_tasks(message: types.Message):
    await message.answer(
        "Выберите задание:",
        reply_markup=await tasks_keyboard_for_user(message.from_user.id)
    )


@dp.message(Command("profile"))
async def cmd_profile(message: types.Message):
    tg_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем, зарегистрирован ли пользователь
        cur = await db.execute("SELECT fio, acad_group, points FROM users WHERE tg_id=?", (tg_id,))
        user = await cur.fetchone()
        if not user:
            await message.answer("❌ Вы ещё не зарегистрированы. Отправьте /start, чтобы начать.")
            return

        fio, acad_group, points = user

        # Подсчитываем задания
        cur_done = await db.execute("SELECT COUNT(*) FROM submissions WHERE user_id=? AND status='accepted'", (tg_id,))
        done = (await cur_done.fetchone())[0]

        cur_pending = await db.execute("SELECT COUNT(*) FROM submissions WHERE user_id=? AND status='pending'",
                                       (tg_id,))
        pending = (await cur_pending.fetchone())[0]

    # Форматируем красивый вывод
    profile_text = (
        f"👤 *Твой профиль*\n\n"
        f"📛 *ФИО:* {fio}\n"
        f"🏫 *Группа:* {acad_group}\n"
        f"⭐ *Баллы:* {points}\n\n"
        f"📘 *Статистика заданий:*\n"
        f"✅ Выполнено: {done}\n"
        f"⏳ На проверке: {pending}\n"
    )

    await message.answer(profile_text, parse_mode="Markdown")


# Команда админа посмотреть статистику кураторов
@dp.message(Command('stats'))
async def cmd_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Команда доступна только администратору")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT idx, fio, telegram_id FROM curators ORDER BY idx")
        rows = await cur.fetchall()
        lines = []
        for r in rows:
            idx, fio, tg = r
            cur2 = await db.execute(
                "SELECT COUNT(*) FROM submissions WHERE status='pending' AND user_id IN (SELECT tg_id FROM users WHERE curator_idx=?)",
                (idx,))
            pending = (await cur2.fetchone())[0]
            lines.append(f"{fio} (idx={idx}, tg={tg}): {pending} pending")
    await message.answer("\n".join(lines) if lines else "Кураторы не найдены")


@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message, state: FSMContext):
    """Команда для начала рассылки всем пользователям"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Команда доступна только главным администраторам.")
        return

    await message.answer("✉️ Введите текст рассылки, который хотите отправить всем пользователям.\n\n"
                         "_Можно использовать форматирование Markdown._", parse_mode="Markdown")
    await state.set_state(BroadcastState.waiting_for_message)


@dp.message(BroadcastState.waiting_for_message)
async def process_broadcast_message(message: types.Message, state: FSMContext):
    text = message.text.strip()
    await state.clear()

    if not text:
        await message.answer("⚠️ Текст рассылки не может быть пустым.")
        return

    await message.answer("🚀 Начинаю рассылку всем пользователям...")

    delivered = 0
    failed = 0

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT tg_id FROM users")
        all_users = [r[0] for r in await cur.fetchall()]

    total = len(all_users)
    if total == 0:
        await message.answer("❌ Нет зарегистрированных пользователей для рассылки.")
        return

    # Отправляем по очереди с небольшой задержкой (чтобы не словить flood limit)
    for i, user_id in enumerate(all_users, 1):
        try:
            await bot.send_message(user_id, text, parse_mode="Markdown")
            delivered += 1
        except Exception as e:
            logging.warning(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
            failed += 1

        # обновляем статус каждые 10 сообщений
        if i % 10 == 0 or i == total:
            try:
                await message.answer(f"📬 Рассылка: {i}/{total} обработано "
                                     f"(✅ {delivered}, ❌ {failed})")
            except Exception:
                pass  # чтобы не спамил в консоль

        await asyncio.sleep(0.2)  # минимальная пауза между отправками

    summary = (
        f"✅ Рассылка завершена!\n\n"
        f"📤 Отправлено: {delivered}\n"
        f"⚠️ Ошибок: {failed}\n"
        f"👥 Всего пользователей: {total}"
    )
    await message.answer(summary)
    logging.info(summary)


@dp.message(StartStates.waiting_for_fio)
async def process_fio(message: types.Message, state: FSMContext):
    fio = message.text.strip()
    await state.update_data(fio=fio)
    await message.answer("Спасибо! Теперь напишите, пожалуйста, номер своей академической группы:")
    await state.set_state(StartStates.waiting_for_group)


@dp.message(StartStates.waiting_for_group)
async def process_group(message: types.Message, state: FSMContext):
    acad_group = message.text.strip()
    data = await state.get_data()
    fio = data.get("fio")
    tg_id = message.from_user.id
    # assign curator
    curator = await get_next_curator()
    curator_idx = curator["idx"] if curator else None
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO users (tg_id, fio, acad_group, curator_idx, points) VALUES (?,?,?,?,0)",
                         (tg_id, fio, acad_group, curator_idx))
        await db.commit()
    await message.answer(
        f"✅ Регистрация завершена!\n"
        f"Ваш куратор: {curator['fio'] if curator else 'не назначен'}.\n"
        f"Удачи в конкурсе, {fio}!"
    )

    # уведомляем куратора
    if curator:
        await bot.send_message(curator['telegram_id'], f"Новый участник {fio} ({acad_group}) назначен вам на проверку.")

    await state.clear()

    # краткое описание заданий с жирным форматированием
    tasks_intro = (
        "Спасибо! Ещё раз добро пожаловать на конкурс 🎉\n\n"
        "*Обязательно подпишись на канал тех. поддержки бота: @SG_RNIMU_tech *\n\n"
        "Там важные объявления, связанные с работой бота, а также там можно задать вопрос тех. поддержке\n\n"
        "Вот список заданий маршрутного листа и их краткое описание:\n\n"
        "*1️⃣ Знакомство* — составить анкету о себе (ФИО с фото, номер группы, институт) и ответить на вопрос, почему я стал(а) старостой.\n\n"
        "*2️⃣ Важные сведения* — чем занимается Второй отдел и где он находится?\n\n"
        "*3️⃣ Документы и не только* — что такое ДРПО, чем занимается, как туда записаться?\n\n"
        "*4️⃣ Дни варенья* — составить график дней рождения для своей группы.\n\n"
        "*5️⃣ Памятный кадр* — сделать необычное фото всей группой, которое вы запомните надолго.\n\n"
        "*6️⃣ Фото со звездой* — фото всей группы с преподавателем.\n\n"
        "*7️⃣ Нетворкинг* — познакомьтесь с тремя старостами из других институтов и сделайте фото вместе.\n\n"
        "*8️⃣ Красные дни календаря* — составьте красивое расписание коллоквиумов на семестр.\n\n"
        "*9️⃣ Часть команды* — вступите в профсоюз студентов.\n\n"
        "*🔟 Свети другим!* — снимите короткое доброе видео.\n\n"
        "*1️⃣1️⃣ Моё любимое!* — видео о вещах, которые вам нравятся в университете.\n\n"
        "*1️⃣2️⃣ Расширь кругозор!* — посетите мероприятие и снимите видео-отчёт.\n\n"
        "*1️⃣3️⃣ Проложи маршрут!* — создайте видео-маршрут.\n\n"
        "*1️⃣4️⃣ Суперзадание* (доступно после выполнения ≥3 заданий) — с группой посетить кино/театр/квиз.\n\n"
        "_С нетерпением ждём твоих ответов! 💪_"
    )

    await message.answer(tasks_intro, parse_mode="Markdown")

    # после описания — показать список доступных заданий
    await message.answer(
        "Теперь можешь выбрать первое задание 👇",
        reply_markup=await tasks_keyboard_for_user(tg_id)
    )


@dp.callback_query(lambda c: c.data and c.data.startswith('back_to_tasks'))
async def back_to_tasks(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "Вот список заданий:",
        reply_markup=await tasks_keyboard_for_user(cb.from_user.id)
    )
    await cb.answer()


@dp.callback_query(lambda c: c.data and c.data.startswith('task_'))
async def on_task_selected(cb: types.CallbackQuery):
    task_id = int(cb.data.split('_')[1])
    user_id = cb.from_user.id
    # check 13th availability
    if task_id == 14:
        accepted_count = await user_has_accepted_count(user_id)
        if accepted_count < 3:
            await cb.answer("Супер-задание доступно только после выполнения как минимум 3 заданий.", show_alert=True)
            return
    # check already accepted
    if await submission_accepted_for_task(user_id, task_id):
        await cb.answer("Вы уже выполнили это задание и оно зачтено.", show_alert=True)
        return
    # check pending
    if await submission_pending_for_task(user_id, task_id):
        await cb.answer("У вас уже есть решение этого задания на проверке.", show_alert=True)
        return
    t = task_by_id(task_id)
    details = TASKS_DETAILS.get(task_id, {})
    instruction = details.get("instruction", "Инструкция не найдена.")
    text = (
        f"*Задание {t['id']}. {t['title']}*\n"
        f"Баллы при подтверждении: {t['points']}\n\n"
        f"📘 *Инструкция:*\n{instruction}"
    )
    await cb.message.edit_text(text, parse_mode="Markdown", reply_markup=task_action_keyboard(task_id))


@dp.callback_query(lambda c: c.data and c.data.startswith('hint_'))
async def on_hint(cb: types.CallbackQuery):
    task_id = int(cb.data.split('_')[1])
    # For simplicity, hints are generic. In a real implementation put specific hints.
    task_id = int(cb.data.split('_')[1])
    hint = TASKS_DETAILS.get(task_id, {}).get("hint", "Подсказка недоступна.")
    await cb.answer(f"💡 {hint}", show_alert=True)


@dp.callback_query(lambda c: c.data and c.data.startswith('send_'))
async def on_send_answer(cb: types.CallbackQuery, state: FSMContext):
    task_id = int(cb.data.split('_')[1])
    user_id = cb.from_user.id

    # Проверки
    if await is_submissions_closed():
        await cb.answer("🚫 Приём заданий завершён. Спасибо за участие!", show_alert=True)
        return
    # --- Проверяем, зарегистрирован ли пользователь и есть ли у него куратор ---
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT curator_idx FROM users WHERE tg_id=?", (user_id,))
        row = await cur.fetchone()

        if not row:
            await cb.answer(
                "🚫 Вы ещё не зарегистрированы!\n\n"
                "Используйте команду /start для регистрации.\n\n"
                "Также подпишитесь на канал поддержки @SG_RNIMU_tech — там публикуются важные объявления "
                "и можно задать вопрос техподдержке.",
                show_alert=True
            )
            return

        curator_idx = row[0]
        if curator_idx is None:
            await cb.answer(
                "⚠️ У вас пока не назначен куратор. Пожалуйста, обратитесь в техподдержку @SG_RNIMU_tech",
                show_alert=True
            )
            return

    if await submission_accepted_for_task(user_id, task_id):
        await cb.answer("Вы уже выполнили это задание и оно зачтено.", show_alert=True)
        return
    if await submission_pending_for_task(user_id, task_id):
        await cb.answer("У вас уже есть решение этого задания на проверке.", show_alert=True)
        return
    if task_id == 14:
        accepted_count = await user_has_accepted_count(user_id)
        if accepted_count < 3:
            await cb.answer("Супер-задание доступно только после выполнения как минимум 3 заданий.", show_alert=True)
            return

    # Получаем задание
    t = task_by_id(task_id)
    required_type = t["type"]

    # Определяем текст подсказки по типу
    format_texts = {
        "text": "✍️ Отправьте текстовый ответ:",
        "photo": "📸 Отправьте фото:",
        "video": "🎥 Отправьте видео:",
        "photo_text": "📸 Сначала отправьте фото, затем ✍️ текстовое пояснение.",
        "photo_multi": "📸 Отправьте несколько фото (можно подряд):",
        "photo_video": "📸📹 Отправьте не больше 10 фото или видео \n\n*Для отправки ответа на проверку используйте команду /done *",
    }
    prompt = format_texts.get(required_type, "Отправьте ответ в нужном формате (текст/фото/видео):")

    # Для photo_text — особый FSM
    if required_type == "photo_text":
        await state.set_state(AnswerFSM.waiting_for_photo)
        await state.update_data(task_id=task_id)
        await cb.message.answer(prompt)
        await cb.answer()
        return

    # Для всех остальных типов
    await state.update_data(task_id=task_id)
    await cb.message.answer(prompt, parse_mode="Markdown")
    await state.set_state(SubmitStates.waiting_for_answer)
    await cb.answer()


@dp.message(AnswerFSM.waiting_for_photo, F.photo)
async def handle_photo_for_task(message: types.Message, state: FSMContext):
    await state.update_data(photo_id=message.photo[-1].file_id)
    await state.set_state(AnswerFSM.waiting_for_text)
    await message.answer("✍ Теперь отправьте текстовое пояснение:")


@dp.message(AnswerFSM.waiting_for_text, F.text)
async def handle_text_for_task(message: types.Message, state: FSMContext):
    data = await state.get_data()
    task_id = data["task_id"]
    user_id = message.from_user.id
    photo_id = data["photo_id"]
    text = message.text.strip()

    content_type = "photo_text"
    content = f"photo:{photo_id}|text:{text}"
    now = datetime.utcnow().isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO submissions (user_id, task_id, status, content_type, content, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (user_id, task_id, 'pending', content_type, content, now, now),
        )
        await db.commit()
        cur2 = await db.execute("SELECT curator_idx, fio FROM users WHERE tg_id=?", (user_id,))
        u = await cur2.fetchone()
        curator_idx, user_name = u[0], u[1]
        cur3 = await db.execute("SELECT telegram_id FROM curators WHERE idx=?", (curator_idx,))
        c = await cur3.fetchone()
        curator_tg = c[0] if c else None

    await message.answer("✅ Ваш ответ отправлен куратору.")
    await cmd_tasks(message)
    if curator_tg:
        await notify_curator_new_answer(curator_tg, curator_idx)

    await state.clear()


# ===== клавиатура для начала проверки =====
def curator_start_check_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Начать проверку ▶️", callback_data="curator_start_check")
    return builder.as_markup()


# ===== клавиатура для куратора при проверке =====
def curator_check_kb(submission_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Зачесть ✅", callback_data=f"cur_accept_{submission_id}")
    builder.button(text="Не зачесть ❌", callback_data=f"cur_reject_{submission_id}")
    builder.adjust(2)
    return builder.as_markup()


# ===== при появлении нового ответа от студента =====
async def notify_curator_new_answer(curator_tg: int, curator_idx: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM submissions WHERE status='pending' "
            "AND user_id IN (SELECT tg_id FROM users WHERE curator_idx=?)",
            (curator_idx,))
        pending_count = (await cur.fetchone())[0]

    await bot.send_message(
        curator_tg,
        f"У вас новый ответ от участника!\nНепроверенных: {pending_count}",
        reply_markup=curator_start_check_kb()
    )


@dp.message(SubmitStates.waiting_for_answer, F.text.casefold() == "/done")
async def handle_done_command(message: types.Message, state: FSMContext):
    """
    Завершает приём фото/видео для заданий типа photo_video.
    """
    data = await state.get_data()
    task_id = data.get('task_id')
    t = task_by_id(task_id)
    user_id = message.from_user.id

    if t["type"] != "photo_video":
        await message.answer("⚠️ Команда /done используется только для заданий с типом фото/видео.")
        return

    collected = data.get("collected_media", [])
    if not collected:
        await message.answer("❗ Вы ещё не отправили ни одного фото или видео.")
        return

    content_type = "photo_video"
    content = "|".join(collected)
    now = datetime.utcnow().isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO submissions (user_id, task_id, status, content_type, content, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (user_id, task_id, 'pending', content_type, content, now, now),
        )
        await db.commit()

        # получаем куратора
        cur2 = await db.execute("SELECT curator_idx, fio FROM users WHERE tg_id=?", (user_id,))
        u = await cur2.fetchone()
        curator_idx, user_name = u[0], u[1]
        cur3 = await db.execute("SELECT telegram_id FROM curators WHERE idx=?", (curator_idx,))
        c = await cur3.fetchone()
        curator_tg = c[0] if c else None

    await message.answer("✅ Все медиа получены и отправлены куратору.")
    if curator_tg:
        await notify_curator_new_answer(curator_tg, curator_idx)

    await state.clear()


@dp.message(SubmitStates.waiting_for_answer)
async def receive_answer(message: types.Message, state: FSMContext):
    data = await state.get_data()
    task_id = data.get('task_id')
    user_id = message.from_user.id
    t = task_by_id(task_id)
    required = t['type']
    is_text = bool(message.text and message.text.strip())
    is_photo = bool(message.photo)
    is_video = bool(message.video or message.video_note)
    is_media_group = hasattr(message, "media_group_id")  # альбом (несколько фото/видео)

    valid = False
    content_type, content = None, None

    # === text ===
    if required == 'text' and is_text:
        valid = True
        content_type, content = 'text', message.text.strip()

    # === photo / photo_multi ===
    elif required in ('photo', 'photo_multi') and is_photo:
        if required == 'photo':
            # одиночное фото
            valid = True
            content_type = 'photo'
            content = message.photo[-1].file_id
        else:
            # === сбор альбома ===
            data = await state.get_data()
            current_group_id = data.get("media_group_id")
            collected = data.get("collected_photos", [])
            group_id = message.media_group_id

            # если новое media_group_id — сбрасываем коллекцию
            if current_group_id != group_id:
                collected = []
                await state.update_data(media_group_id=group_id)

            collected.append(message.photo[-1].file_id)
            await state.update_data(collected_photos=collected)

            # запускаем отложенную отправку альбома
            async def finalize_album():
                await asyncio.sleep(1.5)  # ждём пока Telegram дошлёт остальные фото
                state_data = await state.get_data()
                if state_data.get("media_group_id") == group_id:
                    photos = state_data.get("collected_photos", [])
                    if not photos:
                        return
                    valid_local = True
                    content_type_local = "photo_multi"
                    content_local = "|".join(f"photo:{pid}" for pid in photos)

                    # очищаем временные данные
                    await state.update_data(collected_photos=[], media_group_id=None)

                    now = datetime.utcnow().isoformat()
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            "INSERT INTO submissions (user_id, task_id, status, content_type, content, created_at, updated_at) "
                            "VALUES (?,?,?,?,?,?,?)",
                            (user_id, task_id, 'pending', content_type_local, content_local, now, now),
                        )
                        await db.commit()

                        cur2 = await db.execute("SELECT curator_idx FROM users WHERE tg_id=?", (user_id,))
                        curator_idx = (await cur2.fetchone())[0]
                        cur3 = await db.execute("SELECT telegram_id FROM curators WHERE idx=?", (curator_idx,))
                        curator_tg = (await cur3.fetchone())[0]

                    await message.answer("✅ Ваш альбом успешно отправлен куратору на проверку.")
                    await cmd_tasks(message)
                    if curator_tg:
                        await notify_curator_new_answer(curator_tg, curator_idx)
                    await state.clear()

            # запускаем сбор альбома в фоне
            asyncio.create_task(finalize_album())
            return

    # === video ===
    elif required == 'video' and is_video:
        valid = True
        content_type, content = 'video', (message.video or message.video_note).file_id

    # === photo_text ===
    elif required == 'photo_text' and (is_photo and is_text):
        valid = True
        content_type = 'photo_text'
        content = f"photo:{message.photo[-1].file_id}|text:{message.text.strip()}"

    # === photo_video (расширенный тип) ===
    elif required == 'photo_video':
        media_ids = []

        # Обработка фото
        if is_photo:
            media_ids.append(f"photo:{message.photo[-1].file_id}")
            prev_media = (await state.get_data()).get("collected_media", [])
            prev_media += media_ids
            await state.update_data(collected_media=prev_media)

            if len(prev_media) < 10:
                if not message.media_group_id:
                    await message.answer(
                        "📸 Медиа получено. Можете отправить ещё фото или видео, либо напишите /done когда закончите.\n\n"
                        "*Суммарное количество всех отправленных фото/видео не должно быть больше 10!*",
                        parse_mode="Markdown"
                    )
                return
            else:
                collected = prev_media[:10]
                valid = True
                content_type = "photo_video"
                content = "|".join(collected)
                await message.answer(
                    "❗ Достигнут лимит в 10 медиафайлов!\n\nОтвет автоматически отправляется куратору на проверку!"
                )

        # Обработка видео
        if is_video:
            vid = message.video or message.video_note
            media_ids.append(f"video:{vid.file_id}")
            prev_media = (await state.get_data()).get("collected_media", [])
            prev_media += media_ids
            await state.update_data(collected_media=prev_media)

            if len(prev_media) < 10:
                if not message.media_group_id:
                    await message.answer(
                        "📸 Медиа получено. Можете отправить ещё фото или видео, либо напишите /done когда закончите.\n\n"
                        "*Суммарное количество всех отправленных фото/видео не должно быть больше 10!*",
                        parse_mode="Markdown"
                    )
                return
            else:
                collected = prev_media[:10]
                valid = True
                content_type = "photo_video"
                content = "|".join(collected)
                await message.answer(
                    "❗ Достигнут лимит в 10 медиафайлов!\n\nОтвет автоматически отправляется куратору на проверку!"
                )

        # # Если одиночное фото или видео — принимаем сразу
        # if media_ids:
        #     valid = True
        #     content_type = 'photo_video'
        #     content = "|".join(media_ids)

    # === Завершение отправки альбома (/done) ===
    elif message.text and message.text.strip().lower() == "/done" and required == "photo_video":
        collected = (await state.get_data()).get("collected_media", [])
        if not collected:
            await message.answer("❗ Вы ещё не отправили ни одного фото или видео.")
            return
        if len(collected) > 10:
            collected = collected[:10]
        valid = True
        content_type = "photo_video"
        content = "|".join(collected)

    # === Ошибка формата ===
    if not valid:
        await message.answer("⚠️ Неподходящий тип ответа. Пожалуйста, отправьте ответ с подходящим типом!")
        return

    now = datetime.utcnow().isoformat()

    # === Запись в базу ===
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO submissions (user_id, task_id, status, content_type, content, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (user_id, task_id, 'pending', content_type, content, now, now),
        )
        await db.commit()

        # получаем куратора
        cur2 = await db.execute("SELECT curator_idx, fio FROM users WHERE tg_id=?", (user_id,))
        u = await cur2.fetchone()
        curator_idx, user_name = u[0], u[1]
        cur3 = await db.execute("SELECT telegram_id FROM curators WHERE idx=?", (curator_idx,))
        c = await cur3.fetchone()
        curator_tg = c[0] if c else None

    await message.answer("✅ Ваш ответ отправлен на проверку куратору.")
    await cmd_tasks(message)
    if curator_tg:
        await notify_curator_new_answer(curator_tg, curator_idx)

    await state.clear()


# ===== куратор нажал 'Начать проверку' =====
@dp.callback_query(lambda c: c.data == "curator_start_check")
async def curator_start_check(cb: types.CallbackQuery):
    await send_next_submission_to_curator(cb.from_user.id)
    await cb.answer()


# ===== выдаём куратору следующий ответ =====
async def send_next_submission_to_curator(curator_tg: int):
    async with aiosqlite.connect(DB_PATH) as db:
        while True:
            cur = await db.execute("""
                SELECT s.id, s.user_id, s.task_id, s.content_type, s.content, u.fio
                FROM submissions s
                JOIN users u ON u.tg_id = s.user_id
                WHERE s.status='pending' 
                  AND u.curator_idx=(SELECT idx FROM curators WHERE telegram_id=?)
                ORDER BY s.created_at ASC
                LIMIT 1
            """, (curator_tg,))
            row = await cur.fetchone()

            # нет заданий
            if not row:
                await bot.send_message(curator_tg, "✅ Все задания проверены.")
                asyncio.create_task(export_to_google_sheets())
                return

            submission_id, user_id, task_id, content_type, content, user_name = row

            # 🔍 проверяем, не зачтено ли уже это задание у пользователя
            cur2 = await db.execute("""
                SELECT COUNT(*) FROM submissions 
                WHERE user_id=? AND task_id=? AND status='accepted'
            """, (user_id, task_id))
            already_accepted = (await cur2.fetchone())[0] > 0

            if already_accepted:
                # Удаляем дубликат, не показываем куратору
                await db.execute("DELETE FROM submissions WHERE id=?", (submission_id,))
                await db.commit()
                continue  # берём следующее (если есть)

            # если не зачтено — показываем куратору
            break

    t = task_by_id(task_id)

    async def safe_send_captioned_photo(chat_id: int, photo_id: str, caption: str):
        # Telegram caption limit ≈ 1024. Обрезаем и отправляем остаток отдельным сообщением.
        if not caption:
            await bot.send_photo(chat_id, photo_id)
            return
        caption_to_send = caption[:1024]
        remainder = caption[1024:]
        try:
            await bot.send_photo(chat_id, photo_id, caption=caption_to_send)
        except TelegramBadRequest:
            # если с caption не получилось, отправим фото без caption
            await bot.send_photo(chat_id, photo_id)
            # и отправим caption как текст (в кусках по 4000)
            if caption_to_send:
                for i in range(0, len(caption), 4000):
                    await bot.send_message(chat_id, caption[i:i + 4000])
            return
        # если был остаток — отправляем как отдельный текст
        if remainder:
            for i in range(0, len(remainder), 4000):
                await bot.send_message(chat_id, remainder[i:i + 4000])

    async def safe_send_media_group(chat_id: int, media_list: List[types.InputMedia]):
        try:
            await bot.send_media_group(chat_id, media=media_list)
        except TelegramBadRequest as e:
            # fallback: отправляем по одному элементу (без caption)
            for m in media_list:
                try:
                    if isinstance(m, types.InputMediaPhoto):
                        await bot.send_photo(chat_id, m.media)
                    elif isinstance(m, types.InputMediaVideo):
                        await bot.send_video(chat_id, m.media)
                except Exception:
                    logging.exception("Ошибка при fallback-отправке медиа куратору")

    # отправка в зависимости от типа
    if content_type == "text":
        try:
            await bot.send_message(curator_tg, f"📝 Ответ:\n{content}")
        except Exception:
            logging.exception("Failed to send text to curator")

    elif content_type == "photo":
        text_msg = ""  # если нужно: получить подпись из БД/других полей
        await safe_send_captioned_photo(curator_tg, content, text_msg)

    elif content_type == "video":
        try:
            await bot.send_video(curator_tg, content)
        except TelegramBadRequest:
            # если ошибка — отправим ссылку/ид без подписи
            await bot.send_message(curator_tg,
                                   "Не удалось отправить видео напрямую. Пожалуйста, проверьте исходный файл.")

    elif content_type == "photo_text":
        parts = content.split("|")
        photo_id = parts[0].split(":")[1]
        text_msg = parts[1].split(":", 1)[1] if len(parts) > 1 else ""
        await safe_send_captioned_photo(curator_tg, photo_id, text_msg)

    elif content_type == "photo_multi":
        media_parts = content.split("|")
        media_group = [
            types.InputMediaPhoto(media=part.replace("photo:", ""))
            for part in media_parts if part.startswith("photo:")
        ]
        if len(media_group) == 1:
            await bot.send_photo(curator_tg, media_group[0].media)
        elif len(media_group) > 1:
            await safe_send_media_group(curator_tg, media_group)

    elif content_type == "photo_video":
        media_parts = content.split("|")
        media_group = []
        for part in media_parts:
            if part.startswith("photo:"):
                media_group.append(types.InputMediaPhoto(media=part.replace("photo:", "")))
            elif part.startswith("video:"):
                media_group.append(types.InputMediaVideo(media=part.replace("video:", "")))
        if len(media_group) == 1:
            m = media_group[0]
            if isinstance(m, types.InputMediaPhoto):
                await bot.send_photo(curator_tg, m.media)
            elif isinstance(m, types.InputMediaVideo):
                await bot.send_video(curator_tg, m.media)
        elif len(media_group) > 1:
            await safe_send_media_group(curator_tg, media_group)

    info_text = (
        f"📋 *Задание {t['id']}. {t['title']}*\n"
        f"👤 От участника: {user_name}\n"
        f"🆔 Submission ID: {submission_id}"
    )

    await bot.send_message(
        curator_tg,
        info_text,
        parse_mode="Markdown",
        reply_markup=curator_check_kb(submission_id)
    )


# ===== куратор зачёл =====
@dp.callback_query(lambda c: c.data.startswith("cur_accept_"))
async def curator_accept(cb: types.CallbackQuery):
    submission_id = int(cb.data.split('_')[-1])
    curator_tg = cb.from_user.id
    now = datetime.utcnow().isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        # Получаем данные о задании
        cur = await db.execute("SELECT user_id, task_id, status FROM submissions WHERE id=?", (submission_id,))
        row = await cur.fetchone()
        if not row:
            await cb.answer("Задание не найдено", show_alert=True)
            return

        user_id, task_id, status = row

        # Проверка: уже зачтено или нет
        if status != "pending":
            await cb.answer("Это задание уже проверено ⚠️", show_alert=True)
            return

        # Проверка: нет ли уже зачтённого варианта этого задания у пользователя
        cur = await db.execute("""
            SELECT COUNT(*) FROM submissions 
            WHERE user_id=? AND task_id=? AND status='accepted'
        """, (user_id, task_id))
        already_accepted = (await cur.fetchone())[0] > 0
        if already_accepted:
            await db.execute(
                "UPDATE submissions SET status='duplicate', updated_at=? WHERE id=?",
                (now, submission_id),
            )
            await db.commit()
            await cb.answer("⚠️ Это задание уже зачтено ранее. Баллы не начислены.", show_alert=True)
            return

        # Обновляем статус и начисляем баллы
        await db.execute(
            "UPDATE submissions SET status='accepted', updated_at=? WHERE id=?",
            (now, submission_id),
        )
        points = task_by_id(task_id)['points']
        await db.execute("UPDATE users SET points = points + ? WHERE tg_id=?", (points, user_id))
        await db.commit()

        cur2 = await db.execute("SELECT points FROM users WHERE tg_id=?", (user_id,))
        new_points = (await cur2.fetchone())[0]

    # Уведомляем участника
    await bot.send_message(user_id, f"✅ Ваше задание {task_id} зачтено. +{points} баллов. Всего: {new_points}")

    # Убираем кнопки у куратора
    try:
        await cb.message.edit_reply_markup()
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise

    await cb.answer("✅ Задание зачтено")

    # После — перейти к следующему
    await send_next_submission_to_curator(curator_tg)


# ===== куратор отклонил =====
@dp.callback_query(lambda c: c.data.startswith("cur_reject_"))
async def curator_reject(cb: types.CallbackQuery, state: FSMContext):
    submission_id = int(cb.data.split('_')[-1])

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT status FROM submissions WHERE id=?", (submission_id,))
        row = await cur.fetchone()
        if not row:
            await cb.answer("Задание не найдено", show_alert=True)
            return

        status = row[0]
        if status != "pending":
            await cb.answer("Это задание уже проверено ⚠️", show_alert=True)
            return

    # убираем клавиатуру, чтобы нельзя было жать повторно
    try:
        await cb.message.edit_reply_markup()
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            pass
        else:
            raise

    await cb.message.answer("Напишите причину отказа:")
    await state.update_data(reject_submission=submission_id)
    await cb.answer()


@dp.message()
async def handle_curator_reject_reason(message: types.Message, state: FSMContext):
    data = await state.get_data()
    submission_id = data.get("reject_submission")
    if not submission_id:
        return  # это обычное сообщение, не причина отклонения

    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id, task_id FROM submissions WHERE id=?", (submission_id,))
        user_id, task_id = await cur.fetchone()
        await db.execute(
            "UPDATE submissions SET status='rejected', curator_comment=?, updated_at=? WHERE id=?",
            (message.text, now, submission_id))
        await db.commit()

    await bot.send_message(user_id, f"Ваше задание {task_id} не зачтено ❌\nПричина: {message.text}")
    await message.answer("Комментарий отправлен участнику ✅")
    await state.clear()

    # после отклонения → следующее задание
    await send_next_submission_to_curator(message.from_user.id)


def get_gs_client():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    return gspread.authorize(creds)


async def export_to_google_sheets():
    try:
        client = get_gs_client()

        try:
            spreadsheet = client.open(SPREADSHEET_NAME)
        except gspread.SpreadsheetNotFound:
            spreadsheet = client.create(SPREADSHEET_NAME)
            spreadsheet.share('', perm_type='anyone', role='writer')

        try:
            sheet = spreadsheet.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows="300", cols="20")

        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT tg_id, fio, acad_group, points FROM users")
            users = await cur.fetchall()
            rows = []
            for u in users:
                tg_id, fio, acad_group, points = u
                cur2 = await db.execute(
                    "SELECT task_id FROM submissions WHERE user_id=? AND status='accepted'", (tg_id,))
                tasks = await cur2.fetchall()
                task_list = ",".join(str(t[0]) for t in tasks)
                rows.append([fio, acad_group, points, task_list])

        # сортировка по баллам
        rows.sort(key=lambda r: r[2], reverse=True)

        # добавляем места
        rows_with_rank = []
        rank = 1
        for i, row in enumerate(rows):
            if i > 0 and row[2] < rows[i - 1][2]:
                rank = i + 1
            rows_with_rank.append([rank] + row)

        header = ["Место", "ФИО", "Группа", "Баллы", "Выполненные задания"]

        sheet.clear()
        sheet.update(values=[header] + rows_with_rank, range_name="A1")

        logging.info("✅ Google Sheets обновлены (автоматически).")

    except Exception as e:
        logging.error(f"❌ Ошибка при обновлении Google Sheets: {e}")


async def on_startup(dp):
    await init_db()
    asyncio.create_task(backup_scheduler())

    # регистрируем команды для удобства
    commands = [
        BotCommand(command="start", description="Начать работу"),
        BotCommand(command="tasks", description="Список заданий"),
        BotCommand(command="profile", description="Мой профиль"),
    ]
    await bot.set_my_commands(commands)


# Запуск
async def main():
    await init_db()
    await on_startup(dp)
    print("Bot started")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(main())
