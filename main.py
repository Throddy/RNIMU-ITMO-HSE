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
import csv
import os
from datetime import datetime
from typing import List, Optional
import logging

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import BotCommand
from aiogram import F
from aiogram import Router
import aiosqlite
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from aiogram.exceptions import TelegramBadRequest
import secrets

logging.basicConfig(level=logging.INFO)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # установите в .env
CURATORS_CSV = os.getenv("CURATORS_CSV", "curators.csv")
DB_PATH = os.getenv("DB_PATH", "bot.db")
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
SPREADSHEET_NAME = "Староста года"  # название таблицы
SHEET_NAME = "Рейтинг"  # лист для рейтинга

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не установлен в переменных окружения")

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ---- Конфигурация заданий (соответствует документу пользователя) ----
TASKS = [
    {"id": 1, "title": "Знакомство", "type": "photo_text", "points": 1},
    {"id": 2, "title": "Важные сведения", "type": "text", "points": 1},
    {"id": 3, "title": "Документы и не только", "type": "text", "points": 1},
    {"id": 4, "title": "Дни варенья", "type": "text_photo", "points": 1},
    {"id": 5, "title": "Памятный кадр", "type": "photo", "points": 2},
    {"id": 6, "title": "Фото со звездой", "type": "photo", "points": 2},
    {"id": 7, "title": "Нетворкинг", "type": "photo_multi", "points": 2},
    {"id": 8, "title": "Красные дни календаря", "type": "photo", "points": 2},
    {"id": 9, "title": "Свети другим!", "type": "video", "points": 3},
    {"id": 10, "title": "Моё любимое!", "type": "video", "points": 3},
    {"id": 11, "title": "Расширь кругозор!", "type": "video", "points": 3},
    {"id": 12, "title": "Проложи маршрут!", "type": "video", "points": 3},
    {"id": 13, "title": "Суперзадание", "type": "photo_video", "points": 10},
]


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


async def load_curators_from_csv_if_empty():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM curators")
        r = await cur.fetchone()
        if r[0] == 0:
            # load from CSV
            if not os.path.exists(CURATORS_CSV):
                print(f"curators csv ({CURATORS_CSV}) not found. Create it with fio,telegram_id")
                return
            with open(CURATORS_CSV, encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue
                    cor_row = row[0].split(',')
                    fio = cor_row[0].strip()
                    tg = int(cor_row[1].strip())
                    await db.execute("INSERT INTO curators (fio, telegram_id) VALUES (?,?)", (fio, tg))
            # set meta next_curator_idx
            await db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('next_curator_idx', '1')")
            await db.commit()
            print("Curators loaded from CSV")


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
        "Привет! Добро пожаловать в бот конкурса 'Староста года'! Для начала напишите, пожалуйста, свое ФИО."
    )
    await state.set_state(StartStates.waiting_for_fio)



@dp.message(Command("gen_curator_link"))
async def gen_curator_link(message: types.Message):
    if message.from_user.id != ADMIN_ID:
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
    if message.from_user.id != ADMIN_ID:
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


@dp.message(Command("menu"))
async def cmd_menu(message: types.Message):
    # просто вызываем обработчик tasks
    await cmd_tasks(message)


@dp.message(StartStates.waiting_for_fio)
async def process_fio(message: types.Message, state: FSMContext):
    fio = message.text.strip()
    await state.update_data(fio=fio)
    await message.answer("Спасибо! Теперь напишите, пожалуйста, номер своей академической группы.")
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
        f"Регистрация завершена! Ваш куратор: {curator['fio'] if curator else 'не назначен'}.\nУдачи в конкурсе!")
    # notify curator
    if curator:
        await bot.send_message(curator['telegram_id'], f"Новый участник {fio} ({acad_group}) назначен вам на проверку.")
    await state.clear()
    await message.answer(
        f"Регистрация завершена! Ваш куратор: {curator['fio'] if curator else 'не назначен'}.\n"
        "Вот список заданий:",
        reply_markup=tasks_keyboard_for_user(tg_id)
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
    if task_id == 13:
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
    text = f"Задание {t['id']}. {t['title']}\nБаллы при подтверждении: {t['points']}\n\nИнструкция: ... (см. полное описание)"
    await cb.message.edit_text(text, reply_markup=task_action_keyboard(task_id))
    await cb.answer()


@dp.callback_query(lambda c: c.data and c.data.startswith('hint_'))
async def on_hint(cb: types.CallbackQuery):
    task_id = int(cb.data.split('_')[1])
    # For simplicity, hints are generic. In a real implementation put specific hints.
    await cb.answer("Подсказка: воспользуйтесь идеями из описания задания.")


@dp.callback_query(lambda c: c.data and c.data.startswith('send_'))
async def on_send_answer(cb: types.CallbackQuery, state: FSMContext):
    task_id = int(cb.data.split('_')[1])
    user_id = cb.from_user.id
    # check same validations as above
    if await submission_accepted_for_task(user_id, task_id):
        await cb.answer("Вы уже выполнили это задание и оно зачтено.", show_alert=True)
        return
    if await submission_pending_for_task(user_id, task_id):
        await cb.answer("У вас уже есть решение этого задания на проверке.", show_alert=True)
        return
    if task_id == 13:
        accepted_count = await user_has_accepted_count(user_id)
        if accepted_count < 3:
            await cb.answer("Супер-задание доступно только после выполнения как минимум 3 заданий.", show_alert=True)
            return
    t = task_by_id(task_id)
    if t["type"] == "photo_text":
        await state.set_state(AnswerFSM.waiting_for_photo)
        await state.update_data(task_id=task_id)
        await cb.message.answer("📸 Отправьте фото для этого задания:")
        await cb.answer()
        return
    await state.update_data(task_id=task_id)
    await cb.message.answer("Отправьте ответ в нужном формате (текст/фото/видео):")
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


# ===== студент отправил ответ =====
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

    valid = False
    content_type, content = None, None

    if required == 'text' and is_text:
        valid = True
        content_type, content = 'text', message.text.strip()
    elif required in ('photo', 'photo_text', 'photo_multi') and is_photo:
        valid = True
        content_type, content = 'photo', message.photo[-1].file_id
    elif required == 'video' and is_video:
        valid = True
        content_type, content = 'video', (message.video or message.video_note).file_id
    elif required == 'photo_video' and (is_photo or is_video):
        valid = True
        if is_photo:
            content_type, content = 'photo', message.photo[-1].file_id
        else:
            content_type, content = 'video', message.video.file_id
    elif required == 'photo_text' and (is_photo and is_text):
        valid = True
        content_type = 'photo_text'
        content = f"photo:{message.photo[-1].file_id}|text:{message.text.strip()}"

    if not valid:
        await message.answer("Неподходящий тип ответа. Пожалуйста, отправьте в правильном формате.")
        return

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

    await message.answer("Ваш ответ отправлен на проверку куратору.")

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

    if not row:
        await bot.send_message(curator_tg, "Все задания проверены ✅")
        asyncio.create_task(export_to_google_sheets())
        return

    submission_id, user_id, task_id, content_type, content, user_name = row
    t = task_by_id(task_id)

    # отправляем контент
    if content_type == "text":
        await bot.send_message(curator_tg, content)
    elif content_type == "photo":
        await bot.send_photo(curator_tg, content)
    elif content_type == "video":
        await bot.send_video(curator_tg, content)
    elif content_type == "photo_text":
        parts = content.split("|")
        photo_id = parts[0].split(":")[1]
        text_msg = parts[1].split(":", 1)[1]
        await bot.send_photo(curator_tg, photo_id, caption=text_msg)

    # инфо + кнопки
    text = (
        f"Задание {t['id']}. {t['title']}\n"
        f"От участника: {user_name} (id: {user_id})\n"
        f"Submission ID: {submission_id}"
    )
    await bot.send_message(curator_tg, text, reply_markup=curator_check_kb(submission_id))


# ===== куратор зачёл =====
@dp.callback_query(lambda c: c.data.startswith("cur_accept_"))
async def curator_accept(cb: types.CallbackQuery):
    submission_id = int(cb.data.split('_')[-1])
    curator_tg = cb.from_user.id
    now = datetime.utcnow().isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id, task_id, status FROM submissions WHERE id=?", (submission_id,))
        row = await cur.fetchone()
        if not row:
            await cb.answer("Задание не найдено", show_alert=True)
            return

        user_id, task_id, status = row
        if status != "pending":
            await cb.answer("Это задание уже проверено ⚠️", show_alert=True)
            return

        # обновляем статус
        await db.execute("UPDATE submissions SET status='accepted', updated_at=? WHERE id=?", (now, submission_id))
        points = task_by_id(task_id)['points']
        await db.execute("UPDATE users SET points = points + ? WHERE tg_id=?", (points, user_id))
        await db.commit()

        cur2 = await db.execute("SELECT points FROM users WHERE tg_id=?", (user_id,))
        new_points = (await cur2.fetchone())[0]

    # уведомляем участника
    await bot.send_message(user_id, f"Ваше задание {task_id} зачтено ✅. +{points} баллов. Всего: {new_points}")

    # удаляем кнопки из старого сообщения
    try:
        await cb.message.edit_reply_markup()
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            pass
        else:
            raise

    await cb.answer("Задание зачтено ✅")

    # переходим к следующему
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


# Профиль пользователя
@dp.message(Command("profile"))
async def cmd_profile(message: types.Message):
    tg = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT fio, acad_group, points FROM users WHERE tg_id=?", (tg,))
        u = await cur.fetchone()
        if not u:
            await message.answer("Вы не зарегистрированы. Отправьте /start")
            return
        fio, acad_group, points = u
        cur2 = await db.execute("SELECT COUNT(*) FROM submissions WHERE user_id=? AND status='accepted'", (tg,))
        done = (await cur2.fetchone())[0]
        cur3 = await db.execute("SELECT COUNT(*) FROM submissions WHERE user_id=? AND status='pending'", (tg,))
        pend = (await cur3.fetchone())[0]
        cur4 = await db.execute("SELECT COUNT(*) FROM submissions WHERE user_id=? AND status='rejected'", (tg,))
        rej = (await cur4.fetchone())[0]
    await message.answer(
        f"Профиль:\nФИО: {fio}\nГруппа: {acad_group}\nБаллы: {points}\nРешено: {done}\nНа проверке: {pend}\nОтклонено: {rej}"
    )


# Команда админа посмотреть статистику кураторов
@dp.message(Command('stats'))
async def cmd_stats(message: types.Message):
    if message.from_user.id != ADMIN_ID:
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
    await load_curators_from_csv_if_empty()

    # регистрируем команды для удобства
    commands = [
        BotCommand(command="start", description="Начать работу"),
        BotCommand(command="tasks", description="Список заданий"),
        BotCommand(command="menu", description="Главное меню"),
        BotCommand(command="profile", description="Мой профиль"),
    ]
    await bot.set_my_commands(commands)


# Запуск
async def main():
    await init_db()
    await load_curators_from_csv_if_empty()
    await on_startup(dp)
    print("Bot started")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(main())
