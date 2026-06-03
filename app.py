import asyncio
import csv
import hashlib
import hmac
import json
import os
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
from random import Random
from zoneinfo import ZoneInfo

import razorpay
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, PollAnswerHandler

load_dotenv()

TOTAL_SETS = 85
QUESTIONS_PER_SET = 60

TOPIC_TOTAL_USE = {
    "Horticulture": 788,
    "Animal Husbandry": 608,
    "General Agriculture": 584,
    "Soil Science": 550,
    "Fisheries": 395,
    "Agricultural Engineering": 337,
    "Entomology": 322,
    "Agronomy": 313,
    "Plant Pathology": 195,
    "Meteorology": 168,
    "Forestry": 156,
    "Weed Science": 155,
    "Agricultural Economics": 131,
    "Seed Technology": 118,
    "Genetics and Breeding": 98,
    "Sericulture": 59,
    "Apiculture": 59,
    "Extension Education": 44,
    "Mushroom Cultivation": 20,
}

TOPIC_ALIASES = {
    "manures & fertilizers": "Soil Science",
    "manures and fertilizers": "Soil Science",
    "genetics & breeding": "Genetics and Breeding",
    "genetics and breeding": "Genetics and Breeding",
    "agricultural engineering": "Agricultural Engineering",
    "agricultural economics": "Agricultural Economics",
    "animal husbandry": "Animal Husbandry",
    "general agriculture": "General Agriculture",
    "plant pathology": "Plant Pathology",
    "seed technology": "Seed Technology",
    "weed science": "Weed Science",
    "soil science": "Soil Science",
    "mushroom cultivation": "Mushroom Cultivation",
}


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def int_list(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    telegram_public_group_id: str
    telegram_paid_group_id: str
    telegram_admin_user_ids: list[int]
    mongodb_uri: str
    mongodb_db_name: str
    razorpay_key_id: str
    razorpay_key_secret: str
    razorpay_webhook_secret: str
    google_sheet_id: str
    google_sheet_name: str
    public_base_url: str
    telegram_webhook_secret: str
    timezone: str = "Asia/Kolkata"
    daily_test_time: str = "18:00"
    question_timer_seconds: int = 30
    countdown_seconds: int = 15
    razorpay_amount_inr: int = 251
    paid_group_invite_expire_seconds: int = 86400
    paid_group_invite_member_limit: int = 1


def load_settings() -> Settings:
    return Settings(
        telegram_bot_token=required("TELEGRAM_BOT_TOKEN"),
        telegram_public_group_id=required("TELEGRAM_PUBLIC_GROUP_ID"),
        telegram_paid_group_id=required("TELEGRAM_PAID_GROUP_ID"),
        telegram_admin_user_ids=int_list(required("TELEGRAM_ADMIN_USER_IDS")),
        mongodb_uri=required("MONGODB_URI"),
        mongodb_db_name=os.getenv("MONGODB_DB_NAME", "afo_daily_test"),
        razorpay_key_id=required("RAZORPAY_KEY_ID"),
        razorpay_key_secret=required("RAZORPAY_KEY_SECRET"),
        razorpay_webhook_secret=required("RAZORPAY_WEBHOOK_SECRET"),
        google_sheet_id=os.getenv("GOOGLE_SHEET_ID", "1cPPxwPTgDHfKAwLc_7ZG9WsAMUhYsiZrbJhfV0gN6W4"),
        google_sheet_name=os.getenv("GOOGLE_SHEET_NAME", "Sheet1"),
        public_base_url=required("PUBLIC_BASE_URL").rstrip("/"),
        telegram_webhook_secret=required("TELEGRAM_WEBHOOK_SECRET"),
    )


settings = load_settings()
mongo = AsyncIOMotorClient(settings.mongodb_uri)
db = mongo[settings.mongodb_db_name]
razorpay_client = razorpay.Client(auth=(settings.razorpay_key_id, settings.razorpay_key_secret))


async def ensure_indexes() -> None:
    await db.questions.create_index([("topic", ASCENDING)])
    await db.test_sets.create_index([("set_no", ASCENDING)], unique=True)
    await db.test_runs.create_index([("run_key", ASCENDING)], unique=True)
    await db.poll_map.create_index([("poll_id", ASCENDING)], unique=True)
    await db.responses.create_index(
        [("run_id", ASCENDING), ("telegram_user_id", ASCENDING), ("question_no", ASCENDING)],
        unique=True,
    )
    await db.students.create_index([("telegram_user_id", ASCENDING)], unique=True)
    await db.payments.create_index([("razorpay_payment_id", ASCENDING)], unique=True, sparse=True)
    await db.invites.create_index([("telegram_user_id", ASCENDING), ("created_at", DESCENDING)])


def normalize_topic(topic: str) -> str:
    topic = " ".join((topic or "").strip().split())
    return TOPIC_ALIASES.get(topic.lower(), topic)


def row_value(row: dict, names: list[str]) -> str:
    normalized = {key.strip().lower(): value for key, value in row.items() if key}
    for name in names:
        value = normalized.get(name.strip().lower())
        if value is not None:
            return str(value).strip()
    return ""


def fetch_sheet_rows() -> list[dict]:
    url = (
        f"https://docs.google.com/spreadsheets/d/{settings.google_sheet_id}/gviz/tq"
        f"?tqx=out:csv&sheet={settings.google_sheet_name}"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return list(csv.DictReader(StringIO(response.text)))


async def import_questions_from_sheet() -> int:
    # Run the blocking network request in a separate thread so it doesn't freeze the bot
    rows = await asyncio.to_thread(fetch_sheet_rows)
    questions = []
    skipped_topics = defaultdict(int)

    for source_row, row in enumerate(rows, start=2):
        topic = normalize_topic(row_value(row, ["Topic Name", "Topic"]))
        question_text = row_value(row, ["Question"])
        options = [
            row_value(row, ["A", "Option A"]),
            row_value(row, ["B", "Option B"]),
            row_value(row, ["C", "Option C"]),
            row_value(row, ["D", "Option D"]),
            row_value(row, ["Option E", "E"]),
        ]
        options = [option for option in options if option]
        answer = row_value(row, ["Answer"])
        explanation = row_value(row, ["Explanation"])

        if topic not in TOPIC_TOTAL_USE:
            skipped_topics[topic or "BLANK"] += 1
            continue
        if not question_text or len(options) < 2 or not answer:
            continue

        questions.append(
            {
                "source_row": source_row,
                "topic": topic,
                "question": question_text,
                "options": options,
                "answer": answer,
                "explanation": explanation,
            }
        )

    await db.questions.delete_many({})
    if questions:
        await db.questions.insert_many(questions)

    await db.import_logs.insert_one(
        {
            "imported": len(questions),
            "skipped_topics": dict(skipped_topics),
            "created_at": datetime.now(timezone.utc),
        }
    )
    return len(questions)


def build_topic_plans() -> list[list[str]]:
    remaining = dict(TOPIC_TOTAL_USE)
    plans: list[list[str]] = []

    for set_index in range(TOTAL_SETS):
        plan: list[str] = []
        previous_topic = None

        for _ in range(QUESTIONS_PER_SET):
            sets_left = TOTAL_SETS - set_index
            candidates = [
                topic for topic, count in remaining.items()
                if count > 0 and topic != previous_topic
            ]
            if not candidates:
                candidates = [topic for topic, count in remaining.items() if count > 0]
            if not candidates:
                raise RuntimeError("No topic candidates left while building topic plan")

            topic = max(candidates, key=lambda name: remaining[name] / sets_left)
            plan.append(topic)
            remaining[topic] -= 1
            previous_topic = topic

        if len(plan) != QUESTIONS_PER_SET:
            raise RuntimeError(f"Set plan {set_index + 1} has {len(plan)} topics")
        plans.append(plan)

    leftover = {topic: count for topic, count in remaining.items() if count != 0}
    if leftover:
        raise RuntimeError(f"Topic plan leftover: {leftover}")

    return plans


async def load_question_queues(seed: int) -> dict[str, deque]:
    rng = Random(seed)
    by_topic = defaultdict(list)

    async for question in db.questions.find({}):
        topic = normalize_topic(question.get("topic", ""))
        if topic in TOPIC_TOTAL_USE:
            question["topic"] = topic
            by_topic[topic].append(question)

    missing = []
    for topic, needed in TOPIC_TOTAL_USE.items():
        available = len(by_topic.get(topic, []))
        if available < needed:
            missing.append(f"{topic}: need {needed}, available {available}")
    if missing:
        raise RuntimeError("Not enough questions: " + "; ".join(missing))

    queues = {}
    for topic, questions in by_topic.items():
        rng.shuffle(questions)
        queues[topic] = deque(questions)
    return queues


async def build_test_sets(seed: int = 2026) -> int:
    plans = build_topic_plans()
    queues = await load_question_queues(seed)
    await db.test_sets.delete_many({})

    created = 0
    for set_no, topic_plan in enumerate(plans, start=1):
        selected = []
        previous_topic = None
        for question_no, topic in enumerate(topic_plan, start=1):
            if topic == previous_topic:
                raise RuntimeError(f"Set {set_no} repeated topic at question {question_no}: {topic}")
            question = queues[topic].popleft()
            selected.append(
                {
                    "question_id": question["_id"],
                    "topic": topic,
                    "source_row": question.get("source_row"),
                }
            )
            previous_topic = topic

        if len(selected) != QUESTIONS_PER_SET:
            raise RuntimeError(f"Set {set_no} has {len(selected)} questions, expected 60")

        await db.test_sets.insert_one({"set_no": set_no, "questions": selected})
        created += 1

    return created


def clean(text: str, limit: int = 300) -> str:
    return " ".join((text or "").split())[:limit]


def correct_option_id(question: dict) -> int | None:
    answer = (question.get("answer") or "").strip().lower()
    letter_map = {"a": 0, "b": 1, "c": 2, "d": 3, "e": 4}
    if answer in letter_map and letter_map[answer] < len(question["options"]):
        return letter_map[answer]

    for index, option in enumerate(question["options"]):
        if option.strip().lower() == answer:
            return index

    for index, option in enumerate(question["options"]):
        if answer and answer in option.strip().lower():
            return index

    return None


def is_admin(user_id: int | None) -> bool:
    return bool(user_id and user_id in settings.telegram_admin_user_ids)


def verify_razorpay_webhook(body: bytes, signature: str) -> bool:
    expected = hmac.new(settings.razorpay_webhook_secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature or "")


telegram_app = Application.builder().token(settings.telegram_bot_token).build()


async def start(update: Update, _context) -> None:
    user = update.effective_user
    if not user or not update.message:
        return

    await db.students.update_one(
        {"telegram_user_id": user.id},
        {
            "$set": {
                "telegram_user_id": user.id,
                "name": user.full_name,
                "username": user.username,
                "updated_at": datetime.now(timezone.utc),
            },
            "$setOnInsert": {"paid": False, "created_at": datetime.now(timezone.utc)},
        },
        upsert=True,
    )
    await update.message.reply_text("Welcome to AFO Live Test. Use /pay to join the paid batch.")


async def pay(update: Update, _context) -> None:
    user = update.effective_user
    if not user or not update.message:
        return

    link = razorpay_client.payment_link.create(
        {
            "amount": settings.razorpay_amount_inr * 100,
            "currency": "INR",
            "description": "AFO Batch",
            "customer": {"name": user.full_name or f"Telegram {user.id}"},
            "notify": {"sms": False, "email": False},
            "reminder_enable": False,
            "notes": {"telegram_user_id": str(user.id)},
        }
    )["short_url"]

    await update.message.reply_text(
        "Join AFO Batch @ Rs. 251 only.\n\nPay and get your private group invite.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Pay Now Rs. 251", url=link)]]),
    )


async def import_sheet(update: Update, _context) -> None:
    if not is_admin(update.effective_user.id if update.effective_user else None):
        return
    count = await import_questions_from_sheet()
    await update.message.reply_text(f"Imported {count} questions from Google Sheet.")


async def build_sets_command(update: Update, _context) -> None:
    if not is_admin(update.effective_user.id if update.effective_user else None):
        return
    count = await build_test_sets()
    await update.message.reply_text(f"Built {count} mixed test sets. Each set has 60 questions.")


async def stats(update: Update, _context) -> None:
    if not is_admin(update.effective_user.id if update.effective_user else None):
        return
    
    cursor = db.questions.aggregate([{"$group": {"_id": "$topic", "count": {"$sum": 1}}}, {"$sort": {"_id": 1}}])
    topic_counts = await cursor.to_list(length=None)
    
    lines = ["Question counts:"]
    for row in topic_counts:
        lines.append(f"{row['_id']}: {row['count']}")
    await update.message.reply_text("\n".join(lines[:80]))


async def run_test_command(update: Update, _context) -> None:
    if not is_admin(update.effective_user.id if update.effective_user else None):
        return
    await update.message.reply_text("Starting live test now.")
    asyncio.create_task(start_live_test(manual=True))


async def scheduled_test(_context) -> None:
    asyncio.create_task(start_live_test(manual=False))


async def start_live_test(manual: bool = False) -> None:
    today = datetime.now(ZoneInfo(settings.timezone)).date()
    run_key = f"{today.isoformat()}-manual-{int(datetime.now().timestamp())}" if manual else today.isoformat()

    if not manual and await db.test_runs.find_one({"run_key": run_key}):
        return

    runs_count = await db.test_runs.count_documents({"manual": False})
    set_no = (runs_count % TOTAL_SETS) + 1
    
    test_set = await db.test_sets.find_one({"set_no": set_no})
    if not test_set:
        await telegram_app.bot.send_message(
            settings.telegram_public_group_id,
            "Test sets are not ready. Admin: run /import_sheet and /build_sets first.",
        )
        return

    result = await db.test_runs.insert_one(
        {
            "run_key": run_key,
            "set_no": set_no,
            "manual": manual,
            "status": "running",
            "started_at": datetime.now(timezone.utc),
        }
    )
    run_id = result.inserted_id

    announcement = await telegram_app.bot.send_message(
        settings.telegram_public_group_id,
        "<b>AFO Live Test</b>\n\nTotal Questions: <b>60</b>\nTimer: <b>30 seconds/question</b>",
        parse_mode=ParseMode.HTML,
    )
    await telegram_app.bot.pin_chat_message(
        settings.telegram_public_group_id,
        announcement.message_id,
        disable_notification=True,
    )

    countdown = await telegram_app.bot.send_message(settings.telegram_public_group_id, "Countdown starting...")
    for second in range(settings.countdown_seconds, 0, -1):
        try:
            await telegram_app.bot.edit_message_text(
                chat_id=settings.telegram_public_group_id,
                message_id=countdown.message_id,
                text=f"Test starts in {second} seconds",
            )
        except BadRequest as exc:
            if "Message is not modified" not in str(exc):
                raise
        await asyncio.sleep(1)

    await telegram_app.bot.send_message(
        settings.telegram_public_group_id,
        "<b>Get Ready. Test Starts Now.</b>",
        parse_mode=ParseMode.HTML,
    )

    for question_no, item in enumerate(test_set["questions"], start=1):
        question = await db.questions.find_one({"_id": item["question_id"]})
        if not question:
            continue
        correct_id = correct_option_id(question)
        if correct_id is None:
            continue

        poll_message = await telegram_app.bot.send_poll(
            chat_id=settings.telegram_public_group_id,
            question=f"{question_no}/60 - {clean(question['question'], 255)}",
            options=[clean(option, 100) for option in question["options"]],
            type="quiz",
            correct_option_id=correct_id,
            is_anonymous=False,
            open_period=settings.question_timer_seconds,
            explanation=clean(question.get("explanation", ""), 200) or None,
        )

        await db.poll_map.insert_one(
            {
                "poll_id": poll_message.poll.id,
                "run_id": run_id,
                "question_no": question_no,
                "question_id": question["_id"],
                "correct_option_id": correct_id,
            }
        )
        await asyncio.sleep(settings.question_timer_seconds + 2)

    await db.test_runs.update_one({"_id": run_id}, {"$set": {"status": "completed", "completed_at": datetime.now(timezone.utc)}})
    await send_leaderboard(run_id)
    await send_promo_message()


async def poll_answer(update: Update, _context) -> None:
    answer = update.poll_answer
    poll_data = await db.poll_map.find_one({"poll_id": answer.poll_id})
    if not poll_data or not answer.user:
        return

    selected = answer.option_ids[0] if answer.option_ids else None
    is_correct = selected == poll_data["correct_option_id"]
    marks = 1 if is_correct else -0.25

    await db.students.update_one(
        {"telegram_user_id": answer.user.id},
        {"$set": {"telegram_user_id": answer.user.id, "name": answer.user.full_name, "username": answer.user.username}},
        upsert=True,
    )
    
    await db.responses.update_one(
        {
            "run_id": poll_data["run_id"],
            "telegram_user_id": answer.user.id,
            "question_no": poll_data["question_no"],
        },
        {
            "$set": {
                "selected_option_id": selected,
                "is_correct": is_correct,
                "marks": marks,
                "answered_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )


async def send_leaderboard(run_id) -> None:
    cursor = db.responses.aggregate(
        [
            {"$match": {"run_id": run_id}},
            {
                "$group": {
                    "_id": "$telegram_user_id",
                    "marks": {"$sum": "$marks"},
                    "attempted": {"$sum": 1},
                    "correct": {"$sum": {"$cond": ["$is_correct", 1, 0]}},
                    "wrong": {"$sum": {"$cond": ["$is_correct", 0, 1]}},
                }
            },
            {"$sort": {"marks": -1, "correct": -1, "attempted": 1}},
        ]
    )
    rows = await cursor.to_list(length=None)

    if not rows:
        await telegram_app.bot.send_message(settings.telegram_public_group_id, "No responses received for this test.")
        return

    total = len(rows)
    lines = ["<b>Live Test Leaderboard</b>", ""]
    for rank, row in enumerate(rows, start=1):
        student = await db.students.find_one({"telegram_user_id": row["_id"]}) or {}
        name = student.get("name") or str(row["_id"])
        percentage = (row["marks"] / QUESTIONS_PER_SET) * 100
        percentile = ((total - rank) / max(total - 1, 1)) * 100
        lines.append(
            f"{rank}. {name} | Marks: {row['marks']:.2f} | Attempted: {row['attempted']} | "
            f"Correct: {row['correct']} | Wrong: {row['wrong']} | %: {percentage:.2f} | Percentile: {percentile:.2f}"
        )

    message = ""
    for line in lines:
        if len(message) + len(line) + 1 > 3800:
            await telegram_app.bot.send_message(settings.telegram_public_group_id, message, parse_mode=ParseMode.HTML)
            message = ""
        message += line + "\n"
    if message:
        await telegram_app.bot.send_message(settings.telegram_public_group_id, message, parse_mode=ParseMode.HTML)


async def send_promo_message() -> None:
    bot_username = (await telegram_app.bot.get_me()).username
    message = (
        "<b>Great Effort! Live Test Completed Successfully</b>\n\n"
        "Want to boost your <b>AFO 2026 Preparation</b>?\n"
        "450+ Subject-wise Tests | 90+ Full-Length Tests\n"
        "Monthly Current Affairs Support | 6000+ Question Bank PDF\n\n"
        "<b>Join the AFO Batch @ Rs. 251 only</b> and level up your preparation.\n"
        "Click the button below to join now."
    )
    await telegram_app.bot.send_message(
        settings.telegram_public_group_id,
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Pay Now Rs. 251", url=f"https://t.me/{bot_username}?start=pay")]]),
    )


async def send_paid_invite(telegram_user_id: int) -> None:
    expire_date = datetime.now(timezone.utc) + timedelta(seconds=settings.paid_group_invite_expire_seconds)
    invite = await telegram_app.bot.create_chat_invite_link(
        chat_id=settings.telegram_paid_group_id,
        expire_date=expire_date,
        member_limit=settings.paid_group_invite_member_limit,
        creates_join_request=False,
    )
    
    await db.invites.insert_one(
        {
            "telegram_user_id": telegram_user_id,
            "invite_link": invite.invite_link,
            "created_at": datetime.now(timezone.utc),
            "expire_date": expire_date,
        }
    )
    await telegram_app.bot.send_message(
        telegram_user_id,
        "Payment confirmed. Here is your private AFO Batch group invite link:\n\n"
        f"{invite.invite_link}\n\nThis link is valid for one student only.",
    )


telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(CommandHandler("pay", pay))
telegram_app.add_handler(CommandHandler("import_sheet", import_sheet))
telegram_app.add_handler(CommandHandler("build_sets", build_sets_command))
telegram_app.add_handler(CommandHandler("stats", stats))
telegram_app.add_handler(CommandHandler("run_test", run_test_command))
telegram_app.add_handler(PollAnswerHandler(poll_answer))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await ensure_indexes()
    await telegram_app.initialize()
    await telegram_app.start()

    webhook_url = f"{settings.public_base_url}/telegram/{settings.telegram_webhook_secret}"
    await telegram_app.bot.set_webhook(webhook_url)

    hour, minute = [int(part) for part in settings.daily_test_time.split(":")]
    daily_time = datetime.now(ZoneInfo(settings.timezone)).replace(hour=hour, minute=minute, second=0).timetz()
    telegram_app.job_queue.run_daily(scheduled_test, time=daily_time, name="daily_afo_test")

    yield

    await telegram_app.stop()
    await telegram_app.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"ok": True, "service": "afo-live-test-bot"}


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/telegram/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != settings.telegram_webhook_secret:
        raise HTTPException(status_code=403, detail="Invalid Telegram webhook secret")
    payload = await request.json()
    update = Update.de_json(payload, telegram_app.bot)
    await telegram_app.process_update(update)
    return {"ok": True}


@app.post("/razorpay/webhook")
async def razorpay_webhook(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Razorpay-Signature", "")
    if not verify_razorpay_webhook(body, signature):
        raise HTTPException(status_code=400, detail="Invalid Razorpay signature")

    payload = json.loads(body.decode("utf-8"))
    payment = payload.get("payload", {}).get("payment", {}).get("entity", {})
    notes = payment.get("notes") or {}
    telegram_user_id = int(notes["telegram_user_id"]) if notes.get("telegram_user_id") else None
    razorpay_payment_id = payment.get("id")

    await db.payments.update_one(
        {"razorpay_payment_id": razorpay_payment_id},
        {
            "$setOnInsert": {
                "event": payload.get("event"),
                "razorpay_payment_id": razorpay_payment_id,
                "amount": payment.get("amount"),
                "status": payment.get("status"),
                "telegram_user_id": telegram_user_id,
                "raw": payload,
                "created_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )

    if payload.get("event") in {"payment.captured", "payment_link.paid"} and telegram_user_id:
        await db.students.update_one({"telegram_user_id": telegram_user_id}, {"$set": {"paid": True}}, upsert=True)
        await send_paid_invite(telegram_user_id)

    return {"ok": True}
