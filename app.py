import asyncio
import json
import os
import random
import re
import secrets
import smtplib
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Union

import httpx
import jwt
from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from passlib.context import CryptContext
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Configuration & App Setup
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "")
SECRET_KEY = os.getenv("JWT_SECRET") or os.getenv("SECRET_KEY") or "change-me-in-production-secret"
ADMIN_KEY = os.getenv("ADMIN_KEY", "admin-secret-key")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER or "noreply@example.com")

ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 72
DAILY_HONOR_CAP = 500
MAX_LOUNGE_MEMBERS = 12

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

app = FastAPI(title="AetherRealm Synchronized Social, Voice & Gaming Hub", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db_pool: Optional[ThreadedConnectionPool] = None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_ts() -> float:
    return time.time()


def get_db_connection():
    global db_pool
    if db_pool is None:
        if not DATABASE_URL:
            raise HTTPException(status_code=500, detail="DATABASE_URL is not configured")
        db_pool = ThreadedConnectionPool(minconn=1, maxconn=15, dsn=DATABASE_URL)
    return db_pool.getconn()


def release_db_connection(conn):
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# WebSocket Connection, Presence & WebRTC Voice Signaling Manager
# ---------------------------------------------------------------------------
class RoomManager:
    def __init__(self):
        self.rooms: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.user_rooms: Dict[str, set] = defaultdict(set)
        self.online_users: Dict[str, float] = {}
        self.voice_participants: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
        self.lock = asyncio.Lock()

    async def connect(self, room: str, ws: WebSocket, user: Dict[str, Any]):
        await ws.accept()
        username = user.get("username", "Guest")
        async with self.lock:
            self.rooms[room].append({"ws": ws, "user": user})
            self.user_rooms[username].add(room)
            self.online_users[username] = now_ts()

    async def disconnect(self, room: str, ws: WebSocket):
        left_user = None
        async with self.lock:
            entries = self.rooms.get(room, [])
            remaining = []
            for entry in entries:
                if entry["ws"] is ws:
                    left_user = entry["user"]
                else:
                    remaining.append(entry)
            self.rooms[room] = remaining

            if left_user:
                uname = left_user.get("username")
                if uname:
                    still_in_room = any(
                        e["user"].get("username") == uname for e in self.rooms[room]
                    )
                    if not still_in_room:
                        self.user_rooms[uname].discard(room)
                        self.voice_participants[room].pop(uname, None)

        if left_user:
            uname = left_user.get("username")
            await self.broadcast(
                room,
                {
                    "type": "presence_leave",
                    "room": room,
                    "user": uname,
                    "timestamp": now_utc().isoformat(),
                },
            )
            await self.broadcast(
                room,
                {
                    "type": "voice_state",
                    "room": room,
                    "user": uname,
                    "in_voice": False,
                    "muted": True,
                    "speaking": False,
                },
            )
            await self.broadcast_presence()

    async def send_personal(self, ws: WebSocket, payload: Dict[str, Any]):
        try:
            await ws.send_text(json.dumps(payload, default=str))
        except Exception:
            pass

    async def broadcast(self, room: str, payload: Dict[str, Any]):
        async with self.lock:
            targets = list(self.rooms.get(room, []))
        dead = []
        encoded = json.dumps(payload, default=str)
        for item in targets:
            try:
                await item["ws"].send_text(encoded)
            except Exception:
                dead.append(item["ws"])
        for d in dead:
            await self.disconnect(room, d)

    async def broadcast_presence(self):
        cutoff = now_ts() - 180
        async with self.lock:
            active = [u for u, ts in self.online_users.items() if ts >= cutoff]
            room_counts = {r: len(members) for r, members in self.rooms.items()}
        await self.broadcast(
            "global",
            {
                "type": "presence_sync",
                "online_users": sorted(active),
                "online_count": len(active),
                "room_counts": room_counts,
            },
        )


manager = RoomManager()


# ---------------------------------------------------------------------------
# Auth & Utility Functions
# ---------------------------------------------------------------------------
def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception:
        return False


def create_access_token(data: Dict[str, Any]) -> str:
    to_encode = data.copy()
    to_encode["exp"] = now_utc() + timedelta(hours=TOKEN_EXPIRE_HOURS)
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except Exception:
        return None


def get_current_user(authorization: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Authentication required")
    token = authorization.split(" ", 1)[1].strip()
    payload = decode_token(token)
    if not payload or "username" not in payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE username = %s", (payload["username"],))
            user = cur.fetchone()
            if not user:
                raise HTTPException(status_code=401, detail="User not found")
            u = dict(user)
            u.pop("password_hash", None)
            return u
    finally:
        release_db_connection(conn)


def compute_level(xp: int) -> int:
    return max(1, int((max(0, xp) / 100) ** 0.5) + 1)


def award_xp_and_coins(conn, username: str, xp_gain: int = 15, coins_gain: int = 10) -> Dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT username, xp, level, cowoncy, honor FROM users WHERE username = %s",
            (username,),
        )
        row = cur.fetchone()
        if not row:
            return {"leveled_up": False, "level": 1, "xp": 0, "cowoncy": 0}
        old_level = row["level"] or compute_level(row["xp"] or 0)
        new_xp = (row["xp"] or 0) + xp_gain
        new_level = compute_level(new_xp)
        new_coins = (row["cowoncy"] or 250) + coins_gain
        cur.execute(
            """
            UPDATE users
            SET xp = %s, level = %s, cowoncy = %s
            WHERE username = %s
            RETURNING xp, level, cowoncy, honor
            """,
            (new_xp, new_level, new_coins, username),
        )
        updated = cur.fetchone()
        return {
            "leveled_up": new_level > old_level,
            "level": updated["level"],
            "xp": updated["xp"],
            "cowoncy": updated["cowoncy"],
            "honor": updated["honor"],
        }


def extract_youtube_id(url_or_id: str) -> Optional[str]:
    text = (url_or_id or "").strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", text):
        return text
    patterns = [
        r"(?:v=|/v/|/embed/|youtu\.be/|/shorts/|/live/)([A-Za-z0-9_-]{11})",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(1)
    return None


def send_email_if_configured(to_email: str, subject: str, body: str):
    if not SMTP_HOST or not to_email:
        return
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = SMTP_FROM
        msg["To"] = to_email
        msg.set_content(body)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=8) as server:
            server.starttls()
            if SMTP_USER and SMTP_PASS:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Database Schema Initialization & Safe Migrations
# ---------------------------------------------------------------------------
def init_db():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(64) UNIQUE NOT NULL,
                    email VARCHAR(128) UNIQUE,
                    password_hash TEXT NOT NULL,
                    is_verified BOOLEAN DEFAULT TRUE,
                    verification_code VARCHAR(12),
                    reset_code VARCHAR(12),
                    bio TEXT DEFAULT 'Exploring the realm.',
                    affinity VARCHAR(64) DEFAULT 'Aether',
                    theme VARCHAR(32) DEFAULT 'original',
                    sticker VARCHAR(32) DEFAULT '✨',
                    decoration VARCHAR(64) DEFAULT 'none',
                    role_title VARCHAR(64) DEFAULT 'Voyager',
                    birthday VARCHAR(16),
                    captcha_verified BOOLEAN DEFAULT TRUE,
                    warnings INTEGER DEFAULT 0,
                    xp INTEGER DEFAULT 0,
                    level INTEGER DEFAULT 1,
                    honor INTEGER DEFAULT 100,
                    daily_honor INTEGER DEFAULT 0,
                    last_honor_reset VARCHAR(16) DEFAULT '',
                    cowoncy INTEGER DEFAULT 250,
                    last_daily_claim VARCHAR(16) DEFAULT '',
                    invites_count INTEGER DEFAULT 0,
                    messages_count INTEGER DEFAULT 0,
                    inventory JSONB DEFAULT '["✨"]'::jsonb,
                    notes TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS friends (
                    id SERIAL PRIMARY KEY,
                    requester VARCHAR(64) NOT NULL,
                    addressee VARCHAR(64) NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(requester, addressee)
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS direct_messages (
                    id SERIAL PRIMARY KEY,
                    sender VARCHAR(64) NOT NULL,
                    recipient VARCHAR(64) NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS chat_messages (
                    id SERIAL PRIMARY KEY,
                    room VARCHAR(64) NOT NULL,
                    sender VARCHAR(64) NOT NULL,
                    is_bot BOOLEAN DEFAULT FALSE,
                    bot_name VARCHAR(64),
                    content TEXT NOT NULL,
                    meta JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS confessions (
                    id SERIAL PRIMARY KEY,
                    author VARCHAR(64) DEFAULT 'Anonymous',
                    is_anonymous BOOLEAN DEFAULT TRUE,
                    content TEXT NOT NULL,
                    likes INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS confession_replies (
                    id SERIAL PRIMARY KEY,
                    confession_id INTEGER REFERENCES confessions(id) ON DELETE CASCADE,
                    author VARCHAR(64) DEFAULT 'Anonymous',
                    content TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS lounges (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(80) NOT NULL,
                    topic VARCHAR(160) DEFAULT 'Chill & Listen Together',
                    theme VARCHAR(32) DEFAULT 'sakura',
                    creator VARCHAR(64) NOT NULL,
                    members JSONB DEFAULT '[]'::jsonb,
                    video_id VARCHAR(32) DEFAULT 'jfKfPfyJRdk',
                    video_title VARCHAR(160) DEFAULT 'Lofi Hip Hop Radio - Beats to Relax/Study to',
                    queue JSONB DEFAULT '[]'::jsonb,
                    is_playing BOOLEAN DEFAULT TRUE,
                    base_position DOUBLE PRECISION DEFAULT 0.0,
                    updated_at DOUBLE PRECISION DEFAULT 0.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS media_state (
                    channel_key VARCHAR(64) PRIMARY KEY,
                    video_id VARCHAR(32) DEFAULT 'jfKfPfyJRdk',
                    title VARCHAR(160) DEFAULT 'Synchronized Stream',
                    queue JSONB DEFAULT '[]'::jsonb,
                    is_playing BOOLEAN DEFAULT TRUE,
                    base_position DOUBLE PRECISION DEFAULT 0.0,
                    updated_at DOUBLE PRECISION DEFAULT 0.0,
                    updated_by VARCHAR(64) DEFAULT 'System'
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS stories (
                    id SERIAL PRIMARY KEY,
                    author VARCHAR(64) NOT NULL,
                    content TEXT NOT NULL,
                    mood VARCHAR(32) DEFAULT '✨',
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS polls (
                    id SERIAL PRIMARY KEY,
                    question TEXT NOT NULL,
                    options JSONB NOT NULL,
                    votes JSONB DEFAULT '{}'::jsonb,
                    created_by VARCHAR(64) DEFAULT 'Simple Poll Bot',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS guild_boss (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(80) DEFAULT 'Void Sovereign Aetherion',
                    hp INTEGER DEFAULT 10000,
                    max_hp INTEGER DEFAULT 10000,
                    tier INTEGER DEFAULT 1,
                    last_attacker VARCHAR(64) DEFAULT 'None'
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS arcade_scores (
                    id SERIAL PRIMARY KEY,
                    game VARCHAR(40) NOT NULL,
                    username VARCHAR(64) NOT NULL,
                    score INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS giveaways (
                    id SERIAL PRIMARY KEY,
                    prize VARCHAR(120) NOT NULL,
                    host VARCHAR(64) NOT NULL,
                    entries JSONB DEFAULT '[]'::jsonb,
                    winner VARCHAR(64),
                    ends_at DOUBLE PRECISION NOT NULL,
                    active BOOLEAN DEFAULT TRUE
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS tickets (
                    id SERIAL PRIMARY KEY,
                    creator VARCHAR(64) NOT NULL,
                    reason TEXT NOT NULL,
                    status VARCHAR(20) DEFAULT 'open',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_tags (
                    name VARCHAR(40) PRIMARY KEY,
                    response TEXT NOT NULL,
                    author VARCHAR(64) NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS counting_state (
                    id INTEGER PRIMARY KEY,
                    current_count INTEGER DEFAULT 0,
                    high_score INTEGER DEFAULT 0,
                    last_user VARCHAR(64) DEFAULT ''
                );
            """)

            migrations = [
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS role_title VARCHAR(64) DEFAULT 'Voyager'",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS birthday VARCHAR(16)",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS captcha_verified BOOLEAN DEFAULT TRUE",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS warnings INTEGER DEFAULT 0",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS cowoncy INTEGER DEFAULT 250",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_daily_claim VARCHAR(16) DEFAULT ''",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS invites_count INTEGER DEFAULT 0",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS messages_count INTEGER DEFAULT 0",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS inventory JSONB DEFAULT '[\"✨\"]'::jsonb",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS notes TEXT DEFAULT ''",
                "ALTER TABLE lounges ADD COLUMN IF NOT EXISTS topic VARCHAR(160) DEFAULT 'Chill & Listen Together'",
                "ALTER TABLE lounges ADD COLUMN IF NOT EXISTS theme VARCHAR(32) DEFAULT 'sakura'",
                "ALTER TABLE lounges ADD COLUMN IF NOT EXISTS creator VARCHAR(64) DEFAULT 'System'",
                "ALTER TABLE lounges ADD COLUMN IF NOT EXISTS members JSONB DEFAULT '[]'::jsonb",
                "ALTER TABLE lounges ADD COLUMN IF NOT EXISTS video_id VARCHAR(32) DEFAULT 'jfKfPfyJRdk'",
                "ALTER TABLE lounges ADD COLUMN IF NOT EXISTS video_title VARCHAR(160) DEFAULT 'Lofi Hip Hop Radio - Beats to Relax/Study to'",
                "ALTER TABLE lounges ADD COLUMN IF NOT EXISTS queue JSONB DEFAULT '[]'::jsonb",
                "ALTER TABLE lounges ADD COLUMN IF NOT EXISTS is_playing BOOLEAN DEFAULT TRUE",
                "ALTER TABLE lounges ADD COLUMN IF NOT EXISTS base_position DOUBLE PRECISION DEFAULT 0.0",
                "ALTER TABLE lounges ADD COLUMN IF NOT EXISTS updated_at DOUBLE PRECISION DEFAULT 0.0",
                "ALTER TABLE confessions ADD COLUMN IF NOT EXISTS is_anonymous BOOLEAN DEFAULT TRUE",
                "ALTER TABLE confessions ADD COLUMN IF NOT EXISTS likes INTEGER DEFAULT 0",
                "ALTER TABLE guild_boss ADD COLUMN IF NOT EXISTS tier INTEGER DEFAULT 1",
                "ALTER TABLE guild_boss ADD COLUMN IF NOT EXISTS last_attacker VARCHAR(64) DEFAULT 'None'",
                "ALTER TABLE polls ADD COLUMN IF NOT EXISTS votes JSONB DEFAULT '{}'::jsonb",
                "ALTER TABLE polls ADD COLUMN IF NOT EXISTS created_by VARCHAR(64) DEFAULT 'Simple Poll Bot'",
            ]
            for stmt in migrations:
                cur.execute(stmt)

            # Ensure auto-increment sequence defaults work for both integer and text id columns
            serial_tables = [
                "users",
                "friends",
                "direct_messages",
                "chat_messages",
                "confessions",
                "confession_replies",
                "lounges",
                "stories",
                "polls",
                "arcade_scores",
                "giveaways",
                "tickets",
            ]
            for tbl in serial_tables:
                cur.execute(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s AND column_name = 'id'
                    """,
                    (tbl,),
                )
                col_info = cur.fetchone()
                if not col_info:
                    continue
                dtype = (col_info[0] or "").lower()
                seq_name = f"{tbl}_id_seq"
                cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq_name}")
                if dtype in ("text", "character varying", "varchar"):
                    cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE id !~ '^[0-9]+$'")
                    non_numeric = cur.fetchone()[0]
                    if non_numeric == 0:
                        cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN id TYPE INTEGER USING id::integer")
                        cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN id SET DEFAULT nextval('{seq_name}')")
                        cur.execute(
                            f"SELECT setval('{seq_name}', COALESCE((SELECT MAX(id) FROM {tbl}), 0) + 1, false)"
                        )
                    else:
                        cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN id SET DEFAULT nextval('{seq_name}')::text")
                        cur.execute(
                            f"SELECT setval('{seq_name}', COALESCE((SELECT MAX(id::integer) FROM {tbl} WHERE id ~ '^[0-9]+$'), 0) + 1, false)"
                        )
                else:
                    cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN id SET DEFAULT nextval('{seq_name}')")
                    cur.execute(
                        f"SELECT setval('{seq_name}', COALESCE((SELECT MAX(id) FROM {tbl}), 0) + 1, false)"
                    )

            cur.execute("SELECT id FROM guild_boss WHERE id = 1")
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO guild_boss (id, name, hp, max_hp, tier) VALUES (1, 'Void Sovereign Aetherion', 10000, 10000, 1)"
                )

            cur.execute("SELECT id FROM counting_state WHERE id = 1")
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO counting_state (id, current_count, high_score, last_user) VALUES (1, 0, 0, '')"
                )

            cur.execute("SELECT channel_key FROM media_state WHERE channel_key = 'entertainment'")
            if not cur.fetchone():
                cur.execute(
                    """
                    INSERT INTO media_state (channel_key, video_id, title, queue, is_playing, base_position, updated_at, updated_by)
                    VALUES ('entertainment', 'jfKfPfyJRdk', 'Lofi Hip Hop Radio - Chill Beats', '[]'::jsonb, TRUE, 0.0, %s, 'System')
                    """,
                    (now_ts(),),
                )

            cur.execute("SELECT COUNT(*) FROM lounges")
            if cur.fetchone()[0] == 0:
                ts = now_ts()
                default_lounges = [
                    ("Sakura Shrine Lounge", "Cherry blossoms, lofi & late-night talks", "sakura", "System", '["System"]', "5qap5aO4i9A", "Lofi Hip Hop Radio - Beats to Sleep/Chill to", ts),
                    ("Neon Nightcore & Cinema", "Synchronized watchparty & synthwave", "cyberpunk", "System", '["System"]', "jfKfPfyJRdk", "Lofi Hip Hop Radio - Chill Session", ts),
                    ("Ancient Ruins Campfire", "Cozy acoustic & guild strategy", "ruins", "System", '["System"]', "4xDzrJKXOOY", "Synthwave Radio - Beats to Chill/Game to", ts),
                ]
                for name, topic, theme, creator, members, vid, title, t in default_lounges:
                    cur.execute(
                        """
                        INSERT INTO lounges (name, topic, theme, creator, members, video_id, video_title, is_playing, base_position, updated_at)
                        VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, TRUE, 0.0, %s)
                        """,
                        (name, topic, theme, creator, members, vid, title, t),
                    )

            conn.commit()
    finally:
        release_db_connection(conn)


@app.on_event("startup")
def on_startup():
    if DATABASE_URL:
        init_db()


# ---------------------------------------------------------------------------
# Smart Context-Aware Bot Engine (Gemini + YouTube + Discord Bot Suite)
# ---------------------------------------------------------------------------
def get_recent_room_context(conn, room: str, limit: int = 8) -> str:
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT sender, bot_name, is_bot, content FROM chat_messages WHERE room = %s ORDER BY id DESC LIMIT %s",
                (room, limit),
            )
            rows = cur.fetchall()
            rows.reverse()
            lines = []
            for r in rows:
                who = r["bot_name"] if r["is_bot"] else r["sender"]
                lines.append(f"{who}: {r['content']}")
            return "\n".join(lines)
    except Exception:
        return ""


async def call_smart_gemini(
    system_persona: str,
    user_prompt: str,
    room_history: str = "",
    fallback: str = "✨ I'm online and ready! Try `/help` to explore commands.",
) -> str:
    if not GEMINI_API_KEY:
        return fallback

    full_prompt = system_persona
    if room_history:
        full_prompt += f"\n\nRecent room conversation for context:\n{room_history}"
    full_prompt += f"\n\nCurrent request:\n{user_prompt}"

    models_to_try = ["gemini-2.5-flash", "gemini-2.0-flash"]
    for model_name in models_to_try:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
            payload = {
                "contents": [{"parts": [{"text": full_prompt}]}],
                "generationConfig": {"temperature": 0.85, "maxOutputTokens": 260},
            }
            async with httpx.AsyncClient(timeout=12.0) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    parts = (
                        data.get("candidates", [{}])[0]
                        .get("content", {})
                        .get("parts", [])
                    )
                    if parts and parts[0].get("text"):
                        return parts[0]["text"].strip()
        except Exception:
            continue
    return fallback


async def search_youtube_first(query: str) -> Optional[Dict[str, str]]:
    direct = extract_youtube_id(query)
    if direct:
        return {"video_id": direct, "title": f"YouTube Track ({direct})"}
    if YOUTUBE_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(
                    "https://www.googleapis.com/youtube/v3/search",
                    params={
                        "part": "snippet",
                        "q": query,
                        "type": "video",
                        "maxResults": 1,
                        "key": YOUTUBE_API_KEY,
                    },
                )
                if resp.status_code == 200:
                    items = resp.json().get("items", [])
                    if items:
                        vid = items[0].get("id", {}).get("videoId")
                        title = items[0].get("snippet", {}).get("title", query)
                        if vid:
                            return {"video_id": vid, "title": title}
        except Exception:
            pass
    return None


def build_help_text() -> str:
    return (
        "**🤖 Smart Bot Suite (AetherBot • MEE6 • ProBot • OwO • Carl-bot • Giveaway • Ticket)**\n"
        "• **AetherBot (AI):** `/ask <anything>`, `/oracle <fortune>`, or mention `@aetherbot` in chat\n"
        "• **MEE6 & ProBot:** `/rank [user]`, `/roast [user]`, `/compliment [user]`, `/leaderboard`, `/stats`\n"
        "• **OwO Bot:** `owo hunt`, `owo battle`, `owo daily`, `owo cash`, `owo slots <bet>`\n"
        "• **YouTube Sync DJ:** `/play <song name or YouTube URL>` (searches YouTube & syncs for everyone!)\n"
        "• **GiveawayBot:** `/gstart <seconds> <prize>` or `/gend [id]`\n"
        "• **Ticket Tool & Mod:** `/ticket <reason>`, `/close [id]`, `/warn <user> <reason>`, `/captcha`\n"
        "• **Reaction Roles & Polls:** `/roles <Voyager|Strategist|Bard|Guardian|Streamer>`, `/poll Q | Opt1 | Opt2`, `/birthday MM-DD`\n"
        "• **Carl-bot & Dyno:** `/roll [max]`, `/rps <rock|paper|scissors>`, `/meme`, `/tag set <name> <text>`"
    )


async def process_bot_command(conn, room: str, username: str, raw_text: str) -> Optional[List[Dict[str, Any]]]:
    text = raw_text.strip()
    lower = text.lower()
    bot_replies: List[Dict[str, Any]] = []

    def add_bot(bot_name: str, content: str, extra: Optional[Dict[str, Any]] = None):
        meta = extra or {}
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO chat_messages (room, sender, is_bot, bot_name, content, meta)
                VALUES (%s, %s, TRUE, %s, %s, %s::jsonb)
                RETURNING id, room, sender, is_bot, bot_name, content, meta, created_at
                """,
                (room, bot_name, bot_name, content, json.dumps(meta)),
            )
            row = dict(cur.fetchone())
        bot_replies.append(row)

    # 1. Natural @AetherBot mention anywhere in chat
    if "@aetherbot" in lower and not lower.startswith("/"):
        clean_q = re.sub(r"@aetherbot", "", text, flags=re.IGNORECASE).strip() or "Hey AetherBot, what's up?"
        history = get_recent_room_context(conn, room, limit=8)
        reply = await call_smart_gemini(
            f"You are AetherBot, the witty, intelligent AI host of AetherRealm talking to @{username} in #{room}. Be natural, sharp, and helpful (under 110 words).",
            clean_q,
            room_history=history,
            fallback=f"✨ Hey **@{username}**! Ask me anything or try `/play <song>` to sync music.",
        )
        add_bot("AetherBot", reply)
        return bot_replies

    # 2. Counting Bot auto-check in counting rooms
    if "count" in room.lower() and not text.startswith("/") and not lower.startswith("owo "):
        stripped_num = text.strip()
        if stripped_num.isdigit():
            val = int(stripped_num)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM counting_state WHERE id = 1")
                st = dict(cur.fetchone())
                expected = st["current_count"] + 1
                if val == expected and st["last_user"] != username:
                    new_high = max(st["high_score"], val)
                    cur.execute(
                        "UPDATE counting_state SET current_count = %s, high_score = %s, last_user = %s WHERE id = 1",
                        (val, new_high, username),
                    )
                    award_xp_and_coins(conn, username, xp_gain=5, coins_gain=5)
                else:
                    reason = (
                        "same user posted twice in a row!"
                        if st["last_user"] == username
                        else f"expected **{expected}**, got **{val}**!"
                    )
                    cur.execute("UPDATE counting_state SET current_count = 0, last_user = '' WHERE id = 1")
                    add_bot(
                        "Counting Bot",
                        f"💥 Streak broken by **@{username}** ({reason}) Reset back to **0**! Best record: **{st['high_score']}**.",
                    )
            return bot_replies

    # 3. OwO Bot commands
    if lower.startswith("owo ") or lower == "owo":
        parts = text.split()
        sub = parts[1].lower() if len(parts) > 1 else "help"
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT cowoncy, last_daily_claim FROM users WHERE username = %s", (username,))
            u = cur.fetchone()
            if not u:
                return None
            coins = u["cowoncy"] or 250

            if sub == "daily":
                today_str = now_utc().strftime("%Y-%m-%d")
                if u["last_daily_claim"] == today_str:
                    add_bot("OwO Bot", f"⏳ **@{username}**, you already claimed your daily **250 Cowoncy** today!")
                else:
                    new_c = coins + 250
                    cur.execute(
                        "UPDATE users SET cowoncy = %s, last_daily_claim = %s WHERE username = %s",
                        (new_c, today_str, username),
                    )
                    add_bot("OwO Bot", f"🎁 **@{username}** claimed **+250 Cowoncy**! Balance: **{new_c}**.")
            elif sub in ("cash", "bal", "balance"):
                target = parts[2].lstrip("@") if len(parts) > 2 else username
                cur.execute("SELECT cowoncy FROM users WHERE username = %s", (target,))
                t_row = cur.fetchone()
                c_val = t_row["cowoncy"] if t_row else coins
                add_bot("OwO Bot", f"💳 **@{target}** holds **{c_val} Cowoncy**.")
            elif sub == "hunt":
                cost = 25
                if coins < cost:
                    add_bot("OwO Bot", f"❌ Need at least **{cost} Cowoncy** to hunt! Try `owo daily`.")
                else:
                    creatures = [
                        ("🦊 Celestial Kitsune", 75),
                        ("🐉 Void Drake", 110),
                        ("🦉 Astral Owl", 55),
                        ("🦄 Starlight Unicorn", 150),
                        ("🐾 Shadow Cat", 40),
                    ]
                    caught, reward = random.choice(creatures)
                    new_c = coins - cost + reward
                    cur.execute("UPDATE users SET cowoncy = %s WHERE username = %s", (new_c, username))
                    add_bot(
                        "OwO Bot",
                        f"🏹 **@{username}** spent 25 and caught {caught} (**+{reward} Cowoncy**)! Balance: **{new_c}**.",
                    )
            elif sub == "battle":
                win = random.random() >= 0.44
                delta = random.randint(45, 120) if win else -min(coins, 35)
                new_c = max(0, coins + delta)
                cur.execute("UPDATE users SET cowoncy = %s WHERE username = %s", (new_c, username))
                if win:
                    add_bot("OwO Bot", f"⚔️ **@{username}** won the OwO duel (**+{delta} Cowoncy**)! Balance: **{new_c}**.")
                else:
                    add_bot("OwO Bot", f"💀 **@{username}** lost the duel (**{delta} Cowoncy**). Balance: **{new_c}**.")
            elif sub in ("slots", "s"):
                bet = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 50
                bet = max(10, min(bet, coins))
                if coins < 10:
                    add_bot("OwO Bot", "❌ Not enough Cowoncy to spin slots! Try `owo daily`.")
                else:
                    syms = ["🍒", "🍋", "💎", "7️⃣", "🌸"]
                    reels = [random.choice(syms) for _ in range(3)]
                    if reels[0] == reels[1] == reels[2]:
                        payout = bet * 4
                        new_c = coins + payout
                        msg = f"🎰 `[ {' | '.join(reels)} ]` JACKPOT! **@{username}** won **+{payout} Cowoncy**! (Bal: **{new_c}**)"
                    elif reels[0] == reels[1] or reels[1] == reels[2]:
                        payout = bet
                        new_c = coins + payout
                        msg = f"🎰 `[ {' | '.join(reels)} ]` Nice pair! **@{username}** won **+{payout} Cowoncy**! (Bal: **{new_c}**)"
                    else:
                        new_c = coins - bet
                        msg = f"🎰 `[ {' | '.join(reels)} ]` **@{username}** lost **{bet} Cowoncy**. (Bal: **{new_c}**)"
                    cur.execute("UPDATE users SET cowoncy = %s WHERE username = %s", (new_c, username))
                    add_bot("OwO Bot", msg)
            else:
                add_bot("OwO Bot", "🐾 Commands: `owo daily`, `owo hunt`, `owo battle`, `owo cash`, `owo slots <bet>`.")
        return bot_replies

    # 4. Slash Commands
    if not text.startswith("/"):
        return None

    parts = text.split()
    cmd = parts[0].lower()

    if cmd == "/help":
        add_bot("AetherBot", build_help_text())

    elif cmd in ("/ask", "/oracle"):
        question = text[len(parts[0]):].strip() or "Give me a clever insight for today."
        history = get_recent_room_context(conn, room, limit=8)
        if cmd == "/oracle":
            persona = f"You are the Mystical Oracle Bot of AetherRealm giving a dramatic, poetic yet witty prophecy/answer to @{username}. Under 100 words."
            bot_label = "Oracle Bot"
        else:
            persona = f"You are AetherBot, a smart, conversational AI assistant in AetherRealm talking to @{username}. Be insightful, direct, and engaging. Under 120 words."
            bot_label = "AetherBot"
        reply = await call_smart_gemini(persona, question, room_history=history)
        add_bot(bot_label, f"**@{username}:** {reply}")

    elif cmd in ("/roast", "/compliment"):
        target = parts[1].lstrip("@") if len(parts) > 1 else username
        mode_desc = "a playful, clever comedy roast (PG-13, friendly banter)" if cmd == "/roast" else "an epic, hype-filled gamer/anime compliment"
        reply = await call_smart_gemini(
            f"You are MEE6 Bot giving {mode_desc} to @{target} requested by @{username}. Keep it punchy (1-2 sentences).",
            f"{cmd} @{target}",
            fallback=f"🔥 **@{target}** is too legendary for standard metrics!",
        )
        add_bot("MEE6", reply)

    elif cmd in ("/rank", "/level"):
        target = parts[1].lstrip("@") if len(parts) > 1 else username
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT username, xp, level, honor, role_title, messages_count FROM users WHERE username = %s",
                (target,),
            )
            row = cur.fetchone()
            if not row:
                add_bot("MEE6", f"❌ User **@{target}** not found.")
            else:
                lvl = compute_level(row["xp"] or 0)
                next_xp = (lvl ** 2) * 100
                add_bot(
                    "MEE6",
                    f"📊 **Rank Card — @{row['username']}** (`{row['role_title'] or 'Voyager'}`)\n"
                    f"• **Level:** {lvl} | **XP:** {row['xp']} / {next_xp}\n"
                    f"• **Honor:** {row['honor']} | **Messages:** {row['messages_count']}",
                )

    elif cmd == "/leaderboard":
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT username, xp, honor, cowoncy FROM users ORDER BY xp DESC LIMIT 5")
            rows = cur.fetchall()
        lines = ["🏆 **Top Citizens (ProBot & MEE6)**"]
        for idx, r in enumerate(rows, start=1):
            lines.append(
                f"{idx}. **@{r['username']}** — {r['xp']} XP • {r['honor']} Honor • {r['cowoncy']} Cowoncy"
            )
        add_bot("ProBot", "\n".join(lines))

    elif cmd == "/stats":
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS c FROM users")
            u_cnt = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM chat_messages")
            m_cnt = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM lounges")
            l_cnt = cur.fetchone()["c"]
        add_bot(
            "Statbot",
            f"📈 **Live Telemetry:** **{u_cnt}** citizens • **{m_cnt}** messages • **{l_cnt}** lounges • **{len(manager.online_users)}** online.",
        )

    elif cmd == "/roll":
        max_val = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 100
        max_val = max(2, min(max_val, 1_000_000))
        rolled = random.randint(1, max_val)
        add_bot("Carl-bot", f"🎲 **@{username}** rolled **{rolled}** (1–{max_val}).")

    elif cmd == "/rps":
        choices = ["rock", "paper", "scissors"]
        pick = parts[1].lower() if len(parts) > 1 else random.choice(choices)
        if pick not in choices:
            pick = random.choice(choices)
        bot_pick = random.choice(choices)
        wins = {("rock", "scissors"), ("paper", "rock"), ("scissors", "paper")}
        if pick == bot_pick:
            outcome = "It's a tie!"
        elif (pick, bot_pick) in wins:
            outcome = f"**@{username}** wins!"
        else:
            outcome = "Carl-bot wins!"
        add_bot("Carl-bot", f"✊ **@{username}** chose **{pick}** vs Carl-bot's **{bot_pick}** — {outcome}")

    elif cmd == "/meme":
        meme_text = "😂 *When your WebSocket syncs audio, voice, and YouTube on the first try!*"
        meme_img = None
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get("https://meme-api.com/gimme")
                if r.status_code == 200:
                    data = r.json()
                    meme_text = f"😂 **{data.get('title', 'Fresh Meme')}** — r/{data.get('subreddit', 'memes')}"
                    meme_img = data.get("url")
        except Exception:
            pass
        add_bot("Dyno", meme_text, {"image_url": meme_img} if meme_img else {})

    elif cmd == "/play":
        query_str = text[len(parts[0]):].strip()
        if not query_str:
            add_bot("AetherBot", "🎧 Usage: `/play <song name or YouTube URL>` to search & sync for everyone!")
        else:
            found = await search_youtube_first(query_str)
            if found:
                vid = found["video_id"]
                title = found["title"]
                ts = now_ts()
                if room.startswith("lounge:"):
                    lid = room.split(":", 1)[1].strip()
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE lounges
                            SET video_id = %s, video_title = %s, is_playing = TRUE, base_position = 0.0, updated_at = %s
                            WHERE id::text = %s
                            """,
                            (vid, title, ts, lid),
                        )
                    payload = {
                        "type": "media_sync",
                        "channel_key": room,
                        "lounge_id": lid,
                        "video_id": vid,
                        "title": title,
                        "is_playing": True,
                        "base_position": 0.0,
                        "updated_at": ts,
                        "updated_by": username,
                    }
                    await manager.broadcast(room, payload)
                    await manager.broadcast("global", payload)
                    add_bot("AetherBot", f"🎵 Synced **{title}** in this Lounge for everyone!")
                else:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE media_state
                            SET video_id = %s, title = %s, is_playing = TRUE, base_position = 0.0, updated_at = %s, updated_by = %s
                            WHERE channel_key = 'entertainment'
                            """,
                            (vid, title, ts, username),
                        )
                    await manager.broadcast(
                        "global",
                        {
                            "type": "media_sync",
                            "channel_key": "entertainment",
                            "video_id": vid,
                            "title": title,
                            "is_playing": True,
                            "base_position": 0.0,
                            "updated_at": ts,
                            "updated_by": username,
                        },
                    )
                    add_bot("AetherBot", f"🎬 Synced **{title}** in **Sync Cinema & YT** for everyone!")
            else:
                add_bot("AetherBot", "❌ Could not find a matching YouTube video. Try pasting a YouTube URL.")

    elif cmd == "/gstart":
        sec = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 120
        sec = max(15, min(sec, 86400))
        prize = " ".join(parts[2:]).strip() if len(parts) > 2 else "500 Cowoncy + VIP Badge"
        ends = now_ts() + sec
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO giveaways (prize, host, entries, ends_at, active)
                VALUES (%s, %s, '[]'::jsonb, %s, TRUE)
                RETURNING id
                """,
                (prize, username, ends),
            )
            gid = cur.fetchone()["id"]
        add_bot(
            "GiveawayBot",
            f"🎉 **GIVEAWAY #{gid} STARTED!** Prize: **{prize}** | Hosted by **@{username}** | Ends in **{sec}s**!",
            {"giveaway_id": gid, "prize": prize, "ends_at": ends},
        )

    elif cmd == "/gend":
        gid = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if gid:
                cur.execute("SELECT * FROM giveaways WHERE id = %s AND active = TRUE", (gid,))
            else:
                cur.execute("SELECT * FROM giveaways WHERE active = TRUE ORDER BY id DESC LIMIT 1")
            gw = cur.fetchone()
            if not gw:
                add_bot("GiveawayBot", "ℹ️ No active giveaway found to end.")
            else:
                entries = gw["entries"] or []
                winner = random.choice(entries) if entries else None
                cur.execute(
                    "UPDATE giveaways SET active = FALSE, winner = %s WHERE id = %s",
                    (winner, gw["id"]),
                )
                if winner:
                    award_xp_and_coins(conn, winner, xp_gain=50, coins_gain=200)
                    add_bot("GiveawayBot", f"🎊 Giveaway **#{gw['id']}** (**{gw['prize']}**) ended! Winner: **@{winner}** (+200 Cowoncy)!")
                else:
                    add_bot("GiveawayBot", f"ℹ️ Giveaway **#{gw['id']}** ended with no entries.")

    elif cmd == "/ticket":
        reason = text[len(parts[0]):].strip() or "General support request"
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO tickets (creator, reason, status) VALUES (%s, %s, 'open') RETURNING id",
                (username, reason),
            )
            tid = cur.fetchone()["id"]
        add_bot("Ticket Tool", f"🎫 Ticket **#{tid}** opened by **@{username}**: *{reason}*. Use `/close {tid}` when resolved.")

    elif cmd == "/close":
        tid = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
        with conn.cursor() as cur:
            if tid:
                cur.execute("UPDATE tickets SET status = 'closed' WHERE id = %s", (tid,))
            else:
                cur.execute("UPDATE tickets SET status = 'closed' WHERE creator = %s AND status = 'open'", (username,))
        add_bot("Ticket Tool", f"🔒 Ticket closed by **@{username}**.")

    elif cmd == "/roles":
        allowed = {"voyager", "strategist", "bard", "guardian", "streamer"}
        chosen = parts[1].lower() if len(parts) > 1 else ""
        if chosen in allowed:
            title_cap = chosen.capitalize()
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET role_title = %s WHERE username = %s", (title_cap, username))
            add_bot("Reaction Roles", f"🏷️ **@{username}** equipped the **{title_cap}** role!")
        else:
            add_bot("Reaction Roles", "🏷️ Available roles: `/roles Voyager`, `/roles Strategist`, `/roles Bard`, `/roles Guardian`, `/roles Streamer`.")

    elif cmd == "/captcha":
        a, b = random.randint(2, 9), random.randint(2, 9)
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET captcha_verified = TRUE WHERE username = %s", (username,))
        add_bot("Captcha.bot", f"🛡️ **@{username}** passed verification (`{a} + {b} = {a + b}`)! Access granted.")

    elif cmd == "/warn":
        if len(parts) < 2:
            add_bot("Wick", "⚠️ Usage: `/warn <username> [reason]`")
        else:
            target = parts[1].lstrip("@")
            reason = " ".join(parts[2:]).strip() or "Rule violation"
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "UPDATE users SET warnings = COALESCE(warnings, 0) + 1 WHERE username = %s RETURNING warnings",
                    (target,),
                )
                w_row = cur.fetchone()
            if w_row:
                add_bot("Wick", f"🚨 **@{target}** received a warning (#{w_row['warnings']}) from **@{username}**: *{reason}*.")
            else:
                add_bot("Wick", f"❌ Target **@{target}** not found.")

    elif cmd == "/tag":
        if len(parts) >= 4 and parts[1].lower() == "set":
            tag_name = parts[2].lower()
            tag_body = text.split(parts[2], 1)[1].strip()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bot_tags (name, response, author)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (name) DO UPDATE SET response = EXCLUDED.response, author = EXCLUDED.author
                    """,
                    (tag_name, tag_body, username),
                )
            add_bot("Carl-bot", f"📌 Saved tag `{tag_name}` by **@{username}**.")
        elif len(parts) >= 2:
            tag_name = parts[1].lower()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT response FROM bot_tags WHERE name = %s", (tag_name,))
                t_row = cur.fetchone()
            if t_row:
                add_bot("Carl-bot", f"📌 **{tag_name}:** {t_row['response']}")
            else:
                add_bot("Carl-bot", f"❓ Unknown tag `{tag_name}`. Create one with `/tag set {tag_name} <message>`.")
        else:
            add_bot("Carl-bot", "📌 Usage: `/tag set <name> <reply>` or `/tag <name>`.")

    elif cmd == "/poll":
        raw_poll = text[len(parts[0]):].strip()
        chunks = [c.strip() for c in raw_poll.split("|") if c.strip()]
        if len(chunks) >= 3:
            q_text = chunks[0]
            opts = chunks[1:6]
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "INSERT INTO polls (question, options, votes, created_by) VALUES (%s, %s::jsonb, '{}'::jsonb, %s) RETURNING id",
                    (q_text, json.dumps(opts), username),
                )
                pid = cur.fetchone()["id"]
            add_bot("Simple Poll", f"📊 **Poll #{pid}:** {q_text}\nOptions: " + " • ".join(f"**{o}**" for o in opts))
        else:
            add_bot("Simple Poll", "📊 Usage: `/poll Question | Option 1 | Option 2 | Option 3`")

    elif cmd == "/birthday":
        bday = parts[1].strip() if len(parts) > 1 else ""
        if re.fullmatch(r"(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])", bday):
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET birthday = %s WHERE username = %s", (bday, username))
            add_bot("Birthday Bot", f"🎂 Saved **@{username}**'s birthday as **{bday}**!")
        else:
            add_bot("Birthday Bot", "🎂 Usage: `/birthday MM-DD` (example: `/birthday 07-14`).")

    else:
        # Treat unknown slash command as a smart AI query instead of an error
        history = get_recent_room_context(conn, room, limit=6)
        reply = await call_smart_gemini(
            f"You are AetherBot in AetherRealm replying to @{username}. If they typed a custom slash command `{text}`, play along cleverly or help them. Under 90 words.",
            text,
            room_history=history,
            fallback=f"🤔 Command `{cmd}` not recognized. Type `/help` to view all bot commands.",
        )
        add_bot("AetherBot", reply)

    return bot_replies


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------
class RegisterReq(BaseModel):
    username: str = Field(..., min_length=2, max_length=40)
    email: Optional[str] = None
    password: str = Field(..., min_length=4)
    birthday: Optional[str] = None


class LoginReq(BaseModel):
    username: str
    password: str


class VerifyReq(BaseModel):
    username: str
    code: str


class ResetReq(BaseModel):
    username: str
    code: Optional[str] = None
    new_password: Optional[str] = None


class ProfileUpdateReq(BaseModel):
    bio: Optional[str] = None
    affinity: Optional[str] = None
    theme: Optional[str] = None
    sticker: Optional[str] = None
    decoration: Optional[str] = None
    role_title: Optional[str] = None
    notes: Optional[str] = None


class FriendActionReq(BaseModel):
    target_username: str
    action: str = "request"


class DmSendReq(BaseModel):
    recipient: str
    content: str = Field(..., min_length=1, max_length=2000)


class ChatPostReq(BaseModel):
    room: str = "global"
    content: str = Field(..., min_length=1, max_length=2000)


class ConfessionCreateReq(BaseModel):
    content: str = Field(..., min_length=1, max_length=2000)
    is_anonymous: bool = True


class ConfessionReplyReq(BaseModel):
    content: str = Field(..., min_length=1, max_length=1000)
    is_anonymous: bool = False


class LoungeCreateReq(BaseModel):
    name: str = Field(..., min_length=2, max_length=80)
    topic: str = "Hangout, Voice & Music"
    theme: str = "sakura"
    video_url_or_id: Optional[str] = "jfKfPfyJRdk"


class MediaSyncReq(BaseModel):
    channel_key: str = "entertainment"
    lounge_id: Optional[Union[int, str]] = None
    action: str = "sync"
    video_url_or_id: Optional[str] = None
    title: Optional[str] = None
    position: Optional[float] = None


class StoryCreateReq(BaseModel):
    content: str = Field(..., min_length=1, max_length=300)
    mood: str = "✨"


class PollCreateReq(BaseModel):
    question: str = Field(..., min_length=2, max_length=300)
    options: List[str] = Field(..., min_length=2, max_length=6)


class PollVoteReq(BaseModel):
    option_index: int


class BossAttackReq(BaseModel):
    damage: int = Field(default=150, ge=10, le=2000)


class ScoreSubmitReq(BaseModel):
    game: str
    score: int = Field(..., ge=0)


class ShopBuyReq(BaseModel):
    item_name: str
    cost: int = Field(..., ge=0)


# ---------------------------------------------------------------------------
# Health, Static & Auth Endpoints
# ---------------------------------------------------------------------------
@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "timestamp": now_utc().isoformat(),
        "online_users": len(manager.online_users),
    }


@app.get("/")
def serve_index():
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return JSONResponse({"message": "API running. Place index.html in project root."})


@app.get("/admin.html")
def serve_admin():
    if os.path.exists("admin.html"):
        return FileResponse("admin.html")
    raise HTTPException(status_code=404, detail="admin.html not found")


@app.post("/api/auth/register")
def register(req: RegisterReq):
    clean_user = re.sub(r"[^A-Za-z0-9_-]", "", req.username.strip())
    if len(clean_user) < 2:
        raise HTTPException(status_code=400, detail="Username must be at least 2 alphanumeric characters")

    code = f"{random.randint(100000, 999999)}"
    pw_hash = hash_password(req.password)

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id FROM users WHERE LOWER(username) = LOWER(%s)", (clean_user,))
            if cur.fetchone():
                raise HTTPException(status_code=400, detail="Username already taken")

            if req.email:
                cur.execute("SELECT id FROM users WHERE LOWER(email) = LOWER(%s)", (req.email.strip(),))
                if cur.fetchone():
                    raise HTTPException(status_code=400, detail="Email already registered")

            cur.execute(
                """
                INSERT INTO users (username, email, password_hash, verification_code, is_verified, birthday, xp, level, honor, cowoncy)
                VALUES (%s, %s, %s, %s, TRUE, %s, 25, 1, 100, 250)
                RETURNING id, username, email, bio, affinity, theme, sticker, decoration, role_title, xp, level, honor, cowoncy, inventory
                """,
                (clean_user, req.email.strip() if req.email else None, pw_hash, code, req.birthday),
            )
            user = dict(cur.fetchone())
            cur.execute(
                """
                INSERT INTO chat_messages (room, sender, is_bot, bot_name, content)
                VALUES ('global', 'Welcomer', TRUE, 'Welcomer', %s)
                """,
                (f"🎉 Welcome **@{clean_user}** to AetherRealm! Say hi, join a voice lounge, or type `/help`.",),
            )
            conn.commit()

        if req.email:
            send_email_if_configured(req.email, "Your Verification Code", f"Code: {code}")

        token = create_access_token({"username": clean_user})
        return {"token": token, "user": user, "verification_code": code}
    finally:
        release_db_connection(conn)


@app.post("/api/auth/login")
def login(req: LoginReq):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM users WHERE LOWER(username) = LOWER(%s) OR LOWER(email) = LOWER(%s)",
                (req.username.strip(), req.username.strip()),
            )
            user = cur.fetchone()
            if not user or not verify_password(req.password, user["password_hash"]):
                raise HTTPException(status_code=401, detail="Invalid credentials")

            token = create_access_token({"username": user["username"]})
            u = dict(user)
            u.pop("password_hash", None)
            manager.online_users[user["username"]] = now_ts()
            return {"token": token, "user": u}
    finally:
        release_db_connection(conn)


@app.post("/api/auth/verify")
def verify_email(req: VerifyReq):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT verification_code FROM users WHERE username = %s", (req.username,))
            row = cur.fetchone()
            if not row or row["verification_code"] != req.code.strip():
                raise HTTPException(status_code=400, detail="Invalid verification code")
            cur.execute("UPDATE users SET is_verified = TRUE WHERE username = %s", (req.username,))
            conn.commit()
            return {"verified": True}
    finally:
        release_db_connection(conn)


@app.post("/api/auth/reset-password")
def reset_password(req: ResetReq):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, email FROM users WHERE username = %s", (req.username,))
            user = cur.fetchone()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")
            if not req.code or not req.new_password:
                code = f"{random.randint(100000, 999999)}"
                cur.execute("UPDATE users SET reset_code = %s WHERE id = %s", (code, user["id"]))
                conn.commit()
                if user["email"]:
                    send_email_if_configured(user["email"], "Password Reset Code", f"Code: {code}")
                return {"reset_code_sent": True, "demo_code": code}
            cur.execute("SELECT reset_code FROM users WHERE id = %s", (user["id"],))
            if cur.fetchone()["reset_code"] != req.code.strip():
                raise HTTPException(status_code=400, detail="Invalid reset code")
            cur.execute(
                "UPDATE users SET password_hash = %s, reset_code = NULL WHERE id = %s",
                (hash_password(req.new_password), user["id"]),
            )
            conn.commit()
            return {"password_reset": True}
    finally:
        release_db_connection(conn)


@app.get("/api/me")
def get_me(current_user: Dict[str, Any] = Depends(get_current_user)):
    manager.online_users[current_user["username"]] = now_ts()
    return {
        "user": current_user,
        "online_users": sorted(
            [u for u, ts in manager.online_users.items() if now_ts() - ts < 180]
        ),
    }


@app.put("/api/profile")
@app.patch("/api/profile")
def update_profile(req: ProfileUpdateReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    updates = []
    params: List[Any] = []
    for field in ["bio", "affinity", "theme", "sticker", "decoration", "role_title", "notes"]:
        val = getattr(req, field)
        if val is not None:
            updates.append(f"{field} = %s")
            params.append(val)
    if not updates:
        return {"user": current_user}
    params.append(current_user["username"])
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"UPDATE users SET {', '.join(updates)} WHERE username = %s RETURNING *",
                tuple(params),
            )
            u = dict(cur.fetchone())
            u.pop("password_hash", None)
            conn.commit()
            return {"user": u}
    finally:
        release_db_connection(conn)


@app.post("/api/shop/buy")
def buy_shop_item(req: ShopBuyReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT cowoncy, inventory FROM users WHERE username = %s", (current_user["username"],))
            row = cur.fetchone()
            coins = row["cowoncy"] or 0
            inv = row["inventory"] or []
            if req.item_name in inv:
                return {"cowoncy": coins, "inventory": inv, "already_owned": True}
            if coins < req.cost:
                raise HTTPException(status_code=400, detail="Not enough Cowoncy")
            new_coins = coins - req.cost
            inv.append(req.item_name)
            cur.execute(
                "UPDATE users SET cowoncy = %s, inventory = %s::jsonb WHERE username = %s",
                (new_coins, json.dumps(inv), current_user["username"]),
            )
            conn.commit()
            return {"cowoncy": new_coins, "inventory": inv}
    finally:
        release_db_connection(conn)


# ---------------------------------------------------------------------------
# Users, Honor, Friends & Real-Time DMs
# ---------------------------------------------------------------------------
@app.get("/api/users")
def list_users(current_user: Dict[str, Any] = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT username, bio, affinity, theme, sticker, decoration, role_title, xp, level, honor, cowoncy FROM users ORDER BY honor DESC LIMIT 100"
            )
            users = [dict(r) for r in cur.fetchall()]
            cutoff = now_ts() - 180
            for u in users:
                u["online"] = manager.online_users.get(u["username"], 0) >= cutoff
            return {"users": users}
    finally:
        release_db_connection(conn)


@app.post("/api/honor/{target_username}")
def give_honor(target_username: str, amount: int = 25, current_user: Dict[str, Any] = Depends(get_current_user)):
    amount = max(1, min(amount, 100))
    today_str = now_utc().strftime("%Y-%m-%d")
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT daily_honor, last_honor_reset FROM users WHERE username = %s",
                (current_user["username"],),
            )
            me = cur.fetchone()
            used_today = me["daily_honor"] if me["last_honor_reset"] == today_str else 0
            if used_today + amount > DAILY_HONOR_CAP:
                raise HTTPException(status_code=400, detail="Daily honor cap (500) reached")

            cur.execute(
                "UPDATE users SET honor = honor + %s WHERE username = %s RETURNING honor",
                (amount, target_username),
            )
            t_row = cur.fetchone()
            if not t_row:
                raise HTTPException(status_code=404, detail="Target user not found")

            cur.execute(
                "UPDATE users SET daily_honor = %s, last_honor_reset = %s WHERE username = %s",
                (used_today + amount, today_str, current_user["username"]),
            )
            conn.commit()
            return {"target": target_username, "new_honor": t_row["honor"], "daily_used": used_today + amount}
    finally:
        release_db_connection(conn)


@app.get("/api/friends")
def get_friends(current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM friends WHERE requester = %s OR addressee = %s ORDER BY created_at DESC",
                (uname, uname),
            )
            rows = [dict(r) for r in cur.fetchall()]
            friends_list = []
            incoming = []
            outgoing = []
            for r in rows:
                if r["status"] == "accepted":
                    other = r["addressee"] if r["requester"] == uname else r["requester"]
                    friends_list.append(other)
                elif r["status"] == "pending":
                    if r["addressee"] == uname:
                        incoming.append(r["requester"])
                    else:
                        outgoing.append(r["addressee"])
            return {"friends": friends_list, "incoming": incoming, "outgoing": outgoing}
    finally:
        release_db_connection(conn)


@app.post("/api/friends/action")
async def friend_action(req: FriendActionReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    target = req.target_username.strip()
    if target == uname:
        raise HTTPException(status_code=400, detail="Cannot add yourself")
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT username FROM users WHERE username = %s", (target,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="User not found")

            if req.action == "request":
                cur.execute(
                    """
                    INSERT INTO friends (requester, addressee, status)
                    VALUES (%s, %s, 'pending')
                    ON CONFLICT (requester, addressee) DO UPDATE SET status = 'pending'
                    """,
                    (uname, target),
                )
            elif req.action == "accept":
                cur.execute(
                    "UPDATE friends SET status = 'accepted' WHERE requester = %s AND addressee = %s",
                    (target, uname),
                )
            elif req.action == "decline":
                cur.execute(
                    "DELETE FROM friends WHERE (requester = %s AND addressee = %s) OR (requester = %s AND addressee = %s)",
                    (target, uname, uname, target),
                )
            conn.commit()

        await manager.broadcast(
            "global",
            {
                "type": "friend_update",
                "actor": uname,
                "target": target,
                "action": req.action,
            },
        )
        return {"ok": True, "action": req.action, "target": target}
    finally:
        release_db_connection(conn)


@app.get("/api/dms/{other_user}")
def get_dms(other_user: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM direct_messages
                WHERE (sender = %s AND recipient = %s) OR (sender = %s AND recipient = %s)
                ORDER BY id ASC LIMIT 200
                """,
                (uname, other_user, other_user, uname),
            )
            return {"messages": [dict(r) for r in cur.fetchall()]}
    finally:
        release_db_connection(conn)


@app.post("/api/dms")
async def send_dm(req: DmSendReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT username FROM users WHERE username = %s", (req.recipient,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Recipient not found")
            cur.execute(
                """
                INSERT INTO direct_messages (sender, recipient, content)
                VALUES (%s, %s, %s)
                RETURNING *
                """,
                (uname, req.recipient, req.content.strip()),
            )
            msg = dict(cur.fetchone())
            conn.commit()

        pair = sorted([uname, req.recipient])
        dm_room = f"dm:{pair[0]}:{pair[1]}"
        await manager.broadcast(dm_room, {"type": "dm_message", "message": msg})
        await manager.broadcast("global", {"type": "dm_notify", "message": msg})
        return {"message": msg}
    finally:
        release_db_connection(conn)


# ---------------------------------------------------------------------------
# Chat Rooms, Smart Bots, Giveaways & Counting Endpoints
# ---------------------------------------------------------------------------
@app.get("/api/chat/{room}")
def get_chat_history(room: str, limit: int = 80):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM chat_messages WHERE room = %s ORDER BY id DESC LIMIT %s",
                (room, min(limit, 200)),
            )
            rows = [dict(r) for r in cur.fetchall()]
            rows.reverse()
            cur.execute("SELECT * FROM counting_state WHERE id = 1")
            c_state = dict(cur.fetchone())
            cur.execute("SELECT * FROM giveaways WHERE active = TRUE ORDER BY id DESC LIMIT 5")
            g_list = [dict(r) for r in cur.fetchall()]
            return {
                "messages": rows,
                "counting": c_state,
                "giveaways": g_list,
                "voice_peers": manager.voice_participants.get(room, {}),
            }
    finally:
        release_db_connection(conn)


@app.post("/api/chat/send")
async def post_chat_message(req: ChatPostReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    manager.online_users[uname] = now_ts()
    content = req.content.strip()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO chat_messages (room, sender, is_bot, content)
                VALUES (%s, %s, FALSE, %s)
                RETURNING *
                """,
                (req.room, uname, content),
            )
            user_msg = dict(cur.fetchone())
            cur.execute("UPDATE users SET messages_count = COALESCE(messages_count, 0) + 1 WHERE username = %s", (uname,))
            xp_info = award_xp_and_coins(conn, uname, xp_gain=10, coins_gain=5)

            bot_msgs = await process_bot_command(conn, req.room, uname, content)
            conn.commit()

        await manager.broadcast(
            req.room,
            {"type": "chat_message", "message": user_msg, "xp_info": xp_info},
        )
        if req.room != "global":
            await manager.broadcast(
                "global",
                {"type": "chat_message", "message": user_msg, "xp_info": xp_info},
            )

        for bm in (bot_msgs or []):
            await manager.broadcast(req.room, {"type": "bot_message", "message": bm})
            if req.room != "global":
                await manager.broadcast("global", {"type": "bot_message", "message": bm})

        return {"message": user_msg, "bot_messages": bot_msgs or [], "xp_info": xp_info}
    finally:
        release_db_connection(conn)


@app.post("/api/giveaways/{giveaway_id}/join")
async def join_giveaway(giveaway_id: int, current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
            gw = cur.fetchone()
            if not gw or not gw["active"]:
                raise HTTPException(status_code=404, detail="Giveaway not active")
            entries = gw["entries"] or []
            if uname not in entries:
                entries.append(uname)
                cur.execute(
                    "UPDATE giveaways SET entries = %s::jsonb WHERE id = %s",
                    (json.dumps(entries), giveaway_id),
                )
                conn.commit()
        await manager.broadcast(
            "global",
            {"type": "giveaway_update", "giveaway_id": giveaway_id, "entries_count": len(entries), "user": uname},
        )
        return {"joined": True, "entries_count": len(entries)}
    finally:
        release_db_connection(conn)


# ---------------------------------------------------------------------------
# Lounges & Synchronized YouTube / Music Engine
# ---------------------------------------------------------------------------
@app.get("/api/lounges")
def list_lounges():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM lounges ORDER BY id ASC")
            lounges = [dict(r) for r in cur.fetchall()]
            t = now_ts()
            for l in lounges:
                elapsed = max(0.0, t - (l.get("updated_at") or t)) if l.get("is_playing") else 0.0
                l["computed_position"] = round((l.get("base_position") or 0.0) + elapsed, 2)
                l["server_time"] = t
                l["voice_count"] = len(manager.voice_participants.get(f"lounge:{l['id']}", {}))
            return {"lounges": lounges}
    finally:
        release_db_connection(conn)


@app.post("/api/lounges")
async def create_lounge(req: LoungeCreateReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    found = await search_youtube_first(req.video_url_or_id or "jfKfPfyJRdk")
    vid = found["video_id"] if found else "jfKfPfyJRdk"
    title = found["title"] if found else f"Stream ({vid})"
    ts = now_ts()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO lounges (name, topic, theme, creator, members, video_id, video_title, is_playing, base_position, updated_at)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, TRUE, 0.0, %s)
                RETURNING *
                """,
                (req.name.strip(), req.topic.strip(), req.theme, uname, json.dumps([uname]), vid, title, ts),
            )
            lounge = dict(cur.fetchone())
            conn.commit()
        await manager.broadcast("global", {"type": "lounge_created", "lounge": lounge})
        return {"lounge": lounge}
    finally:
        release_db_connection(conn)


@app.post("/api/lounges/{lounge_id}/join")
async def join_lounge(lounge_id: Union[int, str], current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    lid_str = str(lounge_id)
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM lounges WHERE id::text = %s", (lid_str,))
            l = cur.fetchone()
            if not l:
                raise HTTPException(status_code=404, detail="Lounge not found")
            members = list(l["members"] or [])
            if uname not in members:
                if len(members) >= MAX_LOUNGE_MEMBERS:
                    raise HTTPException(status_code=400, detail="Lounge is full")
                members.append(uname)
                cur.execute("UPDATE lounges SET members = %s::jsonb WHERE id::text = %s", (json.dumps(members), lid_str))
                conn.commit()
            cur.execute("SELECT * FROM lounges WHERE id::text = %s", (lid_str,))
            updated = dict(cur.fetchone())
        await manager.broadcast("global", {"type": "lounge_updated", "lounge": updated})
        await manager.broadcast(f"lounge:{lounge_id}", {"type": "lounge_updated", "lounge": updated})
        return {"lounge": updated}
    finally:
        release_db_connection(conn)


@app.post("/api/lounges/{lounge_id}/leave")
async def leave_lounge(lounge_id: Union[int, str], current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    lid_str = str(lounge_id)
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM lounges WHERE id::text = %s", (lid_str,))
            l = cur.fetchone()
            if not l:
                raise HTTPException(status_code=404, detail="Lounge not found")
            members = [m for m in (l["members"] or []) if m != uname]
            cur.execute("UPDATE lounges SET members = %s::jsonb WHERE id::text = %s RETURNING *", (json.dumps(members), lid_str))
            updated = dict(cur.fetchone())
            conn.commit()
        await manager.broadcast("global", {"type": "lounge_updated", "lounge": updated})
        await manager.broadcast(f"lounge:{lounge_id}", {"type": "lounge_updated", "lounge": updated})
        return {"lounge": updated}
    finally:
        release_db_connection(conn)


@app.get("/api/media/{channel_key}")
def get_media_state(channel_key: str, lounge_id: Optional[Union[int, str]] = None):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            t = now_ts()
            if lounge_id is not None:
                cur.execute("SELECT * FROM lounges WHERE id::text = %s", (str(lounge_id),))
                l = cur.fetchone()
                if not l:
                    raise HTTPException(status_code=404, detail="Lounge not found")
                elapsed = max(0.0, t - (l["updated_at"] or t)) if l["is_playing"] else 0.0
                return {
                    "channel_key": f"lounge:{lounge_id}",
                    "video_id": l["video_id"],
                    "title": l["video_title"],
                    "queue": l["queue"] or [],
                    "is_playing": l["is_playing"],
                    "base_position": l["base_position"] or 0.0,
                    "computed_position": round((l["base_position"] or 0.0) + elapsed, 2),
                    "updated_at": l["updated_at"] or t,
                    "server_time": t,
                }

            cur.execute("SELECT * FROM media_state WHERE channel_key = %s", (channel_key,))
            m = cur.fetchone()
            if not m:
                raise HTTPException(status_code=404, detail="Media channel not found")
            elapsed = max(0.0, t - (m["updated_at"] or t)) if m["is_playing"] else 0.0
            return {
                "channel_key": m["channel_key"],
                "video_id": m["video_id"],
                "title": m["title"],
                "queue": m["queue"] or [],
                "is_playing": m["is_playing"],
                "base_position": m["base_position"] or 0.0,
                "computed_position": round((m["base_position"] or 0.0) + elapsed, 2),
                "updated_at": m["updated_at"] or t,
                "server_time": t,
                "updated_by": m["updated_by"],
            }
    finally:
        release_db_connection(conn)


@app.post("/api/media/sync")
async def sync_media(req: MediaSyncReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    ts = now_ts()
    parsed_vid = extract_youtube_id(req.video_url_or_id or "")
    resolved_title = req.title
    if not parsed_vid and req.video_url_or_id:
        found = await search_youtube_first(req.video_url_or_id)
        if found:
            parsed_vid = found["video_id"]
            resolved_title = resolved_title or found["title"]

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if req.lounge_id is not None:
                lid_str = str(req.lounge_id)
                cur.execute("SELECT * FROM lounges WHERE id::text = %s", (lid_str,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Lounge not found")

                cur_pos = (row["base_position"] or 0.0) + (
                    max(0.0, ts - (row["updated_at"] or ts)) if row["is_playing"] else 0.0
                )
                pos = req.position if req.position is not None else cur_pos
                vid = parsed_vid or row["video_id"]
                title = resolved_title or (f"YouTube Track ({vid})" if parsed_vid else row["video_title"])
                playing = row["is_playing"]
                queue = list(row["queue"] or [])

                if req.action == "play":
                    playing = True
                elif req.action == "pause":
                    playing = False
                elif req.action == "seek":
                    pos = max(0.0, float(req.position or 0.0))
                elif req.action == "queue" and parsed_vid:
                    item = {"video_id": parsed_vid, "title": resolved_title or f"Track ({parsed_vid})", "by": uname}
                    if not vid:
                        vid = parsed_vid
                        title = item["title"]
                        pos = 0.0
                        playing = True
                    else:
                        queue.append(item)
                elif req.action == "skip" and queue:
                    nxt = queue.pop(0)
                    vid = nxt["video_id"]
                    title = nxt["title"]
                    pos = 0.0
                    playing = True

                cur.execute(
                    """
                    UPDATE lounges
                    SET video_id = %s, video_title = %s, queue = %s::jsonb, is_playing = %s, base_position = %s, updated_at = %s
                    WHERE id::text = %s
                    RETURNING *
                    """,
                    (vid, title, json.dumps(queue), playing, pos, ts, lid_str),
                )
                updated_l = dict(cur.fetchone())
                conn.commit()

                payload = {
                    "type": "media_sync",
                    "channel_key": f"lounge:{req.lounge_id}",
                    "lounge_id": req.lounge_id,
                    "video_id": updated_l["video_id"],
                    "title": updated_l["video_title"],
                    "queue": updated_l["queue"],
                    "is_playing": updated_l["is_playing"],
                    "base_position": updated_l["base_position"],
                    "updated_at": updated_l["updated_at"],
                    "updated_by": uname,
                }
                await manager.broadcast(f"lounge:{req.lounge_id}", payload)
                await manager.broadcast("global", payload)
                return payload

            cur.execute("SELECT * FROM media_state WHERE channel_key = %s", (req.channel_key,))
            m = cur.fetchone()
            if not m:
                cur.execute(
                    """
                    INSERT INTO media_state (channel_key, video_id, title, queue, is_playing, base_position, updated_at, updated_by)
                    VALUES (%s, 'jfKfPfyJRdk', 'Lofi Beats', '[]'::jsonb, TRUE, 0.0, %s, %s)
                    RETURNING *
                    """,
                    (req.channel_key, ts, uname),
                )
                m = cur.fetchone()

            cur_pos = (m["base_position"] or 0.0) + (
                max(0.0, ts - (m["updated_at"] or ts)) if m["is_playing"] else 0.0
            )
            pos = req.position if req.position is not None else cur_pos
            vid = parsed_vid or m["video_id"]
            title = resolved_title or (f"YouTube Track ({vid})" if parsed_vid else m["title"])
            playing = m["is_playing"]
            queue = list(m["queue"] or [])

            if req.action == "play":
                playing = True
            elif req.action == "pause":
                playing = False
            elif req.action == "seek":
                pos = max(0.0, float(req.position or 0.0))
            elif req.action == "queue" and parsed_vid:
                queue.append({"video_id": parsed_vid, "title": resolved_title or f"Track ({parsed_vid})", "by": uname})
            elif req.action == "skip" and queue:
                nxt = queue.pop(0)
                vid = nxt["video_id"]
                title = nxt["title"]
                pos = 0.0
                playing = True

            cur.execute(
                """
                UPDATE media_state
                SET video_id = %s, title = %s, queue = %s::jsonb, is_playing = %s, base_position = %s, updated_at = %s, updated_by = %s
                WHERE channel_key = %s
                RETURNING *
                """,
                (vid, title, json.dumps(queue), playing, pos, ts, uname, req.channel_key),
            )
            updated_m = dict(cur.fetchone())
            conn.commit()

            payload = {
                "type": "media_sync",
                "channel_key": req.channel_key,
                "video_id": updated_m["video_id"],
                "title": updated_m["title"],
                "queue": updated_m["queue"],
                "is_playing": updated_m["is_playing"],
                "base_position": updated_m["base_position"],
                "updated_at": updated_m["updated_at"],
                "updated_by": uname,
            }
            await manager.broadcast("global", payload)
            await manager.broadcast("entertainment", payload)
            return payload
    finally:
        release_db_connection(conn)


@app.get("/api/youtube/search")
async def youtube_search(q: str = Query(..., min_length=1)):
    direct_id = extract_youtube_id(q)
    if direct_id:
        return {"items": [{"video_id": direct_id, "title": f"YouTube Video ({direct_id})"}]}

    if YOUTUBE_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(
                    "https://www.googleapis.com/youtube/v3/search",
                    params={
                        "part": "snippet",
                        "q": q,
                        "type": "video",
                        "maxResults": 6,
                        "key": YOUTUBE_API_KEY,
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    items = []
                    for it in data.get("items", []):
                        vid = it.get("id", {}).get("videoId")
                        title = it.get("snippet", {}).get("title", "YouTube Video")
                        if vid:
                            items.append({"video_id": vid, "title": title})
                    if items:
                        return {"items": items}
        except Exception:
            pass

    curated = [
        {"video_id": "jfKfPfyJRdk", "title": "Lofi Hip Hop Radio - Beats to Chill/Study to"},
        {"video_id": "4xDzrJKXOOY", "title": "Synthwave Radio - Beats to Chill/Game to"},
        {"video_id": "5qap5aO4i9A", "title": "Chill Lofi Beats Mix"},
    ]
    return {"items": curated}


# ---------------------------------------------------------------------------
# Confessions, Stories, Polls, Guild Boss & Arcade
# ---------------------------------------------------------------------------
@app.get("/api/confessions")
def list_confessions():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM confessions ORDER BY id DESC LIMIT 100")
            confs = [dict(r) for r in cur.fetchall()]
            cur.execute("SELECT * FROM confession_replies ORDER BY id ASC")
            replies = [dict(r) for r in cur.fetchall()]
            reply_map: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
            for rep in replies:
                reply_map[rep["confession_id"]].append(rep)
            for c in confs:
                c["replies"] = reply_map.get(c["id"], [])
            return {"confessions": confs}
    finally:
        release_db_connection(conn)


@app.post("/api/confessions")
async def create_confession(req: ConfessionCreateReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    author_label = "Anonymous" if req.is_anonymous else current_user["username"]
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO confessions (author, is_anonymous, content, likes)
                VALUES (%s, %s, %s, 0)
                RETURNING *
                """,
                (author_label, req.is_anonymous, req.content.strip()),
            )
            conf = dict(cur.fetchone())
            conf["replies"] = []
            award_xp_and_coins(conn, current_user["username"], xp_gain=15, coins_gain=10)
            conn.commit()
        await manager.broadcast("global", {"type": "confession_created", "confession": conf})
        return {"confession": conf}
    finally:
        release_db_connection(conn)


@app.post("/api/confessions/{confession_id}/like")
async def like_confession(confession_id: int, current_user: Dict[str, Any] = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "UPDATE confessions SET likes = likes + 1 WHERE id = %s RETURNING id, likes",
                (confession_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Confession not found")
            conn.commit()
        await manager.broadcast("global", {"type": "confession_like", "id": row["id"], "likes": row["likes"]})
        return dict(row)
    finally:
        release_db_connection(conn)


@app.post("/api/confessions/{confession_id}/reply")
async def reply_confession(confession_id: int, req: ConfessionReplyReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    author_label = "Anonymous" if req.is_anonymous else current_user["username"]
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO confession_replies (confession_id, author, content)
                VALUES (%s, %s, %s)
                RETURNING *
                """,
                (confession_id, author_label, req.content.strip()),
            )
            rep = dict(cur.fetchone())
            conn.commit()
        await manager.broadcast("global", {"type": "confession_reply", "reply": rep})
        return {"reply": rep}
    finally:
        release_db_connection(conn)


@app.get("/api/stories")
def list_stories():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("DELETE FROM stories WHERE expires_at < CURRENT_TIMESTAMP")
            cur.execute("SELECT * FROM stories ORDER BY id DESC LIMIT 40")
            return {"stories": [dict(r) for r in cur.fetchall()]}
    finally:
        release_db_connection(conn)


@app.post("/api/stories")
async def create_story(req: StoryCreateReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    exp = now_utc() + timedelta(hours=24)
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO stories (author, content, mood, expires_at)
                VALUES (%s, %s, %s, %s)
                RETURNING *
                """,
                (current_user["username"], req.content.strip(), req.mood, exp),
            )
            st = dict(cur.fetchone())
            conn.commit()
        await manager.broadcast("global", {"type": "story_created", "story": st})
        return {"story": st}
    finally:
        release_db_connection(conn)


@app.get("/api/polls")
def list_polls():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM polls ORDER BY id DESC LIMIT 20")
            return {"polls": [dict(r) for r in cur.fetchall()]}
    finally:
        release_db_connection(conn)


@app.post("/api/polls")
async def create_poll(req: PollCreateReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    clean_opts = [o.strip() for o in req.options if o.strip()][:6]
    if len(clean_opts) < 2:
        raise HTTPException(status_code=400, detail="Need at least 2 options")
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO polls (question, options, votes, created_by) VALUES (%s, %s::jsonb, '{}'::jsonb, %s) RETURNING *",
                (req.question.strip(), json.dumps(clean_opts), current_user["username"]),
            )
            p = dict(cur.fetchone())
            conn.commit()
        await manager.broadcast("global", {"type": "poll_created", "poll": p})
        return {"poll": p}
    finally:
        release_db_connection(conn)


@app.post("/api/polls/{poll_id}/vote")
async def vote_poll(poll_id: int, req: PollVoteReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM polls WHERE id = %s", (poll_id,))
            p = cur.fetchone()
            if not p:
                raise HTTPException(status_code=404, detail="Poll not found")
            opts = p["options"] or []
            if req.option_index < 0 or req.option_index >= len(opts):
                raise HTTPException(status_code=400, detail="Invalid option index")
            votes = dict(p["votes"] or {})
            votes[uname] = req.option_index
            cur.execute(
                "UPDATE polls SET votes = %s::jsonb WHERE id = %s RETURNING *",
                (json.dumps(votes), poll_id),
            )
            updated = dict(cur.fetchone())
            conn.commit()
        await manager.broadcast("global", {"type": "poll_updated", "poll": updated})
        return {"poll": updated}
    finally:
        release_db_connection(conn)


@app.get("/api/boss")
def get_boss():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM guild_boss WHERE id = 1")
            return {"boss": dict(cur.fetchone())}
    finally:
        release_db_connection(conn)


@app.post("/api/boss/attack")
async def attack_boss(req: BossAttackReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM guild_boss WHERE id = 1")
            b = dict(cur.fetchone())
            hp = b["hp"] - req.damage
            max_hp = b["max_hp"]
            tier = b["tier"]
            defeated = False
            if hp <= 0:
                defeated = True
                tier += 1
                max_hp = int(max_hp * 1.25)
                hp = max_hp
                award_xp_and_coins(conn, uname, xp_gain=75, coins_gain=150)
            else:
                award_xp_and_coins(conn, uname, xp_gain=12, coins_gain=8)

            cur.execute(
                "UPDATE guild_boss SET hp = %s, max_hp = %s, tier = %s, last_attacker = %s WHERE id = 1 RETURNING *",
                (hp, max_hp, tier, uname),
            )
            updated_boss = dict(cur.fetchone())
            conn.commit()

        payload = {
            "type": "boss_update",
            "boss": updated_boss,
            "attacker": uname,
            "damage": req.damage,
            "defeated": defeated,
        }
        await manager.broadcast("global", payload)
        return payload
    finally:
        release_db_connection(conn)


@app.get("/api/arcade/scores")
def get_scores(game: Optional[str] = None):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if game:
                cur.execute(
                    "SELECT game, username, MAX(score) AS best_score FROM arcade_scores WHERE game = %s GROUP BY game, username ORDER BY best_score DESC LIMIT 15",
                    (game,),
                )
            else:
                cur.execute(
                    "SELECT game, username, MAX(score) AS best_score FROM arcade_scores GROUP BY game, username ORDER BY best_score DESC LIMIT 25"
                )
            return {"scores": [dict(r) for r in cur.fetchall()]}
    finally:
        release_db_connection(conn)


@app.post("/api/arcade/scores")
async def submit_score(req: ScoreSubmitReq, current_user: Dict[str, Any] = Depends(get_current_user)):
    uname = current_user["username"]
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO arcade_scores (game, username, score) VALUES (%s, %s, %s)",
                (req.game.strip(), uname, req.score),
            )
            xp_info = award_xp_and_coins(conn, uname, xp_gain=15, coins_gain=10)
            conn.commit()
        await manager.broadcast(
            "global",
            {"type": "arcade_score", "game": req.game, "username": uname, "score": req.score},
        )
        return {"ok": True, "xp_info": xp_info}
    finally:
        release_db_connection(conn)


# ---------------------------------------------------------------------------
# Admin Endpoints
# ---------------------------------------------------------------------------
def verify_admin(x_admin_key: Optional[str] = Header(default=None)):
    if not x_admin_key or x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Invalid admin key")


@app.get("/api/admin/overview", dependencies=[Depends(verify_admin)])
def admin_overview():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, username, email, xp, level, honor, cowoncy, warnings, created_at FROM users ORDER BY id DESC")
            users = [dict(r) for r in cur.fetchall()]
            cur.execute("SELECT * FROM confessions ORDER BY id DESC LIMIT 100")
            confs = [dict(r) for r in cur.fetchall()]
            return {"users": users, "confessions": confs}
    finally:
        release_db_connection(conn)


@app.delete("/api/admin/confessions/{confession_id}", dependencies=[Depends(verify_admin)])
def admin_delete_confession(confession_id: int):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM confessions WHERE id = %s", (confession_id,))
            conn.commit()
            return {"deleted": confession_id}
    finally:
        release_db_connection(conn)


# ---------------------------------------------------------------------------
# WebSocket Endpoint (/ws/{room}) — Chat, Sync & WebRTC Voice Signaling
# ---------------------------------------------------------------------------
@app.websocket("/ws/{room}")
async def websocket_room(websocket: WebSocket, room: str, token: Optional[str] = Query(default=None)):
    user_info: Dict[str, Any] = {"username": f"Guest_{secrets.token_hex(2)}"}
    if token:
        claims = decode_token(token)
        if claims and claims.get("username"):
            user_info["username"] = claims["username"]

    uname = user_info["username"]
    await manager.connect(room, websocket, user_info)
    await manager.broadcast(
        room,
        {
            "type": "presence_join",
            "room": room,
            "user": uname,
            "timestamp": now_utc().isoformat(),
        },
    )
    await manager.broadcast_presence()

    try:
        while True:
            raw = await websocket.receive_text()
            manager.online_users[uname] = now_ts()
            try:
                data = json.loads(raw)
            except Exception:
                data = {"type": "chat", "content": raw}

            msg_type = data.get("type", "chat")

            if msg_type == "ping":
                await manager.send_personal(websocket, {"type": "pong", "ts": now_ts()})

            elif msg_type == "typing":
                await manager.broadcast(
                    room,
                    {
                        "type": "typing",
                        "room": room,
                        "user": uname,
                        "is_typing": bool(data.get("is_typing", True)),
                    },
                )

            elif msg_type == "voice_state":
                in_voice = bool(data.get("in_voice", False))
                muted = bool(data.get("muted", False))
                speaking = bool(data.get("speaking", False))
                if in_voice:
                    manager.voice_participants[room][uname] = {"muted": muted, "speaking": speaking}
                else:
                    manager.voice_participants[room].pop(uname, None)

                await manager.broadcast(
                    room,
                    {
                        "type": "voice_state",
                        "room": room,
                        "user": uname,
                        "in_voice": in_voice,
                        "muted": muted,
                        "speaking": speaking,
                    },
                )

            elif msg_type == "webrtc_signal":
                await manager.broadcast(
                    room,
                    {
                        "type": "webrtc_signal",
                        "room": room,
                        "from": uname,
                        "to": data.get("to"),
                        "signal": data.get("signal"),
                    },
                )

            elif msg_type == "chat":
                content = (data.get("content") or "").strip()
                if not content:
                    continue
                conn = get_db_connection()
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            """
                            INSERT INTO chat_messages (room, sender, is_bot, content)
                            VALUES (%s, %s, FALSE, %s)
                            RETURNING *
                            """,
                            (room, uname, content),
                        )
                        saved = dict(cur.fetchone())
                        bot_msgs = await process_bot_command(conn, room, uname, content)
                        conn.commit()

                    await manager.broadcast(room, {"type": "chat_message", "message": saved})
                    for bm in (bot_msgs or []):
                        await manager.broadcast(room, {"type": "bot_message", "message": bm})
                finally:
                    release_db_connection(conn)

            elif msg_type == "media_sync":
                await manager.broadcast(
                    room,
                    {
                        "type": "media_sync",
                        "channel_key": data.get("channel_key", room),
                        "lounge_id": data.get("lounge_id"),
                        "video_id": data.get("video_id"),
                        "title": data.get("title"),
                        "is_playing": data.get("is_playing"),
                        "base_position": data.get("base_position"),
                        "updated_at": data.get("updated_at") or now_ts(),
                        "updated_by": uname,
                    },
                )

    except WebSocketDisconnect:
        await manager.disconnect(room, websocket)
        await manager.broadcast_presence()
    except Exception:
        await manager.disconnect(room, websocket)
        await manager.broadcast_presence()
