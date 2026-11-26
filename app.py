import os
import re
import json
import uuid
import time
import random
import secrets
import smtplib
import urllib.request
import urllib.parse
from email.message import EmailMessage
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import jwt
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from passlib.context import CryptContext
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, EmailStr

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET", "c9nnect_dev_secret_change_me")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "admin123")
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgres://" + DATABASE_URL[len("postgres://"):]

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
pg_pool = pool.SimpleConnectionPool(1, 10, dsn=DATABASE_URL) if DATABASE_URL else None

def get_conn():
    if not pg_pool:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not configured")
    conn = pg_pool.getconn()
    conn.autocommit = True
    return conn

def release_conn(conn):
    if pg_pool and conn:
        pg_pool.putconn(conn)

def init_schema():
    if not pg_pool:
        return
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    if os.path.exists(schema_path):
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(open(schema_path, "r", encoding="utf-8").read())
        finally:
            release_conn(conn)

app = FastAPI(title="C9nnect Full-Stack Python + PostgreSQL")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def on_startup():
    init_schema()

# ================= WEBSOCKET CONNECTION & PRESENCE MANAGER =================
class ConnectionManager:
    def __init__(self):
        self.rooms: Dict[str, List[WebSocket]] = {}
        self.socket_meta: Dict[WebSocket, dict] = {}
        self.lounge_music_state: Dict[str, dict] = {}

    async def connect(self, room: str, ws: WebSocket, username: str = "Guest", lounge_id: Optional[str] = None):
        await ws.accept()
        self.rooms.setdefault(room, []).append(ws)
        self.socket_meta[ws] = {
            "room": room,
            "username": username or "Guest",
            "lounge_id": lounge_id,
            "connected_at": time.time(),
        }

    def update_meta(self, ws: WebSocket, username: Optional[str] = None, lounge_id: Optional[str] = None):
        meta = self.socket_meta.get(ws)
        if not meta:
            return
        if username:
            meta["username"] = username
        if lounge_id is not None:
            meta["lounge_id"] = lounge_id

    def disconnect(self, room: str, ws: WebSocket):
        if room in self.rooms and ws in self.rooms[room]:
            self.rooms[room].remove(ws)
        return self.socket_meta.pop(ws, None)

    def get_online_users(self) -> List[dict]:
        seen = {}
        for meta in self.socket_meta.values():
            uname = meta.get("username") or "Guest"
            seen[uname] = {
                "username": uname,
                "lounge_id": meta.get("lounge_id"),
                "room": meta.get("room"),
            }
        return list(seen.values())

    async def broadcast(self, room: str, payload: dict):
        dead = []
        for ws in list(self.rooms.get(room, [])):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(room, ws)

    async def broadcast_all(self, payload: dict):
        all_sockets = set()
        for sockets in self.rooms.values():
            for ws in sockets:
                all_sockets.add(ws)
        dead = []
        for ws in all_sockets:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            meta = self.socket_meta.get(ws)
            if meta and meta.get("room") in self.rooms and ws in self.rooms[meta["room"]]:
                self.rooms[meta["room"]].remove(ws)
            self.socket_meta.pop(ws, None)

ws_manager = ConnectionManager()

# ================= EMAIL HELPER =================
def send_email_notification(to_email: str, subject: str, body: str):
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    if not smtp_user or not smtp_pass:
        return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.set_content(body)
    with smtplib.SMTP(os.getenv("SMTP_HOST", "smtp.gmail.com"), int(os.getenv("SMTP_PORT", "587"))) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)
    return True

# ================= AUTH HELPERS =================
def create_token(uid: str, username: str) -> str:
    return jwt.encode(
        {"uid": uid, "username": username, "exp": datetime.utcnow() + timedelta(days=30)},
        JWT_SECRET,
        algorithm="HS256",
    )

def get_current_user(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization.split(" ", 1)[1]
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

def require_admin(x_admin_key: Optional[str] = Header(None)):
    if x_admin_key != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid Admin Key")

# ================= PYTHON RICH CHAT & BOT COMMAND ENGINE =================
MEME_POOL = [
    ("https://i.imgflip.com/1bij.jpg", "One Does Not Simply Deploy on Friday"),
    ("https://i.imgflip.com/30b1gx.jpg", "Drake Approves: Python + PostgreSQL"),
    ("https://i.imgflip.com/4/1ti1.jpg", "Coding at 3 AM With 47 Browser Tabs"),
    ("https://i.imgflip.com/26am.jpg", "When the WebSocket Syncs on the First Try"),
]

def format_rich_message_py(text: str) -> str:
    """Converts [MEME_IMG:url|title] tokens into HTML image cards in Python."""
    return re.sub(
        r"\[MEME_IMG:(https?://[^|\]]+)\|([^\]]+)\]",
        lambda m: (
            f'<div class="mt-2 rounded-2xl overflow-hidden border border-amber-400/40 bg-black/50 shadow-lg max-w-[260px]">'
            f'<img src="{m.group(1)}" alt="{m.group(2)}" loading="lazy" class="w-full max-h-52 object-cover bg-slate-900" />'
            f'<div class="px-3 py-1.5 text-[11px] font-bold text-amber-200 bg-white/5 border-t border-white/10">{m.group(2)}</div>'
            f'</div>'
        ),
        text or "",
    )

async def execute_bot_command_py(raw_cmd: str, sender: str = "Wanderer", room: str = "global") -> dict:
    """Handles /, !, and - bot commands in Python for #Global and Lounge/Tavern chat."""
    cmd = (raw_cmd or "").strip()
    lower = cmd.lower()

    if lower.startswith(("/meme", "!meme", "-meme")):
        url, title = random.choice(MEME_POOL)
        reply_text = f"[MEME_IMG:{url}|{title}]"
    elif lower.startswith(("/roll", "!roll", "-roll")):
        roll = random.randint(1, 20)
        reply_text = f"🎲 {sender} rolled a **{roll}** (d20)!"
    elif lower.startswith(("/rps", "!rps", "-rps")):
        choice = random.choice(["Rock 🪨", "Paper 📄", "Scissors ✂️"])
        reply_text = f"✊ RealmBot picks **{choice}** against {sender}!"
    elif lower.startswith(("/play", "!play", "-play")):
        query = cmd.split(" ", 1)[1].strip() if " " in cmd else "lofi chill beats"
        api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
        if api_key:
            try:
                url = "https://www.googleapis.com/youtube/v3/search?" + urllib.parse.urlencode({
                    "part": "snippet", "q": query, "type": "video", "videoEmbeddable": "true", "maxResults": 1, "key": api_key
                })
                with urllib.request.urlopen(url, timeout=6) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                item = data["items"][0]
                vid = item["id"]["videoId"]
                title = item["snippet"]["title"]
                reply_text = f"🎵 Now Queued: **{title}** (https://youtu.be/{vid})"
            except Exception:
                reply_text = f"🎵 Queued tavern vibe: **{query}**"
        else:
            reply_text = f"🎵 Queued tavern vibe: **{query}**"
    else:
        reply_text = "🤖 Commands available: `/meme`, `/roll`, `/rps`, `/play <song>`"

    payload = {
        "type": "message:new",
        "sender": "RealmBot",
        "target": room,
        "text": reply_text,
        "html": format_rich_message_py(reply_text),
    }
    if room == "global":
        await ws_manager.broadcast_all(payload)
    else:
        await ws_manager.broadcast(room, payload)
    return payload

def _fetch_all_lounges_sync() -> List[dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM tavern_lounges ORDER BY id ASC")
            rows = cur.fetchall()
            for r in rows:
                music = ws_manager.lounge_music_state.get(r["id"])
                if music:
                    r["music_state"] = music
            return rows
    finally:
        release_conn(conn)

def _remove_member_from_all_lounges(cur, username: str):
    cur.execute("SELECT id, members FROM tavern_lounges")
    for other in cur.fetchall():
        om = list(other["members"] or [])
        if username in om:
            om = [m for m in om if m != username]
            cur.execute(
                "UPDATE tavern_lounges SET members = %s::jsonb WHERE id = %s",
                (json.dumps(om), other["id"]),
            )

# ================= SCHEMAS =================
class RegisterReq(BaseModel):
    username: str
    email: EmailStr
    password: str

class LoginReq(BaseModel):
    email: EmailStr
    password: str

class VerifyReq(BaseModel):
    email: EmailStr
    code: str

class ResetReq(BaseModel):
    email: EmailStr

class ResetConfirmReq(BaseModel):
    email: EmailStr
    token: str
    new_password: str

class ProfileUpdateReq(BaseModel):
    username: Optional[str] = None
    affinity: Optional[str] = None
    bio: Optional[str] = None
    theme: Optional[str] = None
    equipped_sticker: Optional[str] = None
    equipped_deco: Optional[str] = None

class XpReq(BaseModel):
    amount: int = 5

class FriendRequestReq(BaseModel):
    username: str

class FriendAcceptReq(BaseModel):
    requester_uid: str

class ConfessionReq(BaseModel):
    author_name: str = "Anonymous"
    sticker: str = "📜"
    mood: str = "Reflective"
    text: str

class ReplyReq(BaseModel):
    author_name: str = "Anonymous"
    text: str

class MessageReq(BaseModel):
    target: str
    text: str

class HonorReq(BaseModel):
    amount: int = 10

class PollVoteReq(BaseModel):
    index: int

class LoungeCreateReq(BaseModel):
    name: str
    passcode: str = ""
    host_name: str = "Host"

class LoungeJoinPassReq(BaseModel):
    lounge_id: str
    passcode: str = ""
    username: str = "Guest"

class LoungeMusicSyncReq(BaseModel):
    lounge_id: str
    title: str
    video_id: str = ""
    station_idx: int = 0
    is_playing: bool = True
    position_sec: float = 0.0
    updated_by: str = "Host"

class BotChatReq(BaseModel):
    bot_name: str
    bot_role: str
    message: str

class StoryDropReq(BaseModel):
    author_name: str
    mood: str = "⚔️"
    text: str

# ================= STATIC PAGES =================
@app.get("/")
def serve_home():
    return FileResponse("index.html")

@app.get("/admin")
def serve_admin():
    return FileResponse("admin.html")

@app.get("/health")
def health():
    return {"status": "ok", "db_configured": bool(pg_pool)}

@app.get("/api/presence")
def get_presence():
    return {
        "online_users": ws_manager.get_online_users(),
        "online_count": len(ws_manager.get_online_users()),
        "lounges": _fetch_all_lounges_sync(),
    }

# ================= AUTH ROUTES =================
@app.post("/api/auth/register")
def register(req: RegisterReq):
    uid = "u_" + uuid.uuid4().hex[:12]
    code = str(uuid.uuid4().int)[:6]
    pw_hash = pwd_ctx.hash(req.password)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (uid, username, email, password_hash, verification_code) VALUES (%s, %s, %s, %s, %s)",
                (uid, req.username.strip(), req.email.lower(), pw_hash, code),
            )
    except psycopg2.IntegrityError:
        raise HTTPException(status_code=400, detail="Username or email already exists")
    finally:
        release_conn(conn)

    send_email_notification(
        req.email,
        "Verify your C9nnect Account",
        f"Welcome to C9nnect, {req.username}!\n\nYour 6-digit verification code is: {code}",
    )
    return {"token": create_token(uid, req.username), "uid": uid, "username": req.username, "email_verified": False}

@app.post("/api/auth/login")
def login(req: LoginReq):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (req.email.lower(),))
            user = cur.fetchone()
    finally:
        release_conn(conn)
    if not user or not pwd_ctx.verify(req.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return {
        "token": create_token(user["uid"], user["username"]),
        "uid": user["uid"],
        "username": user["username"],
        "email_verified": bool(user["email_verified"]),
        "honor": user["honor"],
        "daily_honor": user["daily_honor"],
        "xp": user["xp"],
        "level": user["level"],
        "bio": user["bio"],
        "affinity": user["affinity"],
        "theme": user["theme"],
        "equipped_sticker": user["equipped_sticker"],
        "equipped_deco": user["equipped_deco"],
    }

@app.post("/api/auth/verify")
def verify_email(req: VerifyReq):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, verification_code FROM users WHERE email = %s", (req.email.lower(),))
            user = cur.fetchone()
            if not user or user["verification_code"] != req.code.strip():
                raise HTTPException(status_code=400, detail="Invalid verification code")
            cur.execute("UPDATE users SET email_verified = TRUE WHERE id = %s", (user["id"],))
    finally:
        release_conn(conn)
    return {"ok": True, "email_verified": True}

@app.post("/api/auth/reset-password")
def reset_password(req: ResetReq):
    token = uuid.uuid4().hex[:8].upper()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET reset_token = %s WHERE email = %s", (token, req.email.lower()))
    finally:
        release_conn(conn)
    send_email_notification(req.email, "C9nnect Password Reset", f"Your password reset token is: {token}")
    return {"ok": True}

@app.post("/api/auth/reset-password/confirm")
def confirm_reset_password(req: ResetConfirmReq):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, reset_token FROM users WHERE email = %s", (req.email.lower(),))
            user = cur.fetchone()
            if not user or not user["reset_token"] or user["reset_token"] != req.token.strip().upper():
                raise HTTPException(status_code=400, detail="Invalid reset token")
            new_hash = pwd_ctx.hash(req.new_password)
            cur.execute("UPDATE users SET password_hash = %s, reset_token = NULL WHERE id = %s", (new_hash, user["id"]))
    finally:
        release_conn(conn)
    return {"ok": True}

# ================= USERS, PROFILE, XP & FRIENDS =================
@app.get("/api/users")
def list_users():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT uid, username, honor, level, xp, equipped_sticker, equipped_deco FROM users ORDER BY honor DESC LIMIT 50")
            return cur.fetchall()
    finally:
        release_conn(conn)

@app.patch("/api/users/me")
def update_profile(req: ProfileUpdateReq, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE users SET
                  username = COALESCE(%s, username),
                  affinity = COALESCE(%s, affinity),
                  bio = COALESCE(%s, bio),
                  theme = COALESCE(%s, theme),
                  equipped_sticker = COALESCE(%s, equipped_sticker),
                  equipped_deco = COALESCE(%s, equipped_deco)
                WHERE uid = %s
                """,
                (req.username, req.affinity, req.bio, req.theme, req.equipped_sticker, req.equipped_deco, user["uid"]),
            )
    finally:
        release_conn(conn)
    return {"ok": True}

@app.post("/api/users/xp")
def add_xp(req: XpReq, user=Depends(get_current_user)):
    gain = min(50, max(1, req.amount))
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT xp FROM users WHERE uid = %s", (user["uid"],))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="User not found")
            new_xp = row["xp"] + gain
            new_level = (new_xp // 100) + 1
            cur.execute("UPDATE users SET xp = %s, level = %s WHERE uid = %s", (new_xp, new_level, user["uid"]))
            return {"xp": new_xp, "level": new_level}
    finally:
        release_conn(conn)

@app.post("/api/users/honor")
def add_honor(req: HonorReq, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT honor, daily_honor, xp FROM users WHERE uid = %s", (user["uid"],))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="User not found")
            remaining = max(0, 500 - row["daily_honor"])
            gained = min(max(0, req.amount), remaining)
            new_honor = row["honor"] + gained
            new_daily = row["daily_honor"] + gained
            new_xp = row["xp"] + gained
            new_level = (new_xp // 100) + 1
            cur.execute(
                "UPDATE users SET honor = %s, daily_honor = %s, xp = %s, level = %s WHERE uid = %s",
                (new_honor, new_daily, new_xp, new_level, user["uid"]),
            )
            return {"honor": new_honor, "daily_honor": new_daily, "xp": new_xp, "level": new_level}
    finally:
        release_conn(conn)

@app.get("/api/friends")
def get_friends(user=Depends(get_current_user)):
    uid = user["uid"]
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT u.uid, u.username, u.honor, u.level, u.equipped_sticker, f.status, f.requester_uid
                FROM friends f
                JOIN users u ON (u.uid = CASE WHEN f.requester_uid = %s THEN f.addressee_uid ELSE f.requester_uid END)
                WHERE f.requester_uid = %s OR f.addressee_uid = %s
                """,
                (uid, uid, uid),
            )
            return cur.fetchall()
    finally:
        release_conn(conn)

@app.post("/api/friends/request")
def send_friend_request(req: FriendRequestReq, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT uid, username FROM users WHERE LOWER(username) = LOWER(%s)", (req.username.strip(),))
            target = cur.fetchone()
            if not target:
                raise HTTPException(status_code=404, detail="Player not found")
            if target["uid"] == user["uid"]:
                raise HTTPException(status_code=400, detail="Cannot add yourself")
            cur.execute(
                """
                INSERT INTO friends (requester_uid, addressee_uid, status)
                VALUES (%s, %s, 'pending')
                ON CONFLICT (requester_uid, addressee_uid) DO NOTHING
                """,
                (user["uid"], target["uid"]),
            )
            return {"ok": True, "friend": target["username"]}
    finally:
        release_conn(conn)

@app.post("/api/friends/accept")
def accept_friend_request(req: FriendAcceptReq, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE friends SET status = 'accepted' WHERE requester_uid = %s AND addressee_uid = %s",
                (req.requester_uid, user["uid"]),
            )
            return {"ok": True}
    finally:
        release_conn(conn)

# ================= SCROLLS / CONFESSIONS (GLOBAL REAL-TIME SYNC) =================
@app.get("/api/confessions")
def get_confessions():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM confessions ORDER BY id DESC LIMIT 40")
            rows = cur.fetchall()
            for r in rows:
                cur.execute("SELECT author_name, text, created_at FROM confession_replies WHERE confession_id = %s ORDER BY id ASC", (r["id"],))
                r["replies"] = cur.fetchall()
            return rows
    finally:
        release_conn(conn)

@app.post("/api/confessions")
async def create_confession(req: ConfessionReq):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO confessions (author_name, sticker, mood, text) VALUES (%s, %s, %s, %s) RETURNING *",
                (req.author_name[:64], req.sticker[:8], req.mood[:32], req.text.strip()),
            )
            created = cur.fetchone()
            created["replies"] = []
    finally:
        release_conn(conn)
    await ws_manager.broadcast_all({"type": "confession:new", "data": created})
    return created

@app.post("/api/confessions/{cid}/like")
async def like_confession(cid: int):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("UPDATE confessions SET likes = likes + 1 WHERE id = %s RETURNING id, likes", (cid,))
            row = cur.fetchone()
    finally:
        release_conn(conn)
    if row:
        await ws_manager.broadcast_all({"type": "confession:like", "id": row["id"], "likes": row["likes"]})
    return {"ok": True, "id": cid, "likes": row["likes"] if row else None}

@app.post("/api/confessions/{cid}/replies")
async def reply_confession(cid: int, req: ReplyReq):
    clean_author = req.author_name[:64]
    clean_text = req.text.strip()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO confession_replies (confession_id, author_name, text) VALUES (%s, %s, %s) RETURNING author_name, text, created_at",
                (cid, clean_author, clean_text),
            )
            reply_row = cur.fetchone()
    finally:
        release_conn(conn)
    await ws_manager.broadcast_all({
        "type": "confession:reply",
        "confession_id": cid,
        "reply": reply_row,
    })
    return {"ok": True, "reply": reply_row}

# ================= GLOBAL CHAT, BOTS & REAL-TIME WEBSOCKETS =================
@app.get("/api/messages/{target}")
def get_messages(target: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM messages WHERE target = %s ORDER BY id ASC LIMIT 80", (target,))
            rows = cur.fetchall()
            for r in rows:
                r["html"] = format_rich_message_py(r.get("text") or "")
            return rows
    finally:
        release_conn(conn)

@app.post("/api/messages")
async def post_message(req: MessageReq, user=Depends(get_current_user)):
    clean_text = req.text.strip()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO messages (sender, target, text) VALUES (%s, %s, %s)", (user["username"], req.target, clean_text))
    finally:
        release_conn(conn)
    payload = {
        "type": "message:new",
        "sender": user["username"],
        "target": req.target,
        "text": clean_text,
        "html": format_rich_message_py(clean_text),
    }
    if req.target == "global":
        await ws_manager.broadcast_all(payload)
    else:
        await ws_manager.broadcast(req.target, payload)
    if clean_text.startswith(("/", "!", "-")):
        await execute_bot_command_py(clean_text, sender=user["username"], room=req.target)
    return payload

@app.post("/api/bots/command")
async def run_bot_command_endpoint(payload: dict):
    cmd = str(payload.get("command", ""))
    sender = str(payload.get("sender", "Wanderer"))
    room = str(payload.get("room", "global"))
    return await execute_bot_command_py(cmd, sender=sender, room=room)

@app.get("/api/chat/format")
def format_chat_endpoint(text: str = Query("")):
    return {"text": text, "html": format_rich_message_py(text)}

@app.websocket("/ws/{room}")
async def websocket_room(ws: WebSocket, room: str):
    await ws_manager.connect(room, ws)
    try:
        while True:
            data = await ws.receive_json()
            ev_type = str(data.get("type") or "message:new")
            sender = str(data.get("sender") or "Guest")
            msg_text = str(data.get("text") or "")

            if sender and sender != "Guest":
                ws_manager.update_meta(ws, username=sender, lounge_id=data.get("loungeId") or data.get("lounge_id"))

            if ev_type == "presence:ping":
                await ws_manager.broadcast_all({
                    "type": "presence:sync",
                    "online_users": ws_manager.get_online_users(),
                    "lounges": _fetch_all_lounges_sync(),
                })
                continue

            if msg_text:
                data["html"] = format_rich_message_py(msg_text)
                if room == "global" or str(data.get("target") or "").lower() == "global":
                    await ws_manager.broadcast_all(data)
                else:
                    await ws_manager.broadcast(room, data)
                if msg_text.startswith(("/", "!", "-")):
                    await execute_bot_command_py(msg_text, sender=sender, room=room)
            else:
                if ev_type in ("lounges:refresh", "lounge:music", "presence:sync"):
                    await ws_manager.broadcast_all(data)
                else:
                    await ws_manager.broadcast(room, data)
    except WebSocketDisconnect:
        meta = ws_manager.disconnect(room, ws)
        if meta and meta.get("username") and meta["username"] != "Guest":
            await ws_manager.broadcast_all({
                "type": "presence:sync",
                "online_users": ws_manager.get_online_users(),
            })

# ================= GUILD BOSS & ADMIN CMS =================
@app.get("/api/guild")
def get_guild():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM guild_state WHERE id = 1")
            return cur.fetchone()
    finally:
        release_conn(conn)

@app.post("/api/guild/attack")
async def attack_guild_boss(req: HonorReq):
    dmg = min(350, max(20, req.amount))
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM guild_state WHERE id = 1")
            g = cur.fetchone()
            new_hp = g["boss_hp"] - dmg
            new_tier = g["boss_tier"]
            new_max = g["boss_max_hp"]
            if new_hp <= 0:
                new_tier += 1
                new_max = 500 + (new_tier - 1) * 300
                new_hp = new_max
            cur.execute(
                "UPDATE guild_state SET boss_hp = %s, boss_tier = %s, boss_max_hp = %s WHERE id = 1 RETURNING *",
                (new_hp, new_tier, new_max),
            )
            updated = cur.fetchone()
    finally:
        release_conn(conn)
    await ws_manager.broadcast_all({"type": "guild:update", "data": updated})
    return updated

@app.get("/api/cms")
def get_cms():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM cms_settings")
            rows = {r["setting_key"]: r["setting_value"] for r in cur.fetchall()}
    finally:
        release_conn(conn)
    return {
        "announcement": rows.get("announcement", ""),
        "pollQuestion": rows.get("poll_question", ""),
        "pollOptions": json.loads(rows.get("poll_options", "[]")),
    }

@app.post("/api/cms/poll/vote")
def vote_poll(req: PollVoteReq):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT setting_value FROM cms_settings WHERE setting_key = 'poll_options'")
            row = cur.fetchone()
            opts = json.loads(row["setting_value"]) if row else []
            if 0 <= req.index < len(opts):
                opts[req.index]["votes"] += 1
                cur.execute("UPDATE cms_settings SET setting_value = %s WHERE setting_key = 'poll_options'", (json.dumps(opts),))
            return opts
    finally:
        release_conn(conn)

@app.delete("/api/admin/confessions/{cid}", dependencies=[Depends(require_admin)])
async def admin_delete_confession(cid: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM confessions WHERE id = %s", (cid,))
    finally:
        release_conn(conn)
    await ws_manager.broadcast_all({"type": "confession:delete", "id": cid})
    return {"ok": True}

# ================= LOUNGES, DISCORD-STYLE ROOM MEMBERS & MUSIC SYNC =================
@app.get("/api/lounges")
def list_lounges():
    return _fetch_all_lounges_sync()

@app.get("/api/lounges/{lounge_id}/state")
def get_lounge_state(lounge_id: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM tavern_lounges WHERE id = %s", (lounge_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Lounge not found")
            row["music_state"] = ws_manager.lounge_music_state.get(lounge_id, {
                "loungeId": lounge_id,
                "title": row.get("current_track") or "Ancient Tavern Hearth & Rain Synth",
                "videoId": row.get("video_id") or "",
                "stationIdx": 0,
                "isPlaying": False,
                "positionSec": 0.0,
                "updatedAt": time.time(),
            })
            row["online_users"] = ws_manager.get_online_users()
            return row
    finally:
        release_conn(conn)

@app.post("/api/lounges/create")
async def create_custom_lounge(req: LoungeCreateReq):
    lid = "lng_" + secrets.token_hex(4)
    host = req.host_name.strip()[:32] or "Host"
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            _remove_member_from_all_lounges(cur, host)
            cur.execute(
                """
                INSERT INTO tavern_lounges (id, name, members, max_capacity, passcode, host_name)
                VALUES (%s, %s, %s::jsonb, %s, %s, %s) RETURNING *
                """,
                (lid, req.name.strip()[:48] or "Private Lounge", json.dumps([host]), 5, req.passcode.strip()[:24], host),
            )
            created = cur.fetchone()
    finally:
        release_conn(conn)
    lounges = _fetch_all_lounges_sync()
    await ws_manager.broadcast_all({
        "type": "lounges:refresh",
        "lounges": lounges,
        "active_lounge": created,
        "online_users": ws_manager.get_online_users(),
    })
    return created

@app.post("/api/lounges/join_pass")
async def join_lounge_with_pass(req: LoungeJoinPassReq):
    uname = req.username.strip()[:32] or "Guest"
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM tavern_lounges WHERE id = %s", (req.lounge_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Lounge not found")
            expected = (row.get("passcode") or "").strip()
            if expected and expected != req.passcode.strip():
                raise HTTPException(status_code=403, detail="Invalid lounge passcode")
            members = list(row["members"] or [])
            if uname not in members:
                if len(members) >= row["max_capacity"]:
                    raise HTTPException(status_code=400, detail="Lounge is full (5/5)")
                _remove_member_from_all_lounges(cur, uname)
                members.append(uname)
                cur.execute(
                    "UPDATE tavern_lounges SET members = %s::jsonb WHERE id = %s RETURNING *",
                    (json.dumps(members), req.lounge_id),
                )
                row = cur.fetchone()
    finally:
        release_conn(conn)
    for ws, meta in ws_manager.socket_meta.items():
        if meta.get("username") == uname:
            meta["lounge_id"] = req.lounge_id
    music = ws_manager.lounge_music_state.get(req.lounge_id)
    if music:
        row["music_state"] = music
    lounges = _fetch_all_lounges_sync()
    await ws_manager.broadcast_all({
        "type": "lounges:refresh",
        "lounges": lounges,
        "active_lounge": row,
        "joined_user": uname,
        "lounge_id": req.lounge_id,
        "online_users": ws_manager.get_online_users(),
    })
    return {"ok": True, "lounge": row, "music_state": music}

@app.post("/api/lounges/{lounge_id}/leave")
async def leave_lounge(lounge_id: str, req: LoungeJoinPassReq):
    uname = req.username.strip()[:32] or "Guest"
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT members FROM tavern_lounges WHERE id = %s", (lounge_id,))
            row = cur.fetchone()
            if row:
                members = [m for m in (row["members"] or []) if m != uname]
                cur.execute("UPDATE tavern_lounges SET members = %s::jsonb WHERE id = %s", (json.dumps(members), lounge_id))
    finally:
        release_conn(conn)
    for meta in ws_manager.socket_meta.values():
        if meta.get("username") == uname and meta.get("lounge_id") == lounge_id:
            meta["lounge_id"] = None
    lounges = _fetch_all_lounges_sync()
    await ws_manager.broadcast_all({
        "type": "lounges:refresh",
        "lounges": lounges,
        "left_user": uname,
        "lounge_id": lounge_id,
        "online_users": ws_manager.get_online_users(),
    })
    return {"ok": True}

@app.post("/api/lounges/sync_music")
async def sync_lounge_music(req: LoungeMusicSyncReq):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tavern_lounges SET current_track = %s, video_id = %s WHERE id = %s",
                (req.title[:120], req.video_id[:32], req.lounge_id),
            )
    finally:
        release_conn(conn)
    payload = {
        "type": "lounge:music",
        "loungeId": req.lounge_id,
        "title": req.title,
        "videoId": req.video_id,
        "stationIdx": req.station_idx,
        "isPlaying": req.is_playing,
        "positionSec": req.position_sec,
        "updatedBy": req.updated_by,
        "updatedAt": time.time(),
    }
    ws_manager.lounge_music_state[req.lounge_id] = payload
    await ws_manager.broadcast_all(payload)
    return {"ok": True, "music_state": payload}

@app.post("/api/lounges/{lounge_id}/join")
async def join_lounge(lounge_id: str, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM tavern_lounges WHERE id = %s", (lounge_id,))
            room = cur.fetchone()
            if not room:
                raise HTTPException(status_code=404, detail="Lounge not found")
            members = list(room["members"] or [])
            uname = user["username"]
            if uname not in members:
                if len(members) >= room["max_capacity"]:
                    raise HTTPException(status_code=400, detail="Lounge is full (5 / 5 max)")
                _remove_member_from_all_lounges(cur, uname)
                members.append(uname)
                cur.execute(
                    "UPDATE tavern_lounges SET members = %s::jsonb WHERE id = %s RETURNING *",
                    (json.dumps(members), lounge_id),
                )
                room = cur.fetchone()
    finally:
        release_conn(conn)
    for meta in ws_manager.socket_meta.values():
        if meta.get("username") == uname:
            meta["lounge_id"] = lounge_id
    lounges = _fetch_all_lounges_sync()
    await ws_manager.broadcast_all({
        "type": "lounges:refresh",
        "lounges": lounges,
        "active_lounge": room,
        "lounge_id": lounge_id,
        "online_users": ws_manager.get_online_users(),
    })
    return {"ok": True, "lounge_id": lounge_id, "members": members}

@app.get("/api/music/youtube")
def search_youtube_music(q: str):
    api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=404, detail="YOUTUBE_API_KEY not configured")
    url = "https://www.googleapis.com/youtube/v3/search?" + urllib.parse.urlencode({
        "part": "snippet", "q": q, "type": "video", "videoEmbeddable": "true", "maxResults": 1, "key": api_key
    })
    with urllib.request.urlopen(url, timeout=6) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    items = data.get("items", [])
    if not items:
        raise HTTPException(status_code=404, detail="No YouTube track found")
    item = items[0]
    return {
        "videoId": item["id"]["videoId"],
        "title": item["snippet"]["title"],
        "channel": item["snippet"]["channelTitle"]
    }

@app.post("/api/bots/chat")
def bot_ai_chat(req: BotChatReq):
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=404, detail="GEMINI_API_KEY not configured")
    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=" + urllib.parse.quote(api_key)
    prompt = f"You are {req.bot_name}, a friendly {req.bot_role} inside the C9nnect social & gaming platform. Reply in 1-2 short, fun sentences to: {req.message}"
    body = json.dumps({"contents": [{"parts": [{"text": prompt}]}]}).encode("utf-8")
    r = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(r, timeout=8) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    reply = data["candidates"][0]["content"]["parts"][0]["text"].strip()
    return {"reply": reply}

@app.get("/api/stories")
def get_stories():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM status_drops WHERE created_at > NOW() - INTERVAL '24 hours' ORDER BY id DESC LIMIT 20")
            return cur.fetchall()
    finally:
        release_conn(conn)

@app.post("/api/stories")
async def create_story(req: StoryDropReq):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO status_drops (author_name, mood, text) VALUES (%s, %s, %s) RETURNING *",
                (req.author_name[:64], req.mood[:16], req.text[:180]),
            )
            created = cur.fetchone()
    finally:
        release_conn(conn)
    await ws_manager.broadcast_all({"type": "story:new", "data": created})
    return created

@app.get("/api/stats/summary")
def platform_summary():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS total_users, COALESCE(SUM(honor), 0) AS total_honor FROM users")
            u = cur.fetchone()
            cur.execute("SELECT COUNT(*) AS total_confessions FROM confessions")
            c = cur.fetchone()
            return {**u, **c}
    finally:
        release_conn(conn)
