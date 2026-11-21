import os
import json
import uuid
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import jwt
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from passlib.context import CryptContext
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, EmailStr

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET", "c9nnect_dev_secret_change_me")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "admin123")
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("Postgres://"):
    DATABASE_URL = "postgres://" + DATABASE_URL[len("Postgres://"):]

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

app = FastAPI(title="C9nnect Python + PostgreSQL Backend")
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

# ================= WEBSOCKET CONNECTION MANAGER =================
class ConnectionManager:
    def __init__(self):
        self.rooms: Dict[str, List[WebSocket]] = {}

    async def connect(self, room: str, ws: WebSocket):
        await ws.accept()
        self.rooms.setdefault(room, []).append(ws)

    def disconnect(self, room: str, ws: WebSocket):
        if room in self.rooms and ws in self.rooms[room]:
            self.rooms[room].remove(ws)

    async def broadcast(self, room: str, payload: dict):
        dead = []
        for ws in self.rooms.get(room, []):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(room, ws)

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

# ================= STATIC PAGES =================
@app.get("/")
def serve_home():
    return FileResponse("index.html")

@app.get("/admin")
def serve_admin():
    return FileResponse("admin.html")

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
        f"Welcome to C9nnect, {req.username}!\n\nYour verification code is: {code}",
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
    token = uuid.uuid4().hex[:10]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET reset_token = %s WHERE email = %s", (token, req.email.lower()))
    finally:
        release_conn(conn)
    send_email_notification(req.email, "C9nnect Password Reset", f"Your password reset token is: {token}")
    return {"ok": True}

# ================= USERS & LEADERBOARD =================
@app.get("/api/users")
def list_users():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT uid, username, honor, level, xp, equipped_sticker, equipped_deco FROM users ORDER BY honor DESC LIMIT 50")
            return cur.fetchall()
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

# ================= CONFESSIONS =================
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
    await ws_manager.broadcast("global", {"type": "confession:new", "data": created})
    return created

@app.post("/api/confessions/{cid}/like")
def like_confession(cid: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE confessions SET likes = likes + 1 WHERE id = %s", (cid,))
    finally:
        release_conn(conn)
    return {"ok": True}

@app.post("/api/confessions/{cid}/replies")
def reply_confession(cid: int, req: ReplyReq):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO confession_replies (confession_id, author_name, text) VALUES (%s, %s, %s)", (cid, req.author_name[:64], req.text.strip()))
    finally:
        release_conn(conn)
    return {"ok": True}

# ================= MESSAGES & TAVERN WEBSOCKETS =================
@app.get("/api/messages/{target}")
def get_messages(target: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM messages WHERE target = %s ORDER BY id ASC LIMIT 80", (target,))
            return cur.fetchall()
    finally:
        release_conn(conn)

@app.post("/api/messages")
async def post_message(req: MessageReq, user=Depends(get_current_user)):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO messages (sender, target, text) VALUES (%s, %s, %s)", (user["username"], req.target, req.text.strip()))
    finally:
        release_conn(conn)
    payload = {"type": "message:new", "sender": user["username"], "target": req.target, "text": req.text.strip()}
    await ws_manager.broadcast(req.target, payload)
    return payload

@app.websocket("/ws/{room}")
async def websocket_room(ws: WebSocket, room: str):
    await ws_manager.connect(room, ws)
    try:
        while True:
            data = await ws.receive_json()
            await ws_manager.broadcast(room, data)
    except WebSocketDisconnect:
        ws_manager.disconnect(room, ws)

# ================= ADMIN CMS =================
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

@app.delete("/api/admin/confessions/{cid}", dependencies=[Depends(require_admin)])
def admin_delete_confession(cid: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM confessions WHERE id = %s", (cid,))
    finally:
        release_conn(conn)
    return {"ok": True}
