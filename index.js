require('dotenv').config();
const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const nodemailer = require('nodemailer');
const { WebSocketServer } = require('ws');
const db = require('./db');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'c9nnect_super_secret_change_in_prod';
const ADMIN_SECRET = process.env.ADMIN_SECRET || 'admin123';

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname)));

// ================= WEBSOCKET ROOMS (/ws/:room) =================
const rooms = new Map();

function broadcastRoom(room, payload) {
  const clients = rooms.get(room);
  if (!clients) return;
  const raw = JSON.stringify(payload);
  for (const ws of clients) {
    if (ws.readyState === 1) ws.send(raw);
  }
}

wss.on('connection', (ws, req) => {
  const urlPath = (req.url || '/ws/Global').split('?')[0];
  const room = decodeURIComponent(urlPath.replace(/^\/ws\//, '') || 'Global');
  if (!rooms.has(room)) rooms.set(room, new Set());
  rooms.get(room).add(ws);

  ws.on('message', (data) => {
    try {
      const parsed = JSON.parse(data.toString());
      broadcastRoom(room, parsed);
    } catch (_) {}
  });

  ws.on('close', () => {
    rooms.get(room)?.delete(ws);
  });
});

// ================= EMAIL HELPER =================
async function sendEmail(to, subject, text) {
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  if (!user || !pass) return false;
  try {
    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST || 'smtp.gmail.com',
      port: Number(process.env.SMTP_PORT || 587),
      secure: false,
      auth: { user, pass }
    });
    await transporter.sendMail({ from: user, to, subject, text });
    return true;
  } catch (err) {
    console.warn('SMTP send failed:', err.message);
    return false;
  }
}

// ================= AUTH MIDDLEWARE =================
function authMiddleware(req, res, next) {
  const header = req.headers.authorization || '';
  if (!header.startsWith('Bearer ')) return res.status(401).json({ detail: 'Missing Bearer token' });
  try {
    req.user = jwt.verify(header.slice(7), JWT_SECRET);
    next();
  } catch (e) {
    res.status(401).json({ detail: 'Invalid or expired token' });
  }
}

function adminMiddleware(req, res, next) {
  if (req.headers['x-admin-key'] !== ADMIN_SECRET) return res.status(403).json({ detail: 'Invalid Admin Key' });
  next();
}

function createToken(uid, username) {
  return jwt.sign({ uid, username }, JWT_SECRET, { expiresIn: '30d' });
}

// ================= HEALTH & STATIC =================
app.get('/health', (req, res) => res.json({ status: 'ok', backend: 'node-express' }));

// ================= AUTH ROUTES =================
app.post('/api/auth/register', async (req, res) => {
  const { username, email, password } = req.body;
  if (!username || !email || !password) return res.status(400).json({ detail: 'Username, email, and password required' });

  const uid = 'u_' + crypto.randomBytes(6).toString('hex');
  const code = String(Math.floor(100000 + Math.random() * 900000));
  const hash = bcrypt.hashSync(password, 10);

  try {
    db.prepare(
      'INSERT INTO users (uid, username, email, password_hash, verification_code) VALUES (?, ?, ?, ?, ?)'
    ).run(uid, username.trim(), email.toLowerCase().trim(), hash, code);
  } catch (e) {
    return res.status(400).json({ detail: 'Username or email already exists' });
  }

  await sendEmail(email, 'Verify your C9nnect Account', `Welcome ${username}!\n\nYour 6-digit verification code is: ${code}`);
  res.json({ token: createToken(uid, username.trim()), uid, username: username.trim(), email_verified: false });
});

app.post('/api/auth/login', (req, res) => {
  const { email, password } = req.body;
  const user = db.prepare('SELECT * FROM users WHERE email = ?').get((email || '').toLowerCase().trim());
  if (!user || !bcrypt.compareSync(password || '', user.password_hash)) {
    return res.status(401).json({ detail: 'Invalid email or password' });
  }
  res.json({
    token: createToken(user.uid, user.username),
    uid: user.uid,
    username: user.username,
    email_verified: Boolean(user.email_verified),
    honor: user.honor,
    daily_honor: user.daily_honor,
    xp: user.xp,
    level: user.level,
    bio: user.bio,
    affinity: user.affinity,
    theme: user.theme,
    equipped_sticker: user.equipped_sticker,
    equipped_deco: user.equipped_deco
  });
});

app.post('/api/auth/verify', (req, res) => {
  const { email, code } = req.body;
  const user = db.prepare('SELECT id, verification_code FROM users WHERE email = ?').get((email || '').toLowerCase().trim());
  if (!user || user.verification_code !== String(code || '').trim()) {
    return res.status(400).json({ detail: 'Invalid verification code' });
  }
  db.prepare('UPDATE users SET email_verified = 1 WHERE id = ?').run(user.id);
  res.json({ ok: true, email_verified: true });
});

app.post('/api/auth/reset-password', async (req, res) => {
  const email = (req.body.email || '').toLowerCase().trim();
  const token = crypto.randomBytes(4).toString('hex').toUpperCase();
  db.prepare('UPDATE users SET reset_token = ? WHERE email = ?').run(token, email);
  await sendEmail(email, 'C9nnect Password Reset', `Your password reset token is: ${token}`);
  res.json({ ok: true });
});

app.post('/api/auth/reset-password/confirm', (req, res) => {
  const { email, token, new_password } = req.body;
  const user = db.prepare('SELECT id, reset_token FROM users WHERE email = ?').get((email || '').toLowerCase().trim());
  if (!user || !user.reset_token || user.reset_token !== String(token || '').trim().toUpperCase()) {
    return res.status(400).json({ detail: 'Invalid reset token' });
  }
  const newHash = bcrypt.hashSync(new_password, 10);
  db.prepare('UPDATE users SET password_hash = ?, reset_token = NULL WHERE id = ?').run(newHash, user.id);
  res.json({ ok: true });
});

// ================= USERS, PROFILE, XP & FRIENDS =================
app.get('/api/users', (req, res) => {
  const rows = db.prepare(
    'SELECT uid, username, honor, level, xp, equipped_sticker, equipped_deco FROM users ORDER BY honor DESC LIMIT 50'
  ).all();
  res.json(rows);
});

app.patch('/api/users/me', authMiddleware, (req, res) => {
  const { username, affinity, bio, theme, equipped_sticker, equipped_deco } = req.body;
  const cur = db.prepare('SELECT * FROM users WHERE uid = ?').get(req.user.uid);
  if (!cur) return res.status(404).json({ detail: 'User not found' });
  db.prepare(
    'UPDATE users SET username = ?, affinity = ?, bio = ?, theme = ?, equipped_sticker = ?, equipped_deco = ? WHERE uid = ?'
  ).run(
    username || cur.username,
    affinity || cur.affinity,
    bio !== undefined ? bio : cur.bio,
    theme || cur.theme,
    equipped_sticker || cur.equipped_sticker,
    equipped_deco || cur.equipped_deco,
    req.user.uid
  );
  res.json({ ok: true });
});

app.post('/api/users/xp', authMiddleware, (req, res) => {
  const gain = Math.min(50, Math.max(1, Number(req.body.amount) || 5));
  const row = db.prepare('SELECT xp FROM users WHERE uid = ?').get(req.user.uid);
  if (!row) return res.status(404).json({ detail: 'User not found' });
  const newXp = row.xp + gain;
  const newLevel = Math.floor(newXp / 100) + 1;
  db.prepare('UPDATE users SET xp = ?, level = ? WHERE uid = ?').run(newXp, newLevel, req.user.uid);
  res.json({ xp: newXp, level: newLevel });
});

app.post('/api/users/honor', authMiddleware, (req, res) => {
  const row = db.prepare('SELECT honor, daily_honor, xp FROM users WHERE uid = ?').get(req.user.uid);
  if (!row) return res.status(404).json({ detail: 'User not found' });
  const remaining = Math.max(0, 500 - row.daily_honor);
  const gained = Math.min(Math.max(0, Number(req.body.amount) || 10), remaining);
  const newHonor = row.honor + gained;
  const newDaily = row.daily_honor + gained;
  const newXp = row.xp + gained;
  const newLevel = Math.floor(newXp / 100) + 1;
  db.prepare('UPDATE users SET honor = ?, daily_honor = ?, xp = ?, level = ? WHERE uid = ?')
    .run(newHonor, newDaily, newXp, newLevel, req.user.uid);
  res.json({ honor: newHonor, daily_honor: newDaily, xp: newXp, level: newLevel });
});

app.get('/api/friends', authMiddleware, (req, res) => {
  const uid = req.user.uid;
  const rows = db.prepare(`
    SELECT u.uid, u.username, u.honor, u.level, u.equipped_sticker, f.status, f.requester_uid
    FROM friends f
    JOIN users u ON (u.uid = CASE WHEN f.requester_uid = ? THEN f.addressee_uid ELSE f.requester_uid END)
    WHERE f.requester_uid = ? OR f.addressee_uid = ?
  `).all(uid, uid, uid);
  res.json(rows);
});

app.post('/api/friends/request', authMiddleware, (req, res) => {
  const target = db.prepare('SELECT uid, username FROM users WHERE LOWER(username) = LOWER(?)').get((req.body.username || '').trim());
  if (!target) return res.status(404).json({ detail: 'Player not found' });
  if (target.uid === req.user.uid) return res.status(400).json({ detail: 'Cannot add yourself' });
  db.prepare('INSERT OR IGNORE INTO friends (requester_uid, addressee_uid, status) VALUES (?, ?, ?)').run(req.user.uid, target.uid, 'pending');
  res.json({ ok: true, friend: target.username });
});

app.post('/api/friends/accept', authMiddleware, (req, res) => {
  db.prepare('UPDATE friends SET status = ? WHERE requester_uid = ? AND addressee_uid = ?').run('accepted', req.body.requester_uid, req.user.uid);
  res.json({ ok: true });
});

// ================= CONFESSIONS =================
app.get('/api/confessions', (req, res) => {
  const rows = db.prepare('SELECT * FROM confessions ORDER BY id DESC LIMIT 40').all();
  const repStmt = db.prepare('SELECT author_name, text, created_at FROM replies WHERE confession_id = ? ORDER BY id ASC');
  res.json(rows.map(r => ({ ...r, replies: repStmt.all(r.id) })));
});

app.post('/api/confessions', (req, res) => {
  const { author_name = 'Anonymous', sticker = '📜', mood = 'Reflective', text } = req.body;
  if (!text || !text.trim()) return res.status(400).json({ detail: 'Confession text required' });
  const info = db.prepare('INSERT INTO confessions (author_name, sticker, mood, text) VALUES (?, ?, ?, ?)')
    .run(author_name.slice(0, 64), sticker.slice(0, 8), mood.slice(0, 32), text.trim());
  const created = { ...db.prepare('SELECT * FROM confessions WHERE id = ?').get(info.lastInsertRowid), replies: [] };
  broadcastRoom('global', { type: 'confession:new', data: created });
  res.json(created);
});

app.post('/api/confessions/:id/like', (req, res) => {
  db.prepare('UPDATE confessions SET likes = likes + 1 WHERE id = ?').run(req.params.id);
  res.json({ ok: true });
});

app.post('/api/confessions/:id/replies', (req, res) => {
  const { author_name = 'Anonymous', text } = req.body;
  if (!text || !text.trim()) return res.status(400).json({ detail: 'Reply text required' });
  db.prepare('INSERT INTO replies (confession_id, author_name, text) VALUES (?, ?, ?)').run(req.params.id, author_name.slice(0, 64), text.trim());
  res.json({ ok: true });
});

// ================= MESSAGES =================
app.get('/api/messages/:target', (req, res) => {
  const rows = db.prepare('SELECT * FROM messages WHERE target = ? ORDER BY id ASC LIMIT 80').all(req.params.target);
  res.json(rows);
});

app.post('/api/messages', authMiddleware, (req, res) => {
  const { target, text } = req.body;
  if (!target || !text || !text.trim()) return res.status(400).json({ detail: 'Target and text required' });
  db.prepare('INSERT INTO messages (sender, target, text) VALUES (?, ?, ?)').run(req.user.username, target, text.trim());
  const payload = { type: 'message:new', sender: req.user.username, target, text: text.trim() };
  broadcastRoom(target, payload);
  res.json(payload);
});

// ================= GUILD & CMS =================
app.get('/api/guild', (req, res) => res.json(db.prepare('SELECT * FROM guild_state WHERE id = 1').get()));

app.post('/api/guild/attack', (req, res) => {
  const dmg = Math.min(350, Math.max(20, Number(req.body.amount) || 45));
  const g = db.prepare('SELECT * FROM guild_state WHERE id = 1').get();
  let newHp = g.boss_hp - dmg;
  let newTier = g.boss_tier;
  let newMax = g.boss_max_hp;
  if (newHp <= 0) {
    newTier += 1;
    newMax = 500 + (newTier - 1) * 300;
    newHp = newMax;
  }
  db.prepare('UPDATE guild_state SET boss_hp = ?, boss_tier = ?, boss_max_hp = ? WHERE id = 1').run(newHp, newTier, newMax);
  const updated = db.prepare('SELECT * FROM guild_state WHERE id = 1').get();
  broadcastRoom('global', { type: 'guild:update', data: updated });
  res.json(updated);
});

app.get('/api/cms', (req, res) => {
  const rows = db.prepare('SELECT * FROM cms_settings').all();
  const map = Object.fromEntries(rows.map(r => [r.key, r.value]));
  res.json({
    announcement: map.announcement || '',
    pollQuestion: map.poll_question || '',
    pollOptions: JSON.parse(map.poll_options || '[]')
  });
});

app.post('/api/cms/poll/vote', (req, res) => {
  const row = db.prepare('SELECT value FROM cms_settings WHERE key = ?').get('poll_options');
  const opts = JSON.parse(row?.value || '[]');
  if (opts[req.body.index]) opts[req.body.index].votes += 1;
  db.prepare('UPDATE cms_settings SET value = ? WHERE key = ?').run(JSON.stringify(opts), 'poll_options');
  res.json(opts);
});

app.delete('/api/admin/confessions/:id', adminMiddleware, (req, res) => {
  db.prepare('DELETE FROM confessions WHERE id = ?').run(req.params.id);
  res.json({ ok: true });
});

server.listen(PORT, () => {
  console.log(`C9nnect Node.js Server running on port ${PORT}`);
});
