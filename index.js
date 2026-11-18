require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const db = require('./db');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'auraconnect_super_secret_key_change_in_prod';
const ADMIN_SECRET = process.env.ADMIN_SECRET || 'admin123';

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

const STICKER_UNLOCKS = [
  { level: 1, icon: '✨' }, { level: 1, icon: '🔥' },
  { level: 2, icon: '🌙' }, { level: 3, icon: '⚔️' },
  { level: 4, icon: '🔮' }, { level: 5, icon: '👑' },
  { level: 6, icon: '🐉' }, { level: 8, icon: '💎' }
];

const FRAME_UNLOCKS = [
  { level: 1, id: 'none', name: 'Plain' },
  { level: 2, id: 'emerald', name: 'Emerald Aura' },
  { level: 3, id: 'gold', name: 'Runic Gold' },
  { level: 5, id: 'mythic', name: 'Mythic Sovereign' }
];

function computeLevel(xp) {
  return Math.min(10, Math.floor(xp / 100) + 1);
}

function authMiddleware(req, res, next) {
  const header = req.headers.authorization;
  if (!header) return res.status(401).json({ error: 'Missing token' });
  try {
    const token = header.replace('Bearer ', '');
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch (e) {
    res.status(401).json({ error: 'Invalid token' });
  }
}

function adminMiddleware(req, res, next) {
  const key = req.headers['x-admin-key'];
  if (key !== ADMIN_SECRET) return res.status(403).json({ error: 'Invalid Admin CMS Key' });
  next();
}

function serializeUser(u) {
  return {
    id: u.id,
    username: u.username,
    email: u.email,
    role: u.role,
    country: u.country,
    theme: u.theme,
    xp: u.xp,
    level: u.level,
    activeBadge: u.active_badge,
    activeFrame: u.active_frame,
    unlockedStickers: JSON.parse(u.unlocked_stickers || '["✨","🔥"]'),
    unlockedFrames: JSON.parse(u.unlocked_frames || '["none"]')
  };
}

// ================= AUTH ROUTES =================
app.post('/api/auth/register', (req, res) => {
  const { username, email, password, role, country } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Username and password required' });
  try {
    const hash = bcrypt.hashSync(password, 10);
    const info = db.prepare(
      'INSERT INTO users (username, email, password_hash, role, country, last_active_at) VALUES (?, ?, ?, ?, ?, ?)'
    ).run(username.trim(), email || null, hash, role || 'Wanderer', country || 'India', Date.now());
    const user = db.prepare('SELECT * FROM users WHERE id = ?').get(info.lastInsertRowid);
    const token = jwt.sign({ id: user.id, username: user.username }, JWT_SECRET, { expiresIn: '30d' });
    res.json({ token, user: serializeUser(user) });
  } catch (err) {
    res.status(400).json({ error: 'Username or email already exists' });
  }
});

app.post('/api/auth/login', (req, res) => {
  const { identifier, password } = req.body;
  const user = db.prepare('SELECT * FROM users WHERE username = ? OR email = ?').get(identifier, identifier);
  if (!user || !bcrypt.compareSync(password, user.password_hash)) {
    return res.status(401).json({ error: 'Invalid credentials' });
  }
  const token = jwt.sign({ id: user.id, username: user.username }, JWT_SECRET, { expiresIn: '30d' });
  res.json({ token, user: serializeUser(user) });
});

app.post('/api/auth/guest', (req, res) => {
  const guestName = 'Wanderer' + Math.floor(1000 + Math.random() * 9000);
  const hash = bcrypt.hashSync('guest_' + Date.now(), 8);
  const info = db.prepare(
    'INSERT INTO users (username, password_hash, last_active_at) VALUES (?, ?, ?)'
  ).run(guestName, hash, Date.now());
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(info.lastInsertRowid);
  const token = jwt.sign({ id: user.id, username: user.username }, JWT_SECRET, { expiresIn: '30d' });
  res.json({ token, user: serializeUser(user) });
});

app.get('/api/auth/me', authMiddleware, (req, res) => {
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(req.user.id);
  if (!user) return res.status(404).json({ error: 'User not found' });
  res.json({ user: serializeUser(user) });
});

app.patch('/api/user/profile', authMiddleware, (req, res) => {
  const { username, role, country, theme, activeBadge, activeFrame } = req.body;
  const current = db.prepare('SELECT * FROM users WHERE id = ?').get(req.user.id);
  if (!current) return res.status(404).json({ error: 'User not found' });
  db.prepare(`
    UPDATE users SET username = ?, role = ?, country = ?, theme = ?, active_badge = ?, active_frame = ?
    WHERE id = ?
  `).run(
    username || current.username,
    role || current.role,
    country || current.country,
    theme || current.theme,
    activeBadge !== undefined ? activeBadge : current.active_badge,
    activeFrame || current.active_frame,
    req.user.id
  );
  const updated = db.prepare('SELECT * FROM users WHERE id = ?').get(req.user.id);
  res.json({ user: serializeUser(updated) });
});

// ================= ANTI-IDLE XP ENGINE =================
app.post('/api/user/xp', authMiddleware, (req, res) => {
  const { amount = 10, activePing = true } = req.body;
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(req.user.id);
  if (!user) return res.status(404).json({ error: 'User not found' });

  const now = Date.now();
  // Anti-idle server verification: reject passive farming if no genuine interaction
  if (!activePing) {
    return res.json({ user: serializeUser(user), gained: 0, idleBlocked: true });
  }

  const safeGain = Math.min(50, Math.max(1, Number(amount) || 5));
  const newXp = user.xp + safeGain;
  const newLevel = computeLevel(newXp);

  const stickers = STICKER_UNLOCKS.filter(s => s.level <= newLevel).map(s => s.icon);
  const frames = FRAME_UNLOCKS.filter(f => f.level <= newLevel).map(f => f.id);

  db.prepare(`
    UPDATE users SET xp = ?, level = ?, unlocked_stickers = ?, unlocked_frames = ?, last_active_at = ?
    WHERE id = ?
  `).run(newXp, newLevel, JSON.stringify(stickers), JSON.stringify(frames), now, req.user.id);

  const updated = db.prepare('SELECT * FROM users WHERE id = ?').get(req.user.id);
  res.json({ user: serializeUser(updated), gained: safeGain, leveledUp: newLevel > user.level });
});

// ================= CONFESSIONS & REPLIES =================
app.get('/api/confessions', (req, res) => {
  const rows = db.prepare('SELECT * FROM confessions ORDER BY id DESC LIMIT 50').all();
  const replyStmt = db.prepare('SELECT * FROM replies WHERE confession_id = ? ORDER BY id ASC');
  const data = rows.map(r => ({ ...r, replies: replyStmt.all(r.id) }));
  res.json(data);
});

app.post('/api/confessions', (req, res) => {
  const { authorName = 'Anonymous', text, mood = 'Reflective' } = req.body;
  if (!text || !text.trim()) return res.status(400).json({ error: 'Confession text required' });
  const info = db.prepare('INSERT INTO confessions (author_name, text, mood) VALUES (?, ?, ?)').run(
    authorName.slice(0, 32), text.trim().slice(0, 400), mood
  );
  const created = { ...db.prepare('SELECT * FROM confessions WHERE id = ?').get(info.lastInsertRowid), replies: [] };
  io.emit('confession:new', created);
  res.json(created);
});

app.post('/api/confessions/:id/like', (req, res) => {
  db.prepare('UPDATE confessions SET likes = likes + 1 WHERE id = ?').run(req.params.id);
  const updated = db.prepare('SELECT * FROM confessions WHERE id = ?').get(req.params.id);
  res.json(updated);
});

app.post('/api/confessions/:id/replies', (req, res) => {
  const { authorName = 'Anonymous', text } = req.body;
  if (!text || !text.trim()) return res.status(400).json({ error: 'Reply text required' });
  const info = db.prepare('INSERT INTO replies (confession_id, author_name, text) VALUES (?, ?, ?)').run(
    req.params.id, authorName.slice(0, 32), text.trim().slice(0, 240)
  );
  const reply = db.prepare('SELECT * FROM replies WHERE id = ?').get(info.lastInsertRowid);
  res.json(reply);
});

// ================= FATE MATCHMAKING & MESSAGES =================
const PEERS = [
  { key: 'clara', name: 'Clara', role: 'Artist 🎨', realm: 'Anime & Art', compatibility: 96, bio: "Anime enthusiast & digital illustrator looking for creative souls." },
  { key: 'kael', name: 'Kael', role: 'Shadow Rogue ⚔️', realm: 'Gaming & Raids', compatibility: 92, bio: "Clearing dungeon raids every weekend. Need a reliable strategist." },
  { key: 'lyra', name: 'Lyra', role: 'Synth Bard 🎧', realm: 'Music & Lofi', compatibility: 94, bio: "Mixing late-night lofi beats and dungeon ambient synths." },
  { key: 'orion', name: 'Orion', role: 'Stargazer 🔮', realm: 'Deep Talks', compatibility: 89, bio: "Let's talk philosophy, manhwa lore, and midnight confessions." }
];

app.get('/api/fate/roll', (req, res) => {
  const realm = req.query.realm;
  const pool = realm && realm !== 'All Realms' ? PEERS.filter(p => p.realm === realm) : PEERS;
  const match = pool[Math.floor(Math.random() * pool.length)] || PEERS[0];
  res.json(match);
});

app.get('/api/messages/:peerKey', authMiddleware, (req, res) => {
  const rows = db.prepare('SELECT * FROM messages WHERE sender_id = ? AND peer_key = ? ORDER BY id ASC').all(req.user.id, req.params.peerKey);
  res.json(rows);
});

app.post('/api/messages', authMiddleware, (req, res) => {
  const { peerKey, text } = req.body;
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(req.user.id);
  if (!peerKey || !text) return res.status(400).json({ error: 'Missing fields' });
  db.prepare('INSERT INTO messages (sender_id, peer_key, sender_name, text) VALUES (?, ?, ?, ?)').run(
    user.id, peerKey, user.username, text.trim()
  );
  // Simulated peer reply stored in SQL
  const peer = PEERS.find(p => p.key === peerKey) || PEERS[0];
  const replyText = `${peer.name}: Resonance received! Love talking about ${peer.realm.toLowerCase()} with you.`;
  db.prepare('INSERT INTO messages (sender_id, peer_key, sender_name, text) VALUES (?, ?, ?, ?)').run(
    user.id, peerKey, peer.name, replyText
  );
  const rows = db.prepare('SELECT * FROM messages WHERE sender_id = ? AND peer_key = ? ORDER BY id ASC').all(user.id, peerKey);
  res.json(rows);
});

// ================= GUILD RAID STATE =================
app.get('/api/guild', (req, res) => {
  const guild = db.prepare('SELECT * FROM guild_state WHERE id = 1').get();
  res.json(guild);
});

app.post('/api/guild/attack', (req, res) => {
  const damage = Math.min(350, Math.max(25, Number(req.body.damage) || 75));
  let guild = db.prepare('SELECT * FROM guild_state WHERE id = 1').get();
  let nextHp = guild.boss_hp - damage;
  let nextHonor = guild.honor_points + Math.round(damage / 3);
  if (nextHp <= 0) {
    nextHp = guild.boss_max_hp;
    nextHonor += 500;
  }
  db.prepare('UPDATE guild_state SET boss_hp = ?, honor_points = ? WHERE id = 1').run(nextHp, nextHonor);
  guild = db.prepare('SELECT * FROM guild_state WHERE id = 1').get();
  io.emit('guild:update', guild);
  res.json(guild);
});

// ================= PUBLIC & ADMIN CMS ROUTES =================
app.get('/api/cms', (req, res) => {
  const rows = db.prepare('SELECT * FROM cms_settings').all();
  const map = {};
  rows.forEach(r => { map[r.key] = r.value; });
  res.json({
    announcement: map.announcement || '',
    pollQuestion: map.poll_question || '',
    pollOptions: JSON.parse(map.poll_options || '[]')
  });
});

app.post('/api/cms/poll/vote', (req, res) => {
  const { index } = req.body;
  const row = db.prepare('SELECT value FROM cms_settings WHERE key = ?').get('poll_options');
  const options = JSON.parse(row.value || '[]');
  if (options[index]) options[index].votes += 1;
  db.prepare('UPDATE cms_settings SET value = ? WHERE key = ?').run(JSON.stringify(options), 'poll_options');
  io.emit('cms:poll', options);
  res.json(options);
});

app.post('/api/admin/cms', adminMiddleware, (req, res) => {
  const { announcement, pollQuestion, pollOptions } = req.body;
  if (announcement !== undefined) {
    db.prepare('INSERT OR REPLACE INTO cms_settings (key, value) VALUES (?, ?)').run('announcement', announcement);
  }
  if (pollQuestion !== undefined) {
    db.prepare('INSERT OR REPLACE INTO cms_settings (key, value) VALUES (?, ?)').run('poll_question', pollQuestion);
  }
  if (Array.isArray(pollOptions)) {
    db.prepare('INSERT OR REPLACE INTO cms_settings (key, value) VALUES (?, ?)').run('poll_options', JSON.stringify(pollOptions));
  }
  io.emit('cms:refresh');
  res.json({ ok: true });
});

app.delete('/api/admin/confessions/:id', adminMiddleware, (req, res) => {
  db.prepare('DELETE FROM confessions WHERE id = ?').run(req.params.id);
  res.json({ ok: true });
});

app.get('/api/admin/users', adminMiddleware, (req, res) => {
  const users = db.prepare('SELECT id, username, email, role, theme, xp, level, created_at FROM users ORDER BY xp DESC LIMIT 50').all();
  res.json(users);
});

// ================= REAL-TIME SOCKET.IO TAVERN =================
io.on('connection', (socket) => {
  socket.on('tavern:chat', (payload) => {
    io.emit('tavern:chat', {
      user: String(payload.user || 'Wanderer').slice(0, 28),
      badge: String(payload.badge || '').slice(0, 4),
      text: String(payload.text || '').slice(0, 240),
      ts: Date.now()
    });
  });

  socket.on('tavern:reaction', (emoji) => {
    io.emit('tavern:reaction', String(emoji || '🔥').slice(0, 4));
  });
});

server.listen(PORT, () => {
  console.log(`AuraConnect Full-Stack Server running at http://localhost:${PORT}`);
  console.log(`Admin CMS Panel available at http://localhost:${PORT}/admin.html`);
});
