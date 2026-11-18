const Database = require('better-sqlite3');
const path = require('path');
const bcrypt = require('bcryptjs');

const dbPath = path.join(__dirname, 'auraconnect.sqlite');
const db = new Database(dbPath);
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT DEFAULT 'Wanderer',
    country TEXT DEFAULT 'India',
    theme TEXT DEFAULT 'original',
    xp INTEGER DEFAULT 0,
    level INTEGER DEFAULT 1,
    active_badge TEXT DEFAULT '',
    active_frame TEXT DEFAULT 'none',
    unlocked_stickers TEXT DEFAULT '["✨","🔥"]',
    unlocked_frames TEXT DEFAULT '["none"]',
    last_active_at INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS confessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    author_name TEXT NOT NULL,
    text TEXT NOT NULL,
    mood TEXT DEFAULT 'Reflective',
    likes INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS replies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    confession_id INTEGER NOT NULL,
    author_name TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (confession_id) REFERENCES confessions(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_id INTEGER NOT NULL,
    peer_key TEXT NOT NULL,
    sender_name TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS guild_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    boss_name TEXT DEFAULT 'Obsidian Wyrm',
    boss_hp INTEGER DEFAULT 4200,
    boss_max_hp INTEGER DEFAULT 5000,
    honor_points INTEGER DEFAULT 1450
  );

  CREATE TABLE IF NOT EXISTS cms_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );
`);

// Seed initial data if empty
const confCount = db.prepare('SELECT COUNT(*) as c FROM confessions').get().c;
if (confCount === 0) {
  const insertConf = db.prepare('INSERT INTO confessions (author_name, text, mood, likes) VALUES (?, ?, ?, ?)');
  insertConf.run('Anonymous381', "I've wasted every second of it. By the end of it, I weigh at 260 and it haunts me all the time. I'm going through...", 'Reflective', 14);
  insertConf.run('Anonymous742', "Sometimes the hardest part of the day is pretending everything is fine when inside my mind is racing with unsaid words.", 'Love', 29);
  insertConf.run('SoulSeeker', "Today I finally chose myself and walked away from a toxic circle. Scary, but my chest feels lighter.", 'Hope', 41);
}

db.prepare('INSERT OR IGNORE INTO guild_state (id, boss_name, boss_hp, boss_max_hp, honor_points) VALUES (1, ?, 4200, 5000, 1450)').run('Obsidian Wyrm');

const seedCms = db.prepare('INSERT OR IGNORE INTO cms_settings (key, value) VALUES (?, ?)');
seedCms.run('announcement', 'Welcome to AuraConnect! Stay active to earn XP, unlock rare stickers, and defeat the Obsidian Wyrm.');
seedCms.run('poll_question', 'Tonight’s Tavern Vibe?');
seedCms.run('poll_options', JSON.stringify([
  { label: 'Rainy Lofi Beats', votes: 18 },
  { label: 'Ancient Dungeon Synth', votes: 24 },
  { label: 'Late-Night Deep Talk', votes: 12 }
]));

module.exports = db;
