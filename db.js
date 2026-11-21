const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.join(__dirname, 'c9nnect.sqlite');
const db = new Database(dbPath);
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT UNIQUE NOT NULL,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    email_verified INTEGER DEFAULT 0,
    verification_code TEXT,
    reset_token TEXT,
    role TEXT DEFAULT 'Wanderer',
    affinity TEXT DEFAULT 'Anime & Art',
    country TEXT DEFAULT 'India',
    theme TEXT DEFAULT 'ruins',
    xp INTEGER DEFAULT 20,
    level INTEGER DEFAULT 1,
    honor INTEGER DEFAULT 0,
    daily_honor INTEGER DEFAULT 0,
    equipped_sticker TEXT DEFAULT '⚔️',
    equipped_deco TEXT DEFAULT 'deco-none',
    bio TEXT DEFAULT '',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS friends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    requester_uid TEXT NOT NULL,
    addressee_uid TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(requester_uid, addressee_uid)
  );

  CREATE TABLE IF NOT EXISTS confessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    author_name TEXT NOT NULL,
    sticker TEXT DEFAULT '📜',
    mood TEXT DEFAULT 'Reflective',
    text TEXT NOT NULL,
    likes INTEGER DEFAULT 1,
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
    sender TEXT NOT NULL,
    target TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS guild_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    boss_name TEXT DEFAULT 'Obsidian Wyrm',
    boss_tier INTEGER DEFAULT 1,
    boss_hp INTEGER DEFAULT 500,
    boss_max_hp INTEGER DEFAULT 500
  );

  CREATE TABLE IF NOT EXISTS cms_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );
`);

const confCount = db.prepare('SELECT COUNT(*) as c FROM confessions').get().c;
if (confCount === 0) {
  const ins = db.prepare('INSERT INTO confessions (author_name, sticker, mood, text, likes) VALUES (?, ?, ?, ?, ?)');
  ins.run('Anonymous381', '📜', 'Reflective', "Trying to rebuild myself day by day—grateful for a place to breathe.", 14);
  ins.run('ShadowSeeker', '⚔️', 'Hope', "Small steps really do add up. Finished my project tonight!", 22);
}

db.prepare('INSERT OR IGNORE INTO guild_state (id, boss_name, boss_tier, boss_hp, boss_max_hp) VALUES (1, ?, 1, 500, 500)').run('Obsidian Wyrm');

const seedCms = db.prepare('INSERT OR IGNORE INTO cms_settings (key, value) VALUES (?, ?)');
seedCms.run('announcement', 'Welcome to C9nnect! Stay active, earn Honor in the Motion Arena, and chat live.');
seedCms.run('poll_question', 'Next Guild Raid Soundtrack?');
seedCms.run('poll_options', JSON.stringify([
  { label: 'Dungeon Synth', votes: 24 },
  { label: 'Lofi Rain', votes: 18 },
  { label: 'Synthwave', votes: 6 }
]));

module.exports = db;
