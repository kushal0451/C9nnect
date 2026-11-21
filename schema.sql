CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  uid VARCHAR(64) UNIQUE NOT NULL,
  username VARCHAR(64) UNIQUE NOT NULL,
  email VARCHAR(191) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  email_verified BOOLEAN DEFAULT FALSE,
  verification_code VARCHAR(16),
  reset_token VARCHAR(64),
  role VARCHAR(64) DEFAULT 'Wanderer',
  country VARCHAR(64) DEFAULT 'India',
  theme VARCHAR(32) DEFAULT 'ruins',
  xp INT DEFAULT 20,
  level INT DEFAULT 1,
  honor INT DEFAULT 0,
  daily_honor INT DEFAULT 0,
  equipped_sticker VARCHAR(16) DEFAULT '⚔️',
  equipped_deco VARCHAR(32) DEFAULT 'deco-none',
  bio TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS confessions (
  id SERIAL PRIMARY KEY,
  author_name VARCHAR(64) NOT NULL,
  sticker VARCHAR(16) DEFAULT '📜',
  mood VARCHAR(32) DEFAULT 'Reflective',
  text TEXT NOT NULL,
  likes INT DEFAULT 1,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS confession_replies (
  id SERIAL PRIMARY KEY,
  confession_id INT NOT NULL REFERENCES confessions(id) ON DELETE CASCADE,
  author_name VARCHAR(64) NOT NULL,
  text TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS messages (
  id SERIAL PRIMARY KEY,
  sender VARCHAR(64) NOT NULL,
  target VARCHAR(128) NOT NULL,
  text TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_messages_target_created ON messages (target, created_at);

CREATE TABLE IF NOT EXISTS tavern_messages (
  id SERIAL PRIMARY KEY,
  author VARCHAR(64) NOT NULL,
  sticker VARCHAR(16) DEFAULT '⚔️',
  text TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS guild_state (
  id INT PRIMARY KEY,
  boss_name VARCHAR(128) DEFAULT 'Obsidian Wyrm',
  boss_tier INT DEFAULT 1,
  boss_hp INT DEFAULT 500,
  boss_max_hp INT DEFAULT 500
);

INSERT INTO guild_state (id, boss_name, boss_tier, boss_hp, boss_max_hp)
VALUES (1, 'Obsidian Wyrm', 1, 500, 500)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS cms_settings (
  setting_key VARCHAR(64) PRIMARY KEY,
  setting_value TEXT NOT NULL
);

INSERT INTO cms_settings (setting_key, setting_value) VALUES
('announcement', 'Welcome to C9nnect! Stay active, earn Honor in the Motion Arena, and chat live.'),
('poll_question', 'Next Guild Raid Soundtrack?'),
('poll_options', '[{"label":"Dungeon Synth","votes":24},{"label":"Lofi Rain","votes":18},{"label":"Synthwave","votes":6}]')
ON CONFLICT (setting_key) DO NOTHING;
