# C9nnect Python + PostgreSQL Backend

## Render Deployment

1. **Create a Web Service on Render** pointing to this folder.
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn app:app --host 0.0.0.0 --port $PORT`
2. **Add Environment Variables in Render**:
   - `DATABASE_URL` = your Aiven PostgreSQL URI (`postgres://...sslmode=require`)
   - `JWT_SECRET` = any long random string
   - `ADMIN_SECRET` = your admin key for `/admin`
   - `SMTP_USER` & `SMTP_PASS` = your Gmail + 16-char App Password (optional, for email verification & password resets)
3. **Database Tables**:
   `app.py` automatically runs `schema.sql` on startup to create all tables (`users`, `confessions`, `confession_replies`, `messages`, `tavern_messages`, `guild_state`, `cms_settings`).
