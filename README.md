# 📊 Rating Monitor

 мониторинг рейтингов Parent ASIN на Amazon.

## Stack
- **Streamlit** — дашборд
- **PostgreSQL** (Heroku) — хранение данных
- **ScrapingDog API** — сбор данных с Amazon

## Deploy
1. Push to GitHub
2. Подключи на [share.streamlit.io](https://share.streamlit.io)
3. В Settings → Secrets добавь:
```toml
DATABASE_URL = "postgresql://user:pass@host:5432/dbname"
```

## Структура
```
streamlit_app.py      — основной дашборд
requirements.txt      — зависимости
.streamlit/config.toml — тема
```
