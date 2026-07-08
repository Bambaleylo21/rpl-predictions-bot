# Project Transfer Report: RPL Predictor Bot + Telegram Mini App

Дата подготовки: 2026-07-08  
Локальный путь проекта: `/Users/romantcykun/Documents/rpl-bot`

## 1. Репозиторий и Git-состояние

### GitHub

- Репозиторий: `https://github.com/Bambaleylo21/rpl-predictions-bot.git`
- Текущая ветка: `main`
- Последний коммит: `230f7d8 duels: add advantage draws for missed outcomes`

### Важное состояние перед переносом

На момент финальной проверки tracked-изменений нет: текущий `HEAD` совпадает с `origin/main`. При этом есть неотслеживаемые файлы, включая сам отчет и два CSV-файла.

Это значит: кодовые изменения закоммичены и запушены, но перед переносом нужно отдельно решить, нужны ли неотслеживаемые CSV-файлы и сам отчет в репозитории.

### Вывод `git status`

```text
On branch main
Your branch is up to date with 'origin/main'.

Untracked files:
  (use "git add <file>..." to include in what will be committed)
	PROJECT_TRANSFER_REPORT.md
	data/wc2026_playoff_schedule_msk.csv
	data/wc2026_schedule_export.csv

nothing added to commit but untracked files present (use "git add" to track)
```

### Вывод `git log -1 --stat --decorate --oneline`

```text
230f7d8 (HEAD -> main, origin/main) duels: add advantage draws for missed outcomes
 app/duel_notify.py                    | 13 ++++++++
 app/duels.py                          | 32 ++++++++++++++++++
 tests/test_duel_tiebreaks_unittest.py | 61 ++++++++++++++++++++++++-----------
 3 files changed, 88 insertions(+), 18 deletions(-)
```

### Незапушенные/незакоммиченные локальные изменения

Tracked-файлы чистые: незакоммиченных изменений в отслеживаемом коде нет. Ветка `main` синхронизирована с `origin/main`.

Неотслеживаемые файлы:

- `PROJECT_TRANSFER_REPORT.md` — этот отчет.
- `data/wc2026_playoff_schedule_msk.csv`
- `data/wc2026_schedule_export.csv`

CSV-файлы не нужно автоматически добавлять в коммит без отдельного решения.

## 2. Технологический стек

### Backend / Telegram Bot / API

- Язык: Python
- Основные библиотеки:
  - `aiogram==3.25.0` — Telegram-бот
  - `aiohttp==3.13.3` — Mini App API
  - `SQLAlchemy==2.0.46` — ORM / база данных
  - `asyncpg` — PostgreSQL async driver
  - `aiosqlite==0.22.1` — локальная SQLite-разработка
  - `python-dotenv==1.2.1` — загрузка `.env`
- Локально обнаруженная версия Python:
  - `Python 3.13.0`

### Mini App

- Framework: React + TypeScript + Vite
- `package.json`:
  - `react`: `^19.2.4`
  - `react-dom`: `^19.2.4`
  - `vite`: `^8.0.4`
  - `typescript`: `~6.0.2`
  - `@twa-dev/sdk`: `^8.0.2`
- Менеджер пакетов: `npm`
- Lockfile: `miniapp/package-lock.json`
- В `package.json` нет поля `engines`, поэтому версия Node явно не зафиксирована.
- Локально обнаруженная версия Node:
  - `v24.15.0`
- Локально обнаруженная версия npm:
  - `11.12.1`

## 3. Переменные окружения

Ниже список переменных, найденных по коду и локальному `.env`. Реальные значения и секреты не включены.

### Backend / Bot / API

| Переменная | Где используется | Назначение | Нужно в Render |
|---|---|---|---|
| `BOT_TOKEN` | `main.py`, `app/config.py`, `app/miniapp_api.py` | Токен Telegram-бота из BotFather. Секрет. | Да, обязательно |
| `ADMIN_IDS` | `app/config.py`, `app/handlers_admin.py`, `app/handlers_user.py`, `app/miniapp_api.py` | Telegram ID админов через запятую. Управляет доступом к админке и админ-командам. | Да, обязательно |
| `DATABASE_URL` | `app/db.py` | Подключение к базе. На Render обычно PostgreSQL URL. Локально fallback: `sqlite+aiosqlite:///./bot.db`. | Да, обязательно для прода |
| `MINIAPP_WEB_URL` | `app/bot_commands.py`, `app/handlers_user.py`, `app/handlers_admin.py`, `app/duel_notify.py`, `app/reminders.py`, `app/miniapp_api.py` | Публичный URL Mini App, который открывается из Telegram-кнопок. Fallback в коде: `https://rpl-predictions-bot-mini-app.onrender.com`. | Да, желательно явно |
| `MINIAPP_API_ENABLED` | `main.py` | Если `1`, бот запускает Mini App API внутри процесса `main.py`. Если API вынесен отдельным Render-сервисом, может быть `0`/не задано у worker-бота. | Зависит от схемы деплоя |
| `MINIAPP_API_HOST` | `app/miniapp_api.py` | Host для aiohttp Mini App API. Default: `0.0.0.0`. | Обычно да для API-сервиса |
| `MINIAPP_API_PORT` | `app/miniapp_api.py` | Port для aiohttp Mini App API. Default: `8081`. На Render обычно должен соответствовать `$PORT`, если сервис web. | Да для API-сервиса |
| `ROUND_DIGEST_CHAT_ID` | `app/handlers_admin.py` | Chat ID для сводного пуша/дайджеста тура, если используется. | Опционально |
| `EXACT_HIT_PUSH_DELAY_SEC` | `app/handlers_admin.py` | Задержка между пушами по точному счету/ачивкам, default `0.12`. Сейчас часть пушей отключалась продуктово, но переменная остается в коде. | Опционально |

### Mini App / Vite

| Переменная | Где используется | Назначение | Нужно в Render/static build |
|---|---|---|---|
| `VITE_API_BASE` | `miniapp/src/App.tsx` | Base URL Mini App API. Локальный fallback: `http://localhost:8081`. В проде должен указывать на публичный API-сервис Render. | Да, обязательно для прод-сборки |
| `VITE_DEBUG_PANELS` | `miniapp/src/App.tsx` | Если `1`, включает debug-панели в UI. В проде должно быть не задано или `0`. | Нет, только для отладки |

### Что уже видно локально

Локальный `.env` содержит только названия:

```text
ADMIN_IDS
BOT_TOKEN
```

### Что уже настроено в Render

Локальный репозиторий не содержит `render.yaml` и не дает прямого доступа к панели Render, поэтому стопроцентно подтвердить текущие Render Environment Variables из кода невозможно.

По рабочей архитектуре проекта в Render должны быть настроены минимум:

- Для Telegram bot/background worker:
  - `BOT_TOKEN`
  - `ADMIN_IDS`
  - `DATABASE_URL`
  - `MINIAPP_WEB_URL`
  - возможно `MINIAPP_API_ENABLED=0`, если API работает отдельным сервисом
- Для Mini App API/web service:
  - `BOT_TOKEN`
  - `ADMIN_IDS`
  - `DATABASE_URL`
  - `MINIAPP_WEB_URL`
  - `MINIAPP_API_HOST=0.0.0.0`
  - `MINIAPP_API_PORT` или `PORT`, в зависимости от Render-команды запуска
- Для static Mini App build:
  - `VITE_API_BASE`
  - `VITE_DEBUG_PANELS` не нужен в проде

При переносе нужно вручную сверить эти переменные в Render Dashboard.

## 4. Команды запуска

### Backend

Установка зависимостей:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Локальный запуск Telegram-бота:

```bash
python main.py
```

Альтернативно через Makefile:

```bash
make install
make run
```

Локальный запуск только Mini App API:

```bash
python -m app.miniapp_api
```

Важно: для локального API нужен `.env` с `BOT_TOKEN`, `ADMIN_IDS`, при необходимости `DATABASE_URL`, `MINIAPP_API_HOST`, `MINIAPP_API_PORT`.

### Mini App

Перейти в папку Mini App:

```bash
cd miniapp
```

Установка зависимостей:

```bash
npm install
```

Локальный dev-запуск:

```bash
npm run dev
```

Сборка:

```bash
npm run build
```

Preview production build:

```bash
npm run preview
```

В `package.json` нет отдельной команды `start`; для продакшена Mini App обычно деплоится как static build из `miniapp/dist`.

## 5. Render-конфигурация

В репозитории не найден `render.yaml`, поэтому Render настроен вручную через Dashboard.

### Сервисы проекта по текущей архитектуре

#### 1. Telegram Bot

- Тип: Background Worker
- Runtime: Python 3
- Имя сервиса в Render по истории проекта: `rpl-predictions-bot`
- Start command:

```bash
python main.py
```

- Build command вероятно:

```bash
pip install -r requirements.txt
```

- Автодеплой: из ветки `main`
- План по скриншотам ранее: `Starter`
- Режим Telegram: long polling, не webhook
- В `main.py` перед polling выполняется:

```python
await bot.delete_webhook(drop_pending_updates=False)
await dp.start_polling(bot)
```

#### 2. Mini App API

- Тип: Web Service
- Runtime: Python 3
- Имя сервиса по скриншотам ранее: `rpl-miniapp-api`
- Start command вероятно:

```bash
python -m app.miniapp_api
```

или, если API запускается внутри `main.py`:

```bash
MINIAPP_API_ENABLED=1 python main.py
```

Рекомендуемая более чистая схема: отдельный API-сервис с `python -m app.miniapp_api`.

- Build command:

```bash
pip install -r requirements.txt
```

- API endpoints начинаются с `/api/miniapp/...`
- Основной endpoint health/service:

```text
GET /api/miniapp/me
GET /api/miniapp/tournaments
GET /api/miniapp/profile
GET /api/miniapp/predict/current
GET /api/miniapp/duels/current
GET /api/miniapp/table/current
```

#### 3. Static Mini App

- Тип: Static Site
- Папка: `miniapp`
- Build command:

```bash
npm install && npm run build
```

- Publish directory:

```text
miniapp/dist
```

- Публичный URL по коду fallback:

```text
https://rpl-predictions-bot-mini-app.onrender.com
```

#### 4. Database

- Тип: PostgreSQL
- Имя по скриншотам ранее: `rpl-predictions-db`
- План по скриншотам ранее: `Basic-256mb`
- Регион по скриншотам ранее: `Oregon (US West)`
- Подключение через `DATABASE_URL`

## 6. Telegram-специфика

### BotFather / Bot Token

- Токен бота хранится в переменной:

```text
BOT_TOKEN
```

- Реальное значение не должно попадать в репозиторий или отчет.

### Webhook

Проект сейчас работает через long polling, не через webhook.

В `main.py` явно сбрасывается webhook:

```python
await bot.delete_webhook(drop_pending_updates=False)
await dp.start_polling(bot)
```

Поэтому webhook URL в BotFather/Telegram API для этого проекта не является основной схемой работы.

### Mini App URL

Mini App открывается через Telegram Web App кнопки. URL задается переменной:

```text
MINIAPP_WEB_URL
```

Fallback в коде:

```text
https://rpl-predictions-bot-mini-app.onrender.com
```

Этот же URL должен быть указан в BotFather для Web App / Menu Button, если используется постоянная кнопка Mini App.

### Telegram Main Button / Menu Button

В `app/bot_commands.py` нижняя кнопка Telegram-бота ведет в Mini App:

```python
web_app=types.WebAppInfo(url=MINIAPP_WEB_URL)
```

### Авторизация Mini App

Mini App использует Telegram init data. Backend проверяет подпись Telegram Mini App в `app/miniapp_api.py`.

Клиент отправляет заголовки, связанные с Telegram WebApp initData, а backend на их основе определяет пользователя.

## 7. Структура проекта

```text
/Users/romantcykun/Documents/rpl-bot
├── app/
│   ├── db.py                 # подключение к БД, init_db, мини-миграции
│   ├── models.py             # SQLAlchemy-модели
│   ├── handlers.py           # регистрация handler-ов
│   ├── handlers_user.py      # пользовательские команды Telegram
│   ├── handlers_admin.py     # админ-команды Telegram
│   ├── miniapp_api.py        # aiohttp API для Mini App
│   ├── scoring.py            # расчет очков прогнозов
│   ├── duels.py              # логика 1x1 и Elo
│   ├── duel_notify.py        # пуши по дуэлям
│   ├── reminders.py          # напоминания о матчах
│   ├── notify_prefs.py       # настройки уведомлений пользователей
│   ├── league_table.py       # таблица и места
│   ├── stats.py              # статистика профиля
│   ├── tournament.py         # логика турниров
│   ├── season_setup.py       # сезонные настройки
│   └── display.py            # отображение названий/раундов/команд
├── miniapp/
│   ├── package.json          # React/Vite зависимости и scripts
│   ├── vite.config.ts        # Vite config
│   ├── index.html
│   ├── src/
│   │   ├── App.tsx           # основной React-компонент Mini App
│   │   ├── App.css           # основной UI/theme styling
│   │   ├── main.tsx
│   │   ├── index.css
│   │   └── assets/
│   │       ├── achievements/ # PNG-ачивки
│   │       └── tournaments/  # иконки турниров WC/RPL
│   └── dist/                 # production build Mini App
├── data/
│   ├── wc2026.xlsx           # данные/расписание WC2026
│   ├── wc2026_playoff_schedule_msk.csv  # неотслеживаемый CSV
│   └── wc2026_schedule_export.csv       # неотслеживаемый CSV
├── scripts/
│   ├── import_wc2026_schedule.py
│   ├── repair_rpl_data.py
│   └── wc2026_summary.py
├── tests/
│   ├── test_duel_tiebreaks_unittest.py
│   ├── test_match_status_unittest.py
│   ├── test_score_parsing_unittest.py
│   └── test_scoring_unittest.py
├── main.py                   # входная точка Telegram bot worker
├── requirements.txt          # Python dependencies
├── Makefile                  # install/run/freeze
├── bot.db                    # локальная SQLite база, отслеживается git
├── data.db                   # локальная база/данные, неясное legacy
└── README.md
```

## 8. Основной функционал приложения

### Mini App

Вкладки:

- `Профиль`
- `Матчи`
- `Таблица`
- `1x1`
- `Админ` для админа

### Турниры

- `WC2026` — активный ручной турнир Чемпионата мира.
- `RPL` — сейчас в приложении используется как будущий/заглушечный турнир, набор закрыт.
- Есть команды создания/открытия/закрытия турниров через админ-команды.

### Матчи

- Активные и завершенные матчи.
- Прогнозы пользователей.
- Community-проценты.
- Плейофф-слоты для WC2026.
- Доп. прогнозы: чемпион и лучший бомбардир.

### Таблица

- Общая таблица.
- Таблицы по турам/стадиям.
- Доп. прогнозы отдельной вкладкой.
- Очки от playoff coefficients учитываются в общем счете, но колонка звезды показывает только long-term/dop-прогнозы.

### 1x1

- Вызовы на конкретный матч.
- Принятие/отклонение/отмена.
- Автоистечение pending-вызова через 3 часа, если до старта матча было 3+ часа.
- Глобальный Elo по всем турнирам.
- Победитель дуэли определяется не только стандартными очками, но и tie-break логикой:
  - исход важнее близости;
  - затем разница;
  - затем близость к счету;
  - если никто не угадал исход, возможна ничья с преимуществом.

### Ачивки

- Визуальные PNG-ачивки.
- Уровневые, уникальные и секретные.
- Секретные до открытия скрыты, но если кто-то получил — остальным может показываться затемненная ачивка с подписью владельца.
- Пуши по новым ачивкам ведут в профиль Mini App.

### Уведомления

- Пользователь может включать/отключать уведомления в профиле.
- Есть пуши по 1x1, ачивкам и напоминаниям о матчах.
- Пуши напоминаний ведут в раздел `Матчи`.

## 9. Известные проблемы и незавершенные задачи

### 1. В репозитории есть неотслеживаемые файлы

Tracked-код чистый и ветка `main` совпадает с `origin/main`, но есть неотслеживаемые файлы:

- `PROJECT_TRANSFER_REPORT.md`
- `data/wc2026_playoff_schedule_msk.csv`
- `data/wc2026_schedule_export.csv`

Перед переносом нужно решить, добавлять ли эти файлы в репозиторий или передавать отдельно.

### 2. Нет `render.yaml`

Render-конфигурация не описана как код. При переносе придется вручную восстановить сервисы в Render или создать новый `render.yaml`.

### 3. Переменные Render нельзя подтвердить из репозитория

Список переменных найден по коду, но фактические значения и наличие в Render нужно сверить вручную в Dashboard.

### 4. В коде есть мини-миграции вместо полноценного Alembic

`app/db.py` содержит много `ALTER TABLE`/`CREATE TABLE` миграций. Это работает, но при дальнейшем развитии лучше перейти на нормальные миграции, например Alembic.

### 5. `bot.db` отслеживается git

Локальная SQLite база `bot.db` находится в git. Для продакшена используется PostgreSQL, но при переносе стоит аккуратно решить, нужна ли эта локальная база в репозитории.

### 6. API автоматического парсинга РПЛ еще не подключен

План обсужден, но реализация еще не начата. WC2026 должен остаться ручным турниром. Для РПЛ планируется внешний футбольный API, например API-FOOTBALL.

### 7. `package.json` не фиксирует Node version

В `miniapp/package.json` нет `engines`. При переносе лучше зафиксировать Node LTS-версию, например 22 или текущую рабочую Render-версию.

### 8. Некоторые настройки продукта живут в коде

Например tournament defaults, stage names, achievements и часть UX-логики сейчас захардкожены в коде. Это нормально для текущего этапа, но при росте числа турниров лучше выносить больше настроек в базу/админку.

## 10. Рекомендуемые первые шаги после переноса

1. Склонировать репозиторий.
2. Проверить ветку `main`.
3. Перенести env-переменные без раскрытия секретов.
4. Запустить backend локально на SQLite или тестовой PostgreSQL.
5. Запустить Mini App локально с `VITE_API_BASE=http://localhost:8081`.
6. Проверить Telegram init data в реальном Telegram Mini App.
7. Настроить Render-сервисы или создать `render.yaml`.
8. После деплоя проверить:
   - `/ping` в боте;
   - открытие Mini App из Telegram;
   - профиль;
   - матчи;
   - таблицу;
   - 1x1;
   - админку;
   - пуши.

## 11. Быстрые команды для нового разработчика

```bash
git clone https://github.com/Bambaleylo21/rpl-predictions-bot.git
cd rpl-predictions-bot
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Mini App:

```bash
cd miniapp
npm install
npm run dev
npm run build
```

API отдельно:

```bash
cd /path/to/rpl-predictions-bot
source .venv/bin/activate
python -m app.miniapp_api
```

## 12. Передача секретов

Секреты нельзя передавать в markdown-файле. Их нужно переносить отдельно через безопасный канал:

- `BOT_TOKEN`
- `DATABASE_URL`
- любые будущие API keys, например `FOOTBALL_API_KEY`

