# Лабораторная работа №3 — подписки и пуш-уведомления

> Построено на базе ЛР2 (двух сервисов: `backend` и `users-api`). Добавлены очередь Redis, Celery-воркер и push-notificator. Роль `posts` из задания выполняют существующие `articles`.

## Архитектура

```
┌────────┐      POST /api/articles        ┌─────────────┐
│  UI /  │ ─────────────────────────────▶ │   backend   │
│ API GW │                                │ (FastAPI)   │
└────────┘                                └─────┬───────┘
                                               │ enqueue Celery task
                                               ▼
                                        ┌─────────────┐
                                        │   Redis     │  queue: article-notifications
                                        └─────┬───────┘
                                              │
                                             pull
                                              ▼
                                        ┌─────────────┐
                                        │   worker    │ Celery
                                        └─────┬───────┘
                          read followers      │ POST /notify
                          ┌───────────────────┘
                          ▼
                    ┌────────────┐              ┌────────────────────────┐
                    │ users DB   │ ◀─────────── │ push-notificator (UI)  │
                    └────────────┘  auth token  └────────────────────────┘
```

- `backend` пишет статьи в `db-main` и ставит Celery-задачу `notify_followers`.
- `worker` обращается к Users DB, читает подписчиков и их `subscription_key`, проверяет идемпотентность через `notification_logs` и вызывает `push-notificator`.
- `push-notificator` развёрнут отдельным сервисом и доступен по `http://localhost:8000`.

## Новые API эндпоинты

| Endpoint | Описание |
| --- | --- |
| `PUT /api/users/me/subscription-key` | сохраняет ключ, полученный в UI push-сервиса |
| `POST /api/users/subscribe` | подписывает текущего пользователя на автора |
| `POST /api/articles` | как и раньше создаёт статью, но теперь помещает задачу в очередь |

Цепочка использования: пользователь заходит в push UI → копирует `subscription_key` → сохраняет его через Users API → подписывается на автора → создание статьи триггерит воркер → push приходит в UI.

## Очередь и воркер

- **Брокер:** Redis 7 (`redis://redis:6379/0`). Настраивается переменной `REDIS_URL`.
- **Очередь:** `article-notifications` (`NOTIFICATIONS_QUEUE`).
- **Воркер:** запускается отдельным сервисом `worker` (тот же образ, что и `backend`) командой `celery -A src.tasks.notifications worker`.
- **Идемпотентность:** таблица `notification_logs` (Users DB) хранит статус (`pending/processing/sent/failed`) и количество попыток для каждой пары `subscriber_id + article_id`. Повторные задачи пропускают уже отправленные уведомления.
- **Retry:** Celery повторяет задачу до 5 раз с экспоненциальной задержкой. Ошибки 4xx со стороны push-сервиса считаются финальными и просто логируются.

## Локальный запуск

```bash
docker-compose up -d
```

1. Откройте `http://localhost:8000` и нажмите «Generate Subscription Key».
2. Сохраните ключ: `PUT /api/users/me/subscription-key` с JWT пользователя.
3. Подпишитесь на автора: `POST /api/users/subscribe {"target_user_id": "<uuid>"}`.
4. Создайте новую статью: `POST /api/articles` — в логах `worker` появится «Sending notification...», а в интерфейсе push-сервиса прилетит уведомление.

## Переменные окружения

| Переменная | Назначение |
| --- | --- |
| `DATABASE_URL` | база статей (backend) |
| `USERS_DATABASE_URL` | база пользователей и подписок |
| `REDIS_URL` | брокер очереди |
| `NOTIFICATIONS_QUEUE` | имя Celery-очереди |
| `PUSH_SERVICE_URL` | endpoint push-notificator |
| `PUSH_TIMEOUT_SECONDS` | таймаут HTTP-запросов к push-сервису |

## Acceptance checklist

- [x] У пользователя есть `subscription_key`, таблица `subscribers` и миграция `002_add_subscriptions`.
- [x] `PUT /api/users/me/subscription-key` и `POST /api/users/subscribe` работают с JWT Users API.
- [x] `POST /api/articles` ставит задачу в очередь (см. логи backend).
- [x] Celery-воркер (`worker` сервис) читает Redis, получает подписчиков и отправляет push через `push-notificator`.
- [x] Подписчики без `subscription_key` пропускаются с предупреждением.
- [x] Идемпотентность обеспечена: `notification_logs` предотвращает повторные отправки для того же `subscriber_id` + `article_id`.
- [x] Docker Compose включает Redis, push-notificator и worker, все переменные вынесены в `.env`.

Готово к демонстрации: после запуска compose можно подписаться и получить push-уведомление в браузере.

