# Лабораторная работа №4 — SAGA Choreography для публикации постов

> Построено на базе ЛР3. Добавлена двухэтапная публикация постов с модерацией через SAGA Choreography паттерн, система внутренних API-ключей и DLQ (Dead Letter Queue).

## Архитектура

```
┌────────┐   POST /api/articles/{slug}/publish   ┌─────────────┐
│  UI /  │ ─────────────────────────────────────▶ │   backend   │
│ API GW │                                        │ (FastAPI)   │
└────────┘                                        └─────┬───────┘
                                                        │ enqueue post.moderate
                                                        ▼
                                                 ┌─────────────┐
                                                 │   Redis     │
                                                 └─────┬───────┘
                                                       │
                                                       │ pull
                                                       ▼
                                            ┌──────────────────┐
                                            │  Moderation      │
                                            │  Worker          │
                                            └─────┬────────────┘
                                                  │
                    ┌─────────────────────────────┼─────────────────────────────┐
                    │                             │                             │
                    ▼                             ▼                             ▼
            [APPROVED]                    [REJECTED]                    [ERROR → DLQ]
                    │                             │                             │
                    │                             │                             │
                    ▼                             │                             │
            enqueue post.generate_preview         │                             │
                    │                             │                             │
                    ▼                             │                             │
            ┌───────────────┐                     │                             │
            │ Preview       │                     │                             │
            │ Worker        │                     │                             │
            └───────┬───────┘                     │                             │
                    │                             │                             │
                    ▼                             │                             │
            enqueue post.publish                  │                             │
                    │                             │                             │
                    ▼                             │                             │
            ┌───────────────┐                     │                             │
            │ Publish       │                     │                             │
            │ Worker        │                     │                             │
            └───────┬───────┘                     │                             │
                    │                             │                             │
                    ▼                             │                             │
            enqueue post.notify                   │                             │
                    │                             │                             │
                    ▼                             │                             │
            ┌───────────────┐                     │                             │
            │ Notification  │                     │                             │
            │ Worker (ЛР3)  │                     │                             │
            └───────────────┘                     │                             │
                                                │                             │
                                                ▼                             ▼
                                        ┌──────────────┐              ┌──────────────┐
                                        │  REJECTED    │              │  DLQ Worker  │
                                        │  status      │              │  (compensate)│
                                        └──────────────┘              └──────────────┘
```

## Жизненный цикл поста

Пост проходит через следующие статусы:

- **DRAFT** — черновик (по умолчанию при создании)
- **PENDING_PUBLISH** — автор запросил публикацию, идёт модерация/обработка
- **PUBLISHED** — пост опубликован (после успешной модерации и генерации превью)
- **REJECTED** — модерация отклонила пост
- **ERROR** — произошла техническая ошибка при обработке

Все переходы состояний происходят только как результат локальных транзакций в Posts Service.

## Новые API эндпоинты

### Пользовательские эндпоинты

| Endpoint | Описание |
| --- | --- |
| `POST /api/articles/{slug}/publish` | Запрос публикации поста (только автор, только из статуса DRAFT) |

### Внутренние эндпоинты (требуют API-ключ)

| Endpoint | Описание |
| --- | --- |
| `POST /internal/articles/{id}/reject` | Отклонить пост (компенсация модерации) |
| `PUT /internal/articles/{id}/preview` | Установить preview URL |
| `POST /internal/articles/{id}/publish` | Опубликовать пост |
| `GET /internal/articles/{id}` | Получить пост по ID |

## SAGA Choreography — поток задач

### 1. Запрос публикации

Пользователь вызывает `POST /api/articles/{slug}/publish`:

- Проверяется, что пользователь — автор поста
- Проверяется, что пост в статусе DRAFT
- Локальная транзакция: статус меняется на PENDING_PUBLISH
- Ставится задача `post.moderate` в очередь

### 2. Модерация (Moderation Worker)

Воркер читает задачу `post.moderate`:

- Получает пост из БД
- Симулирует модерацию (random: 70% одобрение, 30% отклонение)
- **Если одобрено:**
  - Ставит задачу `post.generate_preview` в очередь
- **Если отклонено:**
  - Вызывает компенсацию: `POST /internal/articles/{id}/reject`
  - Пост переходит в статус REJECTED

### 3. Генерация превью (Preview Worker)

Воркер читает задачу `post.generate_preview`:

- Получает данные поста
- Генерирует превью (заглушка: `https://preview.example.com/articles/{id}/preview.png`)
- Сохраняет через `PUT /internal/articles/{id}/preview`
- Ставит задачу `post.publish` в очередь

### 4. Публикация (Publish Worker)

Воркер читает задачу `post.publish`:

- Вызывает `POST /internal/articles/{id}/publish`
- Пост переходит в статус PUBLISHED
- Ставит задачу `post.notify` в очередь (использует существующий воркер из ЛР3)

### 5. Уведомления (Notification Worker из ЛР3)

Существующий воркер обрабатывает `post.notify` и отправляет push-уведомления подписчикам.

## Система внутренних API-ключей

Для защиты внутренних эндпоинтов реализована система API-ключей:

### Генерация ключей

```bash
python scripts/generate_api_keys.py
```

Скрипт создаёт ключи для:
- `moderation-worker`
- `preview-worker`
- `publish-worker`
- `dlq-worker`

### Использование

Воркеры используют API-ключи в заголовке `Authorization`:

```
Authorization: Token <api-key>
```

Внутренние эндпоинты проверяют ключ через middleware `verify_api_key`.

### Настройка

Добавьте в `.env` или `docker-compose.yml`:

```env
INTERNAL_API_KEY=<generated-key>
```

## Dead Letter Queue (DLQ)

### Настройка

DLQ настроен для обработки задач, которые не удалось выполнить после всех ретраев:

- **Очередь:** `dlq`
- **Максимум попыток:** 3 (настраивается в задачах)
- **Backoff:** экспоненциальный

### Компенсирующие действия

DLQ Worker выполняет компенсирующие действия в зависимости от типа задачи:

| Тип задачи | Компенсация |
| --- | --- |
| `moderate_post` | Пост → REJECTED |
| `generate_preview` | Пост → ERROR |
| `publish_post` | Пост → ERROR |
| `notify` | Логирование (пост уже опубликован) |

### Обработка ошибок

Все задачи SAGA имеют:

- `max_retries=3`
- `retry_backoff=True` (экспоненциальная задержка)
- `retry_jitter=True` (случайная вариация)

При исчерпании попыток задача автоматически попадает в DLQ.

## Идемпотентность

Все обработчики идемпотентны:

- **moderate_post:** Проверяет статус поста перед обработкой
- **generate_preview:** Проверяет наличие preview_url перед генерацией
- **publish_post:** Проверяет статус PUBLISHED перед публикацией
- **notify:** Использует существующую логику `notification_logs` из ЛР3

## Тестирование

Подробные инструкции по тестированию через Swagger UI см. в [LAB4_TESTING.md](LAB4_TESTING.md).

## Локальный запуск

### 1. Генерация API-ключей

```bash
# Запустите миграции сначала (если еще не сделано)
docker-compose exec backend alembic upgrade head

# Сгенерируйте ключи (внутри Docker контейнера)
docker-compose exec backend python generate_api_keys.py
```

Скопируйте один из сгенерированных ключей в `.env`:

```env
INTERNAL_API_KEY=<your-generated-key>
```

**Примечание:** Скрипт должен запускаться внутри Docker контейнера, так как он подключается к базе данных через Docker сеть.

### 2. Запуск всех сервисов

```bash
docker-compose up -d
```

Это запустит:
- `backend` — API статей
- `users-api` — управление пользователями
- `worker` — воркер уведомлений (ЛР3)
- `saga-worker` — воркер модерации, превью и публикации
- `dlq-worker` — воркер DLQ
- `redis` — очередь задач
- `push-notificator` — push-сервис
- `api-gateway` — Nginx

### 3. Тестирование

1. **Создайте пост:**
```bash
curl -X POST http://localhost/api/articles \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <jwt-token>" \
  -d '{
    "title": "Test Article",
    "description": "Test Description",
    "body": "Test Body"
  }'
```

2. **Запросите публикацию:**
```bash
curl -X POST http://localhost/api/articles/{slug}/publish \
  -H "Authorization: Bearer <jwt-token>"
```

3. **Проверьте статус:**
```bash
curl http://localhost/api/articles/{slug}
```

4. **Наблюдайте логи:**
```bash
docker-compose logs -f saga-worker
docker-compose logs -f worker
docker-compose logs -f dlq-worker
```

## Переменные окружения

| Переменная | Назначение |
| --- | --- |
| `DATABASE_URL` | База статей (backend) |
| `USERS_DATABASE_URL` | База пользователей |
| `REDIS_URL` | Брокер очереди |
| `NOTIFICATIONS_QUEUE` | Имя основной очереди |
| `DLQ_QUEUE` | Имя DLQ очереди |
| `PUSH_SERVICE_URL` | Endpoint push-notificator |
| `BACKEND_URL` | URL backend сервиса (для внутренних запросов) |
| `INTERNAL_API_KEY` | Внутренний API-ключ для service-to-service коммуникации |

## Acceptance Criteria

- [x] Реализован эндпоинт `POST /api/articles/{slug}/publish`
- [x] Реализован Moderation Worker с случайной модерацией
- [x] Реализован Preview Worker с генерацией превью
- [x] Реализован Publish Worker для финальной публикации
- [x] Настроены ретраи и DLQ для всех задач
- [x] Реализован DLQ Worker с компенсирующими действиями
- [x] Все обработчики идемпотентны
- [x] Реализована система внутренних API-ключей
- [x] Внутренние эндпоинты защищены API-ключами
- [x] Воркеры используют API-ключи для внутренних запросов
- [x] Docker Compose настроен для всех воркеров

## Отличия от ЛР3

- **Статусы постов:** Добавлены статусы DRAFT, PENDING_PUBLISH, PUBLISHED, REJECTED, ERROR
- **Двухэтапная публикация:** Создание поста не триггерит уведомления, только публикация
- **SAGA Choreography:** Публикация проходит через цепочку асинхронных задач
- **Внутренние API-ключи:** Дополнительный уровень безопасности для service-to-service коммуникации
- **DLQ:** Обработка неудачных задач с компенсирующими действиями

## Troubleshooting

### Проблема: API-ключ не принимается

**Решение:** Убедитесь, что:
1. Ключ сгенерирован через `scripts/generate_api_keys.py`
2. Ключ добавлен в `.env` как `INTERNAL_API_KEY`
3. Воркеры перезапущены после изменения переменных окружения

### Проблема: Задачи не обрабатываются

**Решение:** Проверьте:
1. Redis запущен: `docker-compose ps redis`
2. Воркеры запущены: `docker-compose logs saga-worker`
3. Очередь настроена правильно: `docker-compose logs saga-worker | grep queue`

### Проблема: Пост застрял в PENDING_PUBLISH

**Решение:** Проверьте логи воркеров и DLQ. Если задача упала, она должна попасть в DLQ и выполнить компенсацию.

## Дополнительные ресурсы

- [SAGA Pattern](https://microservices.io/patterns/data/saga.html)
- [Celery Retries](https://docs.celeryproject.org/en/stable/userguide/tasks.html#retries)
- [Dead Letter Queue Pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/DeadLetterChannel.html)

