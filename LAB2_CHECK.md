# 🔍 Проверка лабораторной работы 2

## Шаг 1: Запуск всех сервисов

```bash
docker-compose up --build
```

Дождитесь, пока все сервисы запустятся. Вы должны увидеть сообщения о готовности всех контейнеров.

## Шаг 2: Проверка работы сервисов

### 2.1 Проверка, что `/api/users` обслуживает `users-api`

```bash
# Проверка через API Gateway
curl http://localhost/api/users

# Должен вернуть информацию о Users API
# Пример ответа:
# {"success":true,"message":"Users API is running","service":"users-api",...}
```

**Если получаете "Method Not Allowed":**
1. Убедитесь, что сервисы перезапущены после изменений:
```bash
docker-compose restart users-api api-gateway
```

2. Или пересоберите контейнеры:
```bash
docker-compose up --build -d users-api
```

**Альтернативный способ (через браузер):**
Откройте в браузере: `http://localhost/api/users`

**Проверка, что это действительно users-api:**
```bash
# Проверка health check users-api напрямую (внутренний порт)
docker-compose exec api-gateway curl http://users-api:8000/health

# Проверка через API Gateway
curl http://localhost/api/users
```

### 2.2 Проверка, что `/api/articles` обслуживает `backend`

**Примечание**: В задании указано `/api/posts`, но в нашем проекте используется `/api/articles`.

```bash
# Проверка через API Gateway
curl http://localhost/api/articles

# Должен вернуть список статей (может быть пустым)
# Пример ответа:
# {"success":true,"message":"Articles retrieved successfully","data":{"articles":[],"count":0}}
```

**Альтернативный способ (через браузер):**
Откройте в браузере: `http://localhost/api/articles`

**Проверка, что это действительно backend:**
```bash
# Проверка health check backend напрямую (внутренний порт)
docker-compose exec api-gateway curl http://backend:8000/health

# Проверка через API Gateway
curl http://localhost/api/articles
```

### 2.3 Проверка, что у articles есть поле `user_id` без FK

#### Способ 1: Через API (создание статьи)

```bash
# 1. Сначала зарегистрируйте пользователя
curl -X POST http://localhost/api/users \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "username": "testuser",
    "password": "password123"
  }'

# 2. Войдите и получите токен
curl -X POST http://localhost/api/users/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "password": "password123"
  }'

# Сохраните access_token из ответа, например: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

# 3. Создайте статью (замените YOUR_TOKEN на реальный токен)
curl -X POST http://localhost/api/articles \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "title": "Test Article",
    "description": "Test Description",
    "body": "Test Body Content"
  }'

# 4. Получите созданную статью
curl http://localhost/api/articles

# В ответе вы увидите поле "author_id" (это user_id)
```

#### Способ 2: Прямая проверка в базе данных

```bash
# Подключитесь к базе данных backend
docker-compose exec db-main psql -U app -d app_main

# В psql выполните:
\d articles

# Вы должны увидеть:
# Column     | Type                        | Nullable
# -----------+-----------------------------+----------
# id         | uuid                        | not null
# title      | character varying(200)      | not null
# ...
# author_id  | uuid                        | not null  <-- БЕЗ FK!
# ...

# Проверьте, что нет FK constraint:
SELECT 
    conname as constraint_name,
    conrelid::regclass as table_name
FROM pg_constraint
WHERE conrelid = 'articles'::regclass
  AND contype = 'f'
  AND confrelid = 'users'::regclass;

# Должен вернуть 0 строк (нет FK на users)

# Проверьте данные:
SELECT id, title, author_id FROM articles LIMIT 5;

# Вы увидите, что author_id содержит UUID, но без FK constraint
```

#### Способ 3: Проверка через миграции

```bash
# Проверьте последнюю миграцию
cat alembic/versions/004_remove_users_and_fk.py

# В функции upgrade() должно быть:
# op.drop_constraint('fk_articles_author_id', 'articles', type_='foreignkey')
# op.drop_table('users')
```

### 2.4 Проверка, что оба сервиса используют свои отдельные базы данных

#### Проверка 1: Подключение к базам данных

```bash
# Проверка базы данных backend (db-main)
docker-compose exec db-main psql -U app -d app_main -c "\dt"

# Должны увидеть таблицы:
# articles
# comments
# НЕТ таблицы users!

# Проверка базы данных users-api (db-users)
docker-compose exec db-users psql -U app -d app_users -c "\dt"

# Должны увидеть таблицы:
# users
# НЕТ таблиц articles и comments!
```

#### Проверка 2: Проверка данных в разных БД

```bash
# Проверка пользователей в БД users-api
docker-compose exec db-users psql -U app -d app_users -c "SELECT id, email, username FROM users LIMIT 5;"

# Проверка статей в БД backend
docker-compose exec db-main psql -U app -d app_main -c "SELECT id, title, author_id FROM articles LIMIT 5;"

# Обратите внимание: author_id в articles содержит UUID из таблицы users,
# но это просто значение, не FK constraint
```

#### Проверка 3: Проверка переменных окружения

```bash
# Проверка DATABASE_URL в backend
docker-compose exec backend env | grep DATABASE_URL
# Должно быть: DATABASE_URL=postgresql://app:app@db-main:5432/app_main

# Проверка DATABASE_URL в users-api
docker-compose exec users-api env | grep DATABASE_URL
# Должно быть: DATABASE_URL=postgresql://app:app@db-users:5432/app_users
```

## Шаг 3: Полный тест работы системы

### Создание полного сценария использования

```bash
# 1. Регистрация пользователя (users-api)
curl -X POST http://localhost/api/users \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "username": "testuser",
    "password": "password123"
  }'

# 2. Вход (users-api)
TOKEN=$(curl -s -X POST http://localhost/api/users/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "password": "password123"
  }' | jq -r '.data.access_token')

echo "Token: $TOKEN"

# 3. Создание статьи (backend)
curl -X POST http://localhost/api/articles \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "title": "My First Article",
    "description": "This is a test article",
    "body": "This is the body of my first article"
  }'

# 4. Получение статей (backend)
curl http://localhost/api/articles

# 5. Получение информации о текущем пользователе (users-api)
curl -H "Authorization: Bearer $TOKEN" http://localhost/api/user
```

## Шаг 4: Проверка маршрутизации Nginx

### Проверка логов API Gateway

```bash
# Просмотр логов Nginx
docker-compose logs api-gateway

# Вы должны увидеть запросы к разным сервисам
```

### Проверка конфигурации Nginx

```bash
# Проверка конфигурации
docker-compose exec api-gateway cat /etc/nginx/nginx.conf

# Должны увидеть:
# location /api/users { proxy_pass http://users_service; }
# location / { proxy_pass http://backend_service; }
```

### Тест маршрутизации

```bash
# Запрос к users-api через Gateway
curl -v http://localhost/api/users 2>&1 | grep -i "location\|host"

# Запрос к backend через Gateway
curl -v http://localhost/api/articles 2>&1 | grep -i "location\|host"
```

## Шаг 5: Проверка health checks

```bash
# Health check всех сервисов
curl http://localhost/health
curl http://localhost/api/users  # Должен показать users-api
curl http://localhost/api/articles  # Должен показать backend

# Health check напрямую (внутренние порты)
docker-compose exec api-gateway curl http://backend:8000/health
docker-compose exec api-gateway curl http://users-api:8000/health
```

## ✅ Чеклист проверки

- [ ] Все сервисы запущены (`docker-compose ps`)
- [ ] `/api/users` возвращает ответ от users-api
- [ ] `/api/articles` возвращает ответ от backend
- [ ] В таблице `articles` есть поле `author_id` БЕЗ FK constraint
- [ ] Таблица `users` находится в БД `app_users` (users-api)
- [ ] Таблицы `articles` и `comments` находятся в БД `app_main` (backend)
- [ ] API Gateway корректно маршрутизирует запросы
- [ ] JWT токен валидируется в обоих сервисах
- [ ] Можно создать пользователя через users-api
- [ ] Можно создать статью через backend с JWT токеном

## 🐛 Если что-то не работает

### Проблема: Сервисы не запускаются

```bash
# Проверьте логи
docker-compose logs

# Проверьте статус контейнеров
docker-compose ps

# Перезапустите сервисы
docker-compose down
docker-compose up --build
```

### Проблема: База данных не подключается

```bash
# Проверьте health checks БД
docker-compose ps

# Проверьте логи БД
docker-compose logs db-main
docker-compose logs db-users

# Проверьте подключение
docker-compose exec db-main pg_isready -U app -d app_main
docker-compose exec db-users pg_isready -U app -d app_users
```

### Проблема: JWT токен не работает

```bash
# Проверьте SECRET_KEY в обоих сервисах
docker-compose exec backend env | grep SECRET_KEY
docker-compose exec users-api env | grep SECRET_KEY

# Они должны быть одинаковыми!
```

## 📊 Ожидаемые результаты

После всех проверок вы должны убедиться, что:

1. ✅ **Микросервисная архитектура работает**: два независимых сервиса с отдельными БД
2. ✅ **API Gateway маршрутизирует запросы**: Nginx правильно направляет трафик
3. ✅ **Data ownership соблюдается**: нет FK между сервисами, только логические ссылки через UUID
4. ✅ **JWT аутентификация работает**: токены валидируются без обращения к БД пользователей
5. ✅ **Система полностью функциональна**: можно создавать пользователей, статьи, комментарии

