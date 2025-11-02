#!/usr/bin/env python3
"""
Скрипт для инициализации базы данных на Render
"""

import os
import sys
import subprocess
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# Добавляем src в путь
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.config import settings

def wait_for_db(max_retries=30, delay=2):
    """Ожидание доступности базы данных"""
    print("Ожидание доступности базы данных...")
    
    for attempt in range(max_retries):
        try:
            engine = create_engine(settings.database_url)
            with engine.connect() as conn:
                print("✅ База данных доступна!")
                return True
        except OperationalError as e:
            print(f"Попытка {attempt + 1}/{max_retries}: База данных недоступна")
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                print(f"❌ Не удалось подключиться к базе данных: {e}")
                return False
    
    return False

def run_migrations():
    """Запуск миграций Alembic"""
    print("Запуск миграций...")
    
    try:
        # Запускаем миграции
        result = subprocess.run(
            ["alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            check=True
        )
        print("✅ Миграции выполнены успешно!")
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка при выполнении миграций: {e}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return False

def main():
    """Основная функция"""
    print("🚀 Инициализация базы данных для Render...")
    
    # Проверяем переменные окружения
    if not settings.database_url:
        print("❌ DATABASE_URL не установлен!")
        return False
    
    print(f"📊 DATABASE_URL: {settings.database_url}")
    
    # Ждем доступности базы данных
    if not wait_for_db():
        return False
    
    # Запускаем миграции
    if not run_migrations():
        return False
    
    print("🎉 Инициализация базы данных завершена успешно!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
