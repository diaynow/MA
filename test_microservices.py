#!/usr/bin/env python3
"""
Скрипт для проверки работоспособности микросервисной архитектуры
Проверяет работу users-api, backend и API Gateway
"""

import requests
import json
import time
import sys
from typing import Optional, Dict, Any

BASE_URL = "http://localhost"
TIMEOUT = 10

# Цвета для вывода
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def print_success(message: str):
    print(f"{Colors.GREEN}✅ {message}{Colors.RESET}")

def print_error(message: str):
    print(f"{Colors.RED}❌ {message}{Colors.RESET}")

def print_warning(message: str):
    print(f"{Colors.YELLOW}⚠️  {message}{Colors.RESET}")

def print_info(message: str):
    print(f"{Colors.BLUE}ℹ️  {message}{Colors.RESET}")

def print_header(message: str):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{message}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}\n")

def test_endpoint(method: str, url: str, expected_status: int = 200, 
                 headers: Optional[Dict] = None, json_data: Optional[Dict] = None,
                 description: str = "") -> Optional[Dict]:
    """Универсальная функция для тестирования эндпоинтов"""
    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, timeout=TIMEOUT)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, json=json_data, timeout=TIMEOUT)
        elif method.upper() == "PUT":
            response = requests.put(url, headers=headers, json=json_data, timeout=TIMEOUT)
        elif method.upper() == "DELETE":
            response = requests.delete(url, headers=headers, timeout=TIMEOUT)
        else:
            print_error(f"Неподдерживаемый метод: {method}")
            return None
        
        if response.status_code == expected_status:
            print_success(f"{description or f'{method} {url}'} - Status: {response.status_code}")
            try:
                return response.json()
            except:
                return {"text": response.text}
        else:
            print_error(f"{description or f'{method} {url}'} - Status: {response.status_code} (ожидался {expected_status})")
            print_info(f"Response: {response.text[:200]}")
            return None
    except requests.exceptions.ConnectionError:
        print_error(f"Не удалось подключиться к {url}")
        print_info("Убедитесь, что все сервисы запущены: docker-compose up")
        return None
    except requests.exceptions.Timeout:
        print_error(f"Таймаут при запросе к {url}")
        return None
    except Exception as e:
        print_error(f"Ошибка: {e}")
        return None

def test_microservices():
    """Основная функция тестирования микросервисов"""
    print_header("🚀 Тестирование микросервисной архитектуры")
    
    results = {
        "users_api": False,
        "backend": False,
        "api_gateway": False,
        "authentication": False,
        "data_ownership": False,
        "full_flow": False
    }
    
    # ========== 1. Проверка API Gateway и маршрутизации ==========
    print_header("1. Проверка API Gateway и маршрутизации")
    
    # Проверка users-api через Gateway
    print_info("Проверка маршрутизации /api/users → users-api")
    users_response = test_endpoint("GET", f"{BASE_URL}/api/users", 
                                   description="GET /api/users (должен идти в users-api)")
    if users_response and "users-api" in str(users_response).lower() or "Users API" in str(users_response):
        results["users_api"] = True
        print_success("✅ Маршрутизация в users-api работает корректно")
    else:
        print_warning("⚠️  Не удалось подтвердить, что запрос идет в users-api")
    
    # Проверка backend через Gateway
    print_info("Проверка маршрутизации /api/articles → backend")
    articles_response = test_endpoint("GET", f"{BASE_URL}/api/articles",
                                     description="GET /api/articles (должен идти в backend)")
    if articles_response:
        results["backend"] = True
        print_success("✅ Маршрутизация в backend работает корректно")
    
    # Проверка health checks
    print_info("Проверка health checks")
    health_response = test_endpoint("GET", f"{BASE_URL}/health", description="Health check через Gateway")
    
    # Если оба запроса прошли успешно, API Gateway работает
    if users_response and articles_response:
        results["api_gateway"] = True
        print_success("✅ API Gateway работает корректно (маршрутизация в оба сервиса успешна)")
    
    # ========== 2. Проверка Users API ==========
    print_header("2. Проверка Users API (users-api)")
    
    # Генерация уникального email для теста
    timestamp = int(time.time())
    test_email = f"test{timestamp}@example.com"
    test_username = f"testuser{timestamp}"
    test_password = "testpassword123"
    
    # Регистрация пользователя
    print_info(f"Регистрация пользователя: {test_email}")
    register_data = {
        "email": test_email,
        "username": test_username,
        "password": test_password,
        "bio": "Test user for microservices check"
    }
    register_response = test_endpoint("POST", f"{BASE_URL}/api/users", 
                                     expected_status=201,
                                     json_data=register_data,
                                     description="POST /api/users (регистрация)")
    
    if not register_response:
        # Попробуем с другим email, если пользователь уже существует
        test_email = f"test{timestamp}2@example.com"
        test_username = f"testuser{timestamp}2"
        register_data = {
            "email": test_email,
            "username": test_username,
            "password": test_password,
            "bio": "Test user for microservices check"
        }
        register_response = test_endpoint("POST", f"{BASE_URL}/api/users", 
                                         expected_status=201,
                                         json_data=register_data,
                                         description="POST /api/users (повторная попытка)")
    
    user_id = None
    if register_response and "data" in register_response:
        user_data = register_response["data"].get("user", {})
        user_id = user_data.get("id")
        if user_id:
            print_success(f"Пользователь создан с ID: {user_id}")
    
    # Вход пользователя
    print_info(f"Вход пользователя: {test_email}")
    login_data = {
        "email": test_email,
        "password": test_password
    }
    login_response = test_endpoint("POST", f"{BASE_URL}/api/users/login",
                                  json_data=login_data,
                                  description="POST /api/users/login")
    
    token = None
    if login_response and "data" in login_response:
        token = login_response["data"].get("access_token")
        if token:
            print_success(f"Токен получен: {token[:30]}...")
            results["authentication"] = True
        else:
            print_error("Токен не найден в ответе")
    else:
        print_error("Не удалось войти. Проверьте, что пользователь был создан.")
        return results
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Получение информации о текущем пользователе
    print_info("Получение информации о текущем пользователе")
    user_info_response = test_endpoint("GET", f"{BASE_URL}/api/user",
                                      headers=headers,
                                      description="GET /api/user")
    
    # ========== 3. Проверка Backend (Articles API) ==========
    print_header("3. Проверка Backend (Articles API)")
    
    # Создание статьи
    print_info("Создание статьи через backend")
    article_data = {
        "title": f"Test Article {timestamp}",
        "description": "Test article for microservices verification",
        "body": "This is a test article body to verify that backend service works correctly with JWT authentication.",
        "tag_list": ["test", "microservices", "verification"]
    }
    article_response = test_endpoint("POST", f"{BASE_URL}/api/articles",
                                    expected_status=201,
                                    headers=headers,
                                    json_data=article_data,
                                    description="POST /api/articles (создание статьи)")
    
    article_slug = None
    article_author_id = None
    
    if article_response and "data" in article_response:
        article_info = article_response["data"].get("article", {})
        article_slug = article_info.get("slug")
        article_author_id = article_info.get("author_id")
        
        if article_slug:
            print_success(f"Статья создана со slug: {article_slug}")
        if article_author_id:
            print_success(f"Статья имеет author_id: {article_author_id}")
            # Проверяем, что author_id совпадает с user_id из токена
            if user_id and str(article_author_id) == str(user_id):
                print_success("✅ author_id совпадает с user_id из токена")
                results["data_ownership"] = True
            else:
                print_warning(f"⚠️  author_id ({article_author_id}) не совпадает с user_id ({user_id})")
    
    # Получение всех статей
    print_info("Получение списка всех статей")
    articles_list_response = test_endpoint("GET", f"{BASE_URL}/api/articles",
                                          description="GET /api/articles (список статей)")
    
    # Получение статьи по slug
    if article_slug:
        print_info(f"Получение статьи по slug: {article_slug}")
        article_get_response = test_endpoint("GET", f"{BASE_URL}/api/articles/{article_slug}",
                                            description=f"GET /api/articles/{article_slug}")
    
    # ========== 4. Проверка полного цикла работы ==========
    print_header("4. Проверка полного цикла работы")
    
    if token and article_slug:
        # Добавление комментария
        print_info("Добавление комментария к статье")
        comment_data = {
            "body": "This is a test comment to verify the full workflow."
        }
        comment_response = test_endpoint("POST", f"{BASE_URL}/api/articles/{article_slug}/comments",
                                        headers=headers,
                                        json_data=comment_data,
                                        description="POST /api/articles/{slug}/comments")
        
        comment_id = None
        if comment_response and "data" in comment_response:
            comment_info = comment_response["data"].get("comment", {})
            comment_id = comment_info.get("id")
            if comment_id:
                print_success(f"Комментарий создан с ID: {comment_id}")
        
        # Получение комментариев
        print_info("Получение комментариев к статье")
        comments_list_response = test_endpoint("GET", f"{BASE_URL}/api/articles/{article_slug}/comments",
                                              description="GET /api/articles/{slug}/comments")
        
        # Удаление комментария
        if comment_id:
            print_info("Удаление комментария")
            test_endpoint("DELETE", f"{BASE_URL}/api/articles/{article_slug}/comments/{comment_id}",
                         headers=headers,
                         description="DELETE /api/articles/{slug}/comments/{id}")
        
        # Удаление статьи
        print_info("Удаление статьи")
        delete_response = test_endpoint("DELETE", f"{BASE_URL}/api/articles/{article_slug}",
                                       headers=headers,
                                       description="DELETE /api/articles/{slug}")
        
        if delete_response:
            results["full_flow"] = True
    
    # ========== 5. Итоговая сводка ==========
    print_header("5. Итоговая сводка проверок")
    
    total_checks = len(results)
    passed_checks = sum(1 for v in results.values() if v)
    
    print(f"\n{Colors.BOLD}Результаты проверки:{Colors.RESET}\n")
    for check, passed in results.items():
        status = "✅ ПРОЙДЕН" if passed else "❌ НЕ ПРОЙДЕН"
        color = Colors.GREEN if passed else Colors.RED
        print(f"  {color}{status}{Colors.RESET} - {check.replace('_', ' ').title()}")
    
    print(f"\n{Colors.BOLD}Итого: {passed_checks}/{total_checks} проверок пройдено{Colors.RESET}\n")
    
    if passed_checks == total_checks:
        print_success("🎉 Все проверки пройдены! Микросервисная архитектура работает корректно!")
        return 0
    else:
        print_warning("⚠️  Некоторые проверки не пройдены. Проверьте логи выше.")
        return 1

if __name__ == "__main__":
    try:
        exit_code = test_microservices()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print_error("\n\nТестирование прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print_error(f"\n\nКритическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

