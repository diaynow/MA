#!/usr/bin/env python3
"""
Простой тестовый скрипт для проверки API блог-платформы
"""

import requests
import json
import time

# BASE_URL = "http://localhost:8000"
BASE_URL = "https://msa-zfd1.onrender.com"

def test_api():
    print("🚀 Тестирование Blog Platform API")
    print("=" * 50)
    
    # 1. Проверка корневого endpoint
    print("\n1. Проверка корневого endpoint...")
    try:
        response = requests.get(f"{BASE_URL}/")
        print(f"✅ GET / - Status: {response.status_code}")
        print(f"   Response: {response.json()['message']}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    
    # 2. Регистрация пользователя
    print("\n2. Регистрация пользователя...")
    user_data = {
        "email": "testuser@example.com",
        "username": "testuser",
        "password": "password",
        "bio": "testbio"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/api/users", json=user_data)
        if response.status_code == 201:
            print("✅ Пользователь зарегистрирован успешно")
            user_info = response.json()['data']['user']
            print(f"   User ID: {user_info['id']}")
            print(f"   Username: {user_info['username']}")
        elif response.status_code == 409:
            print("⚠️ Пользователь уже существует (это нормально)")
        else:
            print(f"❌ Ошибка регистрации: {response.status_code}")
            print(f"   Response: {response.text}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    
    # 3. Логин пользователя
    print("\n3. Логин пользователя...")
    login_data = {
        "email": "testuser@example.com",
        "password": "password"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/api/users/login", json=login_data)
        if response.status_code == 200:
            print("✅ Логин успешен")
            login_info = response.json()['data']
            token = login_info['access_token']
            print(f"   Token: {token[:50]}...")
            
            # Сохраняем токен для дальнейшего использования
            headers = {"Authorization": f"Bearer {token}"}
            
            # 4. Получение информации о текущем пользователе
            print("\n4. Получение информации о текущем пользователе...")
            try:
                response = requests.get(f"{BASE_URL}/api/user", headers=headers)
                if response.status_code == 200:
                    print("✅ Информация о пользователе получена")
                    user_info = response.json()['data']['user']
                    print(f"   Username: {user_info['username']}")
                    print(f"   Email: {user_info['email']}")
                else:
                    print(f"❌ Ошибка получения пользователя: {response.status_code}")
                    print(f"   Response: {response.text}")
            except Exception as e:
                print(f"❌ Ошибка: {e}")
            
            # 5. Создание статьи
            print("\n5. Создание статьи...")
            article_data = {
                "title": "Моя первая статья",
                "description": "Краткое описание статьи",
                "body": "Полный текст статьи с подробным содержанием...",
                "tag_list": ["блог", "python", "fastapi"]
            }
            
            try:
                response = requests.post(f"{BASE_URL}/api/articles", json=article_data, headers=headers)
                if response.status_code == 201:
                    print("✅ Статья создана успешно")
                    article_info = response.json()['data']['article']
                    print(f"   Article ID: {article_info['id']}")
                    print(f"   Title: {article_info['title']}")
                    print(f"   Slug: {article_info['slug']}")
                    
                    # Сохраняем slug для дальнейшего использования
                    slug = article_info['slug']
                    
                    # 6. Получение всех статей
                    print("\n6. Получение всех статей...")
                    try:
                        response = requests.get(f"{BASE_URL}/api/articles")
                        if response.status_code == 200:
                            print("✅ Список статей получен")
                            articles_info = response.json()['data']
                            print(f"   Количество статей: {articles_info['count']}")
                        else:
                            print(f"❌ Ошибка получения статей: {response.status_code}")
                    except Exception as e:
                        print(f"❌ Ошибка: {e}")
                    
                    # 7. Получение статьи по slug
                    print(f"\n7. Получение статьи по slug: {slug}")
                    try:
                        response = requests.get(f"{BASE_URL}/api/articles/{slug}")
                        if response.status_code == 200:
                            print("✅ Статья получена по slug")
                            article_info = response.json()['data']['article']
                            print(f"   Title: {article_info['title']}")
                        else:
                            print(f"❌ Ошибка получения статьи: {response.status_code}")
                    except Exception as e:
                        print(f"❌ Ошибка: {e}")
                    
                    # 8. Обновление статьи
                    print(f"\n8. Обновление статьи: {slug}")
                    update_data = {
                        "title": "Обновленная статья",
                        "description": "Новое описание"
                    }
                    try:
                        response = requests.put(f"{BASE_URL}/api/articles/{slug}", json=update_data, headers=headers)
                        if response.status_code == 200:
                            print("✅ Статья обновлена")
                            article_info = response.json()['data']['article']
                            print(f"   New Title: {article_info['title']}")
                            # Обновляем slug после изменения статьи
                            slug = article_info['slug']
                            print(f"   New Slug: {slug}")
                        else:
                            print(f"❌ Ошибка обновления статьи: {response.status_code}")
                            print(f"   Response: {response.text}")
                    except Exception as e:
                        print(f"❌ Ошибка: {e}")
                    
                    # 9. Добавление комментария к статье
                    print(f"\n9. Добавление комментария к статье: {slug}")
                    comment_data = {
                        "body": "Отличная статья! Очень полезная информация."
                    }
                    try:
                        response = requests.post(f"{BASE_URL}/api/articles/{slug}/comments", json=comment_data, headers=headers)
                        if response.status_code == 200:
                            print("✅ Комментарий добавлен")
                            comment_info = response.json()['data']['comment']
                            comment_id = comment_info['id']
                            print(f"   Comment ID: {comment_id}")
                        else:
                            print(f"❌ Ошибка добавления комментария: {response.status_code}")
                            print(f"   Response: {response.text}")
                    except Exception as e:
                        print(f"❌ Ошибка: {e}")
                    
                    # 10. Получение комментариев к статье
                    print(f"\n10. Получение комментариев к статье: {slug}")
                    try:
                        response = requests.get(f"{BASE_URL}/api/articles/{slug}/comments")
                        if response.status_code == 200:
                            print("✅ Комментарии получены")
                            comments_info = response.json()['data']
                            print(f"   Количество комментариев: {comments_info['count']}")
                        else:
                            print(f"❌ Ошибка получения комментариев: {response.status_code}")
                    except Exception as e:
                        print(f"❌ Ошибка: {e}")
                    
                    # 11. Удаление комментария
                    if 'comment_id' in locals():
                        print(f"\n11. Удаление комментария: {comment_id}")
                        try:
                            response = requests.delete(f"{BASE_URL}/api/articles/{slug}/comments/{comment_id}", headers=headers)
                            if response.status_code == 200:
                                print("✅ Комментарий удален")
                            else:
                                print(f"❌ Ошибка удаления комментария: {response.status_code}")
                                print(f"   Response: {response.text}")
                        except Exception as e:
                            print(f"❌ Ошибка: {e}")
                    
                    # 12. Удаление статьи
                    print(f"\n12. Удаление статьи: {slug}")
                    try:
                        response = requests.delete(f"{BASE_URL}/api/articles/{slug}", headers=headers)
                        if response.status_code == 200:
                            print("✅ Статья удалена")
                        else:
                            print(f"❌ Ошибка удаления статьи: {response.status_code}")
                            print(f"   Response: {response.text}")
                    except Exception as e:
                        print(f"❌ Ошибка: {e}")
                        
                else:
                    print(f"❌ Ошибка создания статьи: {response.status_code}")
                    print(f"   Response: {response.text}")
            except Exception as e:
                print(f"❌ Ошибка: {e}")
                
        else:
            print(f"❌ Ошибка логина: {response.status_code}")
            print(f"   Response: {response.text}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    
    print("\n" + "=" * 50)
    print("🏁 Тестирование завершено!")

if __name__ == "__main__":
    test_api()
