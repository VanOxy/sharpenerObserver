#!/usr/bin/env python3
"""
Тестовый скрипт для проверки интеграции компонентов без реального Telegram подключения.
Эмулирует отправку токенов через ZMQ и проверяет работу StreamManager.
"""

import time
import threading
import zmq
from ws_ohlcv_manager import StreamManager


def test_zmq_integration():
    """Тест интеграции ZMQ с StreamManager"""
    print("🧪 Тест интеграции ZMQ с StreamManager")
    print("=" * 50)
    
    # Создаем StreamManager
    manager = StreamManager()
    
    # Запускаем в отдельном потоке
    def run_manager():
        try:
            manager.start()
            # Держим работающим 30 секунд
            time.sleep(30)
        finally:
            manager.stop()
    
    manager_thread = threading.Thread(target=run_manager, daemon=True)
    manager_thread.start()
    
    # Даем время на запуск
    time.sleep(2)
    
    # Создаем ZMQ Publisher для отправки тестовых токенов
    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    publisher.bind("tcp://127.0.0.1:5556")
    
    print("📡 ZMQ Publisher запущен")
    time.sleep(1)  # Даем время на подключение subscriber
    
    # Отправляем тестовые токены
    test_tokens = ["btcusdt", "ethusdt", "bnbusdt"]
    
    for i, token in enumerate(test_tokens):
        print(f"📤 Отправляем токен: {token.upper()}")
        publisher.send_string(f"symbol: {token}")
        time.sleep(5)  # Ждем 5 секунд между токенами
        
        # Через 10 секунд обновим первый токен
        if i == 0:
            time.sleep(5)
            print(f"🔄 Обновляем токен: {token.upper()}")
            publisher.send_string(f"symbol: {token}")
    
    print("\n⏳ Ожидаем 15 секунд для наблюдения за работой системы...")
    time.sleep(15)
    
    print("\n🏁 Завершение теста")
    publisher.close()
    context.term()
    
    # Ждем завершения manager
    manager_thread.join(timeout=5)
    
    print("✅ Тест завершен")


def test_token_extraction():
    """Тест извлечения токенов из сообщений"""
    print("\n🧪 Тест извлечения токенов")
    print("=" * 30)
    
    # Импортируем функцию из run.py
    from run import extract_token_from_message
    
    test_messages = [
        "`BTCUSDT` - хороший токен",
        "Покупаем `ETHUSDT` сейчас!",
        "Нет токенов в этом сообщении",
        "`BNBUSDT` и `ADAUSDT` - два токена",
        "`INVALID` - неправильный формат",
        "`SOLUSDT` готов к росту"
    ]
    
    for msg in test_messages:
        token = extract_token_from_message(msg)
        print(f"Сообщение: {msg}")
        print(f"Токен: {token.upper() if token else 'Не найден'}")
        print("-" * 30)


if __name__ == "__main__":
    print("🚀 Запуск интеграционных тестов")
    print("=" * 60)
    
    try:
        # Тест извлечения токенов
        test_token_extraction()
        
        # Тест ZMQ интеграции (требует установленные пакеты)
        print("\n" + "=" * 60)
        test_zmq_integration()
        
    except ImportError as e:
        print(f"❌ Ошибка импорта: {e}")
        print("💡 Убедитесь, что установлены зависимости: pip install -r requirements.txt")
    except Exception as e:
        print(f"❌ Ошибка теста: {e}")
    
    print("\n👋 Тесты завершены")
