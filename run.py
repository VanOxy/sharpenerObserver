#!/usr/bin/env python3
"""
Главный файл для запуска интегрированной системы:
- Telegram Observer для извлечения токенов из сообщений
- ZMQ Publisher для отправки токенов в ws_ohlcv_manager
- StreamManager для управления WebSocket потоками и создания OHLCV баров
"""

import asyncio
import re
import threading
import time
from telethon import TelegramClient, events
from config import Config
from ws_ohlcv_manager import StreamManager


def extract_token_from_message(text):
    """Извлекает токен из сообщения в формате `TOKENUSDT`"""
    if not text:
        return None
    match = re.search(r'`([A-Z0-9]+)`', text)
    return match.group(1).lower() if match else None


class TelegramStreamRunner:
    """
    Интегрированная система для:
    1. Прослушивания Telegram канала
    2. Извлечения токенов из сообщений
    3. Отправки токенов в StreamManager через ZMQ
    4. Управления WebSocket потоками для OHLCV данных
    """
    
    def __init__(self):
        Config.validate()
        
        # Telegram клиент
        self.client = TelegramClient(
            'observer_session',
            Config.API_ID,
            Config.API_HASH
        )
        
        # StreamManager для WebSocket потоков
        self.stream_manager = StreamManager()
        
        # Флаг остановки
        self.stop_event = threading.Event()
        
        print(f"🔧 Создан интегрированный наблюдатель для канала: {Config.CHANNEL_NAME}")

    async def find_channel(self, channel_name):
        """Поиск канала по имени"""
        try:
            if not channel_name.startswith('@'):
                channel_name = '@' + channel_name.replace(' ', '_').lower()
            
            entity = await self.client.get_entity(channel_name)
            return entity
            
        except Exception:
            # Если не нашли по username, ищем среди диалогов
            print(f"🔍 Ищем канал '{channel_name}' среди доступных каналов...")
            
            async for dialog in self.client.iter_dialogs():
                if hasattr(dialog.entity, 'title'):
                    title = dialog.entity.title.lower()
                    search_name = channel_name.replace('@', '').lower()
                    
                    if search_name in title or title in search_name:
                        print(f"✅ Найден канал: '{dialog.entity.title}' (ID: {dialog.entity.id})")
                        return dialog.entity
            
            # Если все еще не нашли, выводим список доступных каналов
            print("\n📋 Доступные каналы и группы:")
            print("-" * 50)
            
            async for dialog in self.client.iter_dialogs():
                if hasattr(dialog.entity, 'title'):
                    entity_type = "Канал" if hasattr(dialog.entity, 'broadcast') and dialog.entity.broadcast else "Группа"
                    print(f"{entity_type}: {dialog.entity.title}")
                    if hasattr(dialog.entity, 'username') and dialog.entity.username:
                        print(f"  Username: @{dialog.entity.username}")
                    print(f"  ID: {dialog.entity.id}")
                    print()
            
            raise Exception(f"Канал '{channel_name}' не найден")


    def start_stream_manager(self):
        """Запуск StreamManager в отдельном потоке"""
        def stream_manager_thread():
            try:
                print("🚀 Запуск StreamManager...")
                self.stream_manager.start()
                
                # Держим StreamManager работающим
                while not self.stop_event.is_set():
                    time.sleep(0.1)
                    
            except Exception as e:
                print(f"❌ Ошибка StreamManager: {e}")
            finally:
                self.stream_manager.stop()
        
        thread = threading.Thread(target=stream_manager_thread, daemon=True, name="StreamManager")
        thread.start()
        return thread

    def send_token_to_stream_manager(self, token):
        """Отправка токена напрямую в StreamManager"""
        try:
            self.stream_manager.touch(token)
        except Exception as e:
            print(f"❌ Ошибка отправки токена {token}: {e}")

    async def listen_for_messages(self):
        """Основной метод для прослушивания Telegram сообщений"""
        try:
            print("🔌 Подключаемся к Telegram...")
            await self.client.start(phone=Config.PHONE_NUMBER)
            print("✅ Подключение к Telegram успешно!")
            
            # Ищем канал
            entity = await self.find_channel(Config.CHANNEL_NAME)
            print(f"✅ Найден канал: {entity.title}")
            
            # Запускаем StreamManager
            stream_thread = self.start_stream_manager()
            
            # Даем время на инициализацию
            await asyncio.sleep(2)
            
            print("👂 Слушаем новые сообщения... (Ctrl+C для остановки)")
            print("=" * 60)
            
            # Обработчик новых сообщений
            @self.client.on(events.NewMessage(chats=entity))
            async def handle_new_message(event):
                message = event.message
                if message.text:
                    token = extract_token_from_message(message.text)
                    if token:
                        print(f"🎯 Найден токен в сообщении: {token.upper()}")
                        # Отправляем токен напрямую в StreamManager
                        self.send_token_to_stream_manager(token)
                    else:
                        print(f"📝 Сообщение без токенов: {message.text[:100]}...")
            
            # Запускаем прослушивание
            await self.client.run_until_disconnected()
            
        except KeyboardInterrupt:
            print("\n⏹️ Остановка мониторинга...")
        except Exception as e:
            print(f"❌ Ошибка: {e}")
        finally:
            # Остановка всех компонентов
            self.stop_event.set()
            
            try:
                await self.client.disconnect()
                print("🔌 Отключились от Telegram")
            except:
                pass
            
            
            print("🏁 Все компоненты остановлены")


async def main():
    runner = TelegramStreamRunner()
    await runner.listen_for_messages()


if __name__ == "__main__":
    print("🚀 Запуск интегрированной системы Telegram Observer + OHLCV Stream Manager")
    print("=" * 60)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Выход...")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
