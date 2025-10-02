#!/usr/bin/env python3

import asyncio
import re
from telethon import TelegramClient, events
from config import Config

def extract_token_from_message(text):
    if not text:
        return ''
    match = re.search(r'`([A-Z][A-Z0-9]*)`', text)
    return match.group(1) if match else None

class TelegramObserver:
    
    def __init__(self):
        Config.validate()
        
        self.client = TelegramClient(
            'observer_session',
            Config.API_ID,
            Config.API_HASH
        )
        
        print(f"🔧 Создан наблюдатель для канала: {Config.CHANNEL_NAME}")
    
    async def find_channel(self, channel_name):
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
    
    async def listen_for_messages(self):
        try:
            print("🔌 Подключаемся к Telegram...")
            await self.client.start(phone=Config.PHONE_NUMBER)
            print("✅ Подключение успешно!")
            
            # Ищем канал
            entity = await self.find_channel(Config.CHANNEL_NAME)
            print(f"✅ Найден канал: {entity.title}")
            
            # Сохраняем entity для использования в обработчике
            self.target_entity = entity
            
            print("👂 Слушаем новые сообщения... (Ctrl+C для остановки)")
            print("=" * 50)
            
            # Обработчик новых сообщений
            @self.client.on(events.NewMessage(chats=entity))
            async def handle_new_message(event):
                message = event.message
                if message.text:
                    token = extract_token_from_message(message.text)
                    if token:
                        print(token)
                    else:
                        print("Токены не найдены")
            
            # Запускаем прослушивание
            await self.client.run_until_disconnected()
            
        except KeyboardInterrupt:
            print("\n⏹️ Остановка мониторинга...")
        except Exception as e:
            print(f"❌ Ошибка: {e}")
        finally:
            await self.client.disconnect()
            print("🔌 Отключились от Telegram")

async def main():
    observer = TelegramObserver()
    await observer.listen_for_messages()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Выход...")
