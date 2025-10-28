# =============================
# file: telegram_observer.py
# =============================
import threading
import asyncio
import re
from typing import Callable, Optional

from telethon import TelegramClient, events
from config import Config


def extract_token_from_TGmessage(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    m = re.search(r'`([A-Z][A-Z0-9]*)`', text)
    token = m.group(1) if m else None
    return token


class TelegramObserver:
    """
    Вызывает on_token(symbol) на каждом подходящем сообщении.
    """
    def __init__(self) -> None:
        Config.validate()

        self._api_id = Config.API_ID
        self._api_hash = Config.API_HASH
        self._channel = Config.CHANNEL_NAME
        self._phone = Config.TELEGRAM_PHONE
        self._session = "tg_session"
        self._target_chat = None

        self._client: Optional[TelegramClient] = None
        self._on_token: Optional[Callable[[str], None]] = None

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # === lifecycle ===
    def start(self, on_token: Callable[[str], None]) -> None:
        self._on_token = on_token
        self._thread = threading.Thread(target=self._thread_main, name="TGTelethon", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    # === thread & async loop ===
    def _thread_main(self) -> None:
        try:
            asyncio.run(self._async_main())
        except Exception as e:
            print(f"[TG] telethon loop error: {e}")

    async def _resolve_channel(self, client: TelegramClient):
        """Резолвим канал один раз и возвращаем его chat_id (или None)."""
        if not self._channel:
            return None
        target = self._channel.strip()
        uname = target.lstrip('@').replace(' ', '_').lower()

        try:
            entity = await client.get_entity('@' + uname)
            if entity:
                print(f"[TG] target resolved by username: @{uname})")
                return entity
        except Exception:
            pass

    async def _async_main(self) -> None:
        client = TelegramClient(self._session, self._api_id, self._api_hash)
        self._client = client

        print("[TG] connecting…")
        await client.start(phone=self._phone)

        print("[TG] connected. Resolving channel…")
        self._target_chat = await self._resolve_channel(client)
        if self._target_chat:
            print(f"[TG] will filter by chat={self._target_chat.username}")
        else:
            print("No channel with a such name found")
            return
        
        print("[TG] Listening … (Ctrl+C to stop)")
        @client.on(events.NewMessage(chats=self._target_chat))
        async def handler(event):
            try:
                message = event.message
                if message.text:
                    token = extract_token_from_TGmessage(message.text)
                    if token and self._on_token:
                        print(f"[TG message] -> {token}")
                        self._on_token(token)
            except Exception as e:
                print(f"[TG] handler error: {e}")

        try:
            while not self._stop.is_set():
                await asyncio.sleep(0.5)
        finally:
            await client.disconnect()
            print("[TG] disconnected")
