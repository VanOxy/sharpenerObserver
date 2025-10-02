# 🚀 Быстрый старт

## 1. Установка зависимостей
```bash
pip install -r requirements.txt
```

## 2. Настройка Telegram API
Создайте файл `.env` в корне проекта:
```env
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash  
TELEGRAM_PHONE=+1234567890
CHANNEL_NAME=your_channel_name
```

### Как получить API данные:
1. Идите на https://my.telegram.org/apps
2. Создайте новое приложение
3. Скопируйте `api_id` и `api_hash`

## 3. Запуск

### Полная система (Telegram + WebSocket):
```bash
python run.py
```

### Только тест интеграции:
```bash
python test_integration.py
```

### Только OHLCV потоки:
```bash
python ws_ohlcv_manager.py
```

## 4. Что происходит

1. **run.py** подключается к Telegram и слушает канал
2. При обнаружении токенов в формате `` `BTCUSDT` `` передает их напрямую в StreamManager
3. **StreamManager** запускает WebSocket потоки к Binance для каждого токена
4. Каждую секунду выводятся OHLCV бары:
   ```
   🚀 Starting stream for BTCUSDT
   BTCUSDT 15:30:45 O:43250.5 H:43251.0 L:43249.8 C:43250.2 V:1.234 N:15
   ⏹️ Stopped BTCUSDT (TTL expired)
   ```

### Тесты для диагностики:

#### Тест только OHLCV менеджера:
```bash
python test_ohlcv_only.py
```

#### Тест интеграции (без Telegram):
```bash
python test_direct_integration.py
```

## 5. Остановка
Нажмите `Ctrl+C` для остановки любого скрипта.

## 🔧 Настройки

В `ws_ohlcv_manager.py`:
- `TTL_SECONDS = 30` - время жизни потока (секунды)
- `AGG_INTERVAL_SEC = 1` - интервал OHLCV баров (секунды)

В `run.py`:
- Поддерживает автоматическое переподключение  
- Минимум логов для чистого вывода
