# Sharpener Observer

Интегрированная система для мониторинга Telegram каналов и автоматического запуска WebSocket потоков для торговых данных с Binance Futures.

## Компоненты

1. **run.py** - Главный файл для запуска всей системы
2. **ws_ohlcv_manager.py** - Менеджер WebSocket потоков для aggTrade данных и создания OHLCV баров
3. **ws_kline_book_manager.py** - Менеджер WebSocket потоков для kline и bookTicker данных
4. **telegram_observer.py** - Базовый наблюдатель Telegram (используется в run.py)
5. **config.py** - Конфигурация для Telegram API

## Установка

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Создайте файл `.env` с настройками Telegram:
```
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_PHONE=your_phone_number
CHANNEL_NAME=channel_name_or_username
```

## Запуск

### Основной режим (рекомендуется)
Запускает Telegram мониторинг + OHLCV WebSocket потоки:
```bash
python run.py
```

### Альтернативные режимы

Только OHLCV потоки (без Telegram):
```bash
python ws_ohlcv_manager.py
```

Только kline+bookTicker потоки (без Telegram):
```bash
python ws_kline_book_manager.py
```

Только Telegram мониторинг:
```bash
python telegram_observer.py
```

## Как это работает

1. **run.py** запускает Telegram клиент и слушает сообщения в указанном канале
2. Когда в сообщении найден токен в формате `` `TOKENUSDT` ``, он извлекается
3. Токен отправляется через ZMQ в **ws_ohlcv_manager.py**
4. **ws_ohlcv_manager.py** автоматически создает WebSocket подключение к Binance для этого токена
5. Система начинает получать aggTrade данные и создавать OHLCV бары каждую секунду
6. Потоки автоматически закрываются через 30 секунд после последнего упоминания токена

## Настройки

В файлах можно изменить:
- `TTL_SECONDS` - время жизни WebSocket потока после последнего упоминания
- `AGG_INTERVAL_SEC` - интервал создания OHLCV баров
- `ZMQ_SUB_URL` - URL для ZMQ коммуникации между компонентами
- `QUEUE_MAXSIZE` - размер очереди тиков

## Формат выходных данных

### OHLCV бары (ws_ohlcv_manager.py):
```
BTCUSDT 15:30:45 O:43250.5 H:43251.0 L:43249.8 C:43250.2 V:1.234 N:15
```

### Kline + Book данные (ws_kline_book_manager.py):
```json
{
  "t": 1697123445,
  "symbol": "btcusdt", 
  "kline_1s": {
    "t": 1697123445,
    "symbol": "btcusdt",
    "o": 43250.5,
    "h": 43251.0,
    "l": 43249.8,
    "c": 43250.2,
    "v": 1.234,
    "n": 15
  },
  "book": {
    "bid": 43250.1,
    "ask": 43250.3,
    "spread": 0.2,
    "mid": 43250.2,
    "imbalance": 0.05,
    "microprice": 43250.15
  }
}
```