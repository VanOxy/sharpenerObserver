# ws_probe.py  (Python 3.10+)
import asyncio, json, sys, signal
import websockets

#URL = sys.argv[1] if len(sys.argv) > 1 else "wss://fstream.binance.com/ws/btcusdt@kline_1m"
# URL = "wss://fstream.binance.com/ws/btcusdt_perpetual@continuousKline_1s"
#URL = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
URL = "wss://fstream.binance.com/ws/ethusdt@depth@100ms"
stop_event = asyncio.Event()

def _handle_signal(*_):
    # вызовется на Ctrl+C (SIGINT) и SIGTERM
    if not stop_event.is_set():
        stop_event.set()

async def reader(ws: websockets.WebSocketClientProtocol):
    # основной цикл чтения сообщений
    while not stop_event.is_set():
        try:
            msg = await asyncio.wait_for(ws.recv(), timeout=60)
        except asyncio.TimeoutError:
            # просто поддерживаем цикл живым, чтобы проверять stop_event
            continue
        except websockets.ConnectionClosedOK:
            break
        except websockets.ConnectionClosedError:
            break

        # печатаем "как есть" JSON (или строку)
        try:
            obj = json.loads(msg)
            print(json.dumps(obj, ensure_ascii=False))
        except Exception:
            print(msg)

async def main():
    print(f"[connect] {URL}")
    # ping_interval/timeout чтобы соединение не «застыло»
    async with websockets.connect(
        URL, ping_interval=20, ping_timeout=20, close_timeout=5
    ) as ws:
        reader_task = asyncio.create_task(reader(ws))

        # ждём Ctrl+C
        await stop_event.wait()

        # мягко закрываем соединение
        print("[closing] sending close frame…")
        await ws.close(code=1000, reason="client shutdown")
        try:
            await asyncio.wait_for(reader_task, timeout=5)
        except asyncio.TimeoutError:
            reader_task.cancel()
    print("[closed] bye")

if __name__ == "__main__":
    # хуки на сигналы
    try:
        signal.signal(signal.SIGINT, _handle_signal)
    except Exception:
        pass
    try:
        signal.signal(signal.SIGTERM, _handle_signal)
    except Exception:
        pass

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # резервный путь, если сигнал не сработал
        pass
