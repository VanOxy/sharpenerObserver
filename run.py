from orchestrator import Orchestrator

def main():
    orch = Orchestrator(poll_delay_ms=120)
    try:
        orch.start()
        import time
        while True:
            time.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        orch.stop()


if __name__ == "__main__":
    main()
