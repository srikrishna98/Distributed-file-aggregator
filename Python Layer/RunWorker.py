from Worker import Worker
import threading

if __name__ == "__main__":

    Worker = Worker()
    
    stop_event = threading.Event()
    # Start the workers
    try:
        Worker.run_workers(6, stop_event)
    except (KeyboardInterrupt, SystemExit):
        stop_event.set()
