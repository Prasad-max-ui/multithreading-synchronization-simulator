import unittest
import threading
import time
from sync.instrumented import InstrumentedLock, InstrumentedSemaphore, InstrumentedCondition


class TestInstrumentedSync(unittest.TestCase):

    def test_instrumented_lock_basic(self):
        events = []
        lock = InstrumentedLock("test_lock", emitter=lambda e: events.append(e))

        self.assertFalse(lock.locked())
        acquired = lock.acquire()
        self.assertTrue(acquired)
        self.assertTrue(lock.locked())
        lock.release()
        self.assertFalse(lock.locked())

        event_types = [e["event_type"] for e in events]
        self.assertIn("LOCK_ATTEMPT", event_types)
        self.assertIn("LOCK_ACQUIRED", event_types)
        self.assertIn("LOCK_RELEASED", event_types)

    def test_instrumented_lock_context_manager(self):
        events = []
        lock = InstrumentedLock("test_lock_ctx", emitter=lambda e: events.append(e))

        with lock:
            self.assertTrue(lock.locked())
        self.assertFalse(lock.locked())

    def test_instrumented_lock_interruptible(self):
        stop_event = threading.Event()
        lock = InstrumentedLock("test_lock_interrupt", emitter=lambda e: None)
        lock.acquire()  # Main thread holds lock

        result = []

        def worker():
            res = lock.acquire_interruptible(stop_event, poll_interval=0.05)
            result.append(res)

        t = threading.Thread(target=worker)
        t.start()
        time.sleep(0.1)
        stop_event.set()  # Signal stop while worker is waiting
        t.join(timeout=1.0)

        self.assertFalse(t.is_alive())
        self.assertEqual(result, [False])
        lock.release()

    def test_instrumented_semaphore(self):
        events = []
        sem = InstrumentedSemaphore(value=2, name="test_sem", emitter=lambda e: events.append(e))

        self.assertTrue(sem.acquire())
        self.assertTrue(sem.acquire())
        # Non-blocking acquire should fail now
        self.assertFalse(sem.acquire(blocking=False))

        sem.release()
        self.assertTrue(sem.acquire())
        sem.release()
        sem.release()

        event_types = [e["event_type"] for e in events]
        self.assertIn("SEMAPHORE_ATTEMPT", event_types)
        self.assertIn("SEMAPHORE_ACQUIRED", event_types)
        self.assertIn("SEMAPHORE_RELEASED", event_types)

    def test_instrumented_condition(self):
        events = []
        cond = InstrumentedCondition(name="test_cond", emitter=lambda e: events.append(e))
        state = {"ready": False}

        def consumer():
            with cond:
                while not state["ready"]:
                    cond.wait(timeout=1.0)

        t = threading.Thread(target=consumer)
        t.start()
        time.sleep(0.05)

        with cond:
            state["ready"] = True
            cond.notify()

        t.join(timeout=1.0)
        self.assertFalse(t.is_alive())

        event_types = [e["event_type"] for e in events]
        self.assertIn("CONDITION_WAIT", event_types)
        self.assertIn("CONDITION_NOTIFY", event_types)
        self.assertIn("CONDITION_AWAKENED", event_types)


if __name__ == "__main__":
    unittest.main()
