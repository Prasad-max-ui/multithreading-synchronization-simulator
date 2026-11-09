import threading
import time
from datetime import datetime
from typing import Callable, Optional, Dict, Any


def get_current_thread_info():
    """Helper to return current thread ID and human-readable name."""
    thread = threading.current_thread()
    return {
        "thread_id": thread.ident or 0,
        "thread_name": thread.name
    }


class InstrumentedSyncBase:
    """Base class for instrumented synchronization primitives."""

    def __init__(self, name: str, emitter: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.name = name
        self.emitter = emitter

    def _emit(self, event_type: str, new_thread_state: str, details: str = "", extra: Optional[Dict[str, Any]] = None):
        if not self.emitter:
            return
        thread_info = get_current_thread_info()
        payload = {
            "event_type": event_type,
            "thread_id": thread_info["thread_id"],
            "thread_name": thread_info["thread_name"],
            "timestamp": time.time(),
            "iso_time": datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "resource": self.name,
            "new_thread_state": new_thread_state,
            "details": details,
        }
        if extra:
            payload.update(extra)
        try:
            self.emitter(payload)
        except Exception as e:
            # Prevent logging errors from crashing the thread
            print(f"[InstrumentedSync] Error emitting event: {e}")


class InstrumentedLock(InstrumentedSyncBase):
    """
    Instrumented wrapper around threading.Lock.
    Emits events on acquire attempt, successful acquisition, and release.
    """

    def __init__(self, name: str, emitter: Optional[Callable[[Dict[str, Any]], None]] = None):
        super().__init__(name, emitter)
        self._lock = threading.Lock()
        self._owner_id: Optional[int] = None
        self._owner_name: Optional[str] = None

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        thread_info = get_current_thread_info()
        self._emit(
            event_type="LOCK_ATTEMPT",
            new_thread_state="BLOCKED",
            details=f"Thread {thread_info['thread_name']} attempting to acquire lock {self.name}"
        )

        success = self._lock.acquire(blocking, timeout)
        if success:
            self._owner_id = thread_info["thread_id"]
            self._owner_name = thread_info["thread_name"]
            self._emit(
                event_type="LOCK_ACQUIRED",
                new_thread_state="RUNNING",
                details=f"Thread {thread_info['thread_name']} acquired lock {self.name}",
                extra={"owner": self._owner_name}
            )
        else:
            self._emit(
                event_type="LOCK_FAILED",
                new_thread_state="RUNNING",
                details=f"Thread {thread_info['thread_name']} timed out/failed acquiring {self.name}"
            )
        return success

    def acquire_interruptible(self, stop_event: threading.Event, poll_interval: float = 0.2) -> bool:
        """
        Acquire lock while checking a stop_event so that threads in deadlock
        or long waits can be interrupted cleanly without spamming duplicate events.
        """
        thread_info = get_current_thread_info()
        self._emit(
            event_type="LOCK_ATTEMPT",
            new_thread_state="BLOCKED",
            details=f"Thread {thread_info['thread_name']} attempting to acquire lock {self.name}"
        )

        while not stop_event.is_set():
            if self._lock.acquire(timeout=poll_interval):
                self._owner_id = thread_info["thread_id"]
                self._owner_name = thread_info["thread_name"]
                self._emit(
                    event_type="LOCK_ACQUIRED",
                    new_thread_state="RUNNING",
                    details=f"Thread {thread_info['thread_name']} acquired lock {self.name}",
                    extra={"owner": self._owner_name}
                )
                return True

        self._emit(
            event_type="LOCK_CANCELLED",
            new_thread_state="FINISHED",
            details=f"Thread {thread_info['thread_name']} wait cancelled due to stop signal on {self.name}"
        )
        return False

    def release(self):
        thread_info = get_current_thread_info()
        prev_owner = self._owner_name
        self._owner_id = None
        self._owner_name = None
        try:
            self._lock.release()
        except RuntimeError:
            # Lock was already unlocked
            return
        self._emit(
            event_type="LOCK_RELEASED",
            new_thread_state="RUNNING",
            details=f"Thread {thread_info['thread_name']} released lock {self.name}",
            extra={"previous_owner": prev_owner}
        )

    def locked(self) -> bool:
        return self._lock.locked()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class InstrumentedSemaphore(InstrumentedSyncBase):
    """
    Instrumented wrapper around threading.Semaphore.
    Emits events on acquire and release.
    """

    def __init__(self, value: int = 1, name: str = "semaphore", emitter: Optional[Callable[[Dict[str, Any]], None]] = None):
        super().__init__(name, emitter)
        self._semaphore = threading.Semaphore(value)
        self._initial_value = value

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        thread_info = get_current_thread_info()
        self._emit(
            event_type="SEMAPHORE_ATTEMPT",
            new_thread_state="BLOCKED",
            details=f"Thread {thread_info['thread_name']} waiting on semaphore {self.name}"
        )

        kwargs = {}
        if timeout is not None:
            kwargs["timeout"] = timeout
        success = self._semaphore.acquire(blocking, **kwargs)

        if success:
            self._emit(
                event_type="SEMAPHORE_ACQUIRED",
                new_thread_state="RUNNING",
                details=f"Thread {thread_info['thread_name']} acquired permit on {self.name}"
            )
        else:
            self._emit(
                event_type="SEMAPHORE_FAILED",
                new_thread_state="RUNNING",
                details=f"Thread {thread_info['thread_name']} failed permit on {self.name}"
            )
        return success

    def release(self, n: int = 1):
        thread_info = get_current_thread_info()
        self._semaphore.release(n)
        self._emit(
            event_type="SEMAPHORE_RELEASED",
            new_thread_state="RUNNING",
            details=f"Thread {thread_info['thread_name']} released permit on {self.name}"
        )

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class InstrumentedCondition(InstrumentedSyncBase):
    """
    Instrumented wrapper around threading.Condition.
    Emits events on wait, notify, and notify_all.
    """

    def __init__(self, lock: Optional[InstrumentedLock] = None, name: str = "condition", emitter: Optional[Callable[[Dict[str, Any]], None]] = None):
        super().__init__(name, emitter)
        self._underlying_lock = lock or InstrumentedLock(f"{name}_internal_lock", emitter)
        self._condition = threading.Condition(self._underlying_lock._lock)

    def acquire(self, *args, **kwargs):
        return self._underlying_lock.acquire(*args, **kwargs)

    def release(self):
        return self._underlying_lock.release()

    def wait(self, timeout: Optional[float] = None) -> bool:
        thread_info = get_current_thread_info()
        self._emit(
            event_type="CONDITION_WAIT",
            new_thread_state="WAITING",
            details=f"Thread {thread_info['thread_name']} waiting on condition {self.name}"
        )
        result = self._condition.wait(timeout)
        self._emit(
            event_type="CONDITION_AWAKENED",
            new_thread_state="RUNNING",
            details=f"Thread {thread_info['thread_name']} awakened on condition {self.name}"
        )
        return result

    def wait_interruptible(self, stop_event: threading.Event, poll_interval: float = 0.2) -> bool:
        """Wait while checking stop_event so reset can terminate cleanly."""
        thread_info = get_current_thread_info()
        self._emit(
            event_type="CONDITION_WAIT",
            new_thread_state="WAITING",
            details=f"Thread {thread_info['thread_name']} waiting on condition {self.name}"
        )
        while not stop_event.is_set():
            if self._condition.wait(timeout=poll_interval):
                self._emit(
                    event_type="CONDITION_AWAKENED",
                    new_thread_state="RUNNING",
                    details=f"Thread {thread_info['thread_name']} awakened on condition {self.name}"
                )
                return True
        self._emit(
            event_type="CONDITION_CANCELLED",
            new_thread_state="FINISHED",
            details=f"Thread {thread_info['thread_name']} condition wait cancelled on {self.name}"
        )
        return False

    def notify(self, n: int = 1):
        thread_info = get_current_thread_info()
        self._condition.notify(n)
        self._emit(
            event_type="CONDITION_NOTIFY",
            new_thread_state="RUNNING",
            details=f"Thread {thread_info['thread_name']} notified condition {self.name} (n={n})"
        )

    def notify_all(self):
        thread_info = get_current_thread_info()
        self._condition.notify_all()
        self._emit(
            event_type="CONDITION_NOTIFY_ALL",
            new_thread_state="RUNNING",
            details=f"Thread {thread_info['thread_name']} notified all on condition {self.name}"
        )

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
