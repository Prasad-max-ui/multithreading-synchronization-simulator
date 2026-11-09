import threading
import time
import traceback
from datetime import datetime
from typing import Callable, Optional, Dict, Any, List


class BaseScenario:
    """
    Abstract base class providing thread lifecycle, pausing/stepping,
    safe execution wrappers, and event dispatching for concurrency simulations.
    """

    def __init__(
        self,
        name: str,
        sync_mode: bool,
        thread_count: int,
        speed: float = 1.0,
        emitter: Optional[Callable[[str, Dict[str, Any]], None]] = None
    ):
        self.name = name
        self.sync_mode = sync_mode
        self.thread_count = max(2, min(10, thread_count))
        self.speed = max(0.1, min(3.0, speed))  # speed factor (higher = faster, lower delay)
        self.emitter = emitter

        self.stop_event = threading.Event()
        self.is_paused = False
        self.step_semaphore = threading.Semaphore(0)
        self.state_lock = threading.Lock()

        self.threads: Dict[str, Dict[str, Any]] = {}
        self.worker_threads: List[threading.Thread] = []
        self.anomaly_count = 0
        self.anomalies: List[Dict[str, Any]] = []
        self.is_deadlocked = False
        self.is_running = False

    def emit(self, event_name: str, data: Dict[str, Any]):
        """Emit an event via the configured emitter callback."""
        if not self.emitter:
            return
        # Ensure timestamp and iso_time are present
        if "timestamp" not in data:
            data["timestamp"] = time.time()
        if "iso_time" not in data:
            data["iso_time"] = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        try:
            self.emitter(event_name, data)
        except Exception as e:
            print(f"[{self.name}] Error emitting {event_name}: {e}")

    def emit_sync_event(self, payload: Dict[str, Any]):
        """Callback supplied to InstrumentedLock / Semaphore / Condition."""
        # Update thread state in our thread registry
        thread_name = payload.get("thread_name")
        new_state = payload.get("new_thread_state")
        resource = payload.get("resource")
        details = payload.get("details", "")

        if thread_name and thread_name in self.threads and new_state:
            with self.state_lock:
                self.threads[thread_name]["state"] = new_state
                self.threads[thread_name]["action"] = details
                # Track held resources
                if payload.get("event_type") in ("LOCK_ACQUIRED", "SEMAPHORE_ACQUIRED"):
                    if resource and resource not in self.threads[thread_name]["held_resources"]:
                        self.threads[thread_name]["held_resources"].append(resource)
                elif payload.get("event_type") in ("LOCK_RELEASED", "SEMAPHORE_RELEASED"):
                    if resource and resource in self.threads[thread_name]["held_resources"]:
                        self.threads[thread_name]["held_resources"].remove(resource)

        self.emit("sync_event", payload)
        self.emit_state_update()

    def set_thread_state(self, thread_name: str, state: str, action: str = "", role: str = ""):
        """Update and broadcast a thread's state."""
        with self.state_lock:
            if thread_name not in self.threads:
                self.threads[thread_name] = {
                    "id": threading.get_ident(),
                    "name": thread_name,
                    "role": role,
                    "state": state,
                    "action": action,
                    "held_resources": []
                }
            else:
                self.threads[thread_name]["state"] = state
                self.threads[thread_name]["action"] = action
                if role:
                    self.threads[thread_name]["role"] = role

        thread_data = dict(self.threads[thread_name])
        self.emit("thread_state_change", thread_data)
        self.emit_state_update()

    def record_anomaly(self, anomaly_type: str, message: str, details: Optional[Dict[str, Any]] = None):
        """Record a race condition or state inconsistency anomaly."""
        with self.state_lock:
            self.anomaly_count += 1
            anomaly_entry = {
                "id": self.anomaly_count,
                "type": anomaly_type,
                "message": message,
                "details": details or {},
                "timestamp": time.time(),
                "iso_time": datetime.now().strftime("%H:%M:%S.%f")[:-3]
            }
            self.anomalies.append(anomaly_entry)

        self.emit("anomaly_detected", {
            "anomaly_count": self.anomaly_count,
            "anomaly": anomaly_entry
        })
        self.emit_state_update()

    def check_pause_and_step(self):
        """Called by worker threads to respect Pause and Step actions."""
        while not self.stop_event.is_set():
            if not self.is_paused:
                break
            # When paused, wait for either resume or a step signal
            stepped = self.step_semaphore.acquire(timeout=0.1)
            if stepped:
                # One step granted; proceed through this step
                break

    def sleep_interruptible(self, base_seconds: float):
        """
        Sleep for a speed-adjusted duration, checking pause and stop events frequently
        so resets and pauses are instantly responsive.
        """
        self.check_pause_and_step()
        # Higher speed factor = shorter delay
        duration = max(0.02, base_seconds / self.speed)
        end_time = time.time() + duration
        while time.time() < end_time and not self.stop_event.is_set():
            self.check_pause_and_step()
            time.sleep(min(0.05, max(0.01, end_time - time.time())))

    def safe_run_wrapper(self, target_fn, thread_name: str, role: str, *args, **kwargs):
        """Wrapper for worker thread entrypoint with exception catching and cleanup."""
        self.set_thread_state(thread_name, "RUNNING", f"Thread {thread_name} started", role=role)
        try:
            target_fn(*args, **kwargs)
        except Exception as e:
            tb = traceback.format_exc()
            print(f"[{self.name}] Exception in {thread_name}: {e}\n{tb}")
            self.set_thread_state(thread_name, "ERROR", f"Error: {e}")
            self.emit("thread_error", {
                "thread_name": thread_name,
                "error": str(e),
                "traceback": tb
            })
        finally:
            self.set_thread_state(thread_name, "FINISHED", f"Thread {thread_name} terminated")

    def start(self):
        """Start the simulation threads."""
        self.is_running = True
        self.stop_event.clear()
        self.is_paused = False
        self.emit("simulation_started", {
            "scenario": self.name,
            "sync_mode": self.sync_mode,
            "thread_count": self.thread_count,
            "speed": self.speed
        })

    def pause(self):
        """Pause the simulation."""
        self.is_paused = True
        self.emit("simulation_paused", {"is_paused": True})

    def resume(self):
        """Resume the paused simulation."""
        self.is_paused = False
        # Wake up any threads that might be waiting in step_semaphore
        self.emit("simulation_resumed", {"is_paused": False})

    def step(self):
        """Advance one thread by releasing the step semaphore once."""
        if self.is_paused:
            self.step_semaphore.release()
            self.emit("simulation_stepped", {})

    def set_speed(self, speed: float):
        """Update simulation speed factor (0.1 - 3.0)."""
        self.speed = max(0.1, min(3.0, speed))
        self.emit("speed_updated", {"speed": self.speed})

    def stop(self, timeout: float = 2.0):
        """Cleanly signal stop and join all worker threads."""
        self.stop_event.set()
        self.is_paused = False
        # Release step semaphore in case any thread is waiting
        for _ in range(len(self.worker_threads) + 5):
            self.step_semaphore.release()

        # Join worker threads with timeout
        for t in self.worker_threads:
            if t.is_alive():
                t.join(timeout=timeout / max(1, len(self.worker_threads)))

        self.is_running = False
        self.emit("simulation_stopped", {
            "scenario": self.name,
            "anomaly_count": self.anomaly_count
        })

    def emit_state_update(self):
        """To be overridden by subclasses to broadcast scenario-specific state."""
        pass
