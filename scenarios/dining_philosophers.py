import time
import random
import threading
from typing import Callable, Optional, Dict, Any, List
from .base_scenario import BaseScenario
from sync.instrumented import InstrumentedLock


class DiningPhilosophersScenario(BaseScenario):
    """
    Dining Philosophers Concurrency Simulation.
    Sync Mode ON: Asymmetric / Resource Hierarchy solution breaks circular wait (Deadlock-Free).
    Sync Mode OFF: Naive symmetric pickup where all philosophers pick left fork first,
                   reliably causing a circular wait deadlock.
    """

    def __init__(
        self,
        sync_mode: bool = True,
        thread_count: int = 5,
        speed: float = 1.0,
        emitter: Optional[Callable[[str, Dict[str, Any]], None]] = None
    ):
        super().__init__(
            name="Dining Philosophers",
            sync_mode=sync_mode,
            thread_count=thread_count,
            speed=speed,
            emitter=emitter
        )
        self.num_philosophers = self.thread_count

        # Create 1 lock per fork
        self.fork_locks: List[InstrumentedLock] = [
            InstrumentedLock(f"Fork-{i}", emitter=self.emit_sync_event)
            for i in range(self.num_philosophers)
        ]

        # Fork ownership tracking for visualization: -1 = on table, else philosopher index
        self.fork_owners: List[int] = [-1] * self.num_philosophers

        # Philosopher states: 'THINKING', 'HUNGRY', 'WAITING_FOR_FORK', 'EATING'
        self.philosopher_status: List[str] = ["THINKING"] * self.num_philosophers
        self.eat_counts: List[int] = [0] * self.num_philosophers

        # Monitor thread for deadlock detection
        self.deadlock_detector_thread: Optional[threading.Thread] = None

    def start(self):
        super().start()

        # Start deadlock monitor thread
        self.deadlock_detector_thread = threading.Thread(
            target=self._monitor_deadlock,
            name="Deadlock-Monitor",
            daemon=True
        )
        self.deadlock_detector_thread.start()

        # Start philosopher threads
        for i in range(self.num_philosophers):
            t_name = f"Philosopher-{i}"
            t = threading.Thread(
                target=self.safe_run_wrapper,
                args=(self._philosopher_worker, t_name, "Philosopher", i),
                name=t_name,
                daemon=True
            )
            self.worker_threads.append(t)
            t.start()

        self.emit_state_update()

    def _philosopher_worker(self, philosopher_id: int):
        thread_name = f"Philosopher-{philosopher_id}"
        left_fork_idx = philosopher_id
        right_fork_idx = (philosopher_id + 1) % self.num_philosophers

        # In Sync Mode: break circular wait by ordering fork acquisitions by index (resource hierarchy)
        if self.sync_mode:
            first_fork_idx = min(left_fork_idx, right_fork_idx)
            second_fork_idx = max(left_fork_idx, right_fork_idx)
        else:
            # Naive symmetric: all pick left first, then right
            first_fork_idx = left_fork_idx
            second_fork_idx = right_fork_idx

        first_lock = self.fork_locks[first_fork_idx]
        second_lock = self.fork_locks[second_fork_idx]

        while not self.stop_event.is_set():
            # 1. THINKING
            self.philosopher_status[philosopher_id] = "THINKING"
            self.set_thread_state(thread_name, "RUNNING", "Contemplating philosophy (Thinking)...", role="Philosopher")
            self.emit_state_update()
            # Think for a bit
            self.sleep_interruptible(0.6 + random.uniform(0.1, 0.4))
            if self.stop_event.is_set():
                break

            # 2. HUNGRY
            self.philosopher_status[philosopher_id] = "HUNGRY"
            self.set_thread_state(thread_name, "WAITING", "Hungry! Attempting to acquire forks...", role="Philosopher")
            self.emit_state_update()
            self.sleep_interruptible(0.2)
            if self.stop_event.is_set():
                break

            # 3. PICK UP FIRST FORK
            self.set_thread_state(thread_name, "BLOCKED", f"Waiting for Fork {first_fork_idx}...", role="Philosopher")
            got_first = first_lock.acquire_interruptible(self.stop_event, poll_interval=0.1)
            if not got_first or self.stop_event.is_set():
                break

            with self.state_lock:
                self.fork_owners[first_fork_idx] = philosopher_id
            self.emit_state_update()

            # In unsync mode, slightly pause after holding 1 fork so all philosophers
            # pick up their left fork before any can grab the right fork -> GUARANTEES DEADLOCK!
            if not self.sync_mode:
                self.philosopher_status[philosopher_id] = "WAITING_FOR_FORK"
                self.set_thread_state(
                    thread_name,
                    "BLOCKED",
                    f"Holding Fork {first_fork_idx}, now waiting for Fork {second_fork_idx}",
                    role="Philosopher"
                )
                self.sleep_interruptible(0.3)
                if self.stop_event.is_set():
                    first_lock.release()
                    with self.state_lock:
                        self.fork_owners[first_fork_idx] = -1
                    break

            # 4. PICK UP SECOND FORK
            self.philosopher_status[philosopher_id] = "WAITING_FOR_FORK"
            self.set_thread_state(thread_name, "BLOCKED", f"Waiting for Fork {second_fork_idx}...", role="Philosopher")
            got_second = second_lock.acquire_interruptible(self.stop_event, poll_interval=0.1)
            if not got_second or self.stop_event.is_set():
                # Clean up held fork
                first_lock.release()
                with self.state_lock:
                    self.fork_owners[first_fork_idx] = -1
                self.emit_state_update()
                break

            with self.state_lock:
                self.fork_owners[second_fork_idx] = philosopher_id

            # 5. EATING
            self.philosopher_status[philosopher_id] = "EATING"
            self.eat_counts[philosopher_id] += 1
            self.set_thread_state(
                thread_name,
                "RUNNING",
                f"Eating with Fork {first_fork_idx} & Fork {second_fork_idx} (Meals: {self.eat_counts[philosopher_id]})",
                role="Philosopher"
            )
            self.emit_state_update()
            # Eating duration
            self.sleep_interruptible(0.7 + random.uniform(0.1, 0.3))

            # 6. RELEASE BOTH FORKS
            second_lock.release()
            with self.state_lock:
                self.fork_owners[second_fork_idx] = -1

            first_lock.release()
            with self.state_lock:
                self.fork_owners[first_fork_idx] = -1

            self.philosopher_status[philosopher_id] = "THINKING"
            self.set_thread_state(thread_name, "RUNNING", "Finished eating, released forks", role="Philosopher")
            self.emit_state_update()
            self.sleep_interruptible(0.3)

    def _monitor_deadlock(self):
        """Monitors philosopher states to detect circular wait deadlock."""
        while not self.stop_event.is_set():
            time.sleep(0.3)
            if self.stop_event.is_set() or self.sync_mode:
                continue

            # Deadlock criteria:
            # All philosophers are in WAITING_FOR_FORK state AND all forks are held (each philosopher holds 1 fork).
            all_blocked = all(
                status == "WAITING_FOR_FORK" for status in self.philosopher_status
            )
            all_forks_held = all(owner != -1 for owner in self.fork_owners)

            if all_blocked and all_forks_held and not self.is_deadlocked:
                self.is_deadlocked = True
                self.record_anomaly(
                    "DEADLOCK_CIRCULAR_WAIT",
                    f"Deadlock detected! All {self.num_philosophers} philosophers are holding one fork and waiting for another.",
                    {
                        "philosopher_states": list(self.philosopher_status),
                        "fork_owners": list(self.fork_owners),
                        "coffman_conditions": [
                            "1. Mutual Exclusion: Forks are unshareable resources.",
                            "2. Hold & Wait: Each philosopher holds 1 fork while waiting for the next.",
                            "3. No Preemption: No philosopher can seize a held fork.",
                            "4. Circular Wait: Every philosopher waits for a fork held by their neighbor."
                        ]
                    }
                )
                self.emit("deadlock_detected", {
                    "scenario": self.name,
                    "num_philosophers": self.num_philosophers,
                    "fork_owners": list(self.fork_owners),
                    "message": "Deadlock Detected! All philosophers are permanently blocked in a circular wait cycle. Click 'Reset' to recover."
                })
                self.emit_state_update()

    def emit_state_update(self):
        """Send dining philosophers table state to frontend."""
        with self.state_lock:
            data = {
                "scenario": self.name,
                "sync_mode": self.sync_mode,
                "num_philosophers": self.num_philosophers,
                "philosopher_status": list(self.philosopher_status),
                "eat_counts": list(self.eat_counts),
                "fork_owners": list(self.fork_owners),
                "is_deadlocked": self.is_deadlocked,
                "anomaly_count": self.anomaly_count,
                "threads": dict(self.threads)
            }
        self.emit("table_update", data)

    def stop(self, timeout: float = 2.0):
        # Set stop event so interruptible acquires exit
        self.stop_event.set()
        super().stop(timeout=timeout)
