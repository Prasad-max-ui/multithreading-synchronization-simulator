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
        # Fork request tracking: -1 = not waiting, else fork index being requested
        self.fork_waiting: List[int] = [-1] * self.num_philosophers

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
            with self.state_lock:
                self.fork_waiting[philosopher_id] = first_fork_idx
            self.set_thread_state(thread_name, "BLOCKED", f"Waiting for Fork {first_fork_idx}...", role="Philosopher")
            self.emit_state_update()
            got_first = first_lock.acquire_interruptible(self.stop_event, poll_interval=0.1)
            with self.state_lock:
                self.fork_waiting[philosopher_id] = -1
            if not got_first or self.stop_event.is_set():
                break

            with self.state_lock:
                self.fork_owners[first_fork_idx] = philosopher_id
            self.emit_state_update()

            # In unsync mode, slightly pause after holding 1 fork so all philosophers
            # pick up their left fork before any can grab the right fork -> GUARANTEES DEADLOCK!
            if not self.sync_mode:
                self.philosopher_status[philosopher_id] = "WAITING_FOR_FORK"
                with self.state_lock:
                    self.fork_waiting[philosopher_id] = second_fork_idx
                self.set_thread_state(
                    thread_name,
                    "BLOCKED",
                    f"Holding Fork {first_fork_idx}, now waiting for Fork {second_fork_idx}",
                    role="Philosopher"
                )
                self.emit_state_update()
                self.sleep_interruptible(0.3)
                if self.stop_event.is_set():
                    first_lock.release()
                    with self.state_lock:
                        self.fork_owners[first_fork_idx] = -1
                        self.fork_waiting[philosopher_id] = -1
                    break

            # 4. PICK UP SECOND FORK
            self.philosopher_status[philosopher_id] = "WAITING_FOR_FORK"
            with self.state_lock:
                self.fork_waiting[philosopher_id] = second_fork_idx
            self.set_thread_state(thread_name, "BLOCKED", f"Waiting for Fork {second_fork_idx}...", role="Philosopher")
            self.emit_state_update()
            got_second = second_lock.acquire_interruptible(self.stop_event, poll_interval=0.1)
            with self.state_lock:
                self.fork_waiting[philosopher_id] = -1
            if not got_second or self.stop_event.is_set():
                # Clean up held fork
                first_lock.release()
                with self.state_lock:
                    self.fork_owners[first_fork_idx] = -1
                    self.fork_waiting[philosopher_id] = -1
                self.emit_state_update()
                break

            with self.state_lock:
                self.fork_owners[second_fork_idx] = philosopher_id
                self.fork_waiting[philosopher_id] = -1

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

    def _compute_rag_and_coffman(self) -> Dict[str, Any]:
        """Build Resource Allocation Graph edges and check Coffman conditions."""
        alloc_edges = []   # Fork -> Philosopher (Held)
        request_edges = [] # Philosopher -> Fork (Requested)

        for f_idx, p_idx in enumerate(self.fork_owners):
            if p_idx != -1:
                alloc_edges.append({"from": f"F{f_idx}", "to": f"P{p_idx}"})

        for p_idx, f_idx in enumerate(self.fork_waiting):
            if f_idx != -1:
                request_edges.append({"from": f"P{p_idx}", "to": f"F{f_idx}"})

        # Check Coffman conditions
        mutual_exclusion = True  # Inherent to fork mutexes
        no_preemption = True     # Inherent to lock semantics

        # Hold & Wait: any philosopher holds >= 1 fork while waiting for another
        hold_and_wait = any(
            (self.fork_owners.count(i) >= 1 and self.fork_waiting[i] != -1)
            for i in range(self.num_philosophers)
        )

        # Circular wait:
        # Check if all 5 hold 1 fork and wait for neighbor or deadlock triggered
        circular_wait = False
        cycle_path = []
        if self.is_deadlocked or (
            all(status == "WAITING_FOR_FORK" for status in self.philosopher_status) and
            all(owner != -1 for owner in self.fork_owners)
        ):
            circular_wait = True
            for i in range(self.num_philosophers):
                cycle_path.append(f"P{i}")
                cycle_path.append(f"F{(i + 1) % self.num_philosophers}")
            cycle_path.append("P0")

        all_met = mutual_exclusion and hold_and_wait and no_preemption and circular_wait

        return {
            "allocation_edges": alloc_edges,
            "request_edges": request_edges,
            "cycle_detected": circular_wait,
            "cycle_path": cycle_path,
            "coffman": {
                "mutual_exclusion": {
                    "met": mutual_exclusion,
                    "desc": "Forks are non-shareable resources (1 philosopher per fork)."
                },
                "hold_and_wait": {
                    "met": hold_and_wait,
                    "desc": "Philosophers hold 1 fork while waiting for the neighbor fork."
                },
                "no_preemption": {
                    "met": no_preemption,
                    "desc": "Forks cannot be forcibly preempted from a philosopher."
                },
                "circular_wait": {
                    "met": circular_wait,
                    "desc": "Circular wait cycle: P0 -> F1 -> P1 -> F2 -> P2 -> F3 -> P3 -> F4 -> P4 -> F0 -> P0" if circular_wait else "Acyclic dependency graph (No cycle)."
                },
                "all_met": all_met
            }
        }

    def emit_state_update(self):
        """Send dining philosophers table state and RAG to frontend."""
        with self.state_lock:
            rag_info = self._compute_rag_and_coffman()
            total_meals = sum(self.eat_counts)
            data = {
                "scenario": self.name,
                "sync_mode": self.sync_mode,
                "num_philosophers": self.num_philosophers,
                "philosopher_status": list(self.philosopher_status),
                "eat_counts": list(self.eat_counts),
                "total_meals": total_meals,
                "fork_owners": list(self.fork_owners),
                "fork_waiting": list(self.fork_waiting),
                "is_deadlocked": self.is_deadlocked,
                "anomaly_count": self.anomaly_count,
                "rag": rag_info,
                "coffman": rag_info.get("coffman", {}),
                "acquisition_orders": [
                    {
                        "phil": i,
                        "first_fork": min(i, (i + 1) % self.num_philosophers),
                        "second_fork": max(i, (i + 1) % self.num_philosophers),
                        "rule": f"min({i},{(i+1)%self.num_philosophers}) then max" if i < self.num_philosophers - 1 else f"min(0,{self.num_philosophers-1}) then max (ASYMMETRIC)",
                        "is_asymmetric": (i == self.num_philosophers - 1)
                    } if self.sync_mode else {
                        "phil": i,
                        "first_fork": i,
                        "second_fork": (i + 1) % self.num_philosophers,
                        "rule": "Symmetric: left then right",
                        "is_asymmetric": False
                    }
                    for i in range(self.num_philosophers)
                ],
                "threads": dict(self.threads)
            }
        self.emit("table_update", data)

    def stop(self, timeout: float = 2.0):
        # Set stop event so interruptible acquires exit
        self.stop_event.set()
        super().stop(timeout=timeout)
