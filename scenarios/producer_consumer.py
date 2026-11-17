import time
import random
import threading
from typing import Callable, Optional, Dict, Any, List, Set
from .base_scenario import BaseScenario
from sync.instrumented import InstrumentedCondition, InstrumentedLock


class ProducerConsumerScenario(BaseScenario):
    """
    Bounded Buffer Producer-Consumer simulation.
    Sync Mode ON: Uses InstrumentedCondition / Lock for atomic operations and boundary safety.
    Sync Mode OFF: Unsynchronized buffer access resulting in buffer overflow, underflow, and lost updates.
    """

    def __init__(
        self,
        sync_mode: bool = True,
        thread_count: int = 4,
        speed: float = 1.0,
        capacity: int = 5,
        emitter: Optional[Callable[[str, Dict[str, Any]], None]] = None
    ):
        super().__init__(
            name="Producer-Consumer",
            sync_mode=sync_mode,
            thread_count=thread_count,
            speed=speed,
            emitter=emitter
        )
        self.capacity = capacity
        self.buffer: List[Dict[str, Any]] = []
        self.item_counter = 0
        self.total_produced = 0
        self.total_consumed = 0
        self.overflow_count = 0
        self.underflow_count = 0
        self.waiting_producers: Set[str] = set()
        self.waiting_consumers: Set[str] = set()
        self.last_sync_action: str = "Initialized"

        # Sync primitives
        self.lock = InstrumentedLock("buffer_mutex", emitter=self.emit_sync_event)
        self.condition = InstrumentedCondition(self.lock, name="buffer_condition", emitter=self.emit_sync_event)

        # Distribute threads between producers and consumers
        self.num_producers = max(1, self.thread_count // 2)
        self.num_consumers = max(1, self.thread_count - self.num_producers)

    def start(self):
        super().start()

        # Launch producer threads
        for i in range(self.num_producers):
            t_name = f"Producer-{i + 1}"
            t = threading.Thread(
                target=self.safe_run_wrapper,
                args=(self._producer_worker, t_name, "Producer", i + 1),
                name=t_name,
                daemon=True
            )
            self.worker_threads.append(t)
            t.start()

        # Launch consumer threads
        for i in range(self.num_consumers):
            t_name = f"Consumer-{i + 1}"
            t = threading.Thread(
                target=self.safe_run_wrapper,
                args=(self._consumer_worker, t_name, "Consumer", i + 1),
                name=t_name,
                daemon=True
            )
            self.worker_threads.append(t)
            t.start()

        self.emit_state_update()

    def _producer_worker(self, producer_id: int):
        thread_name = f"Producer-{producer_id}"

        while not self.stop_event.is_set():
            # Thinking / generating item
            self.set_thread_state(thread_name, "RUNNING", "Preparing new data item...", role="Producer")
            self.sleep_interruptible(0.6 + random.uniform(0.1, 0.4))
            if self.stop_event.is_set():
                break

            if self.sync_mode:
                # SYNCHRONIZED ACCESS
                with self.condition:
                    # Wait if buffer is full
                    while len(self.buffer) >= self.capacity and not self.stop_event.is_set():
                        with self.state_lock:
                            self.waiting_producers.add(thread_name)
                            self.last_sync_action = f"{thread_name} blocked on wait(not_full) - Buffer Full"
                        self.set_thread_state(thread_name, "WAITING", "Buffer is full, waiting on condition (wait)...", role="Producer")
                        self.emit_state_update()
                        self.condition.wait(timeout=0.5)

                    with self.state_lock:
                        self.waiting_producers.discard(thread_name)

                    if self.stop_event.is_set():
                        break

                    self.item_counter += 1
                    self.total_produced += 1
                    item = {
                        "id": self.item_counter,
                        "producer": thread_name,
                        "value": f"DATA-#{self.item_counter:03d}",
                        "timestamp": time.time()
                    }
                    self.buffer.append(item)
                    self.last_sync_action = f"{thread_name} enqueued {item['value']} & called notify_all()"
                    self.set_thread_state(thread_name, "RUNNING", f"Enqueued item {item['value']}", role="Producer")
                    self.condition.notify_all()

                self.emit_state_update()
                self.sleep_interruptible(0.4)

            else:
                # UNSYNCHRONIZED ACCESS (RACE CONDITIONS)
                # Check buffer without lock, simulate preemption/scheduling delay
                curr_size = len(self.buffer)
                self.set_thread_state(thread_name, "RUNNING", f"Unsafe check: buffer has {curr_size} items", role="Producer")
                time.sleep(0.02)

                self.item_counter += 1
                self.total_produced += 1
                item = {
                    "id": self.item_counter,
                    "producer": thread_name,
                    "value": f"DATA-#{self.item_counter:03d}",
                    "timestamp": time.time()
                }

                # Unsafe append directly to buffer without acquiring lock
                self.buffer.append(item)
                new_size = len(self.buffer)

                # Anomaly detection: Buffer overflow
                if new_size > self.capacity:
                    self.overflow_count += 1
                    self.last_sync_action = f"RACE OVERFLOW: {thread_name} pushed to capacity {self.capacity} (size={new_size})!"
                    self.record_anomaly(
                        "BUFFER_OVERFLOW",
                        f"Buffer overflow! Capacity={self.capacity}, but size={new_size} after {thread_name} produced",
                        {"capacity": self.capacity, "actual_size": new_size, "producer": thread_name}
                    )
                else:
                    self.last_sync_action = f"Unsafe write: {thread_name} enqueued {item['value']} without mutex"

                self.set_thread_state(thread_name, "RUNNING", f"Unsafely appended {item['value']} (Size={new_size})", role="Producer")
                self.emit_state_update()
                self.sleep_interruptible(0.4)

    def _consumer_worker(self, consumer_id: int):
        thread_name = f"Consumer-{consumer_id}"

        while not self.stop_event.is_set():
            # Wait or prepare
            self.set_thread_state(thread_name, "RUNNING", "Ready to consume item", role="Consumer")
            self.sleep_interruptible(0.5 + random.uniform(0.1, 0.4))
            if self.stop_event.is_set():
                break

            if self.sync_mode:
                # SYNCHRONIZED ACCESS
                with self.condition:
                    # Wait if buffer is empty
                    while len(self.buffer) == 0 and not self.stop_event.is_set():
                        with self.state_lock:
                            self.waiting_consumers.add(thread_name)
                            self.last_sync_action = f"{thread_name} blocked on wait(not_empty) - Buffer Empty"
                        self.set_thread_state(thread_name, "WAITING", "Buffer is empty, waiting on condition (wait)...", role="Consumer")
                        self.emit_state_update()
                        self.condition.wait(timeout=0.5)

                    with self.state_lock:
                        self.waiting_consumers.discard(thread_name)

                    if self.stop_event.is_set():
                        break

                    item = self.buffer.pop(0)
                    self.total_consumed += 1
                    self.last_sync_action = f"{thread_name} consumed {item['value']} & called notify_all()"
                    self.set_thread_state(thread_name, "RUNNING", f"Consumed item {item['value']}", role="Consumer")
                    self.condition.notify_all()

                self.emit_state_update()
                self.sleep_interruptible(0.5)

            else:
                # UNSYNCHRONIZED ACCESS (RACE CONDITIONS)
                # Consumer checks size without lock
                curr_size = len(self.buffer)
                self.set_thread_state(thread_name, "RUNNING", f"Unsafe check: buffer has {curr_size} items", role="Consumer")
                time.sleep(0.02)

                try:
                    if len(self.buffer) == 0:
                        # Attempting to pop an empty buffer
                        self.underflow_count += 1
                        self.last_sync_action = f"RACE UNDERFLOW: {thread_name} popped from EMPTY buffer (size=0)!"
                        self.record_anomaly(
                            "BUFFER_UNDERFLOW",
                            f"{thread_name} attempted to consume from an empty buffer (Size=0)!",
                            {"buffer_size": 0, "consumer": thread_name}
                        )
                    else:
                        item = self.buffer.pop(0)
                        self.total_consumed += 1
                        self.last_sync_action = f"Unsafe pop: {thread_name} consumed {item['value']} without mutex"
                        self.set_thread_state(thread_name, "RUNNING", f"Unsafely consumed {item['value']}", role="Consumer")
                except IndexError:
                    self.underflow_count += 1
                    self.last_sync_action = f"RACE UNDERFLOW EXCEPTION: {thread_name} popped concurrent empty buffer!"
                    self.record_anomaly(
                        "RACE_CONDITION_UNDERFLOW",
                        f"Race Condition hit! {thread_name} popped an already empty buffer due to concurrent consumer pop!",
                        {"buffer_size": 0, "consumer": thread_name}
                    )

                self.emit_state_update()
                self.sleep_interruptible(0.5)

    def emit_state_update(self):
        """Send live buffer state and metrics to frontend."""
        with self.state_lock:
            # Take a snapshot of buffer items
            buffer_snapshot = list(self.buffer)
            buf_len = len(buffer_snapshot)
            invariant_ok = (0 <= buf_len <= self.capacity)
            data = {
                "scenario": self.name,
                "sync_mode": self.sync_mode,
                "capacity": self.capacity,
                "buffer": buffer_snapshot,
                "buffer_size": buf_len,
                "total_produced": self.total_produced,
                "total_consumed": self.total_consumed,
                "anomaly_count": self.anomaly_count,
                "overflow_count": self.overflow_count,
                "underflow_count": self.underflow_count,
                "waiting_producers": list(self.waiting_producers),
                "waiting_consumers": list(self.waiting_consumers),
                "sem_empty": max(0, self.capacity - buf_len),
                "sem_full": buf_len,
                "sem_mutex": 0 if self.lock.is_locked else 1,
                "mutex_holder": getattr(self.lock, "holder", None),
                "invariant_preserved": invariant_ok,
                "last_sync_action": self.last_sync_action,
                "threads": dict(self.threads)
            }
        self.emit("buffer_update", data)

    def stop(self, timeout: float = 2.0):
        # Wake up condition in case any threads are waiting
        if self.sync_mode:
            try:
                with self.condition:
                    self.condition.notify_all()
            except Exception:
                pass
        super().stop(timeout=timeout)
