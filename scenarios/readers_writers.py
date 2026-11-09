import time
import random
import threading
from typing import Callable, Optional, Dict, Any, List, Set
from .base_scenario import BaseScenario
from sync.instrumented import InstrumentedLock


class ReadersWritersScenario(BaseScenario):
    """
    Readers-Writers Concurrency Simulation.
    Sync Mode ON: Classic Readers-Writers synchronization where multiple readers
                  can read simultaneously, while writers have exclusive access.
    Sync Mode OFF: Unsynchronized access resulting in dirty reads, torn reads,
                   and concurrent writer conflicts.
    """

    def __init__(
        self,
        sync_mode: bool = True,
        thread_count: int = 5,
        speed: float = 1.0,
        emitter: Optional[Callable[[str, Dict[str, Any]], None]] = None
    ):
        super().__init__(
            name="Readers-Writers",
            sync_mode=sync_mode,
            thread_count=thread_count,
            speed=speed,
            emitter=emitter
        )

        # Divide into readers and writers (e.g. 1-2 writers, remaining readers)
        self.num_writers = max(1, self.thread_count // 3)
        self.num_readers = max(1, self.thread_count - self.num_writers)

        # Shared record
        self.record = {
            "title": "OS Concurrency Ledger",
            "version": 1,
            "content": "Initial ledger state verified and consistent.",
            "checksum": 1,  # Invariant: checksum == version
            "last_writer": "Genesis",
            "updated_at": time.time()
        }

        # Active reading/writing trackers
        self.active_readers: Set[str] = set()
        self.active_writers: Set[str] = set()

        # Synchronization primitives
        self.read_count_mutex = InstrumentedLock("read_count_mutex", emitter=self.emit_sync_event)
        self.resource_write_lock = InstrumentedLock("resource_write_lock", emitter=self.emit_sync_event)
        self.read_count = 0

    def start(self):
        super().start()

        # Start readers
        for i in range(self.num_readers):
            t_name = f"Reader-{i + 1}"
            t = threading.Thread(
                target=self.safe_run_wrapper,
                args=(self._reader_worker, t_name, "Reader", i + 1),
                name=t_name,
                daemon=True
            )
            self.worker_threads.append(t)
            t.start()

        # Start writers
        for i in range(self.num_writers):
            t_name = f"Writer-{i + 1}"
            t = threading.Thread(
                target=self.safe_run_wrapper,
                args=(self._writer_worker, t_name, "Writer", i + 1),
                name=t_name,
                daemon=True
            )
            self.worker_threads.append(t)
            t.start()

        self.emit_state_update()

    def _reader_worker(self, reader_id: int):
        thread_name = f"Reader-{reader_id}"

        while not self.stop_event.is_set():
            # Idle / preparing
            self.set_thread_state(thread_name, "RUNNING", "Reading local cache / standby", role="Reader")
            self.sleep_interruptible(0.5 + random.uniform(0.1, 0.4))
            if self.stop_event.is_set():
                break

            if self.sync_mode:
                # SYNCHRONIZED READER
                self.set_thread_state(thread_name, "WAITING", "Requesting read permission...", role="Reader")

                # Acquire mutex to increment read_count
                if not self.read_count_mutex.acquire_interruptible(self.stop_event, poll_interval=0.1):
                    break

                self.read_count += 1
                if self.read_count == 1:
                    # First reader blocks writers
                    got_write_lock = self.resource_write_lock.acquire_interruptible(self.stop_event, poll_interval=0.1)
                    if not got_write_lock:
                        self.read_count -= 1
                        self.read_count_mutex.release()
                        break
                self.read_count_mutex.release()

                # Perform read inside critical section
                with self.state_lock:
                    self.active_readers.add(thread_name)
                v = self.record["version"]
                c = self.record["checksum"]
                self.set_thread_state(
                    thread_name,
                    "RUNNING",
                    f"Reading v{v} (Active Readers: {len(self.active_readers)})",
                    role="Reader"
                )
                self.emit_state_update()
                self.sleep_interruptible(0.6)

                with self.state_lock:
                    self.active_readers.discard(thread_name)

                # Exit protocol
                if not self.read_count_mutex.acquire_interruptible(self.stop_event, poll_interval=0.1):
                    break
                self.read_count -= 1
                if self.read_count == 0:
                    # Last reader unblocks writers
                    self.resource_write_lock.release()
                self.read_count_mutex.release()

                self.set_thread_state(thread_name, "RUNNING", f"Finished reading v{v}", role="Reader")
                self.emit_state_update()
                self.sleep_interruptible(0.4)

            else:
                # UNSYNCHRONIZED READER (RACE CONDITIONS)
                self.set_thread_state(thread_name, "RUNNING", "Unsafely accessing shared record...", role="Reader")
                with self.state_lock:
                    self.active_readers.add(thread_name)

                # Small delay to expose race conditions
                time.sleep(0.02)

                v = self.record["version"]
                c = self.record["checksum"]
                title = self.record["title"]

                # Detect race condition: Dirty read while writer is writing
                with self.state_lock:
                    current_writers = list(self.active_writers)

                if current_writers:
                    self.record_anomaly(
                        "DIRTY_READ",
                        f"Dirty Read! {thread_name} read record while writer(s) {current_writers} were modifying it!",
                        {"reader": thread_name, "writers": current_writers, "record_version": v}
                    )

                # Detect race condition: Torn read where version != checksum
                if v != c:
                    self.record_anomaly(
                        "TORN_READ_INCONSISTENCY",
                        f"Torn Read! {thread_name} observed fractured state: version={v} != checksum={c}",
                        {"reader": thread_name, "version": v, "checksum": c}
                    )

                self.set_thread_state(thread_name, "RUNNING", f"Unsafely read record v{v}", role="Reader")
                self.emit_state_update()
                self.sleep_interruptible(0.4)

                with self.state_lock:
                    self.active_readers.discard(thread_name)
                self.emit_state_update()
                self.sleep_interruptible(0.4)

    def _writer_worker(self, writer_id: int):
        thread_name = f"Writer-{writer_id}"

        while not self.stop_event.is_set():
            # Preparing update
            self.set_thread_state(thread_name, "RUNNING", "Composing database transaction...", role="Writer")
            self.sleep_interruptible(0.7 + random.uniform(0.1, 0.4))
            if self.stop_event.is_set():
                break

            if self.sync_mode:
                # SYNCHRONIZED WRITER
                self.set_thread_state(thread_name, "WAITING", "Waiting for exclusive write lock...", role="Writer")
                got_lock = self.resource_write_lock.acquire_interruptible(self.stop_event, poll_interval=0.1)
                if not got_lock or self.stop_event.is_set():
                    break

                with self.state_lock:
                    self.active_writers.add(thread_name)

                # Mutate record atomically
                new_version = self.record["version"] + 1
                self.record["title"] = f"OS Concurrency Ledger #{new_version:03d}"
                self.record["content"] = f"Atomic committed payload by {thread_name}"
                self.record["version"] = new_version
                self.record["checksum"] = new_version
                self.record["last_writer"] = thread_name
                self.record["updated_at"] = time.time()

                self.set_thread_state(thread_name, "RUNNING", f"Writing update v{new_version} (Exclusive)", role="Writer")
                self.emit_state_update()
                self.sleep_interruptible(0.6)

                with self.state_lock:
                    self.active_writers.discard(thread_name)

                self.resource_write_lock.release()
                self.set_thread_state(thread_name, "RUNNING", f"Released write lock for v{new_version}", role="Writer")
                self.emit_state_update()
                self.sleep_interruptible(0.5)

            else:
                # UNSYNCHRONIZED WRITER (RACE CONDITIONS)
                self.set_thread_state(thread_name, "RUNNING", "Unsafely overwriting shared record...", role="Writer")
                with self.state_lock:
                    self.active_writers.add(thread_name)
                    writers_count = len(self.active_writers)

                # Anomaly: Concurrent writers!
                if writers_count > 1:
                    self.record_anomaly(
                        "CONCURRENT_WRITERS",
                        f"Mutual exclusion violated! {writers_count} writers active simultaneously: {list(self.active_writers)}",
                        {"writers": list(self.active_writers)}
                    )

                # Staggered non-atomic update to widen torn read window
                new_version = self.record["version"] + 1
                self.record["version"] = new_version
                # Window for torn read where version has changed but checksum hasn't yet!
                time.sleep(0.04)
                self.record["title"] = f"CORRUPTED / TORN RECORD #{new_version:03d}"
                self.record["content"] = f"In-flight partial overwrite by {thread_name}"
                time.sleep(0.03)
                self.record["checksum"] = new_version
                self.record["last_writer"] = thread_name
                self.record["updated_at"] = time.time()

                self.set_thread_state(thread_name, "RUNNING", f"Unsafely modified record to v{new_version}", role="Writer")
                self.emit_state_update()
                self.sleep_interruptible(0.5)

                with self.state_lock:
                    self.active_writers.discard(thread_name)
                self.emit_state_update()
                self.sleep_interruptible(0.5)

    def emit_state_update(self):
        """Broadcast shared record state to frontend."""
        with self.state_lock:
            data = {
                "scenario": self.name,
                "sync_mode": self.sync_mode,
                "record": dict(self.record),
                "active_readers": list(self.active_readers),
                "active_writers": list(self.active_writers),
                "active_reader_count": len(self.active_readers),
                "active_writer_count": len(self.active_writers),
                "anomaly_count": self.anomaly_count,
                "threads": dict(self.threads)
            }
        self.emit("record_update", data)

    def stop(self, timeout: float = 2.0):
        # Set stop event so interruptible acquires exit
        self.stop_event.set()
        super().stop(timeout=timeout)
