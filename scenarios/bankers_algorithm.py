import time
import random
import threading
from typing import Callable, Optional, Dict, Any, List, Tuple
from .base_scenario import BaseScenario


class BankersAlgorithmScenario(BaseScenario):
    """
    Dijkstra's Banker's Algorithm (Deadlock Avoidance Laboratory).
    Demonstrates Safe vs Unsafe state evaluation using:
    - Available Vector [R_A, R_B, R_C]
    - Max Claim Matrix
    - Allocation Matrix
    - Need Matrix (Need = Max - Allocation)
    
    Sync Mode ON: Banker's Algorithm Deadlock Avoidance.
                  Requests are only granted if they leave the system in a SAFE STATE.
    Sync Mode OFF: Naive Greedy Allocation (No Avoidance).
                  Requests are granted without safety check, leading to UNSAFE STATE & DEADLOCK.
    """

    def __init__(
        self,
        sync_mode: bool = True,
        thread_count: int = 5,
        speed: float = 1.0,
        emitter: Optional[Callable[[str, Dict[str, Any]], None]] = None
    ):
        super().__init__(
            name="Banker's Algorithm",
            sync_mode=sync_mode,
            thread_count=thread_count,
            speed=speed,
            emitter=emitter
        )
        self.num_processes = self.thread_count
        self.resource_names = ["R_A (CPU)", "R_B (RAM)", "R_C (DISK)"]
        self.num_resources = 3

        # Total System Resources [R_A, R_B, R_C]
        self.total_resources = [10, 5, 7]

        # Standard textbook starting matrices (Silberschatz Chapter 7)
        self.allocation: List[List[int]] = [
            [0, 1, 0],  # P0
            [2, 0, 0],  # P1
            [3, 0, 2],  # P2
            [2, 1, 1],  # P3
            [0, 0, 2],  # P4
        ][:self.num_processes]

        self.max_matrix: List[List[int]] = [
            [7, 5, 3],  # P0
            [3, 2, 2],  # P1
            [9, 0, 2],  # P2
            [2, 2, 2],  # P3
            [4, 3, 3],  # P4
        ][:self.num_processes]

        # Calculate Available vector = Total - Sum(Allocated)
        self.available = self._compute_available()

        # Need matrix: Need[i][j] = Max[i][j] - Allocation[i][j]
        self.need = self._compute_need()

        self.safe_sequence: List[int] = []
        self.is_currently_safe = True
        self.safety_steps: List[str] = []
        self.last_evaluated_request: Optional[Dict[str, Any]] = None

        # Run initial safety check
        self.is_currently_safe, self.safe_sequence, self.safety_steps = self.check_safety(
            self.available, self.allocation, self.need
        )

    def _compute_available(self) -> List[int]:
        allocated_sum = [0] * self.num_resources
        for row in self.allocation:
            for j in range(self.num_resources):
                allocated_sum[j] += row[j]
        return [self.total_resources[j] - allocated_sum[j] for j in range(self.num_resources)]

    def _compute_need(self) -> List[List[int]]:
        need = []
        for i in range(len(self.allocation)):
            row = []
            for j in range(self.num_resources):
                val = max(0, self.max_matrix[i][j] - self.allocation[i][j])
                row.append(val)
            need.append(row)
        return need

    def check_safety(
        self,
        avail: List[int],
        alloc: List[List[int]],
        need_mat: List[List[int]]
    ) -> Tuple[bool, List[int], List[str]]:
        """
        Dijkstra's Safety Algorithm:
        1. Work = Available, Finish[i] = False
        2. Find P_i where Finish[i] == False and Need[i] <= Work
        3. Work = Work + Allocation[i], Finish[i] = True
        4. If all Finish == True -> System is in SAFE STATE.
        """
        n = len(alloc)
        m = self.num_resources
        work = list(avail)
        finish = [False] * n
        sequence = []
        trace_steps = [f"Initial Work Vector: {work}"]

        while len(sequence) < n:
            found = False
            for i in range(n):
                if not finish[i]:
                    # Check if Need[i] <= Work
                    can_allocate = all(need_mat[i][j] <= work[j] for j in range(m))
                    if can_allocate:
                        for j in range(m):
                            work[j] += alloc[i][j]
                        finish[i] = True
                        sequence.append(i)
                        found = True
                        trace_steps.append(
                            f"Step {len(sequence)}: P{i} satisfied (Need {need_mat[i]} <= Work). "
                            f"Process completes & releases resources -> New Work = {list(work)}"
                        )
                        break
            if not found:
                break

        is_safe = (len(sequence) == n)
        if is_safe:
            seq_str = " -> ".join([f"P{p}" for p in sequence])
            trace_steps.append(f"Result: SAFE STATE verified. Valid execution sequence: < {seq_str} >")
        else:
            unfinished = [f"P{i}" for i in range(n) if not finish[i]]
            trace_steps.append(
                f"Result: UNSAFE STATE! Deadlock condition! Processes {unfinished} cannot be satisfied."
            )

        return is_safe, sequence, trace_steps

    def request_resources(self, process_id: int, request_vector: List[int]) -> Dict[str, Any]:
        """
        Banker's Resource-Request Algorithm:
        1. If Request <= Need[i], proceed; else error (exceeded max claim).
        2. If Request <= Available, proceed; else process must wait.
        3. Pretend to allocate resources:
           Available = Available - Request
           Allocation[i] = Allocation[i] + Request
           Need[i] = Need[i] - Request
        4. If Safe: grant request.
        5. If Unsafe (and sync_mode is ON): rollback allocation, reject/delay request.
        """
        with self.state_lock:
            # Validation 1: Request <= Need
            for j in range(self.num_resources):
                if request_vector[j] > self.need[process_id][j]:
                    res = {
                        "status": "ERROR_EXCEEDED_MAX",
                        "process": f"P{process_id}",
                        "request": request_vector,
                        "need": self.need[process_id],
                        "message": f"Process P{process_id} exceeded declared maximum claim!"
                    }
                    self.last_evaluated_request = res
                    self.record_anomaly(
                        "MAX_CLAIM_VIOLATION",
                        f"P{process_id} requested {request_vector} exceeding Need {self.need[process_id]}",
                        res
                    )
                    self.emit_state_update()
                    return res

            # Validation 2: Request <= Available
            for j in range(self.num_resources):
                if request_vector[j] > self.available[j]:
                    res = {
                        "status": "WAIT_INSUFFICIENT_AVAILABLE",
                        "process": f"P{process_id}",
                        "request": request_vector,
                        "available": self.available,
                        "message": f"Insufficient available resources for P{process_id}. Process must wait."
                    }
                    self.last_evaluated_request = res
                    self.set_thread_state(
                        f"Process-P{process_id}",
                        "WAITING",
                        f"Waiting for available resources {request_vector}",
                        role="Process"
                    )
                    self.emit_state_update()
                    return res

            # Pretend to allocate
            new_avail = [self.available[j] - request_vector[j] for j in range(self.num_resources)]
            new_alloc = [list(row) for row in self.allocation]
            new_need = [list(row) for row in self.need]

            for j in range(self.num_resources):
                new_alloc[process_id][j] += request_vector[j]
                new_need[process_id][j] -= request_vector[j]

            # Run Safety Algorithm on tentative state
            is_safe, safe_seq, trace = self.check_safety(new_avail, new_alloc, new_need)

            if self.sync_mode:
                # BANKER'S AVOIDANCE ON:
                if is_safe:
                    # Commit allocation
                    self.available = new_avail
                    self.allocation = new_alloc
                    self.need = new_need
                    self.safe_sequence = safe_seq
                    self.safety_steps = trace
                    self.is_currently_safe = True

                    res = {
                        "status": "GRANTED_SAFE",
                        "process": f"P{process_id}",
                        "request": request_vector,
                        "safe_sequence": safe_seq,
                        "trace": trace,
                        "message": f"Request {request_vector} by P{process_id} is SAFE. Resources allocated!"
                    }
                    self.set_thread_state(
                        f"Process-P{process_id}",
                        "RUNNING",
                        f"Granted {request_vector}. Safe sequence: < {' -> '.join([f'P{p}' for p in safe_seq])} >",
                        role="Process"
                    )
                else:
                    # Rollback tentative state
                    res = {
                        "status": "DENIED_UNSAFE",
                        "process": f"P{process_id}",
                        "request": request_vector,
                        "trace": trace,
                        "message": f"Request {request_vector} by P{process_id} rejected! Would cause UNSAFE STATE (Deadlock Risk)."
                    }
                    self.set_thread_state(
                        f"Process-P{process_id}",
                        "WAITING",
                        f"Request rejected by Banker's Algorithm (Unsafe State)",
                        role="Process"
                    )
            else:
                # NAIVE ALLOCATION (NO DEADLOCK AVOIDANCE):
                # Grant blindly even if unsafe!
                self.available = new_avail
                self.allocation = new_alloc
                self.need = new_need
                self.safe_sequence = safe_seq
                self.safety_steps = trace
                self.is_currently_safe = is_safe

                if not is_safe:
                    self.is_deadlocked = True
                    res = {
                        "status": "GRANTED_UNSAFE_DEADLOCK",
                        "process": f"P{process_id}",
                        "request": request_vector,
                        "trace": trace,
                        "message": f"NAIVE GRANT: Allocated {request_vector} to P{process_id} WITHOUT safety check -> System entered UNSAFE STATE!"
                    }
                    self.record_anomaly(
                        "UNSAFE_STATE_DEADLOCK",
                        f"Deadlock Avoidance Bypassed! System entered UNSAFE STATE after P{process_id} request.",
                        res
                    )
                    self.emit("deadlock_detected", {
                        "scenario": self.name,
                        "message": f"Deadlock / Unsafe State! Available resources depleted. No process can satisfy its remaining Need vector."
                    })
                else:
                    res = {
                        "status": "GRANTED_NAIVE",
                        "process": f"P{process_id}",
                        "request": request_vector,
                        "trace": trace,
                        "message": f"Naive grant: {request_vector} to P{process_id}."
                    }

            self.last_evaluated_request = res
            self.emit_state_update()
            return res

    def start(self):
        super().start()

        # Start worker threads representing active processes
        for i in range(self.num_processes):
            t_name = f"Process-P{i}"
            t = threading.Thread(
                target=self.safe_run_wrapper,
                args=(self._process_worker, t_name, "Process", i),
                name=t_name,
                daemon=True
            )
            self.worker_threads.append(t)
            t.start()

        self.emit_state_update()

    def _process_worker(self, pid: int):
        thread_name = f"Process-P{pid}"

        while not self.stop_event.is_set():
            self.sleep_interruptible(1.0 + random.uniform(0.3, 0.8))
            if self.stop_event.is_set():
                break

            # Process determines a small resource request based on its current Need
            req = [0, 0, 0]
            with self.state_lock:
                current_need = list(self.need[pid])
                current_avail = list(self.available)

            # Generate realistic request: at most Need and Available
            for j in range(self.num_resources):
                if current_need[j] > 0 and current_avail[j] > 0:
                    req[j] = random.randint(0, min(current_need[j], current_avail[j]))

            if sum(req) == 0:
                # If need is zero or fully satisfied, release resources
                with self.state_lock:
                    if sum(self.allocation[pid]) > 0:
                        # Release held resources
                        for j in range(self.num_resources):
                            self.available[j] += self.allocation[pid][j]
                            self.allocation[pid][j] = 0
                            self.need[pid][j] = self.max_matrix[pid][j]
                        self.set_thread_state(
                            thread_name,
                            "RUNNING",
                            f"Task completed! Released all allocated resources back to pool.",
                            role="Process"
                        )
                        self.is_currently_safe, self.safe_sequence, self.safety_steps = self.check_safety(
                            self.available, self.allocation, self.need
                        )
                        self.emit_state_update()
                self.sleep_interruptible(1.5)
                continue

            # Submit request through Banker's engine
            self.request_resources(pid, req)
            self.sleep_interruptible(1.2)

    def emit_state_update(self):
        """Send live matrices and safety state to frontend."""
        with self.state_lock:
            data = {
                "scenario": self.name,
                "sync_mode": self.sync_mode,
                "num_processes": self.num_processes,
                "resource_names": self.resource_names,
                "total_resources": list(self.total_resources),
                "available": list(self.available),
                "allocation": [list(row) for row in self.allocation],
                "max_matrix": [list(row) for row in self.max_matrix],
                "need": [list(row) for row in self.need],
                "is_currently_safe": self.is_currently_safe,
                "safe_sequence": list(self.safe_sequence),
                "safety_steps": list(self.safety_steps),
                "last_evaluated_request": self.last_evaluated_request,
                "anomaly_count": self.anomaly_count,
                "is_deadlocked": self.is_deadlocked,
                "threads": dict(self.threads)
            }
        self.emit("banker_update", data)

