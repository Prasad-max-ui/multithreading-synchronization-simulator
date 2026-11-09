# Multithreading & Synchronization Simulator 🚀
### Interactive Real-Time OS Concurrency & Race Condition Laboratory

An educational web application built with **Python (Flask + Flask-SocketIO + native `threading`)** on the backend and **vanilla HTML5/CSS3/JavaScript** on the frontend. It visualizes operating system multithreading and synchronization concepts by running real server-side threads and streaming state transitions via WebSockets.

---

## 🌟 Key Features

1. **Real Python Multithreading**: All worker threads (producers, consumers, philosophers, readers, writers) run as native `threading.Thread` instances on the server — not simulated or mocked frontend animations.
2. **Instrumented Synchronization Primitives**: Custom wrappers around `threading.Lock`, `threading.Semaphore`, and `threading.Condition` that broadcast micro-events (`ACQUIRE_ATTEMPT`, `ACQUIRED`, `RELEASED`, `WAIT`, `NOTIFY`) in real time.
3. **Interactive Concurrency Controls**:
   - **Start**: Launches threads and begins simulation.
   - **Pause / Resume**: Halts thread execution without killing threads.
   - **Step**: Advances execution by releasing one thread event at a time.
   - **Reset**: Cleanly terminates all worker threads using cooperative `threading.Event` flags (no force-killing or hanging).
   - **Speed Multiplier**: Dynamically adjusts delays from 0.3x to 3.0x.
4. **Four Classic Concurrency Scenarios**:
   - **Producer-Consumer (Bounded Buffer)**
   - **Dining Philosophers (Deadlock Simulation)**
   - **Readers-Writers (Shared Ledger Record)**
   - **Banker's Algorithm (Deadlock Avoidance Laboratory)**
5. **Sync Mode Toggle (ON / OFF)**:
   - **Sync ON**: Primitives guarantee mathematical correctness and mutual exclusion.
   - **Sync OFF**: Unsynchronized memory access reliably produces visible race conditions, dirty reads, buffer corruption, and circular wait deadlocks.
6. **Live Anomaly Counter & Deadlock Banner**: Detects and tallies race conditions in real time, and alerts when the 4 Coffman conditions cause circular wait deadlock.
7. **Monospace Event Stream**: Terminal-style log with auto-scrolling, filter chips (All, Sync, State, Anomalies), and categorized event badges.
8. **Accessible Visualization**: Color-coded thread states with text badges: **[R] Running (Green)**, **[W] Waiting (Yellow)**, **[B] Blocked (Red)**, **[F] Finished (Gray)**.

---

## 🚀 Getting Started

### 1. Prerequisites
- Python 3.10 or higher (tested and verified on Python 3.14)
- `pip` package manager

### 2. Installation
Clone or navigate to the project directory and install the required dependencies:

```powershell
pip install -r requirements.txt
```

### 3. Running the Application
Launch the Flask-SocketIO server:

```powershell
python app.py
```

Open your browser and navigate to:
```
http://127.0.0.1:5000
```

### 4. Running the Unit Tests
Execute the comprehensive test suite (runnable independently without launching the Flask web server):

```powershell
python -m unittest discover tests -v
```

---

## 🔬 Scenarios & Synchronization Concepts

### 1. Producer-Consumer (Bounded Buffer)
- **Concept**: Multiple producer threads enqueue items into a fixed-capacity buffer ($N=5$), while consumer threads dequeue items.
- **Sync Mode ON**:
  - Uses `InstrumentedCondition` (or Semaphore pair + Mutex).
  - Producers wait when `len(buffer) >= capacity`. Consumers wait when `len(buffer) == 0`.
  - Invariant $0 \le \text{buffer size} \le \text{capacity}$ is strictly preserved. Zero anomalies across all runs.
- **Sync Mode OFF**:
  - Threads check and manipulate the shared list without acquiring any lock.
  - Produces visible and counted race conditions:
    - **Buffer Overflow**: Producer appends while buffer is already at or above capacity.
    - **Buffer Underflow**: Consumer attempts to pop from an empty buffer (`pop(0)` on empty list).

---

### 2. Dining Philosophers
- **Concept**: 5 philosophers sit around a circular table. Between each pair lies a single fork. To eat, a philosopher must acquire both their left and right forks.
- **Sync Mode ON (Resource Hierarchy)**:
  - Implements Dijkstra's resource hierarchy solution.
  - Philosophers always pick up the lower-numbered fork first, then the higher-numbered fork:
    $$\text{first} = \min(\text{left}, \text{right}), \quad \text{second} = \max(\text{left}, \text{right})$$
  - Philosopher 4 picks Fork 0 before Fork 4, which mathematically breaks the circular wait condition.
  - The simulation runs continuously with steady progress and zero deadlocks.
- **Sync Mode OFF (Deadlock-Capable)**:
  - All philosophers simultaneously attempt to pick their left fork first.
  - Once each philosopher holds 1 fork, all 5 philosophers simultaneously block waiting for their right fork.
  - **Deadlock Detected**: The server detects that all active philosophers are blocked and each holds 1 fork.
  - The **Deadlock Warning Banner** appears, outlining Coffman's 4 conditions:
    1. *Mutual Exclusion*: Forks cannot be shared.
    2. *Hold and Wait*: Philosophers hold one fork while waiting for the next.
    3. *No Preemption*: Held forks cannot be taken away.
    4. *Circular Wait*: P0 waits for P1, P1 waits for P2, ..., P4 waits for P0.
  - Clicking **Recover via Reset** triggers `stop_event` on the server; all threads release locks and terminate cleanly within milliseconds.

---

### 3. Readers-Writers
- **Concept**: A shared database record with fields (`version`, `checksum`, `title`, `content`).
- **Sync Mode ON**:
  - Implements reader-preference readers-writers synchronization.
  - Multiple readers can read simultaneously (readers share access).
  - When a writer requests access, it acquires an exclusive write lock. No readers can read while a writer writes, and no two writers can write concurrently.
  - Invariant $\text{version} == \text{checksum}$ is always true.
- **Sync Mode OFF**:
  - Readers and writers access and mutate shared memory concurrently without locks.
  - Reliably detects and tallies:
    - **Dirty Reads**: A reader accesses data while a writer is mid-update.
    - **Torn Reads**: A reader reads a fractured record where $\text{version} \ne \text{checksum}$.
    - **Write Collisions**: Multiple writers attempt simultaneous modifications, corrupting version increments.

---

### 4. Banker's Algorithm (Deadlock Avoidance Laboratory)
- **Concept**: Dijkstra's Banker's Algorithm for multi-resource deadlock avoidance. Evaluates whether granting resource requests preserves a **Safe State** ($\langle P_1, P_3, P_4, P_0, P_2 \rangle$) or risks entering an **Unsafe State** (potential deadlock).
- **Interactive OS Matrices Displayed**:
  - **Allocation Matrix**: Current resources allocated to each process $[P_i \times R_j]$.
  - **Max Claim Matrix**: Declared maximum resource requirements.
  - **Need Matrix**: Remaining resources needed before task completion ($\text{Need} = \text{Max} - \text{Allocation}$).
  - **Available Vector**: Physical system resources currently free for allocation.
- **Sync Mode ON (Banker's Avoidance Enabled)**:
  - Every resource request is vetted through Dijkstra's **Safety Algorithm** ($Work = Work + Allocation_i$).
  - If granting a request preserves a safe execution sequence, it is approved; if unsafe, it is rejected or delayed.
  - Zero deadlocks occur.
- **Sync Mode OFF (Naive Allocation Bypassing Avoidance)**:
  - Grants requests blindly whenever $Request \le Available$ without running the safety test.
  - Rapidly leads system into an **Unsafe State** where resources are depleted, no process can complete, and system enters permanent deadlock.
- **Interactive Request Evaluator**: Users can select any process ($P_0 - P_4$) and enter custom requests $[R_A, R_B, R_C]$ to see step-by-step trace evaluation in real time.

---

## 🛠️ Concurrency Controls Guide

| Control | Action | Details |
| :--- | :--- | :--- |
| **Scenario Dropdown** | Select Scenario | Switch between Producer-Consumer, Dining Philosophers, Readers-Writers, and Banker's Algorithm. |
| **Sync Mode Switch** | Toggle ON / OFF | Switch between safe synchronization and unsynchronized race condition mode. |
| **Thread Count** | Adjust Threads | Set worker thread count (2 to 10). |
| **Speed Slider** | Adjust Delays | Scale delay intervals from 0.3x (slow-motion analysis) to 3.0x (high-throughput). |
| **Start** | Launch Simulation | Spawns real server threads and opens the live event stream. |
| **Pause / Resume** | Freeze / Thaw | Pauses worker threads at their next step boundary without killing them. |
| **Step** | Advance 1 Step | Releases a single synchronization step while paused. |
| **Reset** | Clean Stop | Signals `stop_event`, cleanly joins worker threads, and restores initial state. |

---

## 🧪 Unit Test Coverage

The test suite in `/tests` verifies 13 automated unit tests (`python -m unittest discover tests -v`):
- `test_instrumented_lock_basic`: Verifies mutual exclusion and event callbacks (`LOCK_ATTEMPT`, `LOCK_ACQUIRED`, `LOCK_RELEASED`).
- `test_instrumented_lock_context_manager`: Tests Python `with lock:` semantics.
- `test_instrumented_lock_interruptible`: Verifies `acquire_interruptible()` wakes up promptly on `stop_event`.
- `test_instrumented_semaphore`: Verifies permit counting and boundary handling.
- `test_instrumented_condition`: Tests `wait()` and `notify()` synchronization between threads.
- `test_producer_consumer_sync_on`: Confirms buffer never exceeds capacity and produces 0 anomalies.
- `test_producer_consumer_sync_off`: Confirms buffer overflow/underflow anomalies are caught and counted.
- `test_dining_philosophers_sync_on`: Confirms steady dining progress without deadlock.
- `test_dining_philosophers_sync_off_deadlock`: Confirms circular wait deadlock is triggered and clean reset recovers immediately.
- `test_readers_writers_sync_on`: Confirms mutual exclusion for writers and consistent checksums.
- `test_readers_writers_sync_off`: Confirms dirty/torn read anomalies are flagged.
- `test_bankers_safety_check`: Verifies Dijkstra's Safety Algorithm calculates safe state and execution sequence $\langle P_1, P_3, P_4, P_0, P_2 \rangle$.
- `test_bankers_request_evaluation_sync_on`: Tests Banker's resource-request algorithm approving safe requests and denying max claim violations.

---

## 👥 Team Contribution & Module Division (5 Members)

| Member | Role / Designation | Primary Files Owned | Key OS Concepts Handled |
| :--- | :--- | :--- | :--- |
| **Member 1** | **Synchronization & Instrumentation Lead** | `sync/instrumented.py`<br>`sync/__init__.py` | Mutex Locks, Condition Variables, Counting Semaphores, Interruptible Locks, Event Telemetry |
| **Member 2** | **Thread Lifecycle & Producer-Consumer Lead** | `scenarios/base_scenario.py`<br>`scenarios/producer_consumer.py` | Thread Lifecycle, Cooperative Shutdown (`threading.Event`), Single-Stepping Semaphore, Bounded Buffer Invariants |
| **Member 3** | **Deadlock Detection & Dining Philosophers Lead** | `scenarios/dining_philosophers.py` | Coffman’s 4 Conditions, Circular Wait Deadlock, Background Deadlock Detector, Dijkstra’s Resource Hierarchy |
| **Member 4** | **Readers-Writers & Data Integrity Specialist** | `scenarios/readers_writers.py` | Reader-Preference Protocol, Shared vs Exclusive Mutex, Race Conditions, Dirty Reads, Torn Reads |
| **Member 5** | **Deadlock Avoidance & Banker’s Algorithm Lead** | `scenarios/bankers_algorithm.py`<br>`app.py` & `tests/` | Banker's Algorithm, Safety Algorithm, Allocation/Max/Need Matrices, WebSocket Gateway, Unit Testing |

### Detailed Member Responsibilities & Pitch Guide

#### 👤 Member 1: Team Lead & Synchronization Architecture
* **Files:** `sync/instrumented.py`, `sync/__init__.py`
* **Contributions:** Engineered non-invasive instrumentation wrappers around native `threading.Lock`, `threading.Semaphore`, and `threading.Condition`. Implemented `acquire_interruptible()` with timeout polling so blocked threads release and join cleanly on reset.
* **Pitch Line:** *"I built the synchronization and instrumentation layer. Instead of fake frontend animations, every worker thread is an actual OS kernel thread. My wrappers capture micro-telemetry on every acquire, release, and condition signal without altering the core synchronization algorithms."*

#### 👤 Member 2: Thread Lifecycle & Producer-Consumer Lead
* **Files:** `scenarios/base_scenario.py`, `scenarios/producer_consumer.py`
* **Contributions:** Built the `BaseScenario` engine with cooperative cancellation via `threading.Event` (`stop_event`) and interactive single-stepping using an internal `Semaphore(0)`. Implemented the Producer-Consumer bounded buffer problem demonstrating Condition variable synchronization (SYNC ON) vs. buffer overflow/underflow race conditions (SYNC OFF).
* **Pitch Line:** *"I developed the thread lifecycle controller and the Producer-Consumer scenario. I implemented cooperative cancellation so threads exit cleanly on reset, plus a stepping semaphore for granular debugging. In Producer-Consumer, I proved that without condition variables, concurrent buffer checks produce real overflows and underflows in shared memory."*

#### 👤 Member 3: Deadlock Detection & Dining Philosophers Lead
* **Files:** `scenarios/dining_philosophers.py`
* **Contributions:** Implemented the Dining Philosophers scenario demonstrating the 4 Coffman conditions (Mutual Exclusion, Hold & Wait, No Preemption, Circular Wait). Built the background deadlock monitor thread and implemented Dijkstra's Resource Hierarchy solution ($\min$ then $\max$ fork index) to break the circular wait cycle.
* **Pitch Line:** *"I focused on deadlock theory. In SYNC OFF mode, all philosophers pick up their left fork simultaneously, fulfilling all four Coffman conditions and creating a circular wait deadlock. In SYNC ON mode, I implemented Dijkstra's resource hierarchy to order fork acquisition by index, mathematically eliminating circular wait."*

#### 👤 Member 4: Readers-Writers & Data Integrity Specialist
* **Files:** `scenarios/readers_writers.py`
* **Contributions:** Designed the shared database ledger scenario implementing reader-preference mutual exclusion. Built detection checks for dirty reads (reading during write), torn reads ($\text{version} \ne \text{checksum}$), and concurrent writer collisions.
* **Pitch Line:** *"I was responsible for the Readers-Writers problem and data integrity. In SYNC ON mode, I implemented reader-preference mutual exclusion allowing multiple readers while guaranteeing writers exclusive access. In SYNC OFF mode, my verification checks capture real dirty reads and torn multi-field updates."*

#### 👤 Member 5: Deadlock Avoidance (Banker's Algorithm) & QA Lead
* **Files:** `scenarios/bankers_algorithm.py`, `app.py`, `tests/`
* **Contributions:** Implemented Dijkstra's Banker's Algorithm for deadlock avoidance with live Allocation, Max, Need matrices, and Available vector. Built the Safety Algorithm evaluator finding valid execution sequences $\langle P_1 \to P_3 \to P_4 \to P_0 \to P_2 \rangle$ and created the interactive custom resource-request tester. Authored the 13 automated unit tests.
* **Pitch Line:** *"I implemented Dijkstra's Banker's Algorithm to demonstrate active deadlock avoidance. My module evaluates multi-resource matrices and runs the Safety Algorithm to determine whether granting a request leaves the system in a Safe State or Unsafe State. I also built the interactive tester and automated test suite."*

