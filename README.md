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
4. **Three Classic Concurrency Scenarios**:
   - **Producer-Consumer (Bounded Buffer)**
   - **Dining Philosophers (Deadlock Simulation)**
   - **Readers-Writers (Shared Ledger Record)**
5. **Sync Mode Toggle (ON / OFF)**:
   - **Sync ON**: Primitives guarantee mathematical correctness and mutual exclusion.
   - **Sync OFF**: Unsynchronized memory access reliably produces visible race conditions, dirty reads, buffer corruption, and circular wait deadlocks.
6. **Live Anomaly Counter & Deadlock Banner**: Detects and tallies race conditions in real time, and alerts when the 4 Coffman conditions cause circular wait deadlock.
7. **Monospace Event Stream**: Terminal-style log with auto-scrolling, filter chips (All, Sync, State, Anomalies), and categorized event badges.
8. **Accessible Visualization**: Color-coded thread states with text badges: **[R] Running (Green)**, **[W] Waiting (Yellow)**, **[B] Blocked (Red)**, **[F] Finished (Gray)**.

---

## 📂 Project Architecture

```
midsem/
├── app.py                     # Flask + Flask-SocketIO server and WebSocket handlers
├── requirements.txt           # Project dependencies
├── sync/
│   ├── __init__.py
│   └── instrumented.py        # InstrumentedLock, Semaphore, and Condition wrappers
├── scenarios/
│   ├── __init__.py
│   ├── base_scenario.py       # Thread lifecycle, pause/step hooks, exception isolation
│   ├── producer_consumer.py   # Bounded buffer simulation (Sync ON vs OFF)
│   ├── dining_philosophers.py # 5 philosophers, forks, circular wait deadlock
│   └── readers_writers.py     # Shared record mutex, dirty/torn read detection
├── static/
│   ├── css/
│   │   └── style.css          # Dark-mode glassmorphic design system and animations
│   └── js/
│       └── app.js             # WebSocket client, SVG/DOM visualizers, log engine
├── templates/
│   └── index.html             # Single-page application dashboard
├── tests/
│   ├── __init__.py
│   ├── test_sync_wrappers.py  # Unit tests for locks, semaphores, and conditions
│   └── test_scenarios.py      # Unit tests for all three concurrency scenarios
└── README.md
```

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

## 🛠️ Concurrency Controls Guide

| Control | Action | Details |
| :--- | :--- | :--- |
| **Scenario Dropdown** | Select Scenario | Switch between Producer-Consumer, Dining Philosophers, and Readers-Writers. |
| **Sync Mode Switch** | Toggle ON / OFF | Switch between safe synchronization and unsynchronized race condition mode. |
| **Thread Count** | Adjust Threads | Set worker thread count (2 to 10). |
| **Speed Slider** | Adjust Delays | Scale delay intervals from 0.3x (slow-motion analysis) to 3.0x (high-throughput). |
| **Start** | Launch Simulation | Spawns real server threads and opens the live event stream. |
| **Pause / Resume** | Freeze / Thaw | Pauses worker threads at their next step boundary without killing them. |
| **Step** | Advance 1 Step | Releases a single synchronization step while paused. |
| **Reset** | Clean Stop | Signals `stop_event`, cleanly joins worker threads, and restores initial state. |

---

## 🧪 Unit Test Coverage

The test suite in `/tests` verifies:
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
