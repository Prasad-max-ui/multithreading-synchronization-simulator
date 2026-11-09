# 🎓 Operating Systems Project Presentation
## Multithreading & Synchronization Simulator (with Banker's Algorithm)

> **Course:** Operating Systems  
> **Team Size:** 5 Members  
> **Structure:** 8-Slide Master Deck with Spoken Pitch Scripts & Technical Defense

---

## 📑 Slide Deck Overview (8 Slides)

1. **Slide 1:** Project Introduction & Objective
2. **Slide 2:** Project Architecture & File Structure
3. **Slide 3:** Thread Management — BaseScenario
4. **Slide 4:** Synchronization Layer — Lock, Condition & Semaphore
5. **Slide 5:** Producer-Consumer Implementation (Bounded Buffer)
6. **Slide 6:** Dining Philosophers & Deadlock Detection/Prevention
7. **Slide 7:** Readers-Writers + Real-Time WebSocket Telemetry
8. **Slide 8:** Banker's Algorithm (Deadlock Avoidance), Team Division & Testing

---

# 🖥️ Slide 1: Project Introduction & Objective
**Speaker:** Member 1 (Team Lead)

### Slide Content:
* **The Problem:**
  * Concurrency and synchronization (race conditions, deadlocks, critical sections) are fundamental to modern Operating Systems, but traditional education relies on static textbook pseudocode.
  * Web visualizers typically use frontend JavaScript timers (`setTimeout`) rather than executing real concurrency.
* **Our Solution:**
  * A full-stack educational laboratory where **native Python OS threads (`threading.Thread`) execute on the server CPU** managed by the host OS scheduler.
  * Real-time telemetry micro-events (`LOCK_ATTEMPT`, `ACQUIRED`, `RELEASED`, `WAIT`, `NOTIFY`) streamed via WebSockets to a live dashboard.
* **The Core Innovation — Dual-Mode Execution:**
  * **SYNC ON (Safe Mode):** Mathematical correctness enforced via Mutex Locks, Condition Variables, and Semaphores (0 anomalies).
  * **SYNC OFF (Race-Prone Mode):** Synchronization locks bypassed to visibly trigger, measure, and count race conditions, dirty reads, and deadlocks in real time.

> 🗣️ **Member 1 Pitch Script:**  
> *"Good morning, Ma'am. For our Operating Systems project, our team built the Multithreading & Synchronization Simulator. Most concurrency tools are just frontend animations. In our project, every process and thread is an actual OS kernel thread executing in parallel on the server CPU. We engineered a dual-mode engine allowing instructors and students to toggle synchronization ON and OFF to witness real race conditions and deadlocks as they happen."*

---

# 🏗️ Slide 2: Project Architecture & File Structure
**Speaker:** Member 1 (Team Lead)

### Slide Content:
* **Decoupled Client-Server Telemetry Architecture:**
  ```
  [ Browser Client: Vanilla HTML5 / Glassmorphism CSS3 / App.js ]
                     ▲
                     │ WebSockets (Socket.IO Telemetry Events)
                     ▼
  [ Flask + Flask-SocketIO Server Gateway (app.py) ]
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
  [ sync/instrumented.py ]  [ scenarios/base_scenario.py ]
  (Lock, Condition, Sem)    (Lifecycle, Pause, Step, Join)
         │                       │
         └───────────┬───────────┘
                     ▼
     [ Concurrency Scenario Engines ]
     ├── producer_consumer.py (Bounded Buffer)
     ├── dining_philosophers.py (Deadlock & Coffman Conditions)
     ├── readers_writers.py (Shared Mutex & Integrity Invariants)
     └── bankers_algorithm.py (Dijkstra's Deadlock Avoidance)
  ```
* **Modular Codebase Organization:**
  * `app.py`: Flask WebSocket bridge and scenario session controller.
  * `sync/`: Non-invasive telemetry wrapping standard `threading` primitives.
  * `scenarios/`: Dedicated OS problem engines.
  * `tests/`: Automated unit test suite independent of the web interface.

> 🗣️ **Member 1 Pitch Script:**  
> *"Our architecture cleanly separates presentation from kernel execution. Flask and Socket.IO provide the real-time event pipeline, while our synchronization package non-invasively wraps native locks to capture timestamps, thread states, and resource ownership without altering the underlying concurrency algorithms."*

---

# 🧵 Slide 3: Thread Management — BaseScenario
**Speaker:** Member 2 (Thread Lifecycle Lead)

### Slide Content:
* **The BaseScenario Engine (`scenarios/base_scenario.py`):**
  * Abstract foundation managing native OS `threading.Thread` instances.
  * Thread registry tracking thread ID, role, current action, and resource ownership.
* **Kernel Lifecycle Controls:**
  * **Cooperative Cancellation (`threading.Event`):**
    * Uses `stop_event` flag. Avoids dangerous force-killing (`thread.terminate()`); threads check flags and join cleanly within milliseconds.
  * **Interactive Pause & Resume:**
    * Freezes thread execution loops safely at critical section boundaries.
  * **Granular Single-Stepping:**
    * Implemented via `step_semaphore = threading.Semaphore(0)`. Releasing one permit allows exactly one thread to execute its next instruction.
* **Dual-Encoded Accessible Thread States:**
  * **[R] Running (Emerald Green)**, **[W] Waiting (Amber Yellow)**, **[B] Blocked (Crimson Red)**, **[F] Finished (Slate Gray)**.

> 🗣️ **Member 2 Pitch Script:**  
> *"I was responsible for thread lifecycle management in `base_scenario.py`. Rather than force-killing threads on reset, I implemented cooperative cancellation using `threading.Event` so threads release held locks and join cleanly. I also built kernel-level debugging controls: users can pause the running OS threads and step through synchronization events one by one using an internal semaphore."*

---

# 🔒 Slide 4: Synchronization Layer — Lock, Condition & Semaphore
**Speaker:** Member 1 & Member 2

### Slide Content:
* **Purpose:** Instrument native OS primitives to stream real-time events without code invasiveness.
* **1. InstrumentedLock (`sync/instrumented.py`):**
  * Wraps `threading.Lock`.
  * Emits `LOCK_ATTEMPT`, `LOCK_ACQUIRED`, and `LOCK_RELEASED`.
  * **`acquire_interruptible()`:** Implements short timeout polling against `stop_event` so threads trapped in deadlocks can exit instantly on reset.
* **2. InstrumentedCondition:**
  * Wraps `threading.Condition(lock)`.
  * Tracks `wait()`, `notify()`, and `notify_all()`.
  * Emits `CONDITION_WAIT` (thread state $\to$ Waiting) and `CONDITION_AWAKENED` (state $\to$ Running).
* **3. InstrumentedSemaphore:**
  * Wraps `threading.Semaphore(value)`, tracking permit counters and blocked acquisitions.

> 🗣️ **Speaker Pitch Script:**  
> *"Our synchronization layer wraps native Python locks, semaphores, and condition variables. When a thread attempts to enter a critical section, our lock intercepts the call, emits an attempt event, transitions the thread to Blocked, and upon acquisition, notifies all connected clients in real time."*

---

# 📦 Slide 5: Producer-Consumer Implementation
**Speaker:** Member 2 (Producer-Consumer Lead)

### Slide Content:
* **The Bounded Buffer Problem:**
  * Fixed-capacity FIFO buffer ($N = 5$ slots).
  * Multiple Producer threads generate data items; Consumer threads dequeue items.
* **SYNC ON Mode (Condition Variable Synchronization):**
  * Uses `InstrumentedCondition` over a shared mutex.
  * **Producers:** Call `condition.wait()` when buffer is full ($\text{size} \ge 5$).
  * **Consumers:** Call `condition.wait()` when buffer is empty ($\text{size} == 0$).
  * Wakeups triggered via `condition.notify_all()`.
  * **Invariant Preserved:** $0 \le \text{buffer size} \le 5$. Exactly 0 anomalies.
* **SYNC OFF Mode (Race Conditions):**
  * Condition variables bypassed; direct uncoordinated list manipulation.
  * **Live Caught & Counted Anomalies:**
    * **Buffer Overflow:** Concurrent producers check space simultaneously and push beyond capacity ($>5$).
    * **Buffer Underflow:** Consumers race to pop from an empty list (`pop(0)` on empty buffer).

> 🗣️ **Member 2 Pitch Script:**  
> *"In Producer-Consumer, SYNC ON coordinates access via condition variables so producers sleep when the buffer is full and consumers sleep when empty. In SYNC OFF, we bypass these locks: multiple threads read and write simultaneously, triggering real buffer overflow and underflow race conditions captured live by our anomaly counter."*

---

# 🍝 Slide 6: Dining Philosophers & Deadlock Prevention
**Speaker:** Member 3 (Deadlock Detection Lead)

### Slide Content:
* **Problem Formulation:**
  * 5 philosophers arranged around a circular table; 5 shared forks (locks).
  * Each philosopher requires both their left fork ($i$) and right fork ($(i+1)\%5$) to eat.
* **SYNC OFF Mode (Coffman Deadlock Triggered):**
  * **Symmetric Pickup:** Every philosopher grabs their left fork first.
  * All 5 forks are held; all 5 philosophers block waiting for their right fork.
  * **All 4 Coffman Deadlock Conditions Fulfilled:**
    1. *Mutual Exclusion:* Forks are unshareable locks.
    2. *Hold and Wait:* Holding left fork while waiting for right fork.
    3. *No Preemption:* Held forks cannot be forcibly seized.
    4. *Circular Wait:* Closed dependency loop ($P_0 \to P_1 \to P_2 \to P_3 \to P_4 \to P_0$).
  * **Deadlock Monitor:** Background thread detects circular wait and triggers the **Deadlock Warning Banner**.
* **SYNC ON Mode (Dijkstra’s Resource Hierarchy Solution):**
  * Fork acquisition ordered by index: acquire $\min(\text{left}, \text{right})$ first, then $\max(\text{left}, \text{right})$.
  * Philosopher 4 picks Fork 0 before Fork 4 $\to$ **Breaks Circular Wait**! Zero deadlocks.

> 🗣️ **Member 3 Pitch Script:**  
> *"In Dining Philosophers, SYNC OFF demonstrates deadlock in action: all philosophers pick their left fork simultaneously, fulfilling all four Coffman conditions and freezing in a circular wait cycle. In SYNC ON mode, I implemented Dijkstra's Resource Hierarchy solution, ordering fork pickups by index to mathematically eliminate circular wait and ensure continuous dining progress."*

---

# 📖 Slide 7: Readers-Writers + Real-Time Telemetry
**Speaker:** Member 4 (Readers-Writers Specialist)

### Slide Content:
* **The Readers-Writers Concurrency Problem:**
  * Shared database ledger with multi-field record (`title`, `content`, `version`, `checksum`).
  * Challenge: Allowing concurrent reading without permitting data corruption from concurrent writes.
* **SYNC ON Mode (Reader-Preference Protocol):**
  * Multiple readers read concurrently in shared mode (`Active Readers > 0`).
  * Writers obtain **exclusive mutual exclusion**: zero active readers allowed during a write.
  * Invariant strictly maintained: $\text{version} == \text{checksum}$.
* **SYNC OFF Mode (Data Integrity Violations):**
  * **Dirty Reads:** Reader accesses record while writer is halfway through updating fields.
  * **Torn Reads:** Record read in an inconsistent state where $\text{version} \ne \text{checksum}$.
  * **Write Collisions:** Multiple concurrent writers overwrite and corrupt version increments.
* **Frontend Real-Time Telemetry:**
  * Glassmorphism dark UI with live visualizers.
  * Monospace Event Stream with auto-scrolling, clear buffer, and filters (`All`, `Sync`, `State`, `Anomalies`).

> 🗣️ **Member 4 Pitch Script:**  
> *"In Readers-Writers, SYNC ON enforces reader-preference mutual exclusion: multiple readers share access, but writers have exclusive locks. In SYNC OFF, concurrent uncoordinated writes produce real dirty reads and torn reads where multi-field data fractures, visibly shaking the ledger card and incrementing our anomaly HUD."*

---

# 🏦 Slide 8: Banker's Algorithm (Deadlock Avoidance) & Verification
**Speaker:** Member 5 (Deadlock Avoidance & QA Lead)

### Slide Content:
* **Dijkstra's Banker's Algorithm (Deadlock Avoidance Lab):**
  * Multi-resource model: 5 processes ($P_0 - P_4$) and 3 resource types (CPU, RAM, Disk).
  * **Classic OS Matrices Displayed:**
    * **Allocation Matrix:** Current resources held by each process.
    * **Max Claim Matrix:** Maximum declared resource needs.
    * **Need Matrix:** Remaining resources needed ($\text{Need} = \text{Max} - \text{Allocation}$).
    * **Available Vector:** Free physical system resources.
  * **Dijkstra's Safety Algorithm:**
    * Simulates $\text{Work} = \text{Work} + \text{Allocation}_i$ to find safe sequence:
      $$\langle P_1 \to P_3 \to P_4 \to P_0 \to P_2 \rangle$$
    * **SYNC ON (Avoidance):** Unsafe requests rejected/delayed. Zero deadlocks.
    * **SYNC OFF (Naive Allocation):** Requests granted blindly, plunging system into an **Unsafe State** and permanent deadlock!
  * **Interactive Tester:** Live evaluation of custom request vectors with step-by-step kernel trace.
* **Quality Assurance & Verification:**
  * **13/13 Automated Unit Tests Passing** (`python -m unittest discover tests -v`) verifying locks, semaphores, condition variables, all 4 scenarios, and deadlock avoidance.

> 🗣️ **Member 5 Pitch Script:**  
> *"To answer Ma'am's question on how an OS actively avoids deadlocks, I implemented Dijkstra's Banker's Algorithm. Our simulator displays the textbook Allocation, Max, and Need matrices, and runs the Safety Algorithm in real time. In the interactive tester, when an unsafe request is submitted, the engine identifies that no safe sequence exists and rejects it, avoiding deadlock. We also wrote a test suite of 13 unit tests that verify every scenario and wrapper independently."*

---

## 👥 5-Member Team Role Summary Table

| Member | Title | Files Owned | Presentation Topic |
| :--- | :--- | :--- | :--- |
| **Member 1** | Team Lead & Sync Architecture | `sync/instrumented.py` | Project Intro, Real Threads vs JS, Primitives Instrumentation |
| **Member 2** | Thread Lifecycle & P-C Lead | `scenarios/base_scenario.py`, `producer_consumer.py` | Cooperative Reset, Stepping Semaphore, Bounded Buffer |
| **Member 3** | Deadlock Detection Lead | `scenarios/dining_philosophers.py` | Coffman 4 Conditions, Circular Wait, Dijkstra Hierarchy |
| **Member 4** | Readers-Writers Specialist | `scenarios/readers_writers.py` | Shared Mutex, Dirty Reads, Torn Reads Invariants |
| **Member 5** | Deadlock Avoidance & QA Lead | `scenarios/bankers_algorithm.py`, `app.py`, `tests/` | Banker's Algorithm, Safety Matrices, Testing Suite |

