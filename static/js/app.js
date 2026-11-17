/**
 * Multithreading & Synchronization Simulator - Client Logic
 * Handles real-time WebSockets, dynamic DOM rendering, scenario visualizers,
 * thread registry tracking, anomaly counters, and monospace event logging.
 */

(function () {
    "use strict";

    // Application State
    const state = {
        connected: false,
        isRunning: false,
        isPaused: false,
        isDeadlocked: false,
        scenario: "producer_consumer",
        syncMode: true,
        threadCount: 4,
        speed: 1.0,
        anomalyCount: 0,
        anomalyBreakdown: {
            overflow: 0,
            underflow: 0,
            dirtyRead: 0,
            tornRead: 0,
            writerConflict: 0
        },
        activeFilter: "all",
        autoScroll: true,
        threads: {},
        logCount: 0
    };

    // DOM Element References
    const DOM = {
        connectionStatus: document.getElementById("connectionStatus"),
        connectionText: document.getElementById("connectionText"),
        simulationStatusPill: document.getElementById("simulationStatusPill"),
        simStatusText: document.getElementById("simStatusText"),
        deadlockBanner: document.getElementById("deadlockBanner"),
        deadlockResetBtn: document.getElementById("deadlockResetBtn"),
        anomalyPanel: document.getElementById("anomalyPanel"),
        anomalyTotalCount: document.getElementById("anomalyTotalCount"),
        tagOverflow: document.getElementById("tagOverflow"),
        tagUnderflow: document.getElementById("tagUnderflow"),
        tagDirtyRead: document.getElementById("tagDirtyRead"),
        tagTornRead: document.getElementById("tagTornRead"),
        tagWriterConflict: document.getElementById("tagWriterConflict"),
        scenarioSelect: document.getElementById("scenarioSelect"),
        syncToggleBtn: document.getElementById("syncToggleBtn"),
        btnModeProblem: document.getElementById("btnModeProblem"),
        btnModeSolution: document.getElementById("btnModeSolution"),
        syncModeCaption: document.getElementById("syncModeCaption"),
        threadCountInput: document.getElementById("threadCountInput"),
        threadCountLabel: document.getElementById("threadCountLabel"),
        btnThreadDec: document.getElementById("btnThreadDec"),
        btnThreadInc: document.getElementById("btnThreadInc"),
        speedSlider: document.getElementById("speedSlider"),
        speedValue: document.getElementById("speedValue"),
        btnStart: document.getElementById("btnStart"),
        btnPause: document.getElementById("btnPause"),
        pauseBtnText: document.getElementById("pauseBtnText"),
        btnStep: document.getElementById("btnStep"),
        btnReset: document.getElementById("btnReset"),
        scenarioStageTitle: document.getElementById("scenarioStageTitle"),
        scenarioStageSubtitle: document.getElementById("scenarioStageSubtitle"),
        viewProducerConsumer: document.getElementById("viewProducerConsumer"),
        viewDiningPhilosophers: document.getElementById("viewDiningPhilosophers"),
        viewReadersWriters: document.getElementById("viewReadersWriters"),
        viewBankersAlgorithm: document.getElementById("viewBankersAlgorithm"),
        bankerSafetyStateBadge: document.getElementById("bankerSafetyStateBadge"),
        bankerStateText: document.getElementById("bankerStateText"),
        bankerSafeSeqText: document.getElementById("bankerSafeSeqText"),
        availValA: document.getElementById("availValA"),
        availValB: document.getElementById("availValB"),
        availValC: document.getElementById("availValC"),
        tbodyAllocation: document.getElementById("tbodyAllocation"),
        tbodyMax: document.getElementById("tbodyMax"),
        tbodyNeed: document.getElementById("tbodyNeed"),
        reqProcSelect: document.getElementById("reqProcSelect"),
        reqValA: document.getElementById("reqValA"),
        reqValB: document.getElementById("reqValB"),
        reqValC: document.getElementById("reqValC"),
        btnTestBankerReq: document.getElementById("btnTestBankerReq"),
        bankerTestResultBox: document.getElementById("bankerTestResultBox"),
        bufferOccupancy: document.getElementById("bufferOccupancy"),
        metricProduced: document.getElementById("metricProduced"),
        metricConsumed: document.getElementById("metricConsumed"),
        bufferFillBar: document.getElementById("bufferFillBar"),
        bufferSlotsContainer: document.getElementById("bufferSlotsContainer"),
        bufferInvariantBadge: document.getElementById("bufferInvariantBadge"),
        semEmptyVal: document.getElementById("semEmptyVal"),
        semFullVal: document.getElementById("semFullVal"),
        mutexLockVal: document.getElementById("mutexLockVal"),
        mutexHolderDesc: document.getElementById("mutexHolderDesc"),
        condVariablesVal: document.getElementById("condVariablesVal"),
        condWaitingDesc: document.getElementById("condWaitingDesc"),
        bufferLastActionText: document.getElementById("bufferLastActionText"),
        diningTableContainer: document.getElementById("diningTableContainer"),
        badgeCoffmanMutex: document.getElementById("badgeCoffmanMutex"),
        badgeCoffmanHoldWait: document.getElementById("badgeCoffmanHoldWait"),
        badgeCoffmanNoPreempt: document.getElementById("badgeCoffmanNoPreempt"),
        badgeCoffmanCircularWait: document.getElementById("badgeCoffmanCircularWait"),
        descCoffmanCircularWait: document.getElementById("descCoffmanCircularWait"),
        deadlockLikelihoodBadge: document.getElementById("deadlockLikelihoodBadge"),
        ragSvg: document.getElementById("ragSvg"),
        acquisitionOrderList: document.getElementById("acquisitionOrderList"),
        mealsProgressGrid: document.getElementById("mealsProgressGrid"),
        totalMealsCount: document.getElementById("totalMealsCount"),
        deadlockRiskVal: document.getElementById("deadlockRiskVal"),
        livenessGuaranteeBadge: document.getElementById("livenessGuaranteeBadge"),
        rwSyncInstruments: document.getElementById("rwSyncInstruments"),
        rwInvariantWriterMutex: document.getElementById("rwInvariantWriterMutex"),
        rwInvariantIntegrity: document.getElementById("rwInvariantIntegrity"),
        rwReadCountVal: document.getElementById("rwReadCountVal"),
        rwReadCountDesc: document.getElementById("rwReadCountDesc"),
        rwReadMutexVal: document.getElementById("rwReadMutexVal"),
        rwWriteLockVal: document.getElementById("rwWriteLockVal"),
        rwLockHolderDesc: document.getElementById("rwLockHolderDesc"),
        rwAccessStateVal: document.getElementById("rwAccessStateVal"),
        rwAccessStateDesc: document.getElementById("rwAccessStateDesc"),
        blockedReadersBadge: document.getElementById("blockedReadersBadge"),
        blockedReadersContainer: document.getElementById("blockedReadersContainer"),
        blockedWritersBadge: document.getElementById("blockedWritersBadge"),
        blockedWritersContainer: document.getElementById("blockedWritersContainer"),
        rwDirtyReadStat: document.getElementById("rwDirtyReadStat"),
        rwTornReadStat: document.getElementById("rwTornReadStat"),
        rwCollisionStat: document.getElementById("rwCollisionStat"),
        rwIntegrityStat: document.getElementById("rwIntegrityStat"),
        bankerWorkInitial: document.getElementById("bankerWorkInitial"),
        bankerStepByStepList: document.getElementById("bankerStepByStepList"),
        bankerUnsafeExplBox: document.getElementById("bankerUnsafeExplBox"),
        bankerUnsafeExplText: document.getElementById("bankerUnsafeExplText"),
        btnDemoSafeReq: document.getElementById("btnDemoSafeReq"),
        btnDemoUnsafeReq: document.getElementById("btnDemoUnsafeReq"),
        btnDemoExceedReq: document.getElementById("btnDemoExceedReq"),
        bankerRollbackBadge: document.getElementById("bankerRollbackBadge"),
        pcVerificationBadge: document.getElementById("pcVerificationBadge"),
        dpVerificationBadge: document.getElementById("dpVerificationBadge"),
        rwVerificationBadge: document.getElementById("rwVerificationBadge"),
        baVerificationBadge: document.getElementById("baVerificationBadge"),
        activeReadersCount: document.getElementById("activeReadersCount"),
        activeReadersContainer: document.getElementById("activeReadersContainer"),
        rwRecordCard: document.getElementById("rwRecordCard"),
        recordVersionBadge: document.getElementById("recordVersionBadge"),
        recordTitle: document.getElementById("recordTitle"),
        recordChecksumBadge: document.getElementById("recordChecksumBadge"),
        recordContent: document.getElementById("recordContent"),
        recordLastWriter: document.getElementById("recordLastWriter"),
        recordIntegrityStatus: document.getElementById("recordIntegrityStatus"),
        activeWritersCount: document.getElementById("activeWritersCount"),
        activeWritersContainer: document.getElementById("activeWritersContainer"),
        activeThreadCountBadge: document.getElementById("activeThreadCountBadge"),
        threadsGrid: document.getElementById("threadsGrid"),
        eventLogContainer: document.getElementById("eventLogContainer"),
        logEventCount: document.getElementById("logEventCount"),
        chkAutoScroll: document.getElementById("chkAutoScroll"),
        btnClearLog: document.getElementById("btnClearLog"),
        filterChips: document.querySelectorAll(".filter-chip")
    };

    // Initialize Socket.IO Client
    const socket = io({
        transports: ["polling", "websocket"],
        reconnection: true,
        reconnectionAttempts: 20,
        reconnectionDelay: 500
    });

    // =========================================================================
    // UI Helpers & Control Enable/Disable Logic
    // =========================================================================

    function updateConnectionUI(connected) {
        state.connected = connected;
        if (connected) {
            DOM.connectionStatus.className = "status-pill status-connected";
            DOM.connectionText.textContent = "LIVE WS";
        } else {
            DOM.connectionStatus.className = "status-pill status-disconnected";
            DOM.connectionText.textContent = "DISCONNECTED";
        }
    }

    function setSimulationRunning(running, paused = false, deadlocked = false) {
        state.isRunning = running;
        state.isPaused = paused;
        state.isDeadlocked = deadlocked;

        if (deadlocked) {
            DOM.simulationStatusPill.className = "status-pill status-deadlocked";
            DOM.simStatusText.textContent = "DEADLOCKED";
            DOM.deadlockBanner.classList.remove("hidden");
        } else if (running) {
            if (paused) {
                DOM.simulationStatusPill.className = "status-pill status-paused";
                DOM.simStatusText.textContent = "PAUSED";
            } else {
                DOM.simulationStatusPill.className = "status-pill status-running";
                DOM.simStatusText.textContent = "RUNNING";
            }
            DOM.deadlockBanner.classList.add("hidden");
        } else {
            DOM.simulationStatusPill.className = "status-pill status-idle";
            DOM.simStatusText.textContent = "IDLE";
            DOM.deadlockBanner.classList.add("hidden");
        }

        // Disable scenario / sync mode / thread count inputs while simulation is RUNNING
        const disableConfig = running && !paused;
        DOM.scenarioSelect.disabled = running;
        DOM.syncToggleBtn.disabled = running;
        if (DOM.btnModeProblem) DOM.btnModeProblem.disabled = running;
        if (DOM.btnModeSolution) DOM.btnModeSolution.disabled = running;
        DOM.btnThreadDec.disabled = running;
        DOM.btnThreadInc.disabled = running;

        // Button states
        DOM.btnStart.disabled = running;
        DOM.btnPause.disabled = !running;
        DOM.pauseBtnText.textContent = paused ? "Resume" : "Pause";
        DOM.btnStep.disabled = !paused;
        DOM.btnReset.disabled = !running && Object.keys(state.threads).length === 0;
    }

    function switchScenarioView(scenario) {
        state.scenario = scenario;
        DOM.scenarioSelect.value = scenario;

        // Hide all viewports
        DOM.viewProducerConsumer.classList.remove("active");
        DOM.viewDiningPhilosophers.classList.remove("active");
        DOM.viewReadersWriters.classList.remove("active");
        if (DOM.viewBankersAlgorithm) DOM.viewBankersAlgorithm.classList.remove("active");

        if (scenario === "producer_consumer") {
            DOM.viewProducerConsumer.classList.add("active");
            DOM.scenarioStageTitle.textContent = "Bounded Buffer Visualization";
            DOM.scenarioStageSubtitle.textContent = "Condition Variable & Mutex Monitoring";
            DOM.threadCountLabel.textContent = "Threads (Producers + Consumers)";
            if (!state.isRunning) setThreadCount(4);
            renderProducerConsumer({
                capacity: 5,
                buffer: [],
                total_produced: 0,
                total_consumed: 0,
                sem_empty: 5,
                sem_full: 0,
                sem_mutex: 1,
                last_sync_action: "Bounded buffer ready. Set mode and click Start."
            });
        } else if (scenario === "dining_philosophers") {
            DOM.viewDiningPhilosophers.classList.add("active");
            DOM.scenarioStageTitle.textContent = "Dining Philosophers Table";
            DOM.scenarioStageSubtitle.textContent = "Circular Wait & Deadlock Avoidance";
            DOM.threadCountLabel.textContent = "Philosophers (and Forks)";
            if (!state.isRunning) setThreadCount(5);
            renderDiningPhilosophers({
                num_philosophers: 5,
                philosopher_status: ["THINKING", "THINKING", "THINKING", "THINKING", "THINKING"],
                fork_owners: [-1, -1, -1, -1, -1],
                eat_counts: [0, 0, 0, 0, 0],
                total_meals: 0,
                rag: { allocation_edges: [], request_edges: [], cycle_detected: false },
                coffman: { mutual_exclusion: true, hold_and_wait: false, no_preemption: true, circular_wait: false },
                acquisition_orders: [
                    { phil: 0, first_fork: 0, second_fork: 1, rule: "min(0,1) then max(0,1)", is_asymmetric: false },
                    { phil: 1, first_fork: 1, second_fork: 2, rule: "min(1,2) then max(1,2)", is_asymmetric: false },
                    { phil: 2, first_fork: 2, second_fork: 3, rule: "min(2,3) then max(2,3)", is_asymmetric: false },
                    { phil: 3, first_fork: 3, second_fork: 4, rule: "min(3,4) then max(3,4)", is_asymmetric: false },
                    { phil: 4, first_fork: 0, second_fork: 4, rule: "min(0,4) then max(0,4) (ASYMMETRIC)", is_asymmetric: true }
                ]
            });
        } else if (scenario === "readers_writers") {
            DOM.viewReadersWriters.classList.add("active");
            DOM.scenarioStageTitle.textContent = "Shared Database Record";
            DOM.scenarioStageSubtitle.textContent = "Reader Preference vs Exclusive Writer Mutex";
            DOM.threadCountLabel.textContent = "Threads (Readers + Writers)";
            if (!state.isRunning) setThreadCount(5);
            renderReadersWriters({
                record: { title: "OS Concurrency Ledger", version: 1, checksum: 1, content: "Initial verified state", last_writer: "Genesis" },
                active_readers: [],
                active_writers: [],
                waiting_readers: [],
                waiting_writers: [],
                read_count: 0,
                read_count_mutex_locked: false,
                resource_write_lock_locked: false,
                resource_write_lock_holder: null,
                dirty_read_count: 0,
                torn_read_count: 0,
                collision_count: 0,
                invariant_writer_mutex: true,
                invariant_integrity: true
            });
        } else if (scenario === "bankers_algorithm") {
            DOM.viewBankersAlgorithm.classList.add("active");
            DOM.scenarioStageTitle.textContent = "Banker's Algorithm Matrix Lab";
            DOM.scenarioStageSubtitle.textContent = "Dijkstra's Deadlock Avoidance & Safety Algorithm";
            DOM.threadCountLabel.textContent = "Processes (P0 - P4)";
            if (!state.isRunning) setThreadCount(5);
            renderBankersAlgorithm({
                available: [3, 3, 2],
                allocation: [[0, 1, 0], [2, 0, 0], [3, 0, 2], [2, 1, 1], [0, 0, 2]],
                max_matrix: [[7, 5, 3], [3, 2, 2], [9, 0, 2], [2, 2, 2], [4, 3, 3]],
                need: [[7, 4, 3], [1, 2, 2], [6, 0, 0], [0, 1, 1], [4, 3, 1]],
                is_currently_safe: true,
                safe_sequence: [1, 3, 4, 0, 2],
                safety_step_details: [
                    { step: 1, process: 1, need: [1, 2, 2], work_before: [3, 3, 2], can_finish: true, alloc: [2, 0, 0], work_after: [5, 3, 2], finish_array: [false, true, false, false, false], safe_seq_so_far: [1] },
                    { step: 2, process: 3, need: [0, 1, 1], work_before: [5, 3, 2], can_finish: true, alloc: [2, 1, 1], work_after: [7, 4, 3], finish_array: [false, true, false, true, false], safe_seq_so_far: [1, 3] },
                    { step: 3, process: 4, need: [4, 3, 1], work_before: [7, 4, 3], can_finish: true, alloc: [0, 0, 2], work_after: [7, 4, 5], finish_array: [false, true, false, true, true], safe_seq_so_far: [1, 3, 4] },
                    { step: 4, process: 0, need: [7, 4, 3], work_before: [7, 4, 5], can_finish: true, alloc: [0, 1, 0], work_after: [7, 5, 5], finish_array: [true, true, false, true, true], safe_seq_so_far: [1, 3, 4, 0] },
                    { step: 5, process: 2, need: [6, 0, 0], work_before: [7, 5, 5], can_finish: true, alloc: [3, 0, 2], work_after: [10, 5, 7], finish_array: [true, true, true, true, true], safe_seq_so_far: [1, 3, 4, 0, 2] }
                ]
            });
        }
    }

    function setSyncMode(syncOn) {
        state.syncMode = syncOn;
        if (syncOn) {
            if (DOM.btnModeSolution) DOM.btnModeSolution.classList.add("active");
            if (DOM.btnModeProblem) DOM.btnModeProblem.classList.remove("active");
            DOM.syncToggleBtn.className = "sync-switch sync-on hidden";
            DOM.syncToggleBtn.setAttribute("aria-checked", "true");
            DOM.syncModeCaption.className = "sync-caption caption-safe";
            DOM.syncModeCaption.textContent = "Solution Mode: Active synchronization guarantees invariant preservation";
            DOM.anomalyPanel.classList.add("hidden");
        } else {
            if (DOM.btnModeSolution) DOM.btnModeSolution.classList.remove("active");
            if (DOM.btnModeProblem) DOM.btnModeProblem.classList.add("active");
            DOM.syncToggleBtn.className = "sync-switch sync-off hidden";
            DOM.syncToggleBtn.setAttribute("aria-checked", "false");
            DOM.syncModeCaption.className = "sync-caption caption-danger";
            DOM.syncModeCaption.textContent = "Problem Mode: Unsynchronized access exposes races, collisions & deadlocks";
            DOM.anomalyPanel.classList.remove("hidden");
        }
    }

    function setThreadCount(n) {
        state.threadCount = Math.max(2, Math.min(10, n));
        DOM.threadCountInput.value = state.threadCount;
    }

    function resetAnomalies() {
        state.anomalyCount = 0;
        state.anomalyBreakdown = {
            overflow: 0,
            underflow: 0,
            dirtyRead: 0,
            tornRead: 0,
            writerConflict: 0
        };
        DOM.anomalyTotalCount.textContent = "0";
        DOM.tagOverflow.textContent = "Overflows: 0";
        DOM.tagUnderflow.textContent = "Underflows: 0";
        DOM.tagDirtyRead.textContent = "Dirty Reads: 0";
        DOM.tagTornRead.textContent = "Torn Reads: 0";
        DOM.tagWriterConflict.textContent = "Write Collisions: 0";
    }

    function incrementAnomaly(anomalyType) {
        state.anomalyCount++;
        DOM.anomalyTotalCount.textContent = state.anomalyCount;
        DOM.anomalyTotalCount.style.transform = "scale(1.2)";
        setTimeout(() => {
            DOM.anomalyTotalCount.style.transform = "scale(1)";
        }, 150);

        if (anomalyType.includes("OVERFLOW")) {
            state.anomalyBreakdown.overflow++;
            DOM.tagOverflow.textContent = `Overflows: ${state.anomalyBreakdown.overflow}`;
        } else if (anomalyType.includes("UNDERFLOW")) {
            state.anomalyBreakdown.underflow++;
            DOM.tagUnderflow.textContent = `Underflows: ${state.anomalyBreakdown.underflow}`;
        } else if (anomalyType.includes("DIRTY_READ")) {
            state.anomalyBreakdown.dirtyRead++;
            DOM.tagDirtyRead.textContent = `Dirty Reads: ${state.anomalyBreakdown.dirtyRead}`;
        } else if (anomalyType.includes("TORN_READ")) {
            state.anomalyBreakdown.tornRead++;
            DOM.tagTornRead.textContent = `Torn Reads: ${state.anomalyBreakdown.tornRead}`;
        } else if (anomalyType.includes("WRITER")) {
            state.anomalyBreakdown.writerConflict++;
            DOM.tagWriterConflict.textContent = `Write Collisions: ${state.anomalyBreakdown.writerConflict}`;
        }
    }

    // =========================================================================
    // Scenario Visualizer Renderers
    // =========================================================================

    // 1. Producer-Consumer Bounded Buffer View
    function renderBufferSlots(capacity, bufferItems) {
        DOM.bufferSlotsContainer.innerHTML = "";
        const occupancy = bufferItems.length;
        DOM.bufferOccupancy.textContent = `${occupancy} / ${capacity}`;
        const pct = Math.min(100, Math.round((occupancy / capacity) * 100));
        DOM.bufferFillBar.style.width = `${pct}%`;
        if (occupancy > capacity) {
            DOM.bufferFillBar.style.background = "#ef4444";
        } else {
            DOM.bufferFillBar.style.background = "linear-gradient(90deg, #10b981, #06b6d4)";
        }

        const renderSlotsCount = Math.max(capacity, occupancy);
        for (let i = 0; i < renderSlotsCount; i++) {
            const slotEl = document.createElement("div");
            slotEl.className = "buffer-slot";

            const item = bufferItems[i];
            if (item) {
                if (i >= capacity) {
                    slotEl.classList.add("filled", "overflowed");
                } else {
                    slotEl.classList.add("filled");
                }
                slotEl.innerHTML = `
                    <span class="slot-index">[${i}] ${i >= capacity ? 'OVERFLOW!' : ''}</span>
                    <span class="slot-content">${item.value || item.id}</span>
                    <span class="slot-producer">${item.producer || ''}</span>
                `;
            } else {
                slotEl.innerHTML = `
                    <span class="slot-index">[${i}]</span>
                    <span class="slot-content" style="color: #475569;">[EMPTY]</span>
                    <span class="slot-producer">-</span>
                `;
            }
            DOM.bufferSlotsContainer.appendChild(slotEl);
        }
    }

    function renderProducerConsumer(data) {
        const capacity = data.capacity || 5;
        const bufferItems = data.buffer || [];
        renderBufferSlots(capacity, bufferItems);

        // Semaphores & Mutex HUD
        if (DOM.semEmptyVal) {
            DOM.semEmptyVal.textContent = data.sem_empty !== undefined ? data.sem_empty : Math.max(0, capacity - bufferItems.length);
        }
        if (DOM.semFullVal) {
            DOM.semFullVal.textContent = data.sem_full !== undefined ? data.sem_full : bufferItems.length;
        }
        if (DOM.mutexLockVal) {
            if (data.sem_mutex === 0) {
                DOM.mutexLockVal.textContent = "0 (LOCKED)";
                DOM.mutexLockVal.style.color = "#f59e0b";
                if (DOM.mutexHolderDesc) DOM.mutexHolderDesc.textContent = `Held by ${data.mutex_holder || 'worker thread'}`;
            } else {
                DOM.mutexLockVal.textContent = "1 (FREE)";
                DOM.mutexLockVal.style.color = "var(--state-running)";
                if (DOM.mutexHolderDesc) DOM.mutexHolderDesc.textContent = "Critical section unlocked";
            }
        }

        // Condition Variables
        const waitP = data.waiting_producers || 0;
        const waitC = data.waiting_consumers || 0;
        if (DOM.condVariablesVal) {
            if (waitP > 0 || waitC > 0) {
                DOM.condVariablesVal.textContent = "WAITING";
                DOM.condVariablesVal.style.color = "#a78bfa";
            } else {
                DOM.condVariablesVal.textContent = "Ready";
                DOM.condVariablesVal.style.color = "var(--text-primary)";
            }
        }
        if (DOM.condWaitingDesc) {
            DOM.condWaitingDesc.textContent = `Waiting: ${waitP} producers, ${waitC} consumers`;
        }

        // Buffer Boundary Invariant: 0 <= buffer_size <= capacity
        const invariantPreserved = data.invariant_preserved !== undefined ? data.invariant_preserved : (bufferItems.length <= capacity && bufferItems.length >= 0);
        if (DOM.bufferInvariantBadge) {
            if (invariantPreserved) {
                DOM.bufferInvariantBadge.className = "invariant-pill invariant-ok font-mono";
                DOM.bufferInvariantBadge.textContent = `Invariant: 0 ≤ Size (${bufferItems.length}) ≤ ${capacity} [PRESERVED]`;
            } else {
                DOM.bufferInvariantBadge.className = "invariant-pill invariant-violated font-mono";
                DOM.bufferInvariantBadge.textContent = `Invariant: VIOLATED (Size=${bufferItems.length} > ${capacity})`;
            }
        }

        if (DOM.pcVerificationBadge) {
            if (invariantPreserved) {
                DOM.pcVerificationBadge.className = "verification-status-badge status-verified-green font-mono";
                DOM.pcVerificationBadge.textContent = "Verification: PASS (0 ≤ Size ≤ Capacity Guaranteed)";
            } else {
                DOM.pcVerificationBadge.className = "verification-status-badge status-verified-red font-mono";
                DOM.pcVerificationBadge.textContent = "Verification: FAIL (Buffer Boundary Violated)";
            }
        }

        if (DOM.bufferLastActionText && data.last_sync_action) {
            DOM.bufferLastActionText.textContent = data.last_sync_action;
        }
    }

    // 2. Dining Philosophers Table, Coffman HUD, RAG, and Hierarchy View
    function renderDiningTable(numPhilosophers, philosopherStatus, forkOwners, eatCounts) {
        const existingDynamic = DOM.diningTableContainer.querySelectorAll(".philosopher-node, .fork-item");
        existingDynamic.forEach(el => el.remove());

        const radiusPhil = 145; // px from center
        const radiusFork = 92;  // px from center
        const centerX = 190;
        const centerY = 190;

        for (let i = 0; i < numPhilosophers; i++) {
            const angle = (i * 2 * Math.PI) / numPhilosophers - Math.PI / 2;
            const px = centerX + radiusPhil * Math.cos(angle) - 30;
            const py = centerY + radiusPhil * Math.sin(angle) - 30;

            const status = philosopherStatus[i] || "THINKING";
            const meals = eatCounts[i] || 0;

            const philEl = document.createElement("div");
            philEl.className = "philosopher-node";
            philEl.id = `phil-node-${i}`;
            philEl.style.left = `${px}px`;
            philEl.style.top = `${py}px`;

            let statusLabel = "THK";
            if (status === "THINKING") {
                philEl.classList.add("state-thinking");
                statusLabel = "THK";
            } else if (status === "HUNGRY") {
                philEl.classList.add("state-hungry");
                statusLabel = "HUN";
            } else if (status === "WAITING_FOR_FORK") {
                philEl.classList.add("state-waiting-fork");
                statusLabel = "BLK";
            } else if (status === "EATING") {
                philEl.classList.add("state-eating");
                statusLabel = "EAT";
            }

            philEl.innerHTML = `
                <span class="phil-icon font-mono">${statusLabel}</span>
                <span class="phil-name">P${i} [${meals}]</span>
            `;
            philEl.title = `Philosopher ${i}: ${status} | Meals: ${meals}`;
            DOM.diningTableContainer.appendChild(philEl);

            // Fork positioning (between philosopher i and (i+1)%numPhilosophers)
            const forkAngle = angle + Math.PI / numPhilosophers;
            const fx = centerX + radiusFork * Math.cos(forkAngle) - 13;
            const fy = centerY + radiusFork * Math.sin(forkAngle) - 13;

            const forkOwner = forkOwners[i] !== undefined ? forkOwners[i] : -1;
            const forkEl = document.createElement("div");
            forkEl.className = "fork-item";
            forkEl.id = `fork-item-${i}`;
            forkEl.style.left = `${fx}px`;
            forkEl.style.top = `${fy}px`;

            if (forkOwner === -1) {
                forkEl.classList.add("fork-free");
                forkEl.textContent = `F${i}`;
                forkEl.title = `Fork ${i}: On Table (Free)`;
            } else {
                forkEl.classList.add("fork-held");
                forkEl.textContent = `F${i}`;
                forkEl.title = `Fork ${i}: Held by Philosopher ${forkOwner}`;
            }
            DOM.diningTableContainer.appendChild(forkEl);
        }
    }

    function updateCoffmanCard(badgeEl, isMet, metText, unmetText) {
        if (!badgeEl) return;
        if (isMet) {
            badgeEl.className = "coffman-status-badge badge-active";
            badgeEl.textContent = metText;
        } else {
            badgeEl.className = "coffman-status-badge badge-inactive";
            badgeEl.textContent = unmetText;
        }
    }

    function renderRAG(rag) {
        if (!DOM.ragSvg) return;
        const svg = DOM.ragSvg;
        svg.innerHTML = `
            <defs>
                <marker id="arrow-blue" viewBox="0 0 10 10" refX="16" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                    <path d="M 0 1 L 10 5 L 0 9 z" fill="#3b82f6" />
                </marker>
                <marker id="arrow-orange" viewBox="0 0 10 10" refX="16" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                    <path d="M 0 1 L 10 5 L 0 9 z" fill="#f59e0b" />
                </marker>
                <marker id="arrow-red" viewBox="0 0 10 10" refX="16" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                    <path d="M 0 1 L 10 5 L 0 9 z" fill="#ef4444" />
                </marker>
            </defs>
        `;

        const numNodes = 5;
        const pCoords = [];
        const fCoords = [];
        const startX = 76;
        const spacingX = 152;
        const py = 45;
        const fy = 155;

        for (let i = 0; i < numNodes; i++) {
            pCoords.push({ x: startX + i * spacingX, y: py });
            fCoords.push({ x: startX + i * spacingX, y: fy });
        }

        const isCycle = rag && rag.cycle_detected;

        // Draw Allocation Edges: Fork -> Philosopher (Solid Blue / Red if Cycle)
        const allocEdges = (rag && rag.allocation_edges) || [];
        allocEdges.forEach(edge => {
            const from = fCoords[edge.from];
            const to = pCoords[edge.to];
            if (from && to) {
                const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
                line.setAttribute("x1", from.x);
                line.setAttribute("y1", from.y);
                line.setAttribute("x2", to.x);
                line.setAttribute("y2", to.y);
                line.setAttribute("stroke", isCycle ? "#ef4444" : "#3b82f6");
                line.setAttribute("stroke-width", isCycle ? "2.5" : "1.8");
                line.setAttribute("marker-end", isCycle ? "url(#arrow-red)" : "url(#arrow-blue)");
                svg.appendChild(line);
            }
        });

        // Draw Request Edges: Philosopher -> Fork (Dashed Orange / Solid Red if Cycle)
        const reqEdges = (rag && rag.request_edges) || [];
        reqEdges.forEach(edge => {
            const from = pCoords[edge.from];
            const to = fCoords[edge.to];
            if (from && to) {
                const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
                line.setAttribute("x1", from.x);
                line.setAttribute("y1", from.y);
                line.setAttribute("x2", to.x);
                line.setAttribute("y2", to.y);
                line.setAttribute("stroke", isCycle ? "#ef4444" : "#f59e0b");
                line.setAttribute("stroke-width", isCycle ? "2.5" : "1.8");
                if (!isCycle) line.setAttribute("stroke-dasharray", "4,3");
                line.setAttribute("marker-end", isCycle ? "url(#arrow-red)" : "url(#arrow-orange)");
                svg.appendChild(line);
            }
        });

        // Draw Process Nodes (Circles on Top)
        pCoords.forEach((pt, i) => {
            const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
            const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
            circle.setAttribute("cx", pt.x);
            circle.setAttribute("cy", pt.y);
            circle.setAttribute("r", "16");
            circle.setAttribute("fill", isCycle ? "rgba(239, 68, 68, 0.2)" : "#182030");
            circle.setAttribute("stroke", isCycle ? "#ef4444" : "#38bdf8");
            circle.setAttribute("stroke-width", "1.8");

            const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
            text.setAttribute("x", pt.x);
            text.setAttribute("y", pt.y + 4);
            text.setAttribute("text-anchor", "middle");
            text.setAttribute("fill", "#ffffff");
            text.setAttribute("font-family", "ui-monospace, monospace");
            text.setAttribute("font-size", "11px");
            text.setAttribute("font-weight", "bold");
            text.textContent = `P${i}`;

            g.appendChild(circle);
            g.appendChild(text);
            svg.appendChild(g);
        });

        // Draw Resource Nodes (Squares on Bottom)
        fCoords.forEach((pt, i) => {
            const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
            const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
            rect.setAttribute("x", pt.x - 14);
            rect.setAttribute("y", pt.y - 14);
            rect.setAttribute("width", "28");
            rect.setAttribute("height", "28");
            rect.setAttribute("rx", "4");
            rect.setAttribute("fill", "#182030");
            rect.setAttribute("stroke", isCycle ? "#ef4444" : "#a855f7");
            rect.setAttribute("stroke-width", "1.8");

            const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
            text.setAttribute("x", pt.x);
            text.setAttribute("y", pt.y + 4);
            text.setAttribute("text-anchor", "middle");
            text.setAttribute("fill", "#ffffff");
            text.setAttribute("font-family", "ui-monospace, monospace");
            text.setAttribute("font-size", "11px");
            text.setAttribute("font-weight", "bold");
            text.textContent = `F${i}`;

            g.appendChild(rect);
            g.appendChild(text);
            svg.appendChild(g);
        });
    }

    function renderHierarchyMonitor(data) {
        if (DOM.acquisitionOrderList && data.acquisition_orders) {
            DOM.acquisitionOrderList.innerHTML = "";
            data.acquisition_orders.forEach(ao => {
                const pill = document.createElement("div");
                pill.className = `acquisition-pill ${ao.is_asymmetric ? 'asymmetric-pill' : ''}`;
                pill.innerHTML = `
                    <span><strong>P${ao.phil}:</strong> Acquire F${ao.first_fork} then F${ao.second_fork}</span>
                    <span style="color: ${ao.is_asymmetric ? 'var(--accent-cyan)' : 'var(--text-muted)'}; font-size: 0.62rem;">${ao.rule}</span>
                `;
                DOM.acquisitionOrderList.appendChild(pill);
            });
        }

        if (DOM.mealsProgressGrid && data.eat_counts) {
            DOM.mealsProgressGrid.innerHTML = "";
            data.eat_counts.forEach((cnt, idx) => {
                const box = document.createElement("div");
                box.className = "meal-bar-item";
                box.innerHTML = `
                    <div class="meal-bar-pname">P${idx}</div>
                    <div class="meal-bar-count font-mono">${cnt}</div>
                `;
                DOM.mealsProgressGrid.appendChild(box);
            });
        }

        if (DOM.totalMealsCount && data.total_meals !== undefined) {
            DOM.totalMealsCount.textContent = data.total_meals;
        }

        if (DOM.deadlockRiskVal) {
            if (data.is_deadlocked) {
                DOM.deadlockRiskVal.textContent = "100.0% (DEADLOCK)";
                DOM.deadlockRiskVal.style.color = "var(--state-blocked)";
            } else if (state.syncMode) {
                DOM.deadlockRiskVal.textContent = "0.0% (Guaranteed Safe)";
                DOM.deadlockRiskVal.style.color = "var(--state-running)";
            } else {
                DOM.deadlockRiskVal.textContent = "High (Symmetric Contention)";
                DOM.deadlockRiskVal.style.color = "#f59e0b";
            }
        }

        if (DOM.livenessGuaranteeBadge) {
            if (state.syncMode) {
                DOM.livenessGuaranteeBadge.className = "invariant-pill invariant-ok font-mono";
                DOM.livenessGuaranteeBadge.textContent = "Progress Guaranteed (Circular Wait Broken)";
            } else if (data.is_deadlocked) {
                DOM.livenessGuaranteeBadge.className = "invariant-pill invariant-violated font-mono";
                DOM.livenessGuaranteeBadge.textContent = "Progress Blocked (Circular Wait Formed)";
            } else {
                DOM.livenessGuaranteeBadge.className = "invariant-pill font-mono";
                DOM.livenessGuaranteeBadge.style.background = "rgba(245, 158, 11, 0.2)";
                DOM.livenessGuaranteeBadge.style.color = "#fbbf24";
                DOM.livenessGuaranteeBadge.textContent = "Symmetric Contention Active";
            }
        }
    }

    function renderDiningPhilosophers(data) {
        renderDiningTable(
            data.num_philosophers || state.threadCount,
            data.philosopher_status || [],
            data.fork_owners || [],
            data.eat_counts || []
        );

        // Coffman Conditions Evaluation
        if (data.coffman) {
            const c = data.coffman;
            const isMutexMet = typeof c.mutual_exclusion === 'object' ? !!c.mutual_exclusion.met : !!c.mutual_exclusion;
            const isHoldWaitMet = typeof c.hold_and_wait === 'object' ? !!c.hold_and_wait.met : !!c.hold_and_wait;
            const isNoPreemptMet = typeof c.no_preemption === 'object' ? !!c.no_preemption.met : !!c.no_preemption;
            const isCircularWaitMet = typeof c.circular_wait === 'object' ? !!c.circular_wait.met : !!c.circular_wait;
            const isAllMet = c.all_met !== undefined ? !!c.all_met : (isMutexMet && isHoldWaitMet && isNoPreemptMet && isCircularWaitMet);

            updateCoffmanCard(DOM.badgeCoffmanMutex, isMutexMet, "MET", "FREE");
            updateCoffmanCard(DOM.badgeCoffmanHoldWait, isHoldWaitMet, "MET (1 Fork Held)", "INACTIVE");
            updateCoffmanCard(DOM.badgeCoffmanNoPreempt, isNoPreemptMet, "MET", "PREEMPTIBLE");
            updateCoffmanCard(DOM.badgeCoffmanCircularWait, isCircularWaitMet, "CYCLE ACTIVE", "BROKEN (Hierarchy)");

            if (DOM.descCoffmanCircularWait) {
                if (isCircularWaitMet) {
                    DOM.descCoffmanCircularWait.textContent = "Closed wait cycle formed: all philosophers holding 1 fork, blocked on neighbor.";
                    DOM.descCoffmanCircularWait.style.color = "#f87171";
                } else {
                    DOM.descCoffmanCircularWait.textContent = "Asymmetric fork hierarchy (min fork first) prevents circular dependency.";
                    DOM.descCoffmanCircularWait.style.color = "var(--text-secondary)";
                }
            }

            if (DOM.deadlockLikelihoodBadge) {
                if (isAllMet || data.is_deadlocked) {
                    DOM.deadlockLikelihoodBadge.className = "invariant-pill invariant-violated font-mono";
                    DOM.deadlockLikelihoodBadge.textContent = "Coffman Conditions: 4/4 MET (DEADLOCKED)";
                } else if (isHoldWaitMet) {
                    DOM.deadlockLikelihoodBadge.className = "invariant-pill font-mono";
                    DOM.deadlockLikelihoodBadge.style.background = "rgba(245, 158, 11, 0.2)";
                    DOM.deadlockLikelihoodBadge.style.color = "#fbbf24";
                    DOM.deadlockLikelihoodBadge.textContent = "Hold & Wait Detected (Contention)";
                } else {
                    DOM.deadlockLikelihoodBadge.className = "invariant-pill invariant-ok font-mono";
                    DOM.deadlockLikelihoodBadge.textContent = "Progress Guaranteed (Circular Wait Broken)";
                }
            }
        }

        if (DOM.dpVerificationBadge) {
            const isCircularWait = data.coffman && (typeof data.coffman.circular_wait === 'object' ? data.coffman.circular_wait.met : data.coffman.circular_wait);
            if (!data.is_deadlocked && !isCircularWait) {
                DOM.dpVerificationBadge.className = "verification-status-badge status-verified-green font-mono";
                DOM.dpVerificationBadge.textContent = "Verification: PASS (Deadlock-Free, Circular Wait Broken)";
            } else {
                DOM.dpVerificationBadge.className = "verification-status-badge status-verified-red font-mono";
                DOM.dpVerificationBadge.textContent = "Verification: FAIL (Deadlock Cycle Formed)";
            }
        }

        renderRAG(data.rag);
        renderHierarchyMonitor(data);
    }

    // 3. Readers-Writers Shared Record, Blocked Queues, and Mutex Invariants
    function renderReadersWriters(data) {
        const record = data.record || {};
        const activeReaders = data.active_readers || [];
        const activeWriters = data.active_writers || [];
        const waitingReaders = data.waiting_readers || [];
        const waitingWriters = data.waiting_writers || [];

        DOM.activeReadersCount.textContent = activeReaders.length;
        DOM.activeWritersCount.textContent = activeWriters.length;

        // Active Readers Tags
        DOM.activeReadersContainer.innerHTML = "";
        if (activeReaders.length === 0) {
            DOM.activeReadersContainer.innerHTML = '<span class="empty-hint">No active readers</span>';
        } else {
            activeReaders.forEach(reader => {
                const tag = document.createElement("span");
                tag.className = "rw-tag rw-tag-reader";
                tag.textContent = reader;
                DOM.activeReadersContainer.appendChild(tag);
            });
        }

        // Active Writers Tags
        DOM.activeWritersContainer.innerHTML = "";
        if (activeWriters.length === 0) {
            DOM.activeWritersContainer.innerHTML = '<span class="empty-hint">No active writers</span>';
        } else {
            activeWriters.forEach(writer => {
                const tag = document.createElement("span");
                tag.className = "rw-tag rw-tag-writer";
                tag.textContent = writer;
                DOM.activeWritersContainer.appendChild(tag);
            });
        }

        // Waiting / Blocked Queues
        if (DOM.blockedReadersBadge) DOM.blockedReadersBadge.textContent = `${waitingReaders.length} waiting`;
        if (DOM.blockedReadersContainer) {
            DOM.blockedReadersContainer.innerHTML = "";
            if (waitingReaders.length === 0) {
                DOM.blockedReadersContainer.innerHTML = '<span class="empty-hint">No readers waiting for lock</span>';
            } else {
                waitingReaders.forEach(r => {
                    const tag = document.createElement("span");
                    tag.className = "queue-tag queue-tag-reader";
                    tag.textContent = r;
                    DOM.blockedReadersContainer.appendChild(tag);
                });
            }
        }

        if (DOM.blockedWritersBadge) DOM.blockedWritersBadge.textContent = `${waitingWriters.length} waiting`;
        if (DOM.blockedWritersContainer) {
            DOM.blockedWritersContainer.innerHTML = "";
            if (waitingWriters.length === 0) {
                DOM.blockedWritersContainer.innerHTML = '<span class="empty-hint">No writers waiting for lock</span>';
            } else {
                waitingWriters.forEach(w => {
                    const tag = document.createElement("span");
                    tag.className = "queue-tag queue-tag-writer";
                    tag.textContent = w;
                    DOM.blockedWritersContainer.appendChild(tag);
                });
            }
        }

        // Sync Instruments
        if (DOM.rwReadCountVal) DOM.rwReadCountVal.textContent = data.read_count !== undefined ? data.read_count : activeReaders.length;
        if (DOM.rwReadMutexVal) {
            DOM.rwReadMutexVal.textContent = data.read_count_mutex_locked ? "0 (LOCKED)" : "1 (FREE)";
            DOM.rwReadMutexVal.style.color = data.read_count_mutex_locked ? "#f59e0b" : "var(--state-running)";
        }
        if (DOM.rwWriteLockVal) {
            DOM.rwWriteLockVal.textContent = data.resource_write_lock_locked ? "0 (LOCKED)" : "1 (FREE)";
            DOM.rwWriteLockVal.style.color = data.resource_write_lock_locked ? "var(--state-blocked)" : "var(--state-running)";
        }
        if (DOM.rwLockHolderDesc) {
            DOM.rwLockHolderDesc.textContent = data.resource_write_lock_holder ? `Held by ${data.resource_write_lock_holder}` : "Exclusive lock for writers (Free)";
        }

        // Shared Resource Access State
        if (DOM.rwAccessStateVal) {
            if (activeWriters.length > 1) {
                DOM.rwAccessStateVal.textContent = "WRITE COLLISION";
                DOM.rwAccessStateVal.style.color = "var(--state-blocked)";
                if (DOM.rwAccessStateDesc) DOM.rwAccessStateDesc.textContent = "Multiple writers writing simultaneously!";
            } else if (activeWriters.length === 1 && activeReaders.length > 0) {
                DOM.rwAccessStateVal.textContent = "TORN READ CONFLICT";
                DOM.rwAccessStateVal.style.color = "var(--state-blocked)";
                if (DOM.rwAccessStateDesc) DOM.rwAccessStateDesc.textContent = "Reader reading while writer modifies record!";
            } else if (activeWriters.length === 1) {
                DOM.rwAccessStateVal.textContent = "EXCLUSIVE WRITE";
                DOM.rwAccessStateVal.style.color = "#fbbf24";
                if (DOM.rwAccessStateDesc) DOM.rwAccessStateDesc.textContent = "Single writer in critical section";
            } else if (activeReaders.length > 0) {
                DOM.rwAccessStateVal.textContent = `SHARED READ (${activeReaders.length})`;
                DOM.rwAccessStateVal.style.color = "var(--state-running)";
                if (DOM.rwAccessStateDesc) DOM.rwAccessStateDesc.textContent = "Multiple concurrent readers allowed";
            } else {
                DOM.rwAccessStateVal.textContent = "IDLE";
                DOM.rwAccessStateVal.style.color = "var(--text-primary)";
                if (DOM.rwAccessStateDesc) DOM.rwAccessStateDesc.textContent = "No active reader or writer";
            }
        }

        // Invariants
        const writerMutexPreserved = data.invariant_writer_mutex !== undefined ? data.invariant_writer_mutex : (activeWriters.length <= 1 && (!activeWriters.length || !activeReaders.length));
        if (DOM.rwInvariantWriterMutex) {
            if (writerMutexPreserved) {
                DOM.rwInvariantWriterMutex.className = "invariant-pill invariant-ok font-mono";
                DOM.rwInvariantWriterMutex.textContent = "Writer Mutex: PRESERVED";
            } else {
                DOM.rwInvariantWriterMutex.className = "invariant-pill invariant-violated font-mono";
                DOM.rwInvariantWriterMutex.textContent = "Writer Mutex: VIOLATED (Conflict)";
            }
        }

        const integrityPreserved = data.invariant_integrity !== undefined ? data.invariant_integrity : (record.version === record.checksum);
        if (DOM.rwInvariantIntegrity) {
            if (integrityPreserved) {
                DOM.rwInvariantIntegrity.className = "invariant-pill invariant-ok font-mono";
                DOM.rwInvariantIntegrity.textContent = "Data Integrity: Checksum Valid";
            } else {
                DOM.rwInvariantIntegrity.className = "invariant-pill invariant-violated font-mono";
                DOM.rwInvariantIntegrity.textContent = "Data Integrity: CORRUPTED (Torn Read)";
            }
        }

        if (DOM.rwVerificationBadge) {
            if (integrityPreserved && writerMutexPreserved) {
                DOM.rwVerificationBadge.className = "verification-status-badge status-verified-green font-mono";
                DOM.rwVerificationBadge.textContent = "Verification: PASS (Version == Checksum, Mutex Intact)";
            } else {
                DOM.rwVerificationBadge.className = "verification-status-badge status-verified-red font-mono";
                DOM.rwVerificationBadge.textContent = "Verification: FAIL (Torn Read / Write Collision)";
            }
        }

        // Diagnostics Strip
        if (DOM.rwDirtyReadStat) DOM.rwDirtyReadStat.textContent = data.dirty_read_count || 0;
        if (DOM.rwTornReadStat) DOM.rwTornReadStat.textContent = data.torn_read_count || 0;
        if (DOM.rwCollisionStat) DOM.rwCollisionStat.textContent = data.collision_count || 0;
        if (DOM.rwIntegrityStat) {
            DOM.rwIntegrityStat.textContent = integrityPreserved ? "100% OK" : "CORRUPT";
            DOM.rwIntegrityStat.className = integrityPreserved ? "integrity-ok font-mono" : "integrity-corrupt font-mono";
        }

        // Record Display
        DOM.recordVersionBadge.textContent = `v${record.version || 1}`;
        DOM.recordTitle.textContent = record.title || "OS Concurrency Ledger";
        DOM.recordChecksumBadge.textContent = `Checksum: ${record.checksum !== undefined ? record.checksum : record.version}`;
        DOM.recordContent.textContent = record.content || "";
        DOM.recordLastWriter.textContent = record.last_writer || "System";

        if (integrityPreserved && writerMutexPreserved) {
            DOM.recordIntegrityStatus.className = "integrity-tag integrity-ok";
            DOM.recordIntegrityStatus.textContent = "State: Consistent";
            DOM.rwRecordCard.classList.remove("glitch-anomaly");
        } else {
            DOM.recordIntegrityStatus.className = "integrity-tag integrity-corrupt";
            DOM.recordIntegrityStatus.textContent = "State: TORN / DIRTY READ";
            DOM.rwRecordCard.classList.add("glitch-anomaly");
        }
    }

    // 4. Banker's Algorithm Matrix Lab, Safety Steps, and Rollback View
    function renderBankersAlgorithm(data) {
        if (!DOM.viewBankersAlgorithm) return;

        // Safety State Badge
        const isSafe = data.is_currently_safe !== undefined ? data.is_currently_safe : true;
        if (isSafe) {
            DOM.bankerSafetyStateBadge.className = "safety-state-badge badge-safe";
            DOM.bankerStateText.textContent = "SAFE STATE";
        } else {
            DOM.bankerSafetyStateBadge.className = "safety-state-badge badge-unsafe";
            DOM.bankerStateText.textContent = "UNSAFE STATE (DEADLOCK RISK)";
        }

        if (DOM.baVerificationBadge) {
            if (isSafe) {
                DOM.baVerificationBadge.className = "verification-status-badge status-verified-green font-mono";
                DOM.baVerificationBadge.textContent = "Verification: PASS (Safe State Verified)";
            } else {
                DOM.baVerificationBadge.className = "verification-status-badge status-verified-red font-mono";
                DOM.baVerificationBadge.textContent = "Verification: FAIL (Unsafe State Diagnosed)";
            }
        }

        // Safe Sequence Nodes
        if (data.safe_sequence && data.safe_sequence.length > 0) {
            const seqStr = data.safe_sequence.map(p => `P${p}`).join(" → ");
            DOM.bankerSafeSeqText.textContent = `< ${seqStr} >`;
            DOM.bankerSafeSeqText.style.color = "var(--accent-cyan)";
        } else {
            DOM.bankerSafeSeqText.textContent = "< None (No Safe Execution Order) >";
            DOM.bankerSafeSeqText.style.color = "#ef4444";
        }

        // Available Vector
        if (data.available && data.available.length >= 3) {
            DOM.availValA.textContent = data.available[0];
            DOM.availValB.textContent = data.available[1];
            DOM.availValC.textContent = data.available[2];
            if (DOM.bankerWorkInitial) {
                DOM.bankerWorkInitial.textContent = `Init: Work = Available [${data.available.join(', ')}] | Finish = [F, F, F, F, F]`;
            }
        }

        // Matrix Tables: Allocation, Max, Need
        function populateMatrixTable(tbodyEl, matrixData) {
            if (!tbodyEl || !matrixData) return;
            tbodyEl.innerHTML = "";
            matrixData.forEach((row, idx) => {
                const tr = document.createElement("tr");
                tr.innerHTML = `
                    <td class="proc-name-cell">P${idx}</td>
                    <td>${row[0]}</td>
                    <td>${row[1]}</td>
                    <td>${row[2]}</td>
                `;
                tbodyEl.appendChild(tr);
            });
        }

        populateMatrixTable(DOM.tbodyAllocation, data.allocation);
        populateMatrixTable(DOM.tbodyMax, data.max_matrix);
        populateMatrixTable(DOM.tbodyNeed, data.need);

        // Step-by-Step Safety Algorithm Visualization
        if (DOM.bankerStepByStepList) {
            DOM.bankerStepByStepList.innerHTML = "";
            if (data.safety_step_details && data.safety_step_details.length > 0) {
                data.safety_step_details.forEach(step => {
                    const item = document.createElement("div");
                    item.className = "safety-step-item step-passed";
                    const seqStr = step.safe_seq_so_far.map(p => `P${p}`).join(" → ");
                    item.innerHTML = `
                        <span class="step-num">Step ${step.step}</span>
                        <span class="step-proc-badge">Process P${step.process}</span>
                        <span class="step-calc">Need [${step.need.join(',')}] ≤ Work [${step.work_before.join(',')}]</span>
                        <span class="step-work-after">Reclaim Alloc [${step.alloc.join(',')}] → New Work [${step.work_after.join(',')}]</span>
                        <span class="step-seq-cum">&lt; ${seqStr} &gt;</span>
                    `;
                    DOM.bankerStepByStepList.appendChild(item);
                });
            } else if (data.safety_trace && data.safety_trace.length > 0) {
                data.safety_trace.forEach((tr, i) => {
                    const item = document.createElement("div");
                    item.className = "safety-step-item step-passed";
                    item.innerHTML = `<span style="grid-column: 1 / -1;">${tr}</span>`;
                    DOM.bankerStepByStepList.appendChild(item);
                });
            }
        }

        // Unsafe Explanation Box
        if (DOM.bankerUnsafeExplBox) {
            if (!isSafe && data.unsafe_explanation) {
                DOM.bankerUnsafeExplBox.classList.remove("hidden");
                if (DOM.bankerUnsafeExplText) {
                    DOM.bankerUnsafeExplText.textContent = data.unsafe_explanation;
                }
            } else {
                DOM.bankerUnsafeExplBox.classList.add("hidden");
            }
        }

        // Tentative Allocation & Rollback Status
        if (DOM.bankerRollbackBadge && data.last_evaluated_request) {
            const req = data.last_evaluated_request;
            if (req.was_rolled_back) {
                DOM.bankerRollbackBadge.className = "invariant-pill invariant-violated font-mono";
                DOM.bankerRollbackBadge.textContent = `Rollback: P${req.process_id} request reverted (Unsafe state avoided)`;
            } else {
                DOM.bankerRollbackBadge.className = "invariant-pill invariant-ok font-mono";
                DOM.bankerRollbackBadge.textContent = `Rollback: None (P${req.process_id} request safely committed)`;
            }
        }
    }

    // =========================================================================
    // Thread Registry Cards
    // =========================================================================

    function renderThreadsGrid() {
        const threadNames = Object.keys(state.threads);
        DOM.activeThreadCountBadge.textContent = `${threadNames.length} Threads`;

        if (threadNames.length === 0) {
            DOM.threadsGrid.innerHTML = `
                <div class="threads-empty-state">
                    <p>No active simulation threads. Click <strong>Start</strong> to launch server threads.</p>
                </div>
            `;
            return;
        }

        DOM.threadsGrid.innerHTML = "";
        threadNames.sort().forEach(name => {
            const thread = state.threads[name];
            const card = document.createElement("div");
            card.className = "thread-card";

            const stateCode = (thread.state || "RUNNING").charAt(0).toUpperCase();
            let badgeClass = "state-badge-R";
            if (stateCode === "W") badgeClass = "state-badge-W";
            else if (stateCode === "B") badgeClass = "state-badge-B";
            else if (stateCode === "F") badgeClass = "state-badge-F";

            card.innerHTML = `
                <div class="thread-left-info">
                    <div class="thread-name-line">
                        <span class="thread-name">${thread.name}</span>
                        <span class="thread-role-badge">${thread.role || "Worker"}</span>
                    </div>
                    <span class="thread-action" title="${thread.action || ''}">${thread.action || 'Idle'}</span>
                </div>
                <div class="thread-right-status">
                    <span class="state-badge ${badgeClass}">
                        <span class="legend-dot ${stateCode === 'R' ? 'dot-running' : stateCode === 'W' ? 'dot-waiting' : stateCode === 'B' ? 'dot-blocked' : 'dot-finished'}"></span>
                        [${stateCode}] ${thread.state}
                    </span>
                </div>
            `;
            DOM.threadsGrid.appendChild(card);
        });
    }

    // =========================================================================
    // Monospace Event Log
    // =========================================================================

    function addLogEntry(time, thread, type, res, details, category = "all") {
        state.logCount++;
        DOM.logEventCount.textContent = `${state.logCount} events`;

        const entry = document.createElement("div");
        entry.className = `log-entry log-cat-${category}`;
        entry.dataset.category = category;

        let badgeClass = "badge-info";
        if (type.includes("ACQUIRED") || type.includes("SUCCESS") || type.includes("SAFE_GRANT") || type.includes("VERIFIED") || type.includes("STEP_TRIGGERED")) {
            badgeClass = "badge-solution";
            if (category === "sync" || category === "all") {
                category = "solution";
            }
        }
        else if (type.includes("ATTEMPT") || type.includes("WAIT")) badgeClass = "badge-acquire";
        else if (type.includes("RELEASE")) badgeClass = "badge-release";
        else if (type.includes("ANOMALY") || type.includes("OVERFLOW") || type.includes("UNDERFLOW")) badgeClass = "badge-anomaly";
        else if (type.includes("DEADLOCK")) badgeClass = "badge-deadlock";
        else if (type.includes("ERROR")) badgeClass = "badge-error";

        entry.innerHTML = `
            <span class="log-time">${time}</span>
            <span class="log-thread" title="${thread}">${thread}</span>
            <span><span class="log-badge ${badgeClass}">${type}</span></span>
            <span class="log-res" title="${res || '-'}">${res || '-'}</span>
            <span class="log-msg" title="${details || ''}">${details || ''}</span>
        `;

        // Check filter
        if (state.activeFilter !== "all") {
            if (state.activeFilter === "solution" && category !== "solution" && category !== "sync") entry.style.display = "none";
            else if (state.activeFilter === "sync" && category !== "sync" && category !== "solution") entry.style.display = "none";
            else if (state.activeFilter === "state" && category !== "state") entry.style.display = "none";
            else if (state.activeFilter === "anomaly" && category !== "anomaly") entry.style.display = "none";
        }

        DOM.eventLogContainer.appendChild(entry);

        // Keep maximum 250 log entries to maintain smooth DOM performance
        if (DOM.eventLogContainer.children.length > 250) {
            DOM.eventLogContainer.removeChild(DOM.eventLogContainer.firstElementChild);
        }

        if (state.autoScroll) {
            DOM.eventLogContainer.scrollTop = DOM.eventLogContainer.scrollHeight;
        }
    }

    function filterLogs(category) {
        state.activeFilter = category;
        DOM.filterChips.forEach(chip => {
            if (chip.dataset.filter === category) chip.classList.add("active");
            else chip.classList.remove("active");
        });

        const entries = DOM.eventLogContainer.querySelectorAll(".log-entry");
        entries.forEach(entry => {
            const entryCat = entry.dataset.category || "all";
            if (category === "all") {
                entry.style.display = "grid";
            } else if (category === "solution" && (entryCat === "solution" || entryCat === "sync")) {
                entry.style.display = "grid";
            } else if (category === "sync" && (entryCat === "sync" || entryCat === "solution")) {
                entry.style.display = "grid";
            } else if (category === "state" && entryCat === "state") {
                entry.style.display = "grid";
            } else if (category === "anomaly" && entryCat === "anomaly") {
                entry.style.display = "grid";
            } else {
                entry.style.display = "none";
            }
        });
    }

    // =========================================================================
    // Socket.IO Event Handlers
    // =========================================================================

    socket.on("connect", () => {
        updateConnectionUI(true);
        addLogEntry(
            new Date().toLocaleTimeString(),
            "System",
            "CONNECTED",
            "WebSocket",
            "Connected to Flask-SocketIO concurrency engine",
            "all"
        );
    });

    socket.on("disconnect", () => {
        updateConnectionUI(false);
        addLogEntry(
            new Date().toLocaleTimeString(),
            "System",
            "DISCONNECTED",
            "WebSocket",
            "Disconnected from server",
            "all"
        );
    });

    socket.on("simulation_status", data => {
        if (data.is_running) {
            setSimulationRunning(true, data.is_paused, data.is_deadlocked);
            if (data.scenario) switchScenarioView(data.scenario);
            if (data.sync_mode !== undefined) setSyncMode(data.sync_mode);
            if (data.thread_count) setThreadCount(data.thread_count);
            if (data.speed) {
                state.speed = data.speed;
                DOM.speedSlider.value = data.speed;
                DOM.speedValue.textContent = `${data.speed.toFixed(1)}x`;
            }
        }
    });

    socket.on("simulation_started", data => {
        setSimulationRunning(true, false, false);
        resetAnomalies();
        state.threads = {};
        renderThreadsGrid();
        addLogEntry(
            new Date().toLocaleTimeString(),
            "Kernel",
            "SIM_STARTED",
            data.scenario,
            `Threads=${data.thread_count}, SyncMode=${data.sync_mode ? 'ON' : 'OFF'}`,
            "state"
        );
    });

    socket.on("simulation_paused", () => {
        setSimulationRunning(true, true, state.isDeadlocked);
        addLogEntry(
            new Date().toLocaleTimeString(),
            "Kernel",
            "SIM_PAUSED",
            "Simulator",
            "Simulation threads halted on pause flag",
            "state"
        );
    });

    socket.on("simulation_resumed", () => {
        setSimulationRunning(true, false, state.isDeadlocked);
        addLogEntry(
            new Date().toLocaleTimeString(),
            "Kernel",
            "SIM_RESUMED",
            "Simulator",
            "Simulation threads resumed execution",
            "state"
        );
    });

    socket.on("simulation_stepped", () => {
        addLogEntry(
            new Date().toLocaleTimeString(),
            "Kernel",
            "STEP_TRIGGERED",
            "Semaphore",
            "Advanced single synchronization step",
            "sync"
        );
    });

    socket.on("simulation_reset", () => {
        setSimulationRunning(false, false, false);
        state.threads = {};
        renderThreadsGrid();
        resetAnomalies();
        if (state.scenario === "producer_consumer") {
            DOM.metricProduced.textContent = "0";
            DOM.metricConsumed.textContent = "0";
            renderProducerConsumer({
                capacity: 5,
                buffer: [],
                total_produced: 0,
                total_consumed: 0,
                sem_empty: 5,
                sem_full: 0,
                sem_mutex: 1,
                last_sync_action: "Bounded buffer reset complete."
            });
        } else if (state.scenario === "dining_philosophers") {
            renderDiningPhilosophers({
                num_philosophers: state.threadCount,
                philosopher_status: [],
                fork_owners: [],
                eat_counts: [0, 0, 0, 0, 0],
                total_meals: 0,
                rag: { allocation_edges: [], request_edges: [], cycle_detected: false },
                coffman: { mutual_exclusion: true, hold_and_wait: false, no_preemption: true, circular_wait: false },
                acquisition_orders: [
                    { phil: 0, first_fork: 0, second_fork: 1, rule: "min(0,1) then max(0,1)", is_asymmetric: false },
                    { phil: 1, first_fork: 1, second_fork: 2, rule: "min(1,2) then max(1,2)", is_asymmetric: false },
                    { phil: 2, first_fork: 2, second_fork: 3, rule: "min(2,3) then max(2,3)", is_asymmetric: false },
                    { phil: 3, first_fork: 3, second_fork: 4, rule: "min(3,4) then max(3,4)", is_asymmetric: false },
                    { phil: 4, first_fork: 0, second_fork: 4, rule: "min(0,4) then max(0,4) (ASYMMETRIC)", is_asymmetric: true }
                ]
            });
        } else if (state.scenario === "readers_writers") {
            renderReadersWriters({
                record: { title: "OS Concurrency Ledger", version: 1, checksum: 1, content: "Clean reset state", last_writer: "Genesis" },
                active_readers: [],
                active_writers: [],
                waiting_readers: [],
                waiting_writers: [],
                read_count: 0,
                read_count_mutex_locked: false,
                resource_write_lock_locked: false,
                resource_write_lock_holder: null,
                dirty_read_count: 0,
                torn_read_count: 0,
                collision_count: 0,
                invariant_writer_mutex: true,
                invariant_integrity: true
            });
        } else if (state.scenario === "bankers_algorithm") {
            renderBankersAlgorithm({
                available: [3, 3, 2],
                allocation: [[0, 1, 0], [2, 0, 0], [3, 0, 2], [2, 1, 1], [0, 0, 2]],
                max_matrix: [[7, 5, 3], [3, 2, 2], [9, 0, 2], [2, 2, 2], [4, 3, 3]],
                need: [[7, 4, 3], [1, 2, 2], [6, 0, 0], [0, 1, 1], [4, 3, 1]],
                is_currently_safe: true,
                safe_sequence: [1, 3, 4, 0, 2],
                safety_step_details: [
                    { step: 1, process: 1, need: [1, 2, 2], work_before: [3, 3, 2], can_finish: true, alloc: [2, 0, 0], work_after: [5, 3, 2], finish_array: [false, true, false, false, false], safe_seq_so_far: [1] },
                    { step: 2, process: 3, need: [0, 1, 1], work_before: [5, 3, 2], can_finish: true, alloc: [2, 1, 1], work_after: [7, 4, 3], finish_array: [false, true, false, true, false], safe_seq_so_far: [1, 3] },
                    { step: 3, process: 4, need: [4, 3, 1], work_before: [7, 4, 3], can_finish: true, alloc: [0, 0, 2], work_after: [7, 4, 5], finish_array: [false, true, false, true, true], safe_seq_so_far: [1, 3, 4] },
                    { step: 4, process: 0, need: [7, 4, 3], work_before: [7, 4, 5], can_finish: true, alloc: [0, 1, 0], work_after: [7, 5, 5], finish_array: [true, true, false, true, true], safe_seq_so_far: [1, 3, 4, 0] },
                    { step: 5, process: 2, need: [6, 0, 0], work_before: [7, 5, 5], can_finish: true, alloc: [3, 0, 2], work_after: [10, 5, 7], finish_array: [true, true, true, true, true], safe_seq_so_far: [1, 3, 4, 0, 2] }
                ]
            });
            if (DOM.bankerTestResultBox) DOM.bankerTestResultBox.classList.add("hidden");
        }
        addLogEntry(
            new Date().toLocaleTimeString(),
            "Kernel",
            "SIM_RESET",
            "Simulator",
            "Threads joined cleanly via stop_event. Reset complete.",
            "state"
        );
    });

    socket.on("simulation_stopped", data => {
        setSimulationRunning(false, false, state.isDeadlocked);
        addLogEntry(
            new Date().toLocaleTimeString(),
            "Kernel",
            "SIM_FINISHED",
            data.scenario,
            `Simulation stopped. Total Anomalies=${data.anomaly_count || 0}`,
            "state"
        );
    });

    socket.on("sync_event", data => {
        addLogEntry(
            data.iso_time || new Date().toLocaleTimeString(),
            data.thread_name || `T-${data.thread_id}`,
            data.event_type || "SYNC",
            data.resource || "-",
            data.details || "",
            "sync"
        );
    });

    socket.on("thread_state_change", data => {
        if (data.name) {
            state.threads[data.name] = data;
            renderThreadsGrid();
        }
        addLogEntry(
            new Date().toLocaleTimeString(),
            data.name,
            `STATE_${data.state}`,
            data.role || "Thread",
            data.action || "",
            "state"
        );
    });

    socket.on("buffer_update", data => {
        if (data.threads) {
            state.threads = data.threads;
            renderThreadsGrid();
        }
        DOM.metricProduced.textContent = data.total_produced || 0;
        DOM.metricConsumed.textContent = data.total_consumed || 0;
        renderProducerConsumer(data);
    });

    socket.on("table_update", data => {
        if (data.threads) {
            state.threads = data.threads;
            renderThreadsGrid();
        }
        if (data.is_deadlocked) {
            setSimulationRunning(true, false, true);
        }
        renderDiningPhilosophers(data);
    });

    socket.on("record_update", data => {
        if (data.threads) {
            state.threads = data.threads;
            renderThreadsGrid();
        }
        renderReadersWriters(data);
    });

    socket.on("banker_update", data => {
        if (data.threads) {
            state.threads = data.threads;
            renderThreadsGrid();
        }
        if (data.is_deadlocked) {
            setSimulationRunning(true, false, true);
        }
        renderBankersAlgorithm(data);
    });

    socket.on("banker_request_result", data => {
        if (!DOM.bankerTestResultBox) return;
        DOM.bankerTestResultBox.classList.remove("hidden", "result-success", "result-denied");
        const status = data.status || "";
        const isGranted = status.startsWith("GRANTED");
        DOM.bankerTestResultBox.classList.add(isGranted ? "result-success" : "result-denied");

        let traceHtml = `<strong>[EVALUATION] ${data.message || ''}</strong><br>`;
        if (data.trace && data.trace.length > 0) {
            traceHtml += `<div style="margin-top: 6px; font-size: 0.72rem; color: #94a3b8;">`;
            data.trace.forEach(step => {
                traceHtml += `<div>• ${step}</div>`;
            });
            traceHtml += `</div>`;
        }
        DOM.bankerTestResultBox.innerHTML = traceHtml;

        addLogEntry(
            new Date().toLocaleTimeString(),
            data.process || "Banker",
            isGranted ? "BANKER_SAFE_GRANT" : "BANKER_DENIED",
            "Resource Pool",
            data.message || "Evaluated resource request",
            isGranted ? "sync" : "anomaly"
        );
    });

    socket.on("anomaly_detected", data => {
        const anomaly = data.anomaly || {};
        incrementAnomaly(anomaly.type || "ANOMALY");
        addLogEntry(
            anomaly.iso_time || new Date().toLocaleTimeString(),
            "ANOMALY_ENGINE",
            anomaly.type || "RACE_CONDITION",
            "Shared Resource",
            anomaly.message || "State invariant violated",
            "anomaly"
        );
    });

    socket.on("deadlock_detected", data => {
        setSimulationRunning(true, false, true);
        DOM.deadlockDescription.textContent = data.message || "Deadlock detected! All philosophers are in circular wait.";
        addLogEntry(
            new Date().toLocaleTimeString(),
            "DEADLOCK_MONITOR",
            "DEADLOCK",
            "Forks (Circular Wait)",
            data.message || "All Coffman conditions fulfilled. Circular wait cycle formed.",
            "anomaly"
        );
    });

    socket.on("thread_error", data => {
        addLogEntry(
            new Date().toLocaleTimeString(),
            data.thread_name || "Thread",
            "THREAD_ERROR",
            "Worker",
            data.error || "Unhandled exception in thread",
            "anomaly"
        );
    });

    // =========================================================================
    // UI Event Listeners & Control Bindings
    // =========================================================================

    // Scenario select change
    DOM.scenarioSelect.addEventListener("change", e => {
        switchScenarioView(e.target.value);
    });

    // Sync Mode toggle
    DOM.syncToggleBtn.addEventListener("click", () => {
        if (state.isRunning) return;
        setSyncMode(!state.syncMode);
    });

    // Thread count stepper
    DOM.btnThreadDec.addEventListener("click", () => {
        if (state.isRunning) return;
        setThreadCount(state.threadCount - 1);
    });

    DOM.btnThreadInc.addEventListener("click", () => {
        if (state.isRunning) return;
        setThreadCount(state.threadCount + 1);
    });

    // Speed slider
    DOM.speedSlider.addEventListener("input", e => {
        const speed = parseFloat(e.target.value);
        state.speed = speed;
        DOM.speedValue.textContent = `${speed.toFixed(1)}x`;
        socket.emit("update_speed", { speed });
    });

    // Start button
    DOM.btnStart.addEventListener("click", () => {
        if (state.isRunning) return;
        socket.emit("start_simulation", {
            scenario: state.scenario,
            sync_mode: state.syncMode,
            thread_count: state.threadCount,
            speed: state.speed
        });
    });

    // Pause / Resume button
    DOM.btnPause.addEventListener("click", () => {
        if (!state.isRunning) return;
        if (state.isPaused) {
            socket.emit("resume_simulation");
        } else {
            socket.emit("pause_simulation");
        }
    });

    // Step button
    DOM.btnStep.addEventListener("click", () => {
        if (!state.isPaused) return;
        socket.emit("step_simulation");
    });

    // Reset buttons
    function triggerReset() {
        socket.emit("reset_simulation");
    }
    DOM.btnReset.addEventListener("click", triggerReset);
    DOM.deadlockResetBtn.addEventListener("click", triggerReset);

    // Auto-scroll toggle
    DOM.chkAutoScroll.addEventListener("change", e => {
        state.autoScroll = e.target.checked;
    });

    // Clear log button
    DOM.btnClearLog.addEventListener("click", () => {
        DOM.eventLogContainer.innerHTML = "";
        state.logCount = 0;
        DOM.logEventCount.textContent = "0 events";
    });

    // Filter chips
    DOM.filterChips.forEach(chip => {
        chip.addEventListener("click", () => {
            filterLogs(chip.dataset.filter);
        });
    });

    // Banker's Algorithm custom request evaluation button
    if (DOM.btnTestBankerReq) {
        DOM.btnTestBankerReq.addEventListener("click", () => {
            const pid = parseInt(DOM.reqProcSelect.value, 10);
            const reqA = parseInt(DOM.reqValA.value, 10) || 0;
            const reqB = parseInt(DOM.reqValB.value, 10) || 0;
            const reqC = parseInt(DOM.reqValC.value, 10) || 0;
            socket.emit("banker_custom_request", {
                process_id: pid,
                request: [reqA, reqB, reqC]
            });
        });
    }

    // Banker's Algorithm Preset Demos (Problem Mode vs Safe Mode)
    if (DOM.btnDemoSafeReq) {
        DOM.btnDemoSafeReq.addEventListener("click", () => {
            if (DOM.reqProcSelect) DOM.reqProcSelect.value = "1";
            if (DOM.reqValA) DOM.reqValA.value = "1";
            if (DOM.reqValB) DOM.reqValB.value = "0";
            if (DOM.reqValC) DOM.reqValC.value = "2";
            if (DOM.btnTestBankerReq) DOM.btnTestBankerReq.click();
        });
    }

    if (DOM.btnDemoUnsafeReq) {
        DOM.btnDemoUnsafeReq.addEventListener("click", () => {
            if (DOM.reqProcSelect) DOM.reqProcSelect.value = "0";
            if (DOM.reqValA) DOM.reqValA.value = "0";
            if (DOM.reqValB) DOM.reqValB.value = "2";
            if (DOM.reqValC) DOM.reqValC.value = "0";
            if (DOM.btnTestBankerReq) DOM.btnTestBankerReq.click();
        });
    }

    if (DOM.btnDemoExceedReq) {
        DOM.btnDemoExceedReq.addEventListener("click", () => {
            if (DOM.reqProcSelect) DOM.reqProcSelect.value = "1";
            if (DOM.reqValA) DOM.reqValA.value = "5";
            if (DOM.reqValB) DOM.reqValB.value = "5";
            if (DOM.reqValC) DOM.reqValC.value = "5";
            if (DOM.btnTestBankerReq) DOM.btnTestBankerReq.click();
        });
    }

    // Problem Mode / Solution Mode Segmented Control
    if (DOM.btnModeProblem) {
        DOM.btnModeProblem.addEventListener("click", () => {
            if (state.isRunning) return;
            setSyncMode(false);
        });
    }

    if (DOM.btnModeSolution) {
        DOM.btnModeSolution.addEventListener("click", () => {
            if (state.isRunning) return;
            setSyncMode(true);
        });
    }

    // Initial setup on load
    switchScenarioView("producer_consumer");
    setSyncMode(true);
    setThreadCount(4);
    renderThreadsGrid();

})();
