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
        bufferOccupancy: document.getElementById("bufferOccupancy"),
        metricProduced: document.getElementById("metricProduced"),
        metricConsumed: document.getElementById("metricConsumed"),
        bufferFillBar: document.getElementById("bufferFillBar"),
        bufferSlotsContainer: document.getElementById("bufferSlotsContainer"),
        diningTableContainer: document.getElementById("diningTableContainer"),
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

        if (scenario === "producer_consumer") {
            DOM.viewProducerConsumer.classList.add("active");
            DOM.scenarioStageTitle.textContent = "Bounded Buffer Visualization";
            DOM.scenarioStageSubtitle.textContent = "Condition Variable & Mutex Monitoring";
            DOM.threadCountLabel.textContent = "Threads (Producers + Consumers)";
            if (!state.isRunning) setThreadCount(4);
            renderBufferSlots(5, []);
        } else if (scenario === "dining_philosophers") {
            DOM.viewDiningPhilosophers.classList.add("active");
            DOM.scenarioStageTitle.textContent = "Dining Philosophers Table";
            DOM.scenarioStageSubtitle.textContent = "Circular Wait & Deadlock Avoidance";
            DOM.threadCountLabel.textContent = "Philosophers (and Forks)";
            if (!state.isRunning) setThreadCount(5);
            renderDiningTable(5, [], [], []);
        } else if (scenario === "readers_writers") {
            DOM.viewReadersWriters.classList.add("active");
            DOM.scenarioStageTitle.textContent = "Shared Database Record";
            DOM.scenarioStageSubtitle.textContent = "Reader Preference vs Exclusive Writer Mutex";
            DOM.threadCountLabel.textContent = "Threads (Readers + Writers)";
            if (!state.isRunning) setThreadCount(5);
        }
    }

    function setSyncMode(syncOn) {
        state.syncMode = syncOn;
        if (syncOn) {
            DOM.syncToggleBtn.className = "sync-switch sync-on";
            DOM.syncToggleBtn.setAttribute("aria-checked", "true");
            DOM.syncModeCaption.className = "sync-caption caption-safe";
            DOM.syncModeCaption.textContent = "Guaranteed Safe (Locks / Conditions)";
            DOM.anomalyPanel.classList.add("hidden");
        } else {
            DOM.syncToggleBtn.className = "sync-switch sync-off";
            DOM.syncToggleBtn.setAttribute("aria-checked", "false");
            DOM.syncModeCaption.className = "sync-caption caption-danger";
            DOM.syncModeCaption.textContent = "Unsynchronized (Races & Anomalies Visible)";
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

    // 1. Producer-Consumer Buffer View
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

    // 2. Dining Philosophers Circular Table View
    function renderDiningTable(numPhilosophers, philosopherStatus, forkOwners, eatCounts) {
        // Clear previous philosophers & forks
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

            let icon = "🤔";
            if (status === "THINKING") {
                philEl.classList.add("state-thinking");
                icon = "💭";
            } else if (status === "HUNGRY") {
                philEl.classList.add("state-hungry");
                icon = "⏳";
            } else if (status === "WAITING_FOR_FORK") {
                philEl.classList.add("state-waiting-fork");
                icon = "⛔";
            } else if (status === "EATING") {
                philEl.classList.add("state-eating");
                icon = "🍝";
            }

            philEl.innerHTML = `
                <span class="phil-icon">${icon}</span>
                <span class="phil-name">P-${i} (${meals})</span>
            `;
            philEl.title = `Philosopher ${i}: ${status} | Meals: ${meals}`;
            DOM.diningTableContainer.appendChild(philEl);

            // Fork positioning (between philosopher i and (i+1)%numPhilosophers)
            const forkAngle = angle + Math.PI / numPhilosophers;
            const fx = centerX + radiusFork * Math.cos(forkAngle) - 14;
            const fy = centerY + radiusFork * Math.sin(forkAngle) - 14;

            const forkOwner = forkOwners[i] !== undefined ? forkOwners[i] : -1;
            const forkEl = document.createElement("div");
            forkEl.className = "fork-item";
            forkEl.id = `fork-item-${i}`;
            forkEl.style.left = `${fx}px`;
            forkEl.style.top = `${fy}px`;

            if (forkOwner === -1) {
                forkEl.classList.add("fork-free");
                forkEl.innerHTML = `🍴`;
                forkEl.title = `Fork ${i}: On Table (Free)`;
            } else {
                forkEl.classList.add("fork-held");
                forkEl.innerHTML = `🍴`;
                forkEl.title = `Fork ${i}: Held by Philosopher ${forkOwner}`;
            }
            DOM.diningTableContainer.appendChild(forkEl);
        }
    }

    // 3. Readers-Writers Shared Record View
    function renderReadersWriters(data) {
        const record = data.record || {};
        const activeReaders = data.active_readers || [];
        const activeWriters = data.active_writers || [];

        DOM.activeReadersCount.textContent = activeReaders.length;
        DOM.activeWritersCount.textContent = activeWriters.length;

        // Render Active Readers Tags
        DOM.activeReadersContainer.innerHTML = "";
        if (activeReaders.length === 0) {
            DOM.activeReadersContainer.innerHTML = '<span class="empty-hint">No active readers</span>';
        } else {
            activeReaders.forEach(reader => {
                const tag = document.createElement("span");
                tag.className = "rw-tag rw-tag-reader";
                tag.textContent = `📖 ${reader}`;
                DOM.activeReadersContainer.appendChild(tag);
            });
        }

        // Render Active Writers Tags
        DOM.activeWritersContainer.innerHTML = "";
        if (activeWriters.length === 0) {
            DOM.activeWritersContainer.innerHTML = '<span class="empty-hint">No active writers</span>';
        } else {
            activeWriters.forEach(writer => {
                const tag = document.createElement("span");
                tag.className = "rw-tag rw-tag-writer";
                tag.textContent = `✍️ ${writer}`;
                DOM.activeWritersContainer.appendChild(tag);
            });
        }

        // Update Record Display
        DOM.recordVersionBadge.textContent = `v${record.version || 1}`;
        DOM.recordTitle.textContent = record.title || "OS Concurrency Ledger";
        DOM.recordChecksumBadge.textContent = `Checksum: ${record.checksum !== undefined ? record.checksum : record.version}`;
        DOM.recordContent.textContent = record.content || "";
        DOM.recordLastWriter.textContent = record.last_writer || "System";

        const consistent = record.version === record.checksum && (!activeWriters.length || activeWriters.length === 1);
        if (consistent) {
            DOM.recordIntegrityStatus.className = "integrity-tag integrity-ok";
            DOM.recordIntegrityStatus.textContent = "State: Consistent";
            DOM.rwRecordCard.classList.remove("glitch-anomaly");
        } else {
            DOM.recordIntegrityStatus.className = "integrity-tag integrity-corrupt";
            DOM.recordIntegrityStatus.textContent = "State: TORN / DIRTY READ";
            DOM.rwRecordCard.classList.add("glitch-anomaly");
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
        if (type.includes("ACQUIRED") || type.includes("SUCCESS")) badgeClass = "badge-success";
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
            if (state.activeFilter === "sync" && category !== "sync") entry.style.display = "none";
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
            } else if (category === "sync" && entryCat === "sync") {
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
            renderBufferSlots(5, []);
            DOM.metricProduced.textContent = "0";
            DOM.metricConsumed.textContent = "0";
        } else if (state.scenario === "dining_philosophers") {
            renderDiningTable(state.threadCount, [], [], []);
        } else if (state.scenario === "readers_writers") {
            renderReadersWriters({ record: { title: "OS Concurrency Ledger", version: 1, checksum: 1, content: "Clean reset state" } });
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
        renderBufferSlots(data.capacity || 5, data.buffer || []);
    });

    socket.on("table_update", data => {
        if (data.threads) {
            state.threads = data.threads;
            renderThreadsGrid();
        }
        if (data.is_deadlocked) {
            setSimulationRunning(true, false, true);
        }
        renderDiningTable(
            data.num_philosophers || state.threadCount,
            data.philosopher_status || [],
            data.fork_owners || [],
            data.eat_counts || []
        );
    });

    socket.on("record_update", data => {
        if (data.threads) {
            state.threads = data.threads;
            renderThreadsGrid();
        }
        renderReadersWriters(data);
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

    // Initial setup on load
    switchScenarioView("producer_consumer");
    setSyncMode(true);
    setThreadCount(4);
    renderThreadsGrid();

})();
