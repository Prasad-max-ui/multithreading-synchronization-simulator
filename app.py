import os
import threading
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from scenarios.producer_consumer import ProducerConsumerScenario
from scenarios.dining_philosophers import DiningPhilosophersScenario
from scenarios.readers_writers import ReadersWritersScenario
from scenarios.bankers_algorithm import BankersAlgorithmScenario
from scenarios.base_scenario import BaseScenario

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "os-concurrency-simulator-key-2026")

# Use threading async_mode for pure Python native threading compatibility
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# Simulation state manager
sim_lock = threading.Lock()
current_simulation: BaseScenario = None


def broadcast_event(event_name: str, data: dict):
    """Callback passed to scenarios to broadcast real-time events to all clients."""
    try:
        socketio.emit(event_name, data)
    except Exception as e:
        print(f"[SocketIO] Emission failed for {event_name}: {e}")


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/")
def index():
    """Render the main single-page application dashboard."""
    return render_template("index.html")


@app.route("/health")
def health():
    """Health check endpoint."""
    with sim_lock:
        running = current_simulation.is_running if current_simulation else False
        name = current_simulation.name if current_simulation else None
    return jsonify({
        "status": "healthy",
        "active_simulation": name,
        "is_running": running
    })


@socketio.on("connect")
def handle_connect():
    print(f"[Client Connected] {request.sid}")
    with sim_lock:
        if current_simulation and current_simulation.is_running:
            emit("simulation_status", {
                "scenario": current_simulation.name,
                "sync_mode": current_simulation.sync_mode,
                "thread_count": current_simulation.thread_count,
                "speed": current_simulation.speed,
                "is_paused": current_simulation.is_paused,
                "is_deadlocked": current_simulation.is_deadlocked,
                "anomaly_count": current_simulation.anomaly_count,
                "is_running": True
            })
            current_simulation.emit_state_update()
        else:
            emit("simulation_status", {"is_running": False})


@socketio.on("disconnect")
def handle_disconnect():
    print(f"[Client Disconnected] {request.sid}")


@socketio.on("start_simulation")
def handle_start(data):
    global current_simulation
    scenario_type = data.get("scenario", "producer_consumer")
    sync_mode = bool(data.get("sync_mode", True))
    thread_count = int(data.get("thread_count", 4))
    speed = float(data.get("speed", 1.0))

    with sim_lock:
        if current_simulation and current_simulation.is_running:
            print("[Server] Stopping existing simulation before starting new one...")
            current_simulation.stop(timeout=1.5)

        if scenario_type == "producer_consumer":
            current_simulation = ProducerConsumerScenario(
                sync_mode=sync_mode,
                thread_count=thread_count,
                speed=speed,
                capacity=5,
                emitter=broadcast_event
            )
        elif scenario_type == "dining_philosophers":
            current_simulation = DiningPhilosophersScenario(
                sync_mode=sync_mode,
                thread_count=thread_count,
                speed=speed,
                emitter=broadcast_event
            )
        elif scenario_type == "readers_writers":
            current_simulation = ReadersWritersScenario(
                sync_mode=sync_mode,
                thread_count=thread_count,
                speed=speed,
                emitter=broadcast_event
            )
        elif scenario_type == "bankers_algorithm":
            current_simulation = BankersAlgorithmScenario(
                sync_mode=sync_mode,
                thread_count=thread_count,
                speed=speed,
                emitter=broadcast_event
            )
        else:
            emit("simulation_error", {"message": f"Unknown scenario: {scenario_type}"})
            return

        current_simulation.start()

    print(f"[Server] Started {scenario_type} (Sync={sync_mode}, Threads={thread_count}, Speed={speed})")


@socketio.on("pause_simulation")
def handle_pause():
    with sim_lock:
        if current_simulation and current_simulation.is_running:
            current_simulation.pause()
            print("[Server] Simulation paused")


@socketio.on("resume_simulation")
def handle_resume():
    with sim_lock:
        if current_simulation and current_simulation.is_running:
            current_simulation.resume()
            print("[Server] Simulation resumed")


@socketio.on("step_simulation")
def handle_step():
    with sim_lock:
        if current_simulation and current_simulation.is_running:
            current_simulation.step()
            print("[Server] Simulation stepped")


@socketio.on("reset_simulation")
def handle_reset():
    global current_simulation
    with sim_lock:
        if current_simulation:
            print("[Server] Resetting simulation...")
            current_simulation.stop(timeout=1.5)
            current_simulation = None

    socketio.emit("simulation_reset", {"status": "reset"})
    print("[Server] Simulation reset complete")


@socketio.on("update_speed")
def handle_speed(data):
    speed = float(data.get("speed", 1.0))
    with sim_lock:
        if current_simulation:
            current_simulation.set_speed(speed)
            print(f"[Server] Speed updated to {speed}")


@socketio.on("banker_custom_request")
def handle_banker_request(data):
    global current_simulation
    process_id = int(data.get("process_id", 0))
    request_vector = [int(x) for x in data.get("request", [0, 0, 0])]
    with sim_lock:
        if not (current_simulation and isinstance(current_simulation, BankersAlgorithmScenario)):
            current_simulation = BankersAlgorithmScenario(
                sync_mode=True,
                thread_count=5,
                speed=1.0,
                emitter=broadcast_event
            )
        result = current_simulation.request_resources(process_id, request_vector)
        emit("banker_request_result", result)
        current_simulation.emit_state_update()


if __name__ == "__main__":
    print("Starting Multithreading & Synchronization Simulator on http://127.0.0.1:5000 ...")
    socketio.run(app, host="127.0.0.1", port=5000, debug=False, allow_unsafe_werkzeug=True)
