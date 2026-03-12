import json
import multiprocessing as mp
import sys
import os
from plugins.input import GenericCSVReader
from core.worker import CoreWorker, Gatherer
from telemetry import PipelineTelemetry, LiveDashboard

def validate_config(config):
    """Fails fast if the config.json is missing any mandatory keys."""
    try:
        # Check top level
        _ = config["dataset_path"]
        _ = config["pipeline_dynamics"]["stream_queue_max_size"]
        _ = config["pipeline_dynamics"]["core_parallelism"]
        _ = config["pipeline_dynamics"]["input_delay_seconds"]
        _ = config["schema_mapping"]["columns"]
        _ = config["processing"]["running_average_window_size"]
    except KeyError as e:
        print(f"[MAIN ERROR] CRITICAL: config.json is missing required key: {e}")
        sys.exit(1)
    except TypeError:
        print("[MAIN ERROR] CRITICAL: config.json structure is invalid (e.g., expecting a dictionary but got a string).")
        sys.exit(1)


def bootstrap():

    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'config.json')

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print("[MAIN ERROR] CRITICAL: config.json not found in root directory!")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[MAIN ERROR] CRITICAL: config.json is corrupted or incorrectly formatted! Details: {e}")
        sys.exit(1)

    config["dataset_path"] = os.path.join(base_dir, config["dataset_path"])

    # Validate before we start doing heavy lifting
    validate_config(config)

    max_q_size = config["pipeline_dynamics"]["stream_queue_max_size"]
    num_workers = config["pipeline_dynamics"]["core_parallelism"]

    print("[MAIN] Orchestrator initializing pipeline...")

    # Bounded Queues
    raw_data_queue = mp.Queue(maxsize=max_q_size)
    internal_gather_queue = mp.Queue(maxsize=max_q_size)
    processed_data_queue = mp.Queue(maxsize=max_q_size)

    input_module = GenericCSVReader(config, raw_data_queue)
    gatherer_module = Gatherer(config, internal_gather_queue, processed_data_queue)
    
    processes = []

    # 1. Start Producer
    producer_process = mp.Process(target=input_module.run, name="Producer")
    processes.append(producer_process)

    # 2. Start Core Workers
    for i in range(num_workers):
        worker = CoreWorker(config, raw_data_queue, internal_gather_queue)
        p = mp.Process(target=worker.run, name=f"CoreWorker-{i+1}")
        processes.append(p)

    # 3. Start Gatherer
    gatherer_process = mp.Process(target=gatherer_module.run, name="Gatherer")
    processes.append(gatherer_process)

    try:
        for p in processes:
            p.start()
            print(f"[MAIN] Started process: {p.name}")
            
        telemetry_subject = PipelineTelemetry(config, raw_data_queue, processed_data_queue)
        dashboard = LiveDashboard(telemetry_subject)
        dashboard.start_monitoring()

        for p in processes:
            p.join()
            
        print("[MAIN] Pipeline execution complete. Shutting down cleanly.")
        
    except KeyboardInterrupt:
        print("\n[MAIN] Manual interruption detected. Terminating processes...")
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join()
        print("[MAIN] Pipeline forced shutdown complete.")
    except Exception as e:
        print(f"[MAIN ERROR] Fatal pipeline crash: {e}")
        for p in processes:
            if p.is_alive(): p.terminate()

if __name__ == '__main__':
    print("hiiii")
    bootstrap()