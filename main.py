import json
import os
import sys
import multiprocessing as mp
from plugins.input import GenericCSVReader
from core.worker import CoreWorker, Gatherer
from telemetry import PipelineTelemetry, LiveDashboard

def validate_config(config):
    """Fails immediately if the config.json is missing any mandatory keys."""
    try:
        # Check top level & pipeline dynamics
        _ = config["dataset_path"]
        _ = config["pipeline_dynamics"]["stream_queue_max_size"]
        _ = config["pipeline_dynamics"]["core_parallelism"]
        _ = config["pipeline_dynamics"]["input_delay_seconds"]
        
        # Check Schema
        _ = config["schema_mapping"]["columns"]
        
        # Check Processing (Stateless & Stateful)
        _ = config["processing"]["stateless_tasks"]["secret_key"]
        _ = config["processing"]["stateless_tasks"]["iterations"]
        _ = config["processing"]["stateful_tasks"]["running_average_window_size"]
        
        # Check Visualizations
        _ = config["visualizations"]["data_charts"][0]["title"]
        _ = config["visualizations"]["data_charts"][1]["title"]
        
    except KeyError as e:
        print(f"[MAIN ERROR] CRITICAL: config.json is missing required key: {e}")
        sys.exit(1)
    except (TypeError, IndexError):
        print("[MAIN ERROR] CRITICAL: config.json structure is invalid or missing chart configurations.")
        sys.exit(1)



def bootstrap():
    # 1. Absolute Path Handling & Config Loading
    base_dir = os.path.dirname(os.path.abspath(__file__))
    cfg_path = os.path.join(base_dir, 'config.json')

    try:
        with open(cfg_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"[MAIN ERROR] CRITICAL: config.json not found at {cfg_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[MAIN ERROR] CRITICAL: config.json is corrupted! Details: {e}")
        sys.exit(1)

    # 2. Validate Config Structure BEFORE doing anything else
    validate_config(config)

    config["dataset_path"] = os.path.join(base_dir, config["dataset_path"])
    
    if not os.path.exists(config["dataset_path"]):
        print(f"[MAIN ERROR] CRITICAL: Dataset file not found at: {config['dataset_path']}")
        print("Please check your config.json and make sure the data file exists.")
        sys.exit(1)

    # 3. Pipeline Initialization
    max_q_size = config["pipeline_dynamics"]["stream_queue_max_size"]
    num_workers = config["pipeline_dynamics"]["core_parallelism"]

    print("[MAIN] Orchestrator initializing pipeline...")

    raw_data_queue = mp.Queue(maxsize=max_q_size)
    internal_gather_queue = mp.Queue(maxsize=max_q_size)
    processed_data_queue = mp.Queue(maxsize=max_q_size)
    
    raw_data_queue.cancel_join_thread()
    internal_gather_queue.cancel_join_thread()
    processed_data_queue.cancel_join_thread()


    # Global Kill Switch for Zombie Prevention
    shutdown_event = mp.Event()

    # 4. Process Instantiation
    processes = []
    
    input_proc = mp.Process(target=GenericCSVReader(config, raw_data_queue, shutdown_event).run, name="Input")
    processes.append(input_proc)

    for i in range(num_workers):
        worker_proc = mp.Process(target=CoreWorker(config, raw_data_queue, internal_gather_queue, shutdown_event).run, name=f"Worker-{i+1}")
        processes.append(worker_proc)

    gatherer_proc = mp.Process(target=Gatherer(config, internal_gather_queue, processed_data_queue, shutdown_event).run, name="Gatherer")
    processes.append(gatherer_proc)

    # 5. Execution
    try:
        for p in processes:
            p.start()
            print(f"[MAIN] Started process: {p.name}")
            
        telemetry_subject = PipelineTelemetry(config, raw_data_queue, processed_data_queue)
        dashboard = LiveDashboard(telemetry_subject, config)
        
        # This blocks until the visualization finishes
        dashboard.start_monitoring()

    except Exception as e:
        print(f"\n[MAIN] Interrupted: {e}")
    finally:
        print("[MAIN] Visualization finished. Commencing shutdown...")
        shutdown_event.set() 

        # Force terminate all workers instantly so they drop their locks
        for p in processes:
            if p.is_alive():
                p.terminate() 
            p.join(timeout=0.5)
            
        print("[MAIN] Pipeline execution complete. Returning control to OS cleanly.")
        
        # The standard, polite Python exit!
        sys.exit(0)

if __name__ == '__main__':
    bootstrap()