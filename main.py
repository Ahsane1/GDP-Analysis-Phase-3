import json
import multiprocessing as mp
from plugins.input import GenericCSVReader
from core.worker import CoreWorker       # We will build this next!
from telemetry import PipelineTelemetry  # We will build this last!
from telemetry import PipelineTelemetry, LiveDashboard

def bootstrap():
    # 1. Load the "Brain" (config.json)
    with open('config.json', 'r') as f:
        config = json.load(f)

    # Grab our pipeline dynamics from the config
    max_q_size = config["pipeline_dynamics"]["stream_queue_max_size"]
    num_workers = config["pipeline_dynamics"]["core_parallelism"]

    print("[MAIN] Orchestrator initializing pipeline...")

    # 2. Create the Bounded Queues (The Ticket Rails)
    # The 'maxsize' is what creates Backpressure. If it fills up, the producer is forced to wait.
    raw_data_queue = mp.Queue(maxsize=max_q_size)
    processed_data_queue = mp.Queue(maxsize=max_q_size)

    # 3. Instantiate our Module Objects
    # Notice how we pass the queues into the objects so they know where to drop/pull data
    input_module = GenericCSVReader(config, raw_data_queue)
    
    # 4. Wrap the Objects in OS-level Processes
    processes = []

    # A. The single Producer (Input) process
    producer_process = mp.Process(target=input_module.run, name="Producer")
    processes.append(producer_process)

    # B. The multiple Consumer (Core) processes
    for i in range(num_workers):
        worker = CoreWorker(config, raw_data_queue, processed_data_queue)
        p = mp.Process(target=worker.run, name=f"CoreWorker-{i+1}")
        processes.append(p)

    # 5. Start the Pipeline! (Ignition)
    for p in processes:
        p.start()
        print(f"[MAIN] Started process: {p.name}")
        
# 6. Start the Telemetry Dashboard (The Observer Pattern)
    # First, create the Subject that watches the queues
    telemetry_subject = PipelineTelemetry(config, raw_data_queue, processed_data_queue)
    
    # Second, create the Observer and hand it the Subject so it can subscribe
    dashboard = LiveDashboard(telemetry_subject)
    
    # Finally, start the live animation!
    dashboard.start_monitoring()

    # 7. Wait for everything to finish (Cleanup)
    # .join() tells the main script to wait here until all workers have gone home.
    for p in processes:
        p.join()
        
    print("[MAIN] Pipeline execution complete. Shutting down.")

# VERY IMPORTANT FOR MULTIPROCESSING IN PYTHON!
# Without this block, the OS will infinitely crash when trying to spawn new processes.
if __name__ == '__main__':
    bootstrap()