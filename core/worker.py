import hashlib

def calculate_sliding_window(new_value: float, current_window_state: list, max_window_size: int):
    """Pure function: calculates average without mutating external state."""
    new_window = current_window_state.copy()
    new_window.append(new_value)
    if len(new_window) > max_window_size:
        new_window.pop(0)
    
    computed_average = sum(new_window) / len(new_window)
    return computed_average, new_window

def verify_signature(raw_value_str: str, expected_hash: str, key: str, iterations: int) -> bool:
    """Pure function: verifies cryptographic hash."""
    password_bytes = key.encode('utf-8')
    salt_bytes = raw_value_str.encode('utf-8')
    calculated_hash = hashlib.pbkdf2_hmac('sha256', password_bytes, salt_bytes, iterations).hex()
    return calculated_hash == expected_hash

class CoreWorker:
    def __init__(self, config: dict, raw_queue, internal_gather_queue, shutdown_event):
        self.raw_queue = raw_queue
        self.internal_gather_queue = internal_gather_queue
        self.shutdown_event = shutdown_event
        
        self.crypto_cfg = config["processing"]["stateless_tasks"]
        self.secret_key = self.crypto_cfg["secret_key"]
        self.iterations = self.crypto_cfg["iterations"]

    def run(self):
        print(f"[{self.__class__.__name__}] Listening for packets...")
        while not self.shutdown_event.is_set():
            try:
                packet = self.raw_queue.get()
                if packet is None:
                    self.internal_gather_queue.put(None) 
                    break
                    
                if not isinstance(packet, dict):
                    continue

                payload = packet.get("data", {})
                metric = payload.get("metric_value")
                expected_hash = packet.get("hash")
                seq_id = packet.get("sequence_id", "UNKNOWN")
                
                if metric is None or expected_hash is None or not isinstance(metric, (int, float)):
                    continue

                raw_value_str = f"{metric:.2f}"
                is_valid = verify_signature(raw_value_str, expected_hash, self.secret_key, self.iterations)
                
                if not is_valid:
                    print(f"[CORE WARNING] 🚨 SPOOFED PACKET! Dropping Sequence ID {seq_id}")
                    self.internal_gather_queue.put({"sequence_id": seq_id, "skip": True})
                    continue 
                    
                self.internal_gather_queue.put(packet)
            except Exception as e:
                print(f"[CORE ERROR] Unexpected error processing packet: {e}")
                self.shutdown_event.set()
                break

class Gatherer:
    def __init__(self, config: dict, internal_gather_queue, processed_queue, shutdown_event):
        self.internal_gather_queue = internal_gather_queue
        self.processed_queue = processed_queue
        self.shutdown_event = shutdown_event
        self.window_size = config.get("processing", {}).get("stateful_tasks", {}).get("running_average_window_size", 10)
        self.active_workers = config.get("pipeline_dynamics", {}).get("core_parallelism", 1)
        
        # State managed by the Imperative Shell
        self.out_of_order_buffer = {} 
        self.expected_sequence_id = 1
        self.sliding_window = []

    def run(self):
        print(f"[GATHERER] Started sorting and calculating averages...")
        poison_pills_received = 0
        
        while not self.shutdown_event.is_set():
            try:
                packet = self.internal_gather_queue.get()
                if packet is None:
                    poison_pills_received += 1
                    if poison_pills_received >= self.active_workers:
                        print("[GATHERER] All workers finished. Shutting down.")
                        self.processed_queue.put(None)
                        break
                    continue
                    
                seq_id = packet.get("sequence_id")
                if seq_id is None:
                    continue

                self.out_of_order_buffer[seq_id] = packet
                
                while self.expected_sequence_id in self.out_of_order_buffer:
                    ready_packet = self.out_of_order_buffer.pop(self.expected_sequence_id)
                    
                    if ready_packet.get("skip"):
                        self.expected_sequence_id += 1
                        continue
                    
                    try:
                        metric = ready_packet["data"]["metric_value"]
                        if isinstance(metric, (int, float)):
                            # Hand off to the Functional Core
                            computed_avg, new_window_state = calculate_sliding_window(
                                metric, 
                                self.sliding_window, 
                                self.window_size
                            )
                            # Update Shell State
                            self.sliding_window = new_window_state
                            
                            ready_packet["computed_metric"] = computed_avg
                            self.processed_queue.put(ready_packet)
                    except KeyError:
                        pass
                    
                    self.expected_sequence_id += 1
                    
            except Exception as e:
                print(f"[GATHERER ERROR] Unexpected error: {e}")
                self.expected_sequence_id += 1