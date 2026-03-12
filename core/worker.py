import hashlib

SECRET_KEY = "sda_spring_2026_secure_key"
ITERATIONS = 100000

def generate_signature(raw_value_str: str, key: str, iterations: int) -> str:
    password_bytes = key.encode('utf-8')
    salt_bytes = raw_value_str.encode('utf-8')
    hash_bytes = hashlib.pbkdf2_hmac('sha256', password_bytes, salt_bytes, iterations)
    return hash_bytes.hex()

class CoreWorker:
    def __init__(self, config: dict, raw_queue, internal_gather_queue):
        self.raw_queue = raw_queue
        self.internal_gather_queue = internal_gather_queue

    def run(self):
        print(f"[{self.__class__.__name__}] Listening for packets...")
        while True:
            try:
                packet = self.raw_queue.get()
                if packet is None:
                    self.internal_gather_queue.put(None) 
                    break
                    
                if not isinstance(packet, dict):
                    print(f"[{self.__class__.__name__}] Error: Received non-dictionary packet. Dropping.")
                    continue

                payload = packet.get("data", {})
                metric = payload.get("metric_value")
                expected_hash = packet.get("hash")
                seq_id = packet.get("sequence_id", "UNKNOWN")
                
                if metric is None or expected_hash is None:
                    continue

                if not isinstance(metric, (int, float)):
                    print(f"[CORE WARNING] Metric is not a number for Seq ID {seq_id}. Dropping.")
                    continue
                
                raw_value_str = f"{metric:.2f}"
                calculated_hash = generate_signature(raw_value_str, SECRET_KEY, ITERATIONS)
                
                if calculated_hash != expected_hash:
                    print(f"[CORE WARNING] 🚨 SPOOFED OR CORRUPT PACKET! Dropping Sequence ID {seq_id}")
                    # THE FIX: Tell the gatherer to skip this ID!
                    self.internal_gather_queue.put({"sequence_id": seq_id, "skip": True})
                    continue 
                    
                self.internal_gather_queue.put(packet)

            except Exception as e:
                print(f"[CORE ERROR] Unexpected error processing packet: {e}")

class Gatherer:
    def __init__(self, config: dict, internal_gather_queue, processed_queue):
        self.internal_gather_queue = internal_gather_queue
        self.processed_queue = processed_queue
        self.window_size = config.get("processing", {}).get("running_average_window_size", 10)
        self.active_workers = config.get("pipeline_dynamics", {}).get("core_parallelism", 1)
        self.out_of_order_buffer = {} 
        self.expected_sequence_id = 1
        self.sliding_window = []


 
    def run(self):
        print(f"[GATHERER] Started sorting and calculating averages...")
        poison_pills_received = 0
        
        while True:
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
                    print("[GATHERER WARNING] Packet missing sequence_id. Dropping.")
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
                            self.sliding_window.append(metric)
                    except KeyError:
                        self.expected_sequence_id += 1
                        continue

                    if len(self.sliding_window) > self.window_size:
                        self.sliding_window.pop(0) 
                        
                    if len(self.sliding_window) > 0:
                        computed_avg = sum(self.sliding_window) / len(self.sliding_window)
                        ready_packet["computed_metric"] = computed_avg
                        self.processed_queue.put(ready_packet)
                    
                    self.expected_sequence_id += 1
                    
            except Exception as e:
                print(f"[GATHERER ERROR] Unexpected error: {e}")
                # Increment to prevent pipeline jam if a packet totally breaks
                self.expected_sequence_id += 1