import time
import hashlib
import json

# --- Same Hash Rules as Input ---
SECRET_KEY = "sda_spring_2026_secure_key"
ITERATIONS = 100000

def generate_signature(raw_value_str: str, key: str, iterations: int) -> str:
    """Re-calculates the hash to ensure the packet wasn't spoofed."""
    password_bytes = key.encode('utf-8')
    salt_bytes = raw_value_str.encode('utf-8')
    hash_bytes = hashlib.pbkdf2_hmac('sha256', password_bytes, salt_bytes, ITERATIONS)
    return hash_bytes.hex()


# ==========================================
# 1. THE CORE WORKER (SCATTER & VERIFY)
# ==========================================
class CoreWorker:
    def _init_(self, config: dict, raw_queue, internal_gather_queue):
        self.config = config
        self.raw_queue = raw_queue
        self.internal_gather_queue = internal_gather_queue

    def run(self):
        print(f"[{self._class.name_}] Listening for packets...")
        
        while True:
            # 1. Pull the secure packet from the raw stream
            packet = self.raw_queue.get()
            
            # Poison pill check
            if packet is None:
                print(f"[{self._class.name_}] Poison pill received. Shutting down.")
                # Pass the pill down the line to the Gatherer!
                self.internal_gather_queue.put(None) 
                break
                
            payload = packet.get("data", {})
            metric = payload.get("metric_value")
            expected_hash = packet.get("hash")
            
            if metric is None or expected_hash is None:
                continue

            # 2. ANTI-SPOOFING VERIFICATION
            # Recalculate the hash based on the data we received
            raw_value_str = f"{metric:.2f}"
            calculated_hash = generate_signature(raw_value_str, SECRET_KEY, ITERATIONS)
            
            if calculated_hash != expected_hash:
                print(f"[{self._class.name_}] 🚨 SPOOFED PACKET DETECTED! Dropping Sequence ID {packet['sequence_id']}")
                continue # Skip this packet entirely!
                
            # 3. IF AUTHENTIC, PUSH TO GATHERER
            # The worker did the heavy CPU work (the hash verification). 
            # Now we send the safe data to the Gatherer to be sorted.
            self.internal_gather_queue.put(packet)


# ==========================================
# 2. THE GATHERER (SORT & SLIDING WINDOW)
# ==========================================
class Gatherer:
    def _init_(self, config: dict, internal_gather_queue, processed_queue):
        self.internal_gather_queue = internal_gather_queue
        self.processed_queue = processed_queue
        self.window_size = config["processing"]["running_average_window_size"]
        
        # This dictionary is our "pocket" to hold out-of-order tickets
        self.out_of_order_buffer = {} 
        self.expected_sequence_id = 1
        self.sliding_window = []

    def run(self):
        print(f"[GATHERER] Started sorting and calculating averages...")
        active_workers = self.config["pipeline_dynamics"]["core_parallelism"]
        poison_pills_received = 0
        
        while True:
            packet = self.internal_gather_queue.get()
            
            if packet is None:
                poison_pills_received += 1
                # Only shut down when ALL core workers have finished sending their pills
                if poison_pills_received == active_workers:
                    self.processed_queue.put(None) # Tell the dashboard we are done
                    break
                continue
                
            # 1. Store the packet in our temporary buffer
            seq_id = packet["sequence_id"]
            self.out_of_order_buffer[seq_id] = packet
            
            # 2. Process all packets that are now in perfect order!
            while self.expected_sequence_id in self.out_of_order_buffer:
                # Pull the correct packet out of the buffer
                ready_packet = self.out_of_order_buffer.pop(self.expected_sequence_id)
                metric = ready_packet["data"]["metric_value"]
                
                # --- PURE FUNCTIONAL SLIDING WINDOW ---
                # Add the new metric
                self.sliding_window.append(metric)
                
                # Trim the window if it gets too big (e.g., larger than 10)
                if len(self.sliding_window) > self.window_size:
                    self.sliding_window.pop(0) # Remove the oldest item
                    
                # Calculate the mathematical average
                computed_avg = sum(self.sliding_window) / len(self.sliding_window)
                
                # Attach the answer to the packet
                ready_packet["computed_metric"] = computed_avg
                
                # Push the perfectly sorted, averaged packet to the Output!
                self.processed_queue.put(ready_packet)
                
                # Move to the next ticket number
                self.expected_sequence_id += 1