import csv
import time
import hashlib
import sys

SECRET_KEY = "sda_spring_2026_secure_key"
ITERATIONS = 100000

def generate_signature(raw_value_str: str, key: str, iterations: int) -> str:
    password_bytes = key.encode('utf-8')
    salt_bytes = raw_value_str.encode('utf-8')
    hash_bytes = hashlib.pbkdf2_hmac('sha256', password_bytes, salt_bytes, iterations)
    return hash_bytes.hex()

class GenericCSVReader:
    def __init__(self, config: dict, output_queue):
        self.config = config
        self.output_queue = output_queue
        self.file_path = self.config.get("dataset_path", "")
        self.delay = self.config["pipeline_dynamics"].get("input_delay_seconds", 0.1)
        self.columns_config = self.config["schema_mapping"].get("columns", [])

    def cast_type(self, value: str, data_type: str):
        if value is None or str(value).strip() == "":
            return None
        try:
            if data_type == "integer": return int(float(value))
            elif data_type == "float": return float(value)
            else: return str(value)
        except (ValueError, TypeError):
            return None # Safely handle "abc" when expecting a float

    def run(self):
        print(f"[INPUT] Starting to read {self.file_path}...")
        current_sequence_id = 1 
        
        try:
            with open(self.file_path, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    generic_packet = {}
                    try:
                        for col_rule in self.columns_config:
                            raw_name = col_rule.get("source_name")
                            internal_name = col_rule.get("internal_mapping")
                            target_type = col_rule.get("data_type")
                            
                            if not raw_name or not internal_name:
                                continue

                            raw_value = row.get(raw_name)
                            generic_packet[internal_name] = self.cast_type(raw_value, target_type)
                        
                        metric = generic_packet.get("metric_value")
                        
                        # Type check: metric must be a number to format it as .2f
                        if metric is None or not isinstance(metric, (int, float)):
                            print(f"[INPUT WARNING] Skipping row {current_sequence_id}: metric_value is missing or invalid.")
                            continue

                        raw_value_str = f"{metric:.2f}"
                        signature = generate_signature(raw_value_str, SECRET_KEY, ITERATIONS)

                        secure_packet = {
                            "sequence_id": current_sequence_id,
                            "hash": signature,
                            "data": generic_packet
                        }
                        
                        self.output_queue.put(secure_packet)
                        current_sequence_id += 1
                        time.sleep(self.delay)
                        
                    except Exception as e:
                        print(f"[INPUT ERROR] Failed to process row {current_sequence_id}. Error: {e}")
                        continue
                    
        except FileNotFoundError:
            print(f"[INPUT ERROR] CRITICAL: Could not find file at '{self.file_path}'.")
            sys.exit(1)
        except Exception as e:
            print(f"[INPUT ERROR] Unexpected error opening file: {e}")

        print("[INPUT] Finished reading file. Sending poison pills...")
        num_workers = self.config["pipeline_dynamics"].get("core_parallelism", 1)
        for _ in range(num_workers):
            self.output_queue.put(None)