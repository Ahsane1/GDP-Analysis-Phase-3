import csv
import time
import hashlib


SECRET_KEY = "sda_spring_2026_secure_key"
ITERATIONS = 100000

def generate_signature(raw_value_str: str, key: str, iterations: int) -> str:
    """
    Generates a PBKDF2 HMAC SHA-256 signature for the given value.
    Treats the secret key as the password and the raw value as the salt.
    """
    password_bytes = key.encode('utf-8')
    salt_bytes = raw_value_str.encode('utf-8')
    
    hash_bytes = hashlib.pbkdf2_hmac(
        hash_name='sha256', 
        password=password_bytes, 
        salt=salt_bytes, 
        iterations=iterations
    )
    return hash_bytes.hex()


class GenericCSVReader:
    def _init_(self, config: dict, output_queue):
        self.config = config
        self.output_queue = output_queue
        
        self.file_path = self.config["dataset_path"]
        self.delay = self.config["pipeline_dynamics"]["input_delay_seconds"]
        self.columns_config = self.config["schema_mapping"]["columns"]

    def cast_type(self, value: str, data_type: str):
        if value is None or value.strip() == "":
            return None
            
        try:
            if data_type == "integer":
                return int(float(value))
            elif data_type == "float":
                return float(value)
            else:
                return str(value)
        except ValueError:
            return None

    def run(self):
        print(f"[INPUT] Starting to read {self.file_path}...")
        
        # We start our ticket counter at 1 for the Scatter-Gather sorting
        current_sequence_id = 1 
        
        with open(self.file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                generic_packet = {}
                
                # 1. Translate the row
                for col_rule in self.columns_config:
                    raw_name = col_rule["source_name"]
                    internal_name = col_rule["internal_mapping"]
                    target_type = col_rule["data_type"]
                    
                    raw_value = row.get(raw_name)
                    clean_value = self.cast_type(raw_value, target_type)
                    generic_packet[internal_name] = clean_value
                
                # 2. Extract the metric, round it, and Hash it!
                metric = generic_packet.get("metric_value")
                if metric is not None:
                    # Round to 2 decimal places as a string for the salt
                    raw_value_str = f"{metric:.2f}"
                    signature = generate_signature(raw_value_str, SECRET_KEY, ITERATIONS)
                else:
                    signature = None

                # 3. Create the final SECURE wrapper packet
                secure_packet = {
                    "sequence_id": current_sequence_id,
                    "hash": signature,
                    "data": generic_packet  # The actual row data is tucked inside
                }
                
                # 4. Push to queue, increment our ID, and sleep
                self.output_queue.put(secure_packet)
                current_sequence_id += 1
                time.sleep(self.delay)
                
        print("[INPUT] Finished reading file. Sending poison pills...")
        
        # One poison pill for every core worker so they all shut down
        num_workers = self.config["pipeline_dynamics"]["core_parallelism"]
        for _ in range(num_workers):
            self.output_queue.put(None)