import csv
import time

class GenericCSVReader:
    def __init__(self, config: dict, output_queue):
        """
        config: The loaded config.json dictionary.
        output_queue: The multiprocessing.Queue where we drop translated rows.
        """
        self.config = config
        self.output_queue = output_queue
        
        # Grab the rules from the config
        self.file_path = self.config["dataset_path"]
        self.delay = self.config["pipeline_dynamics"]["input_delay_seconds"]
        self.columns_config = self.config["schema_mapping"]["columns"]

    def cast_type(self, value: str, data_type: str):
        """
        Converts the raw text from the CSV into the correct Python data type.
        """
        if value is None or value.strip() == "":
            return None
            
        try:
            if data_type == "integer":
                return int(float(value)) # float() first in case it's like "20.0"
            elif data_type == "float":
                return float(value)
            else:
                return str(value)
        except ValueError:
            return None # If the data is corrupted, return None

    def run(self):
        """
        The main loop. Opens the CSV, reads a row, translates it, and pushes it to the queue.
        """
        print(f"[INPUT] Starting to read {self.file_path}...")
        
        with open(self.file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                # 1. Create a brand new, generic empty packet
                generic_packet = {}
                
                # 2. Translate the row using our config rules
                for col_rule in self.columns_config:
                    raw_name = col_rule["source_name"]       # e.g., "Country Name"
                    internal_name = col_rule["internal_mapping"] # e.g., "entity_name"
                    target_type = col_rule["data_type"]      # e.g., "string"
                    
                    # Grab the raw value from the CSV
                    raw_value = row.get(raw_name)
                    
                    # Clean it and cast it
                    clean_value = self.cast_type(raw_value, target_type)
                    
                    # Store it under the new, generic name
                    generic_packet[internal_name] = clean_value
                
                # 3. Drop the translated packet onto the ticket rail (Queue)
                self.output_queue.put(generic_packet)
                
                # 4. Sleep to simulate a live data stream and respect config delays
                time.sleep(self.delay)
                
        # When the file is done, send a "POISON PILL" to tell the workers to stop
        print("[INPUT] Finished reading file. Sending stop signals...")
        
        # We send one poison pill for each worker we have
        num_workers = self.config["pipeline_dynamics"]["core_parallelism"]
        for _ in range(num_workers):
            self.output_queue.put(None)