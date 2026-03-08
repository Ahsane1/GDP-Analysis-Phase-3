import time

class CoreWorker:
    def __init__(self, config: dict, input_queue, output_queue):
        self.config = config
        self.input_queue = input_queue
        self.output_queue = output_queue
        
        # Grab our processing rules from the "brain"
        self.operation = self.config["processing"]["operation"]
        self.window_size = self.config["processing"]["running_average_window_size"]

    def _compute_running_average(self, current_window: tuple, new_value: float) -> tuple:
        """
        PURE FUNCTIONAL LOGIC:
        Takes an immutable tuple (the old snapshot) and a new value.
        Returns a BRAND NEW tuple (the new snapshot) and the calculated average.
        We do NOT mutate or .append() to any existing lists.
        """
        # Create a brand new tuple by adding the new value to the end
        updated_window = current_window + (new_value,)
        
        # If the tuple is larger than our allowed window (e.g., 10), 
        # slice off the oldest item at the front.
        if len(updated_window) > self.window_size:
            updated_window = updated_window[1:]
            
        # Calculate the mathematical average of this new snapshot
        avg = sum(updated_window) / len(updated_window)
        
        return updated_window, avg

    def run(self):
        """
        The continuous loop where the worker pulls tickets from the queue.
        """
        # This is our local, functional state tracker. 
        # It starts as an empty tuple (). No global variables!
        local_window_state = ()
        
        print(f"[{self.__class__.__name__}] Started listening to the Raw Data Stream...")
        
        while True:
            # 1. Pull a generic packet from the ticket rail (Queue 1)
            packet = self.input_queue.get()
            
            # 2. Check for the Poison Pill (The restaurant is closing!)
            if packet is None:
                print(f"[{self.__class__.__name__}] Received poison pill. Shutting down.")
                break
                
            # 3. Extract the generic data
            metric = packet.get("metric_value")
            
            # If the data is empty or corrupted, skip to the next ticket
            if metric is None:
                continue

            # 4. Perform the Functional Transformation
            if self.operation == "running_average":
                # We pass the old photo in, and get a brand new photo back out
                local_window_state, computed_avg = self._compute_running_average(
                    local_window_state, metric
                )
                
                # 5. Attach the computed result back into the packet
                packet["computed_metric"] = computed_avg
                
            # 6. Push the finished packet to the second ticket rail (Queue 2)
            self.output_queue.put(packet)
            
            # A tiny microscopic sleep to ensure we don't accidentally hog the CPU 
            # and let the other workers grab tickets too.
            time.sleep(0.1)