import matplotlib.pyplot as plt
import matplotlib.animation as animation
import queue

# Import our new abstract blueprints!
from interface import Subject, Observer

# ==========================================
# 1. THE SUBJECT (The YouTuber)
# ==========================================
class PipelineTelemetry(Subject): # <-- Inherits from Abstract Subject
    def _init_(self, config, raw_queue, processed_queue):
        super()._init_() # Initializes the self._observers list from the parent class
        self.config = config
        self.raw_queue = raw_queue
        self.processed_queue = processed_queue
        self.max_size = config["pipeline_dynamics"]["stream_queue_max_size"]

    def notify_observers(self, state: dict):
        """Satisfies the Subject contract: Broadcasts state to all subscribers."""
        for observer in self._observers:
            observer.update(state)

    def poll_queues(self):
        """Checks the pipes and triggers the notification."""
        try:
            # Check how full the ticket rails are
            raw_size = self.raw_queue.qsize()
            processed_size = self.processed_queue.qsize()
        except NotImplementedError:
            # Fallback for some OS systems that block qsize()
            raw_size, processed_size = 0, 0

        # Grab all finished math out of the processed queue for plotting
        new_data = []
        while not self.processed_queue.empty():
            try:
                packet = self.processed_queue.get_nowait()
                if packet is None: # Poison pill check
                    continue
                new_data.append(packet)
            except queue.Empty:
                break

        # Broadcast the state to all subscribers
        state = {
            "raw_size": raw_size,
            "processed_size": processed_size,
            "max_size": self.max_size,
            "new_data": new_data
        }
        
        # Call our new abstract method!
        self.notify_observers(state)

# ==========================================
# 2. THE OBSERVER (The Subscriber / Dashboard)
# ==========================================
class LiveDashboard(Observer): # <-- Inherits from Abstract Observer
    def _init_(self, telemetry_subject):
        self.telemetry = telemetry_subject
        self.telemetry.attach(self) # Subscribe!
        
        # Internal memory to hold the last 50 data points for the live graph
        self.time_x = []
        self.raw_y = []
        self.avg_y = []
        self.memory_limit = 50 
        
        # State variables for the queues
        self.raw_fill = 0
        self.proc_fill = 0
        self.max_q = 1

        # Setup the Matplotlib Figure
        self.fig, (self.ax_queues, self.ax_chart) = plt.subplots(2, 1, figsize=(10, 8))
        self.fig.canvas.manager.set_window_title("Phase 3: Real-Time Telemetry")

    def update(self, state: dict):
        """Satisfies the Observer contract: Reacts to the broadcast."""
        self.raw_fill = state["raw_size"]
        self.proc_fill = state["processed_size"]
        self.max_q = state["max_size"]
        
        # Save the new data packets into our lists for plotting
        for packet in state["new_data"]:
            # NOTE: We access the nested "data" dictionary because of our new secure packet structure
            payload = packet.get("data", {})
            self.time_x.append(payload.get("time_period", 0))
            self.raw_y.append(payload.get("metric_value", 0))
            self.avg_y.append(packet.get("computed_metric", 0)) # Computed metric is attached to the main packet
            
            # Keep the lists from getting infinitely long (scrolling effect)
            if len(self.time_x) > self.memory_limit:
                self.time_x.pop(0)
                self.raw_y.pop(0)
                self.avg_y.pop(0)

    def _get_color(self, size, max_size):
        """Determines the Backpressure warning color."""
        ratio = size / max_size
        if ratio < 0.5:
            return 'green'   # Flowing smoothly
        elif ratio < 0.8:
            return 'yellow'  # Queue filling
        else:
            return 'red'     # Heavy backpressure!

    def animate(self, frame):
        """The 'Flipbook' function. Matplotlib calls this to redraw the screen."""
        self.telemetry.poll_queues() # Ask the subject to check the pipes

        # --- 1. Draw Telemetry (Queue Health) ---
        self.ax_queues.clear()
        self.ax_queues.set_title("Live Pipeline Telemetry (Backpressure Monitor)", fontweight="bold")
        self.ax_queues.set_ylim(0, self.max_q)
        
        labels = ['Raw Data Stream', 'Processed Stream']
        sizes = [self.raw_fill, self.proc_fill]
        colors = [self._get_color(self.raw_fill, self.max_q), self._get_color(self.proc_fill, self.max_q)]
        
        self.ax_queues.bar(labels, sizes, color=colors)
        self.ax_queues.set_ylabel("Items in Queue")

        # --- 2. Draw Data Charts (The Math) ---
        self.ax_chart.clear()
        self.ax_chart.set_title("Live Temperature & Running Average", fontweight="bold")
        
        if len(self.time_x) > 0:
            x_scroll = list(range(len(self.time_x)))
            self.ax_chart.plot(x_scroll, self.raw_y, label='Raw Temp', color='lightblue', marker='o')
            self.ax_chart.plot(x_scroll, self.avg_y, label='Running Avg (Window=10)', color='red', linewidth=2)
            
            self.ax_chart.legend(loc="upper left")
            self.ax_chart.set_ylabel("Metric Value")

        plt.tight_layout()

    def start_monitoring(self):
        """Starts the live animation loop."""
        self.ani = animation.FuncAnimation(self.fig, self.animate, interval=100, cache_frame_data=False)
        plt.show()