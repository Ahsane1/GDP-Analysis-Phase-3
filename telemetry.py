import matplotlib.pyplot as plt
import matplotlib.animation as animation
import queue
import os 
from interface import Subject, Observer

class PipelineTelemetry(Subject):
    def __init__(self, config, raw_queue, processed_queue):
        super().__init__()
        self.raw_queue = raw_queue
        self.processed_queue = processed_queue
        try:
            self.max_size = config["pipeline_dynamics"]["stream_queue_max_size"]
        except KeyError:
            self.max_size = 50 

    def notify_observers(self, state: dict):
        for observer in self._observers:
            observer.update(state)

    def poll_queues(self):
        try:
            raw_size = self.raw_queue.qsize()
            processed_size = self.processed_queue.qsize()
        except NotImplementedError:
            raw_size, processed_size = 0, 0

        new_data = []
        shutdown_signal = False  # <-- Add this flag
        
        while not self.processed_queue.empty():
            try:
                packet = self.processed_queue.get_nowait()
                if packet is None: 
                    shutdown_signal = True # <-- Catch the poison pill!
                    continue
                new_data.append(packet)
            except queue.Empty:
                break

        state = {
            "raw_size": raw_size,
            "processed_size": processed_size,
            "max_size": self.max_size,
            "new_data": new_data,
            "shutdown": shutdown_signal  # <-- Add it to the state dictionary
        }
        self.notify_observers(state)



        

class LiveDashboard(Observer):
    def __init__(self, telemetry_subject, config):
        self.telemetry = telemetry_subject
        self.telemetry.attach(self)
        self.time_x = []
        self.raw_y = []
        self.avg_y = []
        self.memory_limit = 50 
        self.raw_fill, self.proc_fill, self.max_q = 0, 0, 1

        # Dynamic Titles from Config
        charts_cfg = config["visualizations"]["data_charts"]
        self.chart_title = f"{charts_cfg[0]['title']} & {charts_cfg[1]['title']}"
        self.y_axis_label = charts_cfg[0]["y_axis"]

        self.fig, (self.ax_queues, self.ax_chart) = plt.subplots(2, 1)
        self.fig.subplots_adjust(left=0.08, right=0.95, top=0.92, bottom=0.25, hspace=0.4)
        self.fig.canvas.manager.set_window_title("Phase 3: Real-Time Telemetry")

        try:
            self.fig.canvas.manager.window.state('zoomed')
        except Exception:
            pass

    def update(self, state: dict):

        if state.get("shutdown"):
            # Use a custom flag to ensure we only print and pause once
            if not getattr(self, '_is_finished', False):
                self._is_finished = True
                print("\n[DASHBOARD] Pipeline empty. Processing complete!")
                print("[DASHBOARD] The graph will remain open for analysis. Close the window to exit.")
                
                try:
                    # Safely pause the animation without destroying the underlying timer
                    if hasattr(self, 'ani'):
                        self.ani.pause() 
                    
                    # Update the window title
                    self.fig.canvas.manager.set_window_title("Phase 3: Real-Time Telemetry [FINISHED]")
                except Exception as e:
                    pass
            
            # Keep the window alive
            return
        
        self.raw_fill = state.get("raw_size", 0)
        self.proc_fill = state.get("processed_size", 0)
        self.max_q = state.get("max_size", 1)
        
        for packet in state.get("new_data", []):
            try:
                payload = packet.get("data", {})
                time_val = payload.get("time_period", 0)
                raw_val = payload.get("metric_value", 0)
                avg_val = packet.get("computed_metric", 0)
                
                if all(isinstance(v, (int, float)) for v in [time_val, raw_val, avg_val]):
                    self.time_x.append(time_val)
                    self.raw_y.append(raw_val)
                    self.avg_y.append(avg_val)
                
                if len(self.time_x) > self.memory_limit:
                    self.time_x.pop(0)
                    self.raw_y.pop(0)
                    self.avg_y.pop(0)
            except Exception:
                pass

    def _get_color(self, size, max_size):
        if max_size <= 0: return 'green'
        ratio = size / max_size
        if ratio < 0.5: return 'green'
        elif ratio < 0.8: return 'yellow'
        else: return 'red'

    def animate(self, frame):
        try:
            self.telemetry.poll_queues()
            self.ax_queues.clear()
            self.ax_queues.set_title("Live Pipeline Telemetry (Backpressure Monitor)", fontweight="bold")
            self.ax_queues.set_ylim(0, self.max_q if self.max_q > 0 else 50)
            
            labels = ['Raw Data Stream', 'Processed Stream']
            sizes = [self.raw_fill, self.proc_fill]
            colors = [self._get_color(self.raw_fill, self.max_q), self._get_color(self.proc_fill, self.max_q)]
            
            self.ax_queues.bar(labels, sizes, color=colors)
            self.ax_queues.set_ylabel("Items in Queue")

            self.ax_chart.clear()
            self.ax_chart.set_title(self.chart_title, fontweight="bold")
            
            if len(self.time_x) > 0 and len(self.time_x) == len(self.raw_y) == len(self.avg_y):
                x_scroll = list(range(len(self.time_x)))
                self.ax_chart.plot(x_scroll, self.raw_y, label='Raw Value', color='lightblue', marker='o')
                self.ax_chart.plot(x_scroll, self.avg_y, label='Running Avg', color='red', linewidth=2)
                self.ax_chart.legend(loc="upper left")
                self.ax_chart.set_ylabel(self.y_axis_label)

        except Exception as e:
            print(f"[ANIMATION ERROR] Graph failed to draw frame: {e}")

    def start_monitoring(self):
        try:
            self.ani = animation.FuncAnimation(self.fig, self.animate, interval=100, cache_frame_data=False)
            plt.show()
        except Exception as e:
            print(f"[DASHBOARD ERROR] Could not start animation backend: {e}")