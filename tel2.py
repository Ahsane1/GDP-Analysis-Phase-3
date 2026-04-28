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



import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from interface import Observer

class LiveDashboard(Observer):
    def __init__(self, telemetry_subject, config):
        self.telemetry = telemetry_subject
        self.telemetry.attach(self)
        
        # --- State Buffers ---
        self.time_x = []
        self.raw_y = []
        self.avg_y = []
        self.memory_limit = 50 
        self.raw_fill, self.proc_fill, self.max_q = 0, 0, 1
        self._is_finished = False

        # --- Dynamic Titles from Config ---
        charts_cfg = config["visualizations"]["data_charts"]
        self.chart_title = f"{charts_cfg[0]['title']} & {charts_cfg[1]['title']}"
        self.y_axis_label = charts_cfg[0]["y_axis"]

        # --- Dash Application Setup ---
        self.app = dash.Dash(__name__)
        self._setup_layout()
        self._setup_callbacks()

    def _setup_layout(self):
        """Defines the dark-themed HTML layout of the dashboard."""
        self.app.layout = html.Div(style={'backgroundColor': '#111111', 'minHeight': '100vh', 'padding': '20px'}, children=[
            html.H2(id='dashboard-title', children="Phase 3: Real-Time Telemetry", 
                    style={'color': '#ffffff', 'textAlign': 'center', 'fontFamily': 'sans-serif'}),
            
            dcc.Graph(id='live-backpressure-graph', animate=False, style={'height': '80vh'}),
            
            # The interval component replaces matplotlib's FuncAnimation timer
            dcc.Interval(id='graph-update', interval=100, n_intervals=0)
        ])

    def update(self, state: dict):
        """Observer callback triggered by the telemetry subject."""
        if state.get("shutdown"):
            if not getattr(self, '_is_finished', False):
                self._is_finished = True
                print("\n[DASHBOARD] Pipeline empty. Processing complete!")
                print("[DASHBOARD] The graph will remain open for analysis in the browser.")
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
                
                # Sliding window logic
                if len(self.time_x) > self.memory_limit:
                    self.time_x.pop(0)
                    self.raw_y.pop(0)
                    self.avg_y.pop(0)
            except Exception:
                pass

    def _get_color(self, size, max_size):
        """Explicit threshold logic mapped to Plotly hex colors."""
        if max_size <= 0: return '#00ff88' # Neon Green
        ratio = size / max_size
        if ratio < 0.5: return '#00ff88'   # Neon Green
        elif ratio < 0.8: return '#ffcc00' # Yellow/Orange
        else: return '#ff3366'             # Warning Red

    def _setup_callbacks(self):
        """Binds the polling mechanism to the Dash UI."""
        @self.app.callback(
            [Output('live-backpressure-graph', 'figure'),
             Output('dashboard-title', 'children')],
            [Input('graph-update', 'n_intervals')]
        )
        def render_frame(n):
            title_text = "Phase 3: Real-Time Telemetry"
            
            if self._is_finished:
                title_text = "Phase 3: Real-Time Telemetry [FINISHED]"
                # Dash.no_update prevents the graph from re-rendering once shut down
                return dash.no_update, title_text

            # Trigger the subject to poll the queues, which fires self.update()
            self.telemetry.poll_queues()

            # --- Construct the Figure ---
            fig = make_subplots(
                rows=2, cols=1, 
                row_heights=[0.4, 0.6], 
                vertical_spacing=0.15,
                subplot_titles=("Live Pipeline Telemetry (Backpressure Monitor)", self.chart_title)
            )

            # 1. Bar Chart (Queues)
            queue_colors = [
                self._get_color(self.raw_fill, self.max_q),
                self._get_color(self.proc_fill, self.max_q)
            ]
            fig.add_trace(go.Bar(
                x=['Raw Data Stream', 'Processed Stream'],
                y=[self.raw_fill, self.proc_fill],
                marker_color=queue_colors,
                width=0.4
            ), row=1, col=1)

            # 2. Line Chart (Metrics)
            if len(self.time_x) > 0:
                x_scroll = list(range(len(self.time_x)))
                
                # Raw Value (Light Blue, with markers)
                fig.add_trace(go.Scatter(
                    x=x_scroll, y=self.raw_y,
                    name='Raw Value',
                    mode='lines+markers',
                    line=dict(color='lightblue', width=1),
                    marker=dict(size=6)
                ), row=2, col=1)

                # Running Average (Red, thicker line)
                fig.add_trace(go.Scatter(
                    x=x_scroll, y=self.avg_y,
                    name='Running Avg',
                    mode='lines',
                    line=dict(color='#ff3366', width=3, shape='spline') # Spline makes it smooth
                ), row=2, col=1)

            # --- Apply Dark Theme Formatting ---
            fig.update_layout(
                template='plotly_dark',
                plot_bgcolor='#111111',
                paper_bgcolor='#111111',
                showlegend=True,
                margin=dict(l=40, r=40, t=60, b=40)
            )

            # Format Y-Axes
            y_limit = self.max_q if self.max_q > 0 else 50
            fig.update_yaxes(title_text="Items in Queue", range=[0, y_limit], row=1, col=1, showgrid=True, gridcolor='#333333')
            fig.update_yaxes(title_text=self.y_axis_label, row=2, col=1, showgrid=True, gridcolor='#333333')

            return fig, title_text
        
    def start_monitoring(self):
        """Starts the local web server instead of matplotlib."""
        try:
            print("[DASHBOARD] Starting UI. Open http://127.0.0.1:8051 in your browser.")
            # Changed .run_server() to .run() to support modern Dash versions
            self.app.run(debug=False, port=8051) 
        except Exception as e:
            print(f"[DASHBOARD ERROR] Could not start Dash backend: {e}")