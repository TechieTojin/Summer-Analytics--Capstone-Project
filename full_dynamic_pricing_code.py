
# Dynamic Pricing for Urban Parking Lots
# Summer Analytics 2025 – Model 1 and Model 2 (Real-time Simulation using Pathway)
# Author:Jaiby Joseph

# -------------------------------------------
# STEP 0: INSTALL REQUIRED LIBRARIES
# -------------------------------------------
# Run this only once if not installed
# pip install pathway bokeh panel pandas numpy

# -------------------------------------------
# STEP 1: IMPORTS
# -------------------------------------------
import pandas as pd
import numpy as np
import pathway as pw
import panel as pn
import bokeh.plotting
from datetime import datetime, timedelta

# -------------------------------------------
# STEP 2: LOAD AND PREPROCESS DATA
# -------------------------------------------
df = pd.read_csv("dataset (1).csv")

# Combine LastUpdatedDate and LastUpdatedTime into one timestamp
df["Timestamp"] = pd.to_datetime(df["LastUpdatedDate"] + " " + df["LastUpdatedTime"], format="%d-%m-%Y %H:%M:%S")
df["TrafficConditionNearby"] = df["TrafficConditionNearby"].str.lower()
df["VehicleType"] = df["VehicleType"].str.lower()

# Save preprocessed data for streaming
df.to_csv("parking_stream.csv", index=False)

# -------------------------------------------
# STEP 3: DEFINE PATHWAY SCHEMA
# -------------------------------------------
class ParkingSchema(pw.Schema):
    ID: int
    SystemCodeNumber: str
    Capacity: int
    Latitude: float
    Longitude: float
    Occupancy: int
    VehicleType: str
    TrafficConditionNearby: str
    QueueLength: int
    IsSpecialDay: int
    LastUpdatedDate: str
    LastUpdatedTime: str
    Timestamp: str

# Load the stream
stream = pw.demo.replay_csv("parking_stream.csv", schema=ParkingSchema, input_rate=500)

# -------------------------------------------
# STEP 4: FEATURE ENGINEERING
# -------------------------------------------
fmt = "%Y-%m-%d %H:%M:%S"

enriched = stream.with_columns(
    ts = stream.Timestamp.dt.strptime(fmt),
    day = stream.Timestamp.dt.strptime(fmt).dt.strftime("%Y-%m-%dT00:00:00"),

    vehicle_weight = pw.case([
        (stream.VehicleType == "car", 1.0),
        (stream.VehicleType == "bike", 0.5),
        (stream.VehicleType == "truck", 1.5)
    ], default=1.0),

    traffic_weight = pw.case([
        (stream.TrafficConditionNearby == "low", 0.5),
        (stream.TrafficConditionNearby == "medium", 1.0),
        (stream.TrafficConditionNearby == "high", 1.5)
    ], default=1.0)
)

# -------------------------------------------
# STEP 5: MODEL 2 – DEMAND-BASED PRICING
# -------------------------------------------
@pw.udf
def compute_price(base, occ, cap, queue, traffic_w, is_special, veh_w):
    # Demand formula
    demand = (occ / cap) * 1.2 + queue * 0.5 - traffic_w * 0.7 + is_special * 0.5 + veh_w * 0.3
    demand = max(0.0, min(demand, 2.0))  # Normalize to [0, 2]
    price = base * (1 + 0.5 * demand)    # Moderate scaling
    return round(price, 2)

prices = enriched.with_columns(
    base_price = pw.const(10.0),
    dynamic_price = compute_price(
        10.0,
        enriched.Occupancy,
        enriched.Capacity,
        enriched.QueueLength,
        enriched.traffic_weight,
        enriched.IsSpecialDay,
        enriched.vehicle_weight
    )
)

# -------------------------------------------
# STEP 6: WINDOWING FOR DAILY AGGREGATE VISUALIZATION
# -------------------------------------------
windowed = (
    prices.windowby(
        pw.this.ts,
        instance=pw.this.day + "_" + pw.this.SystemCodeNumber,
        window=pw.temporal.tumbling(timedelta(days=1)),
        behavior=pw.temporal.exactly_once_behavior()
    )
    .reduce(
        ts = pw.this._pw_window_end,
        avg_price = pw.reducers.mean(pw.this.dynamic_price),
        lot = pw.reducers.first(pw.this.SystemCodeNumber)
    )
)

# -------------------------------------------
# STEP 7: VISUALIZATION WITH BOKEH
# -------------------------------------------
pn.extension()

def price_plotter(source):
    fig = bokeh.plotting.figure(
        height=400,
        width=800,
        title="Daily Avg Dynamic Price per Parking Lot",
        x_axis_type="datetime"
    )
    fig.line("ts", "avg_price", source=source, line_width=2, color="blue")
    fig.circle("ts", "avg_price", source=source, size=5, color="red")
    return fig

viz = windowed.plot(price_plotter, sorting_col="ts")
pn.Column(viz).servable()

# -------------------------------------------
# STEP 8: RUN THE PIPELINE
# -------------------------------------------
# To start real-time processing, uncomment the following line:
pw.run()
