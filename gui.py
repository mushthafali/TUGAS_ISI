import tkinter as tk
from tkinter import ttk
import requests
import threading
import time
import csv
from io import StringIO
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from collections import deque

# Konfigurasi InfluxDB
INFLUX_QUERY_URL = "http://localhost:8086/api/v2/query"
ORG = "percobaan"
BUCKET = "monitoring"
TOKEN = "R34N4zE4idIagbLqE7SOgv01XzmwwxFUyWTwZvILbH0_nWdBIfe_0AXIaz29emjH246t0v1twZtNpLzfFMTj9g=="

# Riwayat data
history_length = 50
temp_history = deque(maxlen=history_length)
rh_history = deque(maxlen=history_length)
time_history = deque(maxlen=history_length)

def get_latest_data():
    flux_query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -1m)
      |> filter(fn: (r) => r._measurement == "monitoring")
      |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
      |> last()
    '''

    headers = {
        "Authorization": f"Token {TOKEN}",
        "Content-Type": "application/vnd.flux",
        "Accept": "application/csv"
    }

    try:
        response = requests.post(
            INFLUX_QUERY_URL,
            params={"org": ORG},
            headers=headers,
            data=flux_query
        )

        reader = csv.DictReader(StringIO(response.text))
        data = {}
        for row in reader:
            try:
                field = row["_field"]
                value = float(row["_value"])
                data[field] = value
            except:
                continue

        if "temperature" in data and "humidity" in data:
            return data["temperature"], data["humidity"]
        return None
    except Exception as e:
        print("❌ Exception query Influx:", e)
        return None

def get_data_range(start_time, end_time):
    flux_query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: {start_time}, stop: {end_time})
      |> filter(fn: (r) => r._measurement == "monitoring")
      |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
    '''

    headers = {
        "Authorization": f"Token {TOKEN}",
        "Content-Type": "application/vnd.flux",
        "Accept": "application/csv"
    }

    try:
        response = requests.post(
            INFLUX_QUERY_URL,
            params={"org": ORG},
            headers=headers,
            data=flux_query
        )

        reader = csv.DictReader(StringIO(response.text))
        temp_map = {}
        rh_map = {}

        for row in reader:
            try:
                t = row["_time"]
                field = row["_field"]
                value = float(row["_value"])
                if field == "temperature":
                    temp_map[t] = value
                elif field == "humidity":
                    rh_map[t] = value
            except:
                continue

        sorted_keys = sorted(set(temp_map.keys()) & set(rh_map.keys()))
        temps = [temp_map[t] for t in sorted_keys]
        rhs = [rh_map[t] for t in sorted_keys]
        times = [t[11:19] for t in sorted_keys]  # jam:menit:detik

        return temps, rhs, times
    except Exception as e:
        print("❌ Exception query Influx:", e)
        return [], [], []

def update_data():
    while True:
        result = get_latest_data()
        current_time = time.strftime('%H:%M:%S')

        if result:
            temp, rh = result
            label_temp.config(text=f"Suhu: {temp:.1f} °C")
            label_rh.config(text=f"Kelembaban: {rh:.1f} %")
            status_label.config(text="✅ Data dari Influx")

            temp_history.append(temp)
            rh_history.append(rh)
            time_history.append(current_time)

            plot_graph()
        else:
            label_temp.config(text="Suhu: ---")
            label_rh.config(text="Kelembaban: ---")
            status_label.config(text="❌ Gagal ambil data")

        time.sleep(2)

def plot_graph():
    ax1.clear()
    ax2.clear()

    # Set background ke hitam
    fig.patch.set_facecolor('black')
    ax1.set_facecolor('black')
    ax2.set_facecolor('black')

    x = list(range(len(time_history)))
    times = list(time_history)

    # Plot data dengan garis dan warna yang kontras
    ax1.plot(x, list(temp_history), label='Suhu (°C)', color='red', marker='o', linestyle='-')
    ax2.plot(x, list(rh_history), label='Kelembaban (%)', color='cyan', marker='x', linestyle='-')

    ax1.set_title("Grafik Suhu", color='white')
    ax2.set_title("Grafik Kelembaban", color='white')
    ax1.set_ylabel("°C", color='white')
    ax2.set_ylabel("%", color='white')

    # Tampilkan hanya label waktu setiap 5 data
    interval = 5
    tick_positions = x[::interval]
    tick_labels = times[::interval]

    ax1.set_xticks(tick_positions)
    ax2.set_xticks(tick_positions)
    ax1.set_xticklabels(tick_labels, rotation=45, ha="right", color='white')
    ax2.set_xticklabels(tick_labels, rotation=45, ha="right", color='white')

    for ax in [ax1, ax2]:
        ax.tick_params(axis='y', colors='white')
        ax.tick_params(axis='x', colors='white')
        ax.grid(True, linestyle='--', alpha=0.3, color='gray')
        for spine in ax.spines.values():
            spine.set_color('white')

    fig.tight_layout()
    canvas.draw()

def show_history():
    start = entry_start.get()
    end = entry_end.get()
    temps, rhs, times = get_data_range(start, end)

    if temps and rhs:
        temp_history.clear()
        rh_history.clear()
        time_history.clear()

        temp_history.extend(temps)
        rh_history.extend(rhs)
        time_history.extend(times)

        label_temp.config(text="(Hist) Suhu: -- °C")
        label_rh.config(text="(Hist) RH: -- %")
        status_label.config(text="📈 Menampilkan data historis")
        plot_graph()
    else:
        status_label.config(text="❌ Tidak ada data historis")

# GUI Setup
root = tk.Tk()
root.title("Monitor SHT20 dari InfluxDB")
root.geometry("800x650")

label_temp = tk.Label(root, text="Suhu: -- °C", font=("Helvetica", 16))
label_temp.pack(pady=5)

label_rh = tk.Label(root, text="Kelembaban: -- %", font=("Helvetica", 16))
label_rh.pack(pady=5)

status_label = tk.Label(root, text="Status: ---", fg="blue")
status_label.pack(pady=5)

frame_input = tk.Frame(root)
frame_input.pack(pady=5)

tk.Label(frame_input, text="Start (RFC3339):").grid(row=0, column=0, padx=5)
entry_start = tk.Entry(frame_input, width=30)
entry_start.grid(row=0, column=1)

tk.Label(frame_input, text="End (RFC3339):").grid(row=1, column=0, padx=5)
entry_end = tk.Entry(frame_input, width=30)
entry_end.grid(row=1, column=1)

btn_show = tk.Button(frame_input, text="Tampilkan Riwayat", command=show_history)
btn_show.grid(row=0, column=2, rowspan=2, padx=10)

fig = Figure(figsize=(6, 4), dpi=100)
ax1 = fig.add_subplot(211)
ax2 = fig.add_subplot(212)

canvas = FigureCanvasTkAgg(fig, master=root)
canvas.get_tk_widget().pack(pady=10)

# Mulai update realtime
threading.Thread(target=update_data, daemon=True).start()

root.mainloop()
