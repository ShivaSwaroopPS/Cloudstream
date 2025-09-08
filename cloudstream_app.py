import websocket
import json
import threading
import time
import pandas as pd
from datetime import datetime
import os
import pytz
import numpy as np
import sys

# === CONFIG ===
london_tz = pytz.timezone("Europe/London")
API_KEY = "23b50ec8c155e07c0513000000006a302a896661cdd15c0ef8d93ec6ff38a8ef6b55355fa4a91651cbe0"
output_folder = r"C:\Users\SHIVA.SWAROOP.P.S\OneDrive - S&P Global\Desktop\CSV_Files"
AUTO_SAVE_INTERVAL = 30 * 60  # seconds (30 min)
DASHBOARD_INTERVAL = 30       # seconds (live summary)

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# ✅ Only entitled streams
stream_names_list = [
    "energy_gas_reference",
    "energy_gas_trx_cegh-vtp",
    "energy_gas_trx_cz-vtp",
    "energy_gas_trx_etf",
    "energy_gas_trx_nbp",
    "energy_gas_trx_peg",
    "energy_gas_trx_pvb",
    "energy_gas_trx_the",
    "energy_gas_trx_ttf",
    "energy_gas_trx_ztp"
]

# === STREAM REGISTRY ===
streams = {}
for stream_name in stream_names_list:
    streams[stream_name] = {
        "subscribed": False,
        "in_snapshot_cycle": False,
        "market_data": [],
        "instrument": [],
        "order": [],
        "trade": [],
        "settlement": [],
        "counters": {"market_data": 0, "instrument": 0, "order": 0, "trade": 0, "settlement": 0}
    }

error_log = []


# === HELPERS ===
def get_value(entry, key, subkey="Value"):
    """Extract nested dict values safely."""
    val = entry.get(key)
    if isinstance(val, dict):
        return val.get(subkey, np.nan)
    return val if val is not None else np.nan


# === SAVE TO DISK ===
def save_data(folder_path, autosave=False):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for stream, stream_data in streams.items():
        if not stream_data.get("subscribed", False):
            continue  # skip failed/not entitled streams

        file_tag = "autosave" if autosave else "final"
        base_path = os.path.join(folder_path, f"{stream}_{file_tag}_{timestamp}")

        dfs = {}
        if stream_data["trade"]:
            dfs["trade"] = pd.DataFrame(stream_data["trade"])
        if stream_data["order"]:
            dfs["order"] = pd.DataFrame(stream_data["order"])
        if stream_data["settlement"]:
            dfs["settlement"] = pd.DataFrame(stream_data["settlement"])
        if stream_data["instrument"]:
            dfs["instrument"] = pd.DataFrame(stream_data["instrument"])
        if stream_data["market_data"]:
            dfs["market_data"] = pd.DataFrame(stream_data["market_data"])

        if dfs:
            # Excel snapshot
            with pd.ExcelWriter(base_path + ".xlsx", engine="xlsxwriter") as writer:
                for name, df in dfs.items():
                    df.to_excel(writer, sheet_name=name, index=False)

            # Parquet for scalable storage
            for name, df in dfs.items():
                df.to_parquet(base_path + f"_{name}.parquet", index=False)

            print(f"💾 {'[AUTO-SAVE]' if autosave else ''} Saved data for {stream}")

    if error_log:
        pd.DataFrame(error_log).to_excel(
            os.path.join(folder_path, f"error_log_{timestamp}.xlsx"), index=False
        )
        print("⚠️ Error log saved.")


# === AUTO-SAVE THREAD ===
def autosave_loop():
    while True:
        time.sleep(AUTO_SAVE_INTERVAL)
        print("\n⏳ Auto-save triggered...")
        save_data(output_folder, autosave=True)


# === DASHBOARD THREAD (CLI only) ===
def dashboard_loop():
    while True:
        time.sleep(DASHBOARD_INTERVAL)
        print("\n📊 ====== Live Data Dashboard ======")
        for stream, data in streams.items():
            if not data.get("subscribed", False):
                continue
            c = data["counters"]
            print(f"{stream:<25} | Trades={c['trade']:<6} Orders={c['order']:<6} "
                  f"Instruments={c['instrument']:<6} Settlements={c['settlement']:<6} "
                  f"MarketData={c['market_data']:<6}")
        print("===================================")


# === MESSAGE HANDLERS ===
def send_subscribe(ws, stream_names_list, request_id=123123001):
    subscribe_message = {
        "event": "subscribe",
        "requestId": request_id,
        "subscribe": {"stream": [{"stream": i} for i in stream_names_list]}
    }
    for stream in stream_names_list:
        streams[stream]["requestId"] = request_id
    ws.send(json.dumps(subscribe_message))
    print("📡 Sent subscription request")


def increment_counter(stream, category):
    streams[stream]["counters"][category] += 1


def on_message(ws, message):
    msg = json.loads(message)
    stream = msg.get("subs")
    if stream not in streams:
        return

    entry = msg.get("messages")[0]
    msg_type = entry.get("@type")

    if msg_type == "type.googleapis.com/Client.Response":
        request_id = entry.get("requestId")
        if int(request_id) == int(streams[stream].get("requestId")):
            status = entry.get("status")
            if status is None:
                streams[stream]["subscribed"] = True
                print(f"✅ Subscription successful for {stream}")
            else:
                print(f"❌ Subscription failed for {stream}, status: {status}")
                error_log.append({"stream": stream, "status": status})
                # Auto-drop streams that are not entitled
                streams.pop(stream, None)
        return

    if not streams[stream].get("subscribed"):
        return

    if msg_type == "type.googleapis.com/dbag.energy.MarketDataReport":
        streams[stream]["market_data"].append({
            'subs': msg.get('subs'),
            'seq': msg.get('seq'),
            'Evt': entry.get('Evt'),
            'Cnt': entry.get('Cnt', 0),
            'Message_Received_at': datetime.now()
        })
        increment_counter(stream, "market_data")

    elif msg_type == "type.googleapis.com/dbag.energy.Instrument":
        streams[stream]["instrument"].append(entry)
        increment_counter(stream, "instrument")

    elif msg_type == "type.googleapis.com/dbag.energy.Trade":
        streams[stream]["trade"].append({
            "subs": msg.get("subs"),
            "seq": msg.get("seq"),
            "ID": entry.get("ID", np.nan),
            "TrdID": entry.get("TrdID", np.nan),
            "UpdtAct": get_value(entry, "UpdtAct"),
            "Px": entry.get("Px", np.nan),
            "Sz": entry.get("Sz", np.nan),
            "Tm": entry.get("Tm", np.nan),
        })
        increment_counter(stream, "trade")

    elif msg_type == "type.googleapis.com/dbag.energy.Order":
        streams[stream]["order"].append({
            "subs": msg.get("subs"),
            "seq": msg.get("seq"),
            "ID": entry.get("ID", np.nan),
            "OrdID": entry.get("OrdID", np.nan),
            "Typ": entry.get("Typ", np.nan),
            "UpdtAct": get_value(entry, "UpdtAct"),
            "Px": entry.get("Px", np.nan),
            "Sz": entry.get("Sz", np.nan),
            "MDQteTyp": get_value(entry, "MDQteTyp"),
            "TmInForce": get_value(entry, "TmInForce"),
            "ImpldMktInd": entry.get("ImpldMktInd", np.nan),
            "ExecInst": entry.get("ExecInst", np.nan),
            "RefOrdID": entry.get("RefOrdID", np.nan),
            "Tm": entry.get("Tm", np.nan),
        })
        increment_counter(stream, "order")

    elif msg_type == "type.googleapis.com/dbag.energy.Settlement":
        streams[stream]["settlement"].append({
            "subs": msg.get("subs"),
            "seq": msg.get("seq"),
            "ID": entry.get("ID", np.nan),
            "SetlPx": entry.get("SetlPx", np.nan),
            "Tm": entry.get("Tm", np.nan),
        })
        increment_counter(stream, "settlement")


def on_error(ws, error):
    print(f"⚠️ WebSocket error: {error}")


def on_close(ws, code, reason):
    print(f"🔒 Connection closed: {reason}")


def on_open(ws):
    print("🔗 Connection opened")
    send_subscribe(ws, stream_names_list)


# === MAIN RUNNER ===
def connect_to_stream():
    stream_url = "wss://stream-ccs-prod-eu.cef-cloud-stream.prod.fra.gcp.dbgservice.com/stream?format=json"
    headers = {"X-API-Key": API_KEY}
    ws = websocket.WebSocketApp(
        stream_url,
        header=headers,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever(ping_interval=30, ping_timeout=10)


# === CLI MODE ONLY ===
if __name__ == "__main__" and "streamlit" not in sys.modules:
    import signal
    def handle_exit(sig, frame):
        print("\n🛑 Ctrl+C detected → Saving data before exit...")
        save_data(output_folder)
        sys.exit(0)
    signal.signal(signal.SIGINT, handle_exit)

    print("🚀 Starting CloudStream Collector (press Ctrl+C to stop)...")
    threading.Thread(target=autosave_loop, daemon=True).start()
    threading.Thread(target=dashboard_loop, daemon=True).start()
    connect_to_stream()


# === STREAMLIT UI ===
try:
    import streamlit as st
except ImportError:
    st = None

if st:
    import io
    from streamlit_autorefresh import st_autorefresh

    # Session flags
    if "collector_thread" not in st.session_state:
        st.session_state.collector_thread = None
    if "is_running" not in st.session_state:
        st.session_state.is_running = False
    if "start_time" not in st.session_state:
        st.session_state.start_time = None

    def start_collector():
        if not st.session_state.is_running:
            st.session_state.is_running = True
            st.session_state.start_time = datetime.utcnow()
            t = threading.Thread(target=connect_to_stream, daemon=True)
            t.start()
            st.session_state.collector_thread = t

    def stop_collector():
        if st.session_state.is_running:
            st.session_state.is_running = False
            save_data(output_folder)
            st.success("💾 Data saved and collector stopped.")

    def download_data():
        st.success("✅ Data exported. Click below to download:")

        for stream, stream_data in streams.items():
            if not stream_data.get("subscribed", False):
                continue

            dfs = {}
            if stream_data["trade"]:
                dfs["Trade"] = pd.DataFrame(stream_data["trade"])
            if stream_data["order"]:
                dfs["Order"] = pd.DataFrame(stream_data["order"])
            if stream_data["settlement"]:
                dfs["Settlement"] = pd.DataFrame(stream_data["settlement"])
            if stream_data["instrument"]:
                dfs["Instrument"] = pd.DataFrame(stream_data["instrument"])
            if stream_data["market_data"]:
                dfs["MarketData"] = pd.DataFrame(stream_data["market_data"])

            if dfs:
                # Create a fresh buffer for each stream
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                    for sheet_name, df in dfs.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                buffer.seek(0)

                st.download_button(
                    label=f"⬇️ Download {stream}.xlsx",
                    data=buffer.getvalue(),
                    file_name=f"{stream}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

    # === UI Layout ===
    st.title("⚡ CloudStream Collector")

    col1, col2, col3 = st.columns(3)
    if col1.button("▶️ Start Collector"):
        start_collector()
    if col2.button("⏹️ Stop Collector"):
        stop_collector()
    if col3.button("💾 Download Data"):
        download_data()

    st.write("Running:", st.session_state.is_running)

    # Auto-refresh UI every 5 seconds if running
    if st.session_state.is_running:
        st_autorefresh(interval=5000, limit=None, key="dashboard_refresh")
        st.info("Collector is running...")

        # Live dashboard
        st.subheader("📊 Live Dashboard")
        for stream, data in streams.items():
            if not data.get("subscribed", False):
                continue
            c = data["counters"]
            st.write(
                f"{stream}: Trades={c['trade']} | Orders={c['order']} | "
                f"Instruments={c['instrument']} | Settlements={c['settlement']} | "
                f"MarketData={c['market_data']}"
            )

        # Status block
        if st.session_state.start_time:
            st.subheader("🕒 Collection Status")

            start_utc = st.session_state.start_time.replace(tzinfo=pytz.UTC)
            start_gmt = start_utc.astimezone(pytz.timezone("Etc/GMT"))
            start_ist = start_utc.astimezone(pytz.timezone("Asia/Kolkata"))
            start_mst = start_utc.astimezone(pytz.timezone("US/Mountain"))

            st.write("✅ Collection started at:")
            st.write(f"- GMT : {start_gmt.strftime('%Y-%m-%d %H:%M:%S')}")
            st.write(f"- UTC : {start_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            st.write(f"- IST : {start_ist.strftime('%Y-%m-%d %H:%M:%S')}")
            st.write(f"- MST : {start_mst.strftime('%Y-%m-%d %H:%M:%S')}")

            st.write("🌍 Subscribed Streams:")
            for stream in streams.keys():
                st.write(f"- {stream}")

    else:
        st.warning("Collector is stopped.")
