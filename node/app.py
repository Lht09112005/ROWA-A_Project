import os
import time
import json
import threading
import requests
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

# ─────────────────────────────────────────────────────────────────────────────
# ENVIRONMENT & STATE
# ─────────────────────────────────────────────────────────────────────────────
NODE_ID = os.environ.get("NODE_ID", "WHBDG")
PORT = int(os.environ.get("PORT", 5001))

DATA_DIR = "/app/data"
STOCK_FILE = os.path.join(DATA_DIR, "stock.json")
WAL_FILE = os.path.join(DATA_DIR, "wal.jsonl")

# Standard Initial State
state = "RECOVERING"  # UP, DOWN, RECOVERING
stock = []  # Relational dataset: List of {"SKU": str, "Quantity": int, "WarehouseID": str}
last_tx_id = 0
recover_progress = 0.0

# Relational dataset helpers for Stock_Levels (SKU, Quantity, WarehouseID)
def get_stock_quantity(sku):
    for item in stock:
        if item.get("SKU") == sku:
            return item.get("Quantity", 0)
    return 100

def update_stock_quantity(sku, qty):
    for item in stock:
        if item.get("SKU") == sku:
            item["Quantity"] = qty
            return
    stock.append({
        "SKU": sku,
        "Quantity": qty,
        "WarehouseID": "UNKNOWN"
    })

lock = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING UTILITIES (Prints beautifully to Docker Logs)
# ─────────────────────────────────────────────────────────────────────────────
def log(msg, level="INFO"):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] [NODE-{NODE_ID}] {msg}", flush=True)

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE PERSISTENCE (Persistent WAL & Stock file)
# ─────────────────────────────────────────────────────────────────────────────
def load_db():
    global stock, last_tx_id
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
        
    if os.path.exists(STOCK_FILE):
        try:
            with open(STOCK_FILE, "r") as f:
                data = json.load(f)
                stock = data.get("stock", [])
                last_tx_id = data.get("last_tx_id", 0)
                log(f"Stock restored: {stock} | Last TX: {last_tx_id}")
        except Exception as e:
            log(f"Error reading stock file, resetting: {e}", "WARNING")
            init_default_stock()
    else:
        init_default_stock()

def init_default_stock():
    global stock, last_tx_id
    csv_path = "/app/stock_levels.csv"
    if os.path.exists(csv_path):
        try:
            import csv
            new_stock = []
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Support new dataset columns: SKU_ID, Warehouse, Quantity_On_Hand
                    sku = row.get("SKU_ID") or row.get("Product ID") or row.get("SKU")
                    wh_id = row.get("Warehouse") or row.get("Warehouse_ID") or row.get("Store ID") or row.get("WarehouseID")
                    qty_str = row.get("Quantity_On_Hand") or row.get("Inventory Level") or row.get("Quantity")
                    
                    if not sku or not qty_str:
                        continue
                        
                    qty = int(qty_str)
                    
                    # Fully Replicated: We load ALL rows into the node.
                    new_stock.append({
                        "SKU": sku.strip(),
                        "Quantity": qty,
                        "WarehouseID": wh_id.strip() if wh_id else "UNKNOWN"
                    })
            if new_stock:
                stock = new_stock
                last_tx_id = 0
                save_stock()
                log(f"Successfully loaded {len(stock)} items from external CSV dataset: {csv_path}")
                return
        except Exception as e:
            log(f"Error reading external CSV dataset, falling back to default: {e}", "WARNING")

    # Fallback to default small dataset
    stock = [
        {"SKU": "SKU0001", "Quantity": 359, "WarehouseID": "WHBDG"},
        {"SKU": "SKU0002", "Quantity": 314, "WarehouseID": "WHDPS"},
        {"SKU": "SKU0004", "Quantity": 281, "WarehouseID": "WHJKT"}
    ]
    last_tx_id = 0
    save_stock()
    log(f"Initialized default dataset: {stock}")

def save_stock():
    try:
        with open(STOCK_FILE, "w") as f:
            json.dump({"stock": stock, "last_tx_id": last_tx_id}, f)
    except Exception as e:
        log(f"Error saving stock file: {e}", "ERROR")

def write_wal(tx_id, sku, old_qty, new_qty):
    try:
        entry = {
            "tx_id": tx_id,
            "timestamp": time.time(),
            "sku": sku,
            "old_qty": old_qty,
            "new_qty": new_qty
        }
        with open(WAL_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        log(f"Error writing WAL: {e}", "ERROR")

# ─────────────────────────────────────────────────────────────────────────────
# REST ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/status", methods=["GET"])
@app.route("/status", methods=["GET"])
def get_status():
    with lock:
        # If simulated DOWN, we return 503 Service Unavailable to act like a real down server
        if state == "DOWN":
            return jsonify({"error": "Node DOWN"}), 503
            
        return jsonify({
            "node_id": NODE_ID,
            "status": state,
            "quantity": get_stock_quantity("SKU0001"),
            "lastTxId": last_tx_id,
            "recoverProgress": recover_progress,
            "dataset": stock  # Return full relational dataset schema Stock_Levels(SKU, Quantity, WarehouseID)
        })

@app.route("/")
def serve_index():
    return send_from_directory(app.static_folder, "index.html")

@app.route("/api/wal", methods=["GET"])
def get_wal():
    # Helper endpoint for UI to read local WAL
    logs = []
    if os.path.exists(WAL_FILE):
        try:
            with open(WAL_FILE, "r") as f:
                for line in f:
                    if line.strip():
                        logs.append(json.loads(line))
        except Exception as e:
            log(f"Error reading WAL for API: {e}", "WARNING")
    return jsonify({"wal": logs})

@app.route("/read", methods=["GET"])
def read_data():
    global state
    with lock:
        if state == "DOWN":
            log("Read request rejected: Node is DOWN", "WARNING")
            return jsonify({"error": "Service Unavailable. Node is DOWN."}), 503
            
        # ★ STALE READ PREVENTION ★
        # Ref: Özsu & Valduriez §14.5.3 - recovering site must NOT serve reads
        if state == "RECOVERING":
            log("Read request REJECTED! Stale Read Prevented.", "ERROR")
            return jsonify({
                "status": "STALE_READ_PREVENTED",
                "error": "Stale Read Prevented",
                "reason": "Node is currently replaying WAL in RECOVERING state. Reads are disabled until catch-up is complete."
            }), 400
 
        sku = request.args.get("sku", "SKU0001")
        qty = get_stock_quantity(sku)
        log(f"Served READ for {sku} -> Quantity={qty} (State={state})", "SUCCESS")
        return jsonify({
            "node_id": NODE_ID,
            "status": "OK",
            "sku": sku,
            "quantity": qty,
            "lastTxId": last_tx_id
        })

@app.route("/write", methods=["POST"])
def write_data():
    global state, stock, last_tx_id
    with lock:
        if state == "DOWN":
            log("Write request rejected: Node is DOWN", "WARNING")
            return jsonify({"error": "Service Unavailable. Node is DOWN."}), 503

        data = request.json or {}
        tx_id = data.get("tx_id")
        sku = data.get("sku", "SKU0001")
        delta = data.get("delta")

        if not tx_id or delta is None:
            return jsonify({"error": "Missing tx_id or delta"}), 400

        old_qty = get_stock_quantity(sku)
        new_qty = old_qty + delta

        log(f"Write Transaction Received {tx_id} | {sku}: {old_qty} {delta:+} -> {new_qty}", "INFO")
        
        # Simulate local commit delay
        time.sleep(0.05)
        
        # Write to WAL
        write_wal(tx_id, sku, old_qty, new_qty)
        
        # Commit to stock
        update_stock_quantity(sku, new_qty)
        last_tx_id = tx_id
        save_stock()
        
        log(f"Committed {tx_id} successfully.", "SUCCESS")
        return jsonify({
            "status": "COMMITTED",
            "node_id": NODE_ID,
            "tx_id": tx_id,
            "sku": sku,
            "old_qty": old_qty,
            "new_qty": new_qty
        })

@app.route("/proxy_write", methods=["POST"])
def proxy_write():
    """Proxy endpoint for Node UI to send writes to the Coordinator."""
    coordinator_url = os.environ.get("COORDINATOR_URL", "http://coordinator:5000")
    try:
        resp = requests.post(f"{coordinator_url}/api/write", json=request.json, timeout=10.0)
        return jsonify(resp.json()), resp.status_code
    except Exception as e:
        log(f"Proxy write to coordinator failed: {e}", "WARNING")
        return jsonify({"error": f"Không thể kết nối đến Trạm Điều Phối: {e}"}), 500

@app.route("/crash", methods=["POST"])
def crash():
    global state
    with lock:
        state = "DOWN"
        log("💥 Simulated CRASH triggered! Node status is now DOWN.", "RED")
        return jsonify({"status": "CRASHED", "node_id": NODE_ID})

# Background thread for simulated network & log replay catch-up
def run_recovery_process():
    global state, recover_progress, stock, last_tx_id
    
    log("Recovery background worker started.", "INFO")
    
    # 1. Ask coordinator for missed transactions while this node was down
    missed_txs = []
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Coordinator container name is "coordinator" in docker-compose rowa-network
            response = requests.get(f"http://coordinator:5000/api/missed-txs?node_id={NODE_ID}", timeout=5)
            if response.status_code == 200:
                missed_txs = response.json().get("missed_txs", [])
                log(f"Fetched {len(missed_txs)} missed transactions from Coordinator.", "INFO")
                break
            else:
                log(f"Failed to fetch missed txs, HTTP {response.status_code}", "WARNING")
        except Exception as e:
            if attempt < max_retries - 1:
                log(f"Coordinator unreachable, retrying in 2s... ({e})", "WARNING")
                time.sleep(2)
            else:
                log(f"Coordinator completely unreachable during recovery: {e}", "ERROR")

    # 2. Simulate WAL catch-up progress bar (4.5s total time, increments every 90ms)
    steps = 50
    delay = 4.5 / steps
    for step in range(1, steps + 1):
        # Allow checking for sudden crash during recovery
        with lock:
            if state != "RECOVERING":
                log("Recovery aborted mid-way.", "WARNING")
                return
            recover_progress = (step / steps) * 100
        time.sleep(delay)

    # 3. Apply missed transactions in WAL & DB
    with lock:
        if state != "RECOVERING":
            return
            
        for tx in missed_txs:
            tx_id = tx.get("txId")
            old_val = tx.get("oldQty", 100)
            new_val = tx.get("newQty", 100)
            sku = tx.get("sku", "SKU0001")
            
            if tx_id > last_tx_id:
                log(f"Applying log entry during recovery catch-up: TX-{tx_id:03d} ({sku} {old_val}->{new_val})")
                write_wal(tx_id, sku, old_val, new_val)
                update_stock_quantity(sku, new_val)
                last_tx_id = tx_id
                time.sleep(0.1) # small simulated replay delay per transaction
                
        save_stock()
        state = "UP"
        recover_progress = 0.0
        log(f"✅ Recovery completed. Stock synced to {stock} | Last TX: {last_tx_id}", "SUCCESS")
        
    # Notify coordinator that recovery is complete so it purges this node from the pending recovery log
    try:
        requests.post(f"http://coordinator:5000/api/node/recovery-complete?node_id={NODE_ID}", timeout=5)
        log("Sent recovery completion notification to Coordinator.", "SUCCESS")
    except Exception as e:
        log(f"Failed to notify Coordinator of recovery completion: {e}", "WARNING")

@app.route("/recover", methods=["POST"])
def initiate_recover():
    global state, recover_progress
    with lock:
        if state != "DOWN":
            return jsonify({"error": "Node is not DOWN, cannot recover"}), 400
            
        state = "RECOVERING"
        recover_progress = 0.0
        log("🔄 Initiated Recovery Process. Replaying WAL...", "MAGENTA")
        
        # Spawn thread so node remains responsive to /status pings from frontend!
        threading.Thread(target=run_recovery_process, daemon=True).start()
        
        return jsonify({"status": "RECOVERING_STARTED", "node_id": NODE_ID})

@app.route("/reset", methods=["POST"])
def reset_node():
    global state, stock, last_tx_id, recover_progress
    with lock:
        state = "UP"
        last_tx_id = 0
        recover_progress = 0.0
        
        # Clear files
        if os.path.exists(STOCK_FILE):
            os.remove(STOCK_FILE)
        if os.path.exists(WAL_FILE):
            os.remove(WAL_FILE)
            
        init_default_stock()
        log("🔄 Database has been RESET to initial state.", "INFO")
        return jsonify({"status": "RESET_DONE", "node_id": NODE_ID})

# ─────────────────────────────────────────────────────────────────────────────
# START SERVER
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    load_db()
    
    # Kích hoạt tiến trình Recovery ngầm ngay khi khởi động container (Hard Crash Recovery)
    threading.Thread(target=run_recovery_process, daemon=True).start()
    
    log(f"Starting server on port {PORT}...")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
