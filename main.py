import os
import shutil
import socket
import time
import multiprocessing
import subprocess
import usb.core
from gpiozero import LED, Button
import socketserver

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================

# --- Network Settings ---
# The static IP of the machine that will act as the central SERVER.
SERVER_IP = "192.168.0.60"
SERVER_PORT = 3333

# --- Hardware Pins ---
# Both clients and servers need these pins for their logic.
GATE_LED_PIN = 26
CONFIG_BUTTON_PIN = 27
# --- File Paths ---
BARCODE_DIR = "/home/admin/Barcodes"
SOUND_PATH = "/home/admin/Barcodes/sounds/{}.mp3"
# Define files/folders to protect from deletion.
FILES_TO_KEEP = ["sounds", "cron.log", "install guide barcode.txt", "main.py", "requirements.txt", "install.sh"]


# --- Barcode Rules (Original 16-Digit Format) ---
VALID_YEAR_RANGE = (23, 50) # Valid years are 2023-2050

# --- USB Scanner (Client) ---
USB_VENDORS = [(0x1eab, 0x1a03), (0x27dd, 0x0103)]
PING_TIMEOUT = 0.2


# ==============================================================================
# --- SHARED VALIDATION LOGIC ---
# This logic is used by the Server for network requests, and by the Client in offline mode.
# ==============================================================================

def is_valid_date(date_str):
    try:
        day, month, year = int(date_str[0:2]), int(date_str[2:4]), int(date_str[4:6])
        if not (1 <= day <= 31 and 1 <= month <= 12 and VALID_YEAR_RANGE[0] <= year <= VALID_YEAR_RANGE[1]):
            return False
        return True
    except (ValueError, IndexError):
        return False

def create_barcode_file(date, site, registry, code):
    try:
        path = os.path.join(BARCODE_DIR, site, registry, date)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, f"{code}.txt"), "w") as f:
            pass
        print(f"Successfully created file for barcode.")
        return True
    except OSError as e:
        print(f"Error creating file: {e}")
        return False

def process_barcode_locally(barcode, config_button):
    """Validates a barcode against the local filesystem."""
    if not isinstance(barcode, str) or len(barcode) != 16:
        print(f"LOCAL VALIDATION FAILED: Incorrect length.")
        return False

    date, site, registry, code = barcode[0:6], barcode[6:10], barcode[10:12], barcode[12:16]
    print(f"LOCAL VALIDATION: Date={date}, Site={site}, Registry={registry}, Code={code}")

    site_path = os.path.join(BARCODE_DIR, site)
    if barcode == f"123456{site}123456" and os.path.isdir(site_path):
        print("LOCAL ACCESS GRANTED: Master code.")
        return True
    if config_button and config_button.is_pressed:
        print("LOCAL ACCESS GRANTED: Manual override.")
        # In offline mode, a manual override should also create the file
        return create_barcode_file(date, site, registry, code)
    if not is_valid_date(date):
        print("LOCAL VALIDATION FAILED: Invalid date.")
        return False
    if not os.path.isdir(site_path):
        print(f"LOCAL VALIDATION FAILED: Site '{site}' does not exist.")
        return False

    barcode_file_path = os.path.join(site_path, registry, date, f"{code}.txt")
    if os.path.exists(barcode_file_path):
        print("LOCAL VALIDATION FAILED: Barcode already used.")
        return False

    return create_barcode_file(date, site, registry, code)


# ==============================================================================
# --- SERVER-SIDE LOGIC ---
# ==============================================================================

class Server:
    def __init__(self):
        try:
            self.config_button = Button(CONFIG_BUTTON_PIN, pull_up=True)
        except Exception as e:
            print(f"SERVER WARNING: Could not initialize button. Error: {e}")
            self.config_button = None
        os.makedirs(BARCODE_DIR, exist_ok=True)

    def start(self, host_ip):
        class BarcodeTCPHandler(socketserver.BaseRequestHandler):
            # This class variable will be set to the Server instance
            server_logic = self
            def handle(self):
                try:
                    message = self.request.recv(1024).strip().decode("utf-8")
                    if not message: return
                    print(f"NETWORK REQUEST from {self.client_address[0]}: {message}")
                    # Use the shared validation logic
                    is_valid = process_barcode_locally(message, self.server_logic.config_button)
                    self.request.sendall(b"open" if is_valid else b"close")
                except Exception as e:
                    print(f"An unexpected error in handler: {e}")

        BarcodeTCPHandler.server_logic = self
        class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
            pass

        with ThreadedTCPServer((host_ip, SERVER_PORT), BarcodeTCPHandler) as server_instance:
            print(f"--- SERVER PROCESS STARTED --- Listening on {host_ip}:{SERVER_PORT}")
            server_instance.serve_forever()


# ==============================================================================
# --- CLIENT-SIDE LOGIC ---
# ==============================================================================

class Client:
    def __init__(self):
        self.gate = LED(GATE_LED_PIN, active_high=True, initial_value=False)
        try:
            self.config_button = Button(CONFIG_BUTTON_PIN, pull_up=True)
        except Exception as e:
            print(f"CLIENT WARNING: Could not initialize button. Error: {e}")
            self.config_button = None

    def delete_database(self):
        """Selectively deletes items from the database, sparing items in FILES_TO_KEEP."""
        print("\n--- WARNING: Config button held. Performing selective database cleanup... ---")
        
        if not os.path.isdir(BARCODE_DIR):
            print("Database directory not found, nothing to delete.")
            return

        # Loop through every file and folder in the BARCODE_DIR
        for item_name in os.listdir(BARCODE_DIR):
            # Check if the current item is in our list of things to save
            if item_name not in FILES_TO_KEEP:
                full_path = os.path.join(BARCODE_DIR, item_name)
                try:
                    # If it's a directory, use rmtree to delete it and its contents
                    if os.path.isdir(full_path):
                        print(f"Deleting data directory: {item_name}")
                        shutil.rmtree(full_path)
                    # Otherwise, it's a file, so use remove
                    else:
                        print(f"Deleting data file: {item_name}")
                        os.remove(full_path)
                except OSError as e:
                    print(f"--- ERROR: Could not delete {item_name}. {e} ---")

        print("--- Selective cleanup complete. ---")
        # Play a sound to confirm the action is done
        self.play_sound("beep")

    def play_sound(self, sound_name):
        mp3_file = SOUND_PATH.format(sound_name)
        if os.path.exists(mp3_file):
            subprocess.Popen(["mpg123", "-a", "hw:2,0", mp3_file])

    def ping_server(self, ip):
        try:
            result = subprocess.run(["timeout", str(PING_TIMEOUT), 'ping', '-c', '1', ip], capture_output=True, text=True, check=False)
            return result.returncode == 0
        except (subprocess.SubprocessError, FileNotFoundError):
            return False

    def open_gate(self):
        try:
            self.gate.blink(on_time=0.2, off_time=0, n=1)
            print("Gate opened.")
        except Exception as e:
            print(f"Error controlling gate: {e}")

    def send_data(self, data, play_main_sound):
        # --- OFFLINE MODE LOGIC ---
        if not self.ping_server(SERVER_IP):
            print("Server unreachable. Switching to OFFLINE mode.")
            is_valid_locally = process_barcode_locally(data, self.config_button)
            if is_valid_locally:
                self.open_gate()
                if play_main_sound:
                    self.play_sound("sound")
            else:
                self.play_sound("beep")
            return # Stop execution here after handling offline

        # --- ONLINE MODE LOGIC ---
        print("Server online. Sending data for validation...")
        response = ""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect((SERVER_IP, SERVER_PORT))
                client_socket.sendall(data.encode("utf-8"))
                response = client_socket.recv(1024).decode("utf-8")
                print(f"Server responded: {response}")
        except socket.error as e:
            print(f"Socket error: {e}")
            return

        if response == "open":
            self.open_gate()
            if play_main_sound:
                self.play_sound("sound")
        elif response == "close":
            self.play_sound("beep")

    def get_scanner(self):
        for vendor_id, product_id in USB_VENDORS:
            dev = usb.core.find(idVendor=vendor_id, idProduct=product_id)
            if dev: return dev
        return None

    def reader_process(self, queue, master_queue):
        KEYCODE_MAP = {30: '1', 31: '2', 32: '3', 33: '4', 34: '5', 35: '6', 36: '7', 37: '8', 38: '9', 39: '0'}
        dev = None
        while True:
            if dev is None:
                dev = self.get_scanner()
                if dev is None:
                    print("Scanner not found. Retrying in 5 seconds...")
                    time.sleep(5)
                    continue
            try:
                if dev.is_kernel_driver_active(0):
                    dev.detach_kernel_driver(0)
                dev.set_configuration()
                ep = dev[0].interfaces()[0].endpoints()[0]
                eaddr = ep.bEndpointAddress

                barcode_chars = []
                while True:
                    data = dev.read(eaddr, ep.wMaxPacketSize, timeout=0)
                    if not data or len(data) < 3 or data[2] == 0: continue

                    keycode = data[2]
                    if keycode == 40: # Enter
                        barcode = "".join(barcode_chars)
                        print(f"Barcode scanned: {barcode}")
                        if barcode.startswith("123456") and barcode.endswith("123456"):
                            master_queue.put(barcode)
                        else:
                            queue.put(barcode)
                        barcode_chars = []
                    else:
                        char = KEYCODE_MAP.get(keycode)
                        if char: barcode_chars.append(char)
            except usb.core.USBError as e:
                dev = None
                time.sleep(1)

    def start(self):
        print(f"--- CLIENT PROCESS STARTED ---")
        queue = multiprocessing.Queue()
        master_queue = multiprocessing.Queue()

        reader = multiprocessing.Process(target=self.reader_process, args=(queue, master_queue))
        reader.daemon = True
        reader.start()

        last_barcode = None
        button_held_start_time = None
        db_deleted_this_hold = False
        try:
            while True:
                if not queue.empty():
                    barcode = queue.get()
                    
                    button_is_pressed = self.config_button and self.config_button.is_pressed

                    if button_is_pressed:
                        print(f"OVERRIDE: Button pressed. Processing '{barcode}'...")
                        self.send_data(barcode, play_main_sound=True)
                        last_barcode = barcode 
                    
                    elif barcode != last_barcode:
                        self.send_data(barcode, play_main_sound=True)
                        last_barcode = barcode
                    
                    else:
                        print(f"Duplicate scan ignored: {barcode}")
                        self.play_sound("beep")

                # --- NEW: Check for the config button being held down ---
                if self.config_button and self.config_button.is_pressed:
                    # If button has just been pressed, record the start time
                    if button_held_start_time is None:
                        button_held_start_time = time.time()
                        db_deleted_this_hold = False # Reset flag on a new press

                    hold_duration = time.time() - button_held_start_time

                    # If held for 10s and we haven't already deleted in this press cycle
                    if hold_duration >= 10 and not db_deleted_this_hold:
                        self.delete_database()
                        db_deleted_this_hold = True # Set flag to prevent re-deleting
                else:
                    # If the button is released, reset the timer
                    button_held_start_time = None

                if not master_queue.empty():
                    self.send_data(master_queue.get(), play_main_sound=False)
                    while not master_queue.empty(): master_queue.get()
                time.sleep(0.02)
        except KeyboardInterrupt:
            pass
        finally:
            if reader.is_alive():
                reader.terminate()
            self.gate.off()
            print("Client process cleaned up.")


# ==============================================================================
# --- MAIN EXECUTION ---
# ==============================================================================

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

if __name__ == "__main__":
    local_ip = get_local_ip()
    print(f"Local IP detected: {local_ip}")

    server_process = None

    if local_ip == SERVER_IP:
        print("This machine is the designated server. Starting server process in background.")
        server_app = Server()
        server_process = multiprocessing.Process(target=server_app.start, args=(local_ip,))
        server_process.daemon = True
        server_process.start()

    # Every machine runs the client logic.
    print("Starting client logic.")
    client_app = Client()
    try:
        client_app.start()
    except KeyboardInterrupt:
        print("\nShutting down main process...")
    finally:
        if server_process and server_process.is_alive():
            print("Terminating server process...")
            server_process.terminate()
            server_process.join()
        print("System shutdown complete.")
