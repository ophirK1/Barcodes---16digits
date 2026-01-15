import os
import shutil
import socket
import time
import multiprocessing
import subprocess
import usb.core
import usb.util
from gpiozero import LED, Button
import socketserver

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================

SERVER_IP = "192.168.0.60"
SERVER_PORT = 3333

GATE_LED_PIN = 26
CONFIG_BUTTON_PIN = 27

BARCODE_DIR = "/home/admin/Barcodes"
SOUND_PATH = "/home/admin/Barcodes/sounds/{}.mp3"
FILES_TO_KEEP = ["sounds", "main.py", "install.sh",
                 "requirements.txt", "cron.log", "setup_guide.txt"]

VALID_YEAR_RANGE = (23, 50)
USB_VENDORS = [(0x1eab, 0x1a03), (0x27dd, 0x0103)]

# ==============================================================================
# --- SHARED VALIDATION LOGIC ---
# ==============================================================================


def is_valid_date(date_str):
    try:
        day, month, year = int(date_str[0:2]), int(
            date_str[2:4]), int(date_str[4:6])
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


def process_barcode_locally(barcode, config_button_is_pressed):
    if not isinstance(barcode, str) or len(barcode) != 16:
        print(f"LOCAL VALIDATION FAILED: Incorrect length.")
        return False

    date, site, registry, code = barcode[0:6], barcode[6:10], barcode[10:12], barcode[12:16]
    print(
        f"LOCAL VALIDATION: Date={date}, Site={site}, Registry={registry}, Code={code}")

    site_path = os.path.join(BARCODE_DIR, site)

    if barcode == f"123456{site}123456" and os.path.isdir(site_path):
        print("LOCAL ACCESS GRANTED: Master code.")
        return True

    if config_button_is_pressed:
        print("LOCAL ACCESS GRANTED: Manual override.")
        return create_barcode_file(date, site, registry, code)

    if not is_valid_date(date):
        print("LOCAL VALIDATION FAILED: Invalid date.")
        return False

    if not os.path.isdir(site_path):
        print(f"LOCAL VALIDATION FAILED: Site '{site}' does not exist.")
        return False

    barcode_file_path = os.path.join(
        site_path, registry, date, f"{code}.txt")
    if os.path.exists(barcode_file_path):
        print("LOCAL VALIDATION FAILED: Barcode already used.")
        return False

    return create_barcode_file(date, site, registry, code)

# ==============================================================================
# --- SERVER-SIDE LOGIC ---
# ==============================================================================


class Server:
    def __init__(self):
        os.makedirs(BARCODE_DIR, exist_ok=True)

    def delete_database(self):
        print("\n--- WARNING: Received remote command to delete database... ---")
        if not os.path.isdir(BARCODE_DIR):
            return
        for item_name in os.listdir(BARCODE_DIR):
            if item_name not in FILES_TO_KEEP:
                full_path = os.path.join(BARCODE_DIR, item_name)
                try:
                    if os.path.isdir(full_path):
                        shutil.rmtree(full_path)
                    else:
                        os.remove(full_path)
                except OSError as e:
                    print(f"--- ERROR: Could not delete {item_name}. {e} ---")
        print("--- Selective cleanup complete. ---")

    def start(self):
        class BarcodeTCPHandler(socketserver.BaseRequestHandler):
            server_logic = self

            def handle(self):
                try:
                    message = self.request.recv(1024).strip().decode("utf-8")
                    if not message:
                        return

                    if message == "DELETE_DATABASE":
                        self.server_logic.delete_database()
                        self.request.sendall(b"done")
                        return

                    parts = message.split(':')
                    barcode = parts[0]
                    button_is_pressed = len(parts) > 1 and parts[1] == "True"

                    print(
                        f"NETWORK REQUEST from {self.client_address[0]}: Barcode={barcode}, Override={button_is_pressed}")

                    is_valid = process_barcode_locally(
                        barcode, button_is_pressed)
                    self.request.sendall(b"open" if is_valid else b"close")
                except Exception as e:
                    print(f"An unexpected error in handler: {e}")

        BarcodeTCPHandler.server_logic = self

        class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
            pass

        with ThreadedTCPServer(('0.0.0.0', SERVER_PORT), BarcodeTCPHandler) as server_instance:
            print(
                f"--- SERVER PROCESS STARTED --- Listening on port {SERVER_PORT}")
            server_instance.serve_forever()

# ==============================================================================
# --- CLIENT-SIDE LOGIC ---
# ==============================================================================


def reader_process(output_queue):
    def get_scanner():
        for vendor_id, product_id in USB_VENDORS:
            dev = usb.core.find(idVendor=vendor_id, idProduct=product_id)
            if dev:
                return dev
        return None

    def flush_usb_buffer(device, endpoint_addr, max_packet_size):
        try:
            while True:
                # Kept safe 64-byte buffer size to prevent Abort/Crash
                device.read(endpoint_addr, 64, timeout=50)
        except usb.core.USBTimeoutError:
            pass
        except usb.core.USBError:
            pass

    KEYCODE_MAP = {30: '1', 31: '2', 32: '3', 33: '4', 34: '5',
                   35: '6', 36: '7', 37: '8', 38: '9', 39: '0'}
    dev = None
    barcode_chars = []
    last_barcode_read = None
    last_read_time = 0
    consecutive_errors = 0
    max_consecutive_errors = 3
    scan_timeout_start = None

    while True:
        try:
            if dev is None:
                print("Attempting to connect to scanner...")
                dev = get_scanner()
                if dev is None:
                    time.sleep(2)
                    continue

                consecutive_errors = 0
                if dev.is_kernel_driver_active(0):
                    dev.detach_kernel_driver(0)

                dev.set_configuration()
                ep = dev[0].interfaces()[0].endpoints()[0]
                eaddr = ep.bEndpointAddress
                max_packet_size = ep.wMaxPacketSize

                flush_usb_buffer(dev, eaddr, max_packet_size)
                print("Scanner connected and configured successfully.")

            # Kept safe 64-byte buffer size to prevent Abort/Crash
            data = dev.read(eaddr, 64, timeout=1000)

            if len(data) >= 3 and data[2] != 0:
                keycode = data[2]
                current_time = time.time()
                scan_timeout_start = None

                if keycode == 40:
                    if barcode_chars:
                        barcode = "".join(barcode_chars)

                        if not (barcode == last_barcode_read and (current_time - last_read_time) < 0.5):
                            print(f"Barcode scanned: {barcode}")
                            output_queue.put(barcode)
                            last_barcode_read = barcode
                            last_read_time = current_time
                        else:
                            print(f"Duplicate scan filtered: {barcode}")

                        barcode_chars = []
                        flush_usb_buffer(dev, eaddr, max_packet_size)
                else:
                    char = KEYCODE_MAP.get(keycode)
                    if char:
                        if not barcode_chars:
                            scan_timeout_start = current_time
                        barcode_chars.append(char)

                        if len(barcode_chars) > 50:
                            print("Barcode too long, discarding...")
                            barcode_chars = []
                            scan_timeout_start = None

        except usb.core.USBTimeoutError:
            if barcode_chars and scan_timeout_start:
                if (time.time() - scan_timeout_start) > 3.0:
                    print(
                        f"Incomplete scan timed out, discarding: {''.join(barcode_chars)}")
                    barcode_chars = []
                    scan_timeout_start = None
            continue

        except usb.core.USBError as e:
            consecutive_errors += 1
            print(
                f"USB error occurred ({consecutive_errors}/{max_consecutive_errors}): {e}")

            if consecutive_errors >= max_consecutive_errors:
                print("Too many consecutive USB errors. Resetting connection...")
                if dev:
                    try:
                        usb.util.dispose_resources(dev)
                    except:
                        pass
                dev = None
                barcode_chars = []
                scan_timeout_start = None
                consecutive_errors = 0
                time.sleep(2)
            else:
                barcode_chars = []
                scan_timeout_start = None
                if dev:
                    try:
                        flush_usb_buffer(dev, eaddr, max_packet_size)
                    except:
                        pass
                time.sleep(0.1)

        except Exception as e:
            print(f"Unexpected error in scanner reader: {e}")
            if dev:
                try:
                    usb.util.dispose_resources(dev)
                except:
                    pass
            dev = None
            barcode_chars = []
            scan_timeout_start = None
            consecutive_errors = 0
            time.sleep(2)


class Client:
    def __init__(self):
        self.gate = None
        self.config_button = None
        try:
            self.gate = LED(GATE_LED_PIN, active_high=True, initial_value=False)
            self.config_button = Button(CONFIG_BUTTON_PIN, pull_up=True)
        except Exception as e:
            print(f"WARNING (Client): Could not initialize GPIO pins. Error: {e}")

    def play_sound(self, sound_name):
        mp3_file = SOUND_PATH.format(sound_name)
        if os.path.exists(mp3_file):
            subprocess.Popen(["mpg123", "-q", "-o", "alsa", mp3_file])

    def open_gate(self):
        if self.gate:
            self.gate.blink(on_time=0.2, off_time=0, n=1)
            print("Gate opened.")

    def delete_database(self):
        print("\n--- WARNING: Deleting local database... ---")
        if not os.path.isdir(BARCODE_DIR):
            return
        for item_name in os.listdir(BARCODE_DIR):
            if item_name not in FILES_TO_KEEP:
                full_path = os.path.join(BARCODE_DIR, item_name)
                try:
                    if os.path.isdir(full_path):
                        shutil.rmtree(full_path)
                    else:
                        os.remove(full_path)
                except OSError as e:
                    print(f"--- ERROR: Could not delete {item_name}. {e} ---")
        print("--- Local cleanup complete. ---")
        self.play_sound("beep")

    def send_delete_request(self):
        print("Sending delete request to the server...")
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect((SERVER_IP, SERVER_PORT))
                client_socket.sendall(b"DELETE_DATABASE")
        except socket.error as e:
            print(f"Could not send delete request: {e}")

    def process_barcode(self, barcode, button_is_pressed):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.settimeout(0.25)
                client_socket.connect((SERVER_IP, SERVER_PORT))

                message = f"{barcode}:{button_is_pressed}"
                client_socket.sendall(message.encode("utf-8"))
                response = client_socket.recv(1024).decode("utf-8")

                if response == "open":
                    self.open_gate()
                    # Calculate master code for this barcode's site to check silence
                    master_code = "123456" + \
                        barcode[6:10] + "123456" if len(barcode) >= 10 else ""

                    if barcode != master_code:
                        self.play_sound("sound")
                else:
                    self.play_sound("beep")

        except socket.error:
            print("Server unreachable. Switching to OFFLINE mode.")
            is_valid = process_barcode_locally(barcode, button_is_pressed)
            if is_valid:
                self.open_gate()
                # Calculate master code for this barcode's site to check silence
                master_code = "123456" + \
                    barcode[6:10] + "123456" if len(barcode) >= 10 else ""

                if barcode != master_code:
                    self.play_sound("sound")
            else:
                self.play_sound("beep")

    def start(self):
        print(f"--- CLIENT PROCESS STARTED ---")

        scan_queue = multiprocessing.Queue()
        reader = multiprocessing.Process(
            target=reader_process, args=(scan_queue,))
        reader.daemon = True
        reader.start()

        button_held_start_time = None
        db_deleted_this_hold = False

        try:
            while True:
                while not scan_queue.empty():
                    barcode = scan_queue.get()
                    button_is_pressed = self.config_button and self.config_button.is_pressed
                    self.process_barcode(barcode, button_is_pressed)

                if self.config_button and self.config_button.is_pressed:
                    if button_held_start_time is None:
                        button_held_start_time = time.time()
                        db_deleted_this_hold = False
                    hold_duration = time.time() - button_held_start_time
                    if hold_duration >= 10 and not db_deleted_this_hold:
                        self.delete_database()
                        if get_local_ip() != SERVER_IP:
                            self.send_delete_request()
                        else:
                            print("This is the server. Local database deleted.")
                        db_deleted_this_hold = True
                else:
                    button_held_start_time = None

                time.sleep(0.02)
        except KeyboardInterrupt:
            pass
        finally:
            if reader.is_alive():
                reader.terminate()
            if self.gate:
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
        server_process = multiprocessing.Process(target=server_app.start)
        server_process.daemon = True
        server_process.start()

    print("Starting client logic.")
    client_app = Client()
    try:
        client_app.start()
    except KeyboardInterrupt:
        print("\nShutting down main process...")
    finally:
        if server_process and server_process.is_alive():
            server_process.terminate()
            server_process.join()
        print("System shutdown complete.")
