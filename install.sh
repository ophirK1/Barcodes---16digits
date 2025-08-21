#!/bin/bash

# ==============================================================================
# --- Barcode Scanner Installation Script ---
# This script automates the setup of the barcode scanner application on a
# fresh Raspberry Pi OS Lite (64-bit or 32-bit).
# It should be run with sudo privileges.
# ==============================================================================

# --- Ensure the script is run as root ---
if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run as root. Please use 'sudo bash install.sh'" >&2
  exit 1
fi

echo "--- (1/6) Starting Barcode Scanner Installation ---"

# --- Configuration ---
GIT_REPO_URL="https://github.com/your-username/your-barcode-repo.git" # !!! IMPORTANT: REPLACE THIS URL !!!
APP_DIR="/home/admin/Barcodes"
SERVICE_NAME="barcode-scanner"
USER_NAME="admin"

# --- Step 2: Update System and Install Dependencies ---
echo "--- (2/6) Updating package lists and installing system dependencies... ---"
apt-get update -y
apt-get install git python3-pip python3-venv mpg123 -y

# --- Step 3: Clone the Application Repository ---
echo "--- (3/6) Cloning project repository from Git... ---"
# Remove the directory if it exists to ensure a fresh clone
rm -rf "$APP_DIR"
git clone "$GIT_REPO_URL" "$APP_DIR"
chown -R $USER_NAME:$USER_NAME "$APP_DIR"

# --- Step 4: Set up Python Virtual Environment & Install Packages ---
echo "--- (4/6) Setting up Python environment and installing packages... ---"
# Run the following commands as the target user to avoid permission issues
sudo -u $USER_NAME bash << EOF
cd "$APP_DIR"
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
EOF

# --- Step 5: Create the systemd Service File ---
echo "--- (5/6) Creating systemd service to run the application on boot... ---"
# Using a 'here document' to write the service file configuration
cat > /etc/systemd/system/${SERVICE_NAME}.service << EOF
[Unit]
Description=Barcode Scanner Service
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=$USER_NAME
Group=$USER_NAME
WorkingDirectory=$APP_DIR
# Use the full path to the python executable in the virtual environment
ExecStart=$APP_DIR/venv/bin/python $APP_DIR/main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# --- Step 6: Enable and Start the Service ---
echo "--- (6/6) Enabling and starting the barcode scanner service... ---"
systemctl daemon-reload
systemctl enable ${SERVICE_NAME}.service
systemctl start ${SERVICE_NAME}.service

echo ""
echo "========================================================"
echo "          INSTALLATION COMPLETE! 🎉"
echo "========================================================"
echo "The barcode scanner service is now running."
echo "You can check its status with: sudo systemctl status ${SERVICE_NAME}"
echo "The system will automatically start the script on every boot."
