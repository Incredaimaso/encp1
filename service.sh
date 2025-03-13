#!/bin/bash

# Create systemd service file
cat > /etc/systemd/system/encoder-bot.service << EOL
[Unit]
Description=Telegram Video Encoder Bot
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)
ExecStart=$(pwd)/run.sh
Restart=always
RestartSec=10
StartLimitInterval=0
LimitNOFILE=131072

[Install]
WantedBy=multi-user.target
EOL

# Reload systemd and enable service
sudo systemctl daemon-reload
sudo systemctl enable encoder-bot
sudo systemctl start encoder-bot

echo "Service installed and started!"
