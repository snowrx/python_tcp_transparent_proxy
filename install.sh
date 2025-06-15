#!/bin/bash

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit
fi

if [ -d /opt/proxy ]; then
  systemctl disable --now proxy.service
  rm -rf /opt/proxy
fi

mkdir /opt/proxy
chown proxy:proxy /opt/proxy
cp {proxy.py,requirements.txt,run.sh} /opt/proxy/
chmod a+rx /opt/proxy/run.sh
cp proxy.service /etc/systemd/system/
cd /opt/proxy
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
deactivate
systemctl daemon-reload
systemctl enable --now proxy.service
systemctl restart proxy.service
