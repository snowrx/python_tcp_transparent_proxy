#!/bin/bash

DEST=/opt/proxy

# check privileges
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit
fi

# check if proxy is already installed
if [ -f "/etc/systemd/system/proxy.service" ]; then
  systemctl stop proxy.service
  for arg in "$@"; do
    if [ "$arg" == "--clean" ]; then
      echo "Cleaning up old installation"
      rm -rf $DEST
      mkdir -p $DEST
    fi
  done
else
  mkdir -p $DEST
fi

cp -r {proxy,requirements.txt,main.py,core} $DEST
cp proxy.service /etc/systemd/system/

cd $DEST
touch proxy.env
python3 -m venv .venv
[ ! -f .venv/bin/activate ] && echo "venv not found" && exit 1
source .venv/bin/activate
pip install -U -r requirements.txt
deactivate

chown -R proxy:proxy $DEST
chmod +x $DEST/proxy

systemctl daemon-reload
systemctl enable proxy.service
systemctl start proxy.service
