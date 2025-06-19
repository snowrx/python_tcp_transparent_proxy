#!/bin/bash

DEST=/opt/proxy

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit
fi

if [ -d "$DEST" ]; then
  systemctl disable --now proxy.service
  rm -rf $DEST
fi

mkdir -p $DEST
cp {proxy.py,requirements.txt,proxy} $DEST/
cp proxy.service /etc/systemd/system/
chown -R proxy:proxy $DEST
chmod a+rx $DEST/proxy

cd $DEST
python3 -m venv .venv
source .venv/bin/activate
pip install -U -r requirements.txt
deactivate
systemctl daemon-reload
systemctl enable --now proxy.service
systemctl restart proxy.service
