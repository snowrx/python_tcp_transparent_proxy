#!/bin/bash

DEST=/opt/proxy

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit
fi

if [ ! -d $DEST ] ; then
  mkdir $DEST
  chown proxy:proxy $DEST
  cd $DEST
  python3 -m venv .venv
fi

cd "$(dirname "$0")"
cp {proxy.py,requirements.txt,run.sh} $DEST/
chmod a+rx $DEST/run.sh
cp proxy.service /etc/systemd/system/

cd $DEST
source .venv/bin/activate
pip install -U -r requirements.txt
deactivate
systemctl daemon-reload
systemctl enable --now proxy.service
systemctl restart proxy.service
