#!/bin/bash

DEST=/opt/proxy
CLEAN=0
EXEC="$(which python3)"

# check privileges
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit
fi

while [ "$#" -gt 0 ]; do
  case $1 in
    --clean)
      CLEAN=1
      ;;
    --exec)
      shift
      if [ "$1" != "" ]; then
        EXEC="$1"
      fi
  esac
  shift
done

if [ $EXEC == "" ]; then
  echo "Error: python executable not found"
  exit 1
fi

if [ -f "/etc/systemd/system/proxy.service" ]; then
  systemctl stop proxy.service
  if [ $CLEAN -eq 1 ]; then
    echo "Cleaning up old installation"
    rm -rf $DEST
    mkdir -p $DEST
  fi
else
  mkdir -p $DEST
fi

cp -r {proxy,requirements.txt,main.py,core} $DEST
cp proxy.service /etc/systemd/system/

cd $DEST
touch proxy.env
$EXEC -m venv .venv
[ ! -f .venv/bin/activate ] && echo "venv not found" && exit 1
source .venv/bin/activate
pip install -U pip
pip install -U -r requirements.txt
deactivate

chown -R proxy:proxy $DEST
chmod +x $DEST/proxy

systemctl daemon-reload
systemctl enable proxy.service
systemctl start proxy.service
