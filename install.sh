#!/bin/bash

DEST=/opt/proxy
DRY=0
CLEAN=0
EXEC=python3

# check privileges
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit
fi

while [ "$#" -gt 0 ]; do
  case $1 in
    --dry)
      DRY=1
      echo "dry run"
      ;;
    --clean)
      CLEAN=1
      echo "clean=$CLEAN"
      ;;
    --exec)
      shift
      if [ "$1" != "" ]; then
        EXEC=$1
      fi
      if [ "$EXEC" != "" ]; then
        CLEAN=1
        echo "exec=$EXEC"
      fi
  esac
  shift
done

# check if proxy is already installed
if [ -f "/etc/systemd/system/proxy.service" ]; then
  systemctl stop proxy.service
  if [ $CLEAN -eq 1 ]; then
    echo "Cleaning up old installation"
    if [ $DRY -ne 1 ]; then
      rm -rf $DEST
      mkdir -p $DEST
    fi
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
pip install -U -r requirements.txt
deactivate

chown -R proxy:proxy $DEST
chmod +x $DEST/proxy

systemctl daemon-reload
systemctl enable proxy.service
systemctl start proxy.service
