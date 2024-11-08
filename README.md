It will work with iptables REDIRECT target.

# setup
```
sudo cp proxy.py /opt/ && sudo cp proxy.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart proxy.service
```
