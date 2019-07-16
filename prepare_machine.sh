#!/usr/bin/env bash

# TODO: comment this file
sudo timedatectl set-timezone UTC

mkdir utils
wget http://repo1.maven.org/maven2/org/apache/orc/orc-tools/1.5.6/orc-tools-1.5.6-uber.jar -P ./utils/

sudo apt -y update
sudo apt -y upgrade
sudo apt install -y python3-pip openjdk-8-jre

pip3 install --trusted-host pypi.python.org -r ~/requirements.txt

# TODO: create config.ini if not exists
chmod 0600 ~/config.ini

crontab -l | { cat; echo "5 0 * * * python3 ~/upload_files.py && python3 ~/validate_urls.py"; } | crontab -
