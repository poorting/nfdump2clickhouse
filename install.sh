#!/usr/bin/env sh

COL='\033[1;31m'
INF='\033[0;32m'
NC='\033[0m' # No Color

if [ `id -u` -ne "0" ]
  then echo "${COL}Please run this script as root or with sudo${NC}"
  exit
fi

if [ ! -f nfdump2clickhouse.conf ]
then
  echo "${COL}Please provide a configuration file 'nfdump2clickhouse.conf' ${NC}"
  echo "${COL}See nfdump2clickhouse.conf.default for an example${NC}"
  exit
fi

# create virtual environment (if not already existing)
if [ ! -e venv ]
then
  echo "${INF}Creating python virtual environment${NC}"
  python3 -m venv venv
  echo "${INF}Installing requirements${NC}"
  venv/bin/pip install -r requirements.txt
fi


# Create service file from the template with current directory
envsubst \$PWD < service.template > nfdump2clickhouse.service

# remove service file (link) if it exists
if [ -f /etc/systemd/system/nfdump2clickhouse.service ]
then
  echo "${INF}Removing existing service file${NC}"
  systemctl daemon-reload
  systemctl stop nfdump2clickhouse.service
  rm /etc/systemd/system/nfdump2clickhouse.service
fi

# copy and link files

echo "${INF}Copying conf file to /usr/local/etc/${NC}"
cp nfdump2clickhouse.conf /usr/local/etc/nfdump2clickhouse.conf
echo "${INF}Creating symlink in /etc/systemd/system/${NC}"
ln -s $PWD/nfdump2clickhouse.service /etc/systemd/system/nfdump2clickhouse.service
echo "${INF}Reloading systemctl daemon${NC}"
systemctl daemon-reload
echo "${INF}Enabling nfdump2clickhouse service${NC}"
systemctl enable nfdump2clickhouse.service
echo "${INF}Starting nfdump2clickhouse service${NC}"
systemctl start nfdump2clickhouse.service

echo "${INF}Done!${NC}"