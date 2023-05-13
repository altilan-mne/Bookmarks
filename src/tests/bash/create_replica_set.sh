#!/bin/bash

# batch file to create a replica set with 3 mongod members
# execute at sudo privileges at the directory that contains db0.conf, db1.conf, db1.conf
# store 3 PIDS of mongod processes to the file 'pids.mongo'

if [ ! -d "/var/lib/mongodata" ];
then
  echo "Create data directories"
  mkdir -p /var/lib//mongodata/db0
  mkdir -p /var/lib//mongodata/db1
  mkdir -p /var/lib//mongodata/db2
else
  echo "Data directories already exist"
fi

if [ ! -d "/var/log/mongologs" ];
then
  echo "Create log directories"
  mkdir -p /var/log/mongologs/log0
  mkdir -p /var/log/mongologs/log1
  mkdir -p /var/log/mongologs/log2
else
  echo "Log directories already exist"
fi

  echo "Change the owner to mongodb"
  chown -R mongodb:mongodb /var/lib/mongodata/
  chown -R mongodb:mongodb /var/log/mongologs/

if [ ! -d "/etc/mongoconfigs" ];
then
  echo "Create config directory and copy db0(1,2).conf to it "
  mkdir /etc/mongoconfigs
  cp db0.conf /etc/mongoconfigs/db0.conf
  cp db1.conf /etc/mongoconfigs/db1.conf
  cp db2.conf /etc/mongoconfigs/db2.conf
else
  echo "Config directory already exist"
fi

echo "Run mongod0 member of the replica set:"
sudo mongod -f /etc/mongoconfigs/db0.conf
sudo mongod -f /etc/mongoconfigs/db1.conf
sudo mongod -f /etc/mongoconfigs/db2.conf

ps -o pid,comm ax | grep mongod | awk '{print $1}' > pids.mongod
cat pids.mongod