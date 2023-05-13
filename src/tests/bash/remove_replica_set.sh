#!/bin/bash
# remove replica set of mpngo DB created by 'create_replica_set.sh' script.
echo "Kill 3 mongod processes"
ps -o pid,comm ax | grep mongod | awk '{print $1}'| xargs kill -9
# all mongod procs has been killed
# remove directories of the replica set
echo "Remove directories of the replica set"
sudo rm -rf /etc/mongoconfigs
sudo rm -rf /var/lib/mongodata
sudo rm -rf /var/log/mongologs