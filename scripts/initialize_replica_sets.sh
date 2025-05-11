#!/bin/bash

# Function to check if a command was successful
function check_status {
  if [ $? -ne 0 ]; then
    echo "❌ An error occurred with the last command. Exiting script."
    exit 1
  fi
}

# ---------- Initiate Config Server Replica Set ----------
echo "⚙️  Initiating Config Server Replica Set..."
docker exec configsvr1 mongosh --port 27017 --eval '
rs.initiate({
  _id: "cfgRS",
  configsvr: true,
  members: [
    { _id: 0, host: "configsvr1:27017" },
    { _id: 1, host: "configsvr2:27017" },
    { _id: 2, host: "configsvr3:27017" }
  ]
})'

check_status

# Allow some time for the config server setup
sleep 5

# ---------- Initiate Shard 1 Replica Set ----------
echo "⚙️  Initiating Shard 1 Replica Set..."
docker exec shard1-node1 mongosh --port 27018 --eval '
rs.initiate({
  _id: "shard1RS",
  members: [
    { _id: 0, host: "shard1-node1:27018" },
    { _id: 1, host: "shard1-node2:27018" },
    { _id: 2, host: "shard1-node3:27018" }
  ]
})'

check_status

# Allow some time for the shard 1 setup
sleep 5

# ---------- Initiate Shard 2 Replica Set ----------
echo "⚙️  Initiating Shard 2 Replica Set..."
docker exec shard2-node1 mongosh --port 27019 --eval '
rs.initiate({
  _id: "shard2RS",
  members: [
    { _id: 0, host: "shard2-node1:27019" },
    { _id: 1, host: "shard2-node2:27019" },
    { _id: 2, host: "shard2-node3:27019" }
  ]
})'

check_status

echo "✅ Replica sets initialized!"