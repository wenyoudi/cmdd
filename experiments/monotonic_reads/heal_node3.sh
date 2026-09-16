#!/bin/bash

set -e

NODE="cassandra-3"

echo "Healing network partition for $NODE..."

docker exec --user root "$NODE" sh -c "
    iptables -D INPUT  -p tcp --dport 7000 -j DROP || true
    iptables -D OUTPUT -p tcp --dport 7000 -j DROP || true

    iptables -D INPUT  -p tcp --dport 7001 -j DROP || true
    iptables -D OUTPUT -p tcp --dport 7001 -j DROP || true
"

echo
echo "Partition healed."
echo "Cassandra internode communication restored."