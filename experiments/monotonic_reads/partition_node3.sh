#!/bin/bash

set -e

NODE="cassandra-3"

echo "Creating network partition for $NODE..."

add_rule_if_missing() {
    CHAIN="$1"
    PORT="$2"

    if ! docker exec --user root "$NODE" \
        iptables -C "$CHAIN" -p tcp --dport "$PORT" -j DROP \
        2>/dev/null
    then
        docker exec --user root "$NODE" \
            iptables -I "$CHAIN" -p tcp --dport "$PORT" -j DROP
    fi
}

add_rule_if_missing INPUT 7000
add_rule_if_missing OUTPUT 7000
add_rule_if_missing INPUT 7001
add_rule_if_missing OUTPUT 7001

echo
echo "Partition active."
echo "$NODE can still accept CQL requests on port 9042,"
echo "but Cassandra internode communication is blocked."