#!/usr/bin/bash
ip link set vm2 down
ip link set brm down

for var in "$@"
do
  ip addr del "$(echo "$var" | sed -r 's/\.0$/.1/')" dev brm
  ip addr del "$(echo "$var" | sed -r 's/\.0$/.2/')" dev vm2
done

ip link del brm type bridge
ip link set dev tapm down
ip tuntap del tapm mode tap
ip link set dev vm1 down
ip link del dev vm1 type veth peer name vm2
