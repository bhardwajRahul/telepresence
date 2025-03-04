#!/usr/bin/bash
ip link add dev vm1 type veth peer name vm2
ip link set dev vm1 up
ip tuntap add tapm mode tap
ip link set dev tapm up
ip link add brm type bridge

ip link set tapm master brm
ip link set vm1 master brm

for var in "$@"
do
  ip addr add "$(echo "$var" | sed -r 's/\.0$/.1/')" dev brm
  ip addr add "$(echo "$var" | sed -r 's/\.0$/.2/')" dev vm2
done

ip link set brm up
ip link set vm2 up