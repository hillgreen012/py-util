#!/bin/sh -

host="192.168.100.132"
read line
brace="'"
cmd="ssh $host \"echo ${brace}${line}${brace} | /root/XData4S/XDS-Eco.py\""
echo "$cmd" | sh
