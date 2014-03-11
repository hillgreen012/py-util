#!/bin/sh -

read line
host="10.0.33.133"
brace="'"
cmd="ssh $host \"echo ${brace}${line}${brace} | /root/XData4S/Forward.sh\""
echo "$cmd" | sh
