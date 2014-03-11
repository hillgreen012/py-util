#!/bin/sh -

host="$1"
read line
brace="'"
cmd="ssh $host \"echo ${brace}${line}${brace} | /root/XData4S/Strategy--Forward.sh\""
echo "$cmd" | sh
