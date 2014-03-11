#!/bin/sh -

host="192.168.100.132"
read line
brace="'"
cmd="ssh $host \"echo ${brace}${line}${brace} | /root/XData4S/Strategy-Ebuy.py\""
(echo "$line" >>/opt/gridview/Strategy.db 2>/dev/null) && (echo "$cmd" | sh)
