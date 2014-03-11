#!/bin/sh -

eco_host=""
ebuy_host=""
com_host=""

echo -n 'input eco(金融) host: '
read eco_host
echo -e "\033[1;32m""deploying eco to $eco_host ...""\033[0m"
scp /root/XData4S/Eco root@${eco_host}:/root/XData4S

echo -n 'input ebuy(电商) host: '
read ebuy_host
echo -e "\033[1;32m""deploying ebuy to $ebuy_host ...""\033[0m"
scp /root/XData4S/Ebuy root@${ebuy_host}:/root/XData4S

echo -n 'input com(电信) host: '
read com_host
echo -e "\033[1;32m""deploying com to $com_host ...""\033[0m"
scp /root/XData4S/Com root@${com_host}:/root/XData4S
