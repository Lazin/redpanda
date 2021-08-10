#!/bin/bash

echo "" > $2

while :
do
	date +"pulling metrics %T.%N" >> $2
	curl -s -XGET $1/metrics | grep "^vectorized" >> $2
	sleep 5
done

