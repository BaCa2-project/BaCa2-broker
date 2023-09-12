#!/bin/bash

ZAD=$1
COUNT=0

for FILE in $(ls ./in | grep .in)
do
	TEST_ID="${FILE//.in/.}"
	printf "Generating "$TEST_ID".."
	PROG="./prog/"$ZAD".py"
	FILE_OUT="./out/"$TEST_ID"out"
	START=$(date +%s.%N)
	$(python3 $PROG < "./in/"$FILE > $FILE_OUT)
	END=$(date +%s.%N)
	printf "OK (time: "
	printf "%.3f" $(echo "$END - $START" | bc)
	printf "s )\n"
	COUNT=$((COUNT+1))
done
printf "Generated "$COUNT" outputs for task "$ZAD"!\n\n"
	