#!/bin/bash

echo "count of arguments = $#"
exclKey=
exclVal=
sbt projects | sed 's/.*info.*[\t ][\t ]*\([A-Za-z0-9_][A-Za-z0-9_]*\).*$/\1/g' | grep -v 'trunk' | csplit - 5 >/dev/null
if [ "$#" -eq 2 ]; then
	exclKey=$1
	exclVal=$2
fi
echo "exclKey = $exclKey"
echo "exclVal = $exclVal"
allDeps.scala "$exclKey" "\"$exclVal\"" `cat xx01`
rm -f xx00 xx01
