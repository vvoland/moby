#!/bin/bash
set -e

# TODO: This should really be generated by Go code as a subrecord, but
# importing in Python is hard. This is quick and dirty.

FIELDS=$(\
	cat extended_key_usage.go |\
	grep json |\
	cut -d ':' -f 2 |\
       	sed 's|,omitempty||g' |\
	tr -d '`')
echo "extended_key_usage = SubRecord({"
for f in $FIELDS; do
	if [ $f == "\"unknown\"" ]; then
		echo "    $f: ListOf(OID())"
	else
		echo "    $f: Boolean(),"
	fi
done
echo "})"