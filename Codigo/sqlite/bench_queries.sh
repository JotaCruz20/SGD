#!/usr/bin/env bash

timeout()
{
    local cmd_pid sleep_pid retval
    (shift; "$@") &   # shift out sleep value and run rest as command in background job
    cmd_pid=$!
    (sleep "$1"; kill "$cmd_pid" 2>/dev/null) &
    sleep_pid=$!
    wait "$cmd_pid"
    retval=$?
    kill "$sleep_pid" 2>/dev/null
    return "$retval"
}

function check_q {
	local query=queries/$*.sql
	(
		echo $query
		time (sqlite3 -batch tcp-h.db< $query  > /dev/null )
	)
}


export -f check_q
for i in 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22; do
	timeout 14400 check_q $i
done
