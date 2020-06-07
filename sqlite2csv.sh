#!/bin/sh

case $# in
    1) sqlite3 -csv "$1" ".tables" ;;
    2) sqlite3 -csv -header "$1" "SELECT * FROM $2" ;;
    *)
        echo "usage: sqlite2csv.sh DATABASE TABLENAME"
        echo "omit TABLENAME to list table names."
        exit 2
    ;;
esac
