#! /bin/sh -e


conf_TST=true

# Copy permissions ... without --reference=
function cp_perms {
#     chmod --reference="$1" "${@:2}"
    perms=$(ls -l $1 | cut -c2-10)
    uperms=$(echo $perms | cut -c1-3)
    gperms=$(echo $perms | cut -c4-6)
    operms=$(echo $perms | cut -c7-)
    chmod "u=$uperms,g=$gperms,o=$operms" "${@:2}"
}

# CMP $1 vs. $2 ... if they are different: mv $1 $2, if not: rm -f $1
function cmp_mv_rm {
    rename=true
    exists=false
    if [ -f "$2" ]; then
        exists=true
    fi

    if $conf_TST && $exists; then
        if cmp -s "$1" "$2"; then
            rename=false
        fi
    fi


    if $rename; then
        $exists && cp_perms "$2" "$1"

        mv "$1" "$2"
    else
        rm -f -- "$1"
    fi
}


for filename in "$@"; do

path=$(dirname "$filename")
orig=$(basename "$filename" .db)



tmpfile=$(mktemp $path/split-totals.XXXXXX)

trap 'rm -f -- "$tmpfile"' INT TERM HUP EXIT


sqlite3 "$tmpfile" <<EOL
CREATE TABLE countme_totals (
  hits INTEGER NOT NULL,
  weeknum TEXT NOT NULL,
  os_name TEXT NOT NULL,
  os_version TEXT NOT NULL,
  os_variant TEXT NOT NULL,
  os_arch TEXT NOT NULL,
  sys_age TEXT NOT NULL,
  repo_tag TEXT NOT NULL,
  repo_arch TEXT NOT NULL);
CREATE INDEX weeknum_idx ON countme_totals (weeknum);

ATTACH '$filename' as 'tot';

INSERT INTO countme_totals SELECT * FROM tot.countme_totals WHERE sys_age = -1;
EOL

cp_perms "$filename" "$tmpfile"
cmp_mv_rm "$tmpfile" "$path/$orig-unique.db"

trap - INT TERM HUP EXIT



tmpfile=$(mktemp $path/split-totals.XXXXXX)

trap 'rm -f -- "$tmpfile"' INT TERM HUP EXIT

sqlite3 "$tmpfile" <<EOL
CREATE TABLE countme_totals (
  hits INTEGER NOT NULL,
  weeknum TEXT NOT NULL,
  os_name TEXT NOT NULL,
  os_version TEXT NOT NULL,
  os_variant TEXT NOT NULL,
  os_arch TEXT NOT NULL,
  sys_age TEXT NOT NULL,
  repo_tag TEXT NOT NULL,
  repo_arch TEXT NOT NULL);
CREATE INDEX weeknum_idx ON countme_totals (weeknum);

ATTACH '$filename' as 'tot';

INSERT INTO countme_totals SELECT * FROM tot.countme_totals WHERE sys_age != -1;
EOL

cp_perms "$filename" "$tmpfile"
cmp_mv_rm "$tmpfile" "$path/$orig-countme.db"

trap - INT TERM HUP EXIT
done
