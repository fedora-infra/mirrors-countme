#! /bin/sh -e

cd /var/lib/countme/

conf_LOGDIR="/mnt/fedora_stats/combined-http"

conf_LOGNAME="mirrors.fedoraproject.org-access.log"

conf_progress=--progress
# conf_progress=

conf_RW=true

if $conf_RW && [ "x$(whoami)" != "xcountme" ]; then
  echo "Need to be run as countme."
  exit 1
fi

#  Reload this month and the previous N ... put them in order so
# they come out backwards (and are loaded oldest to newest).
months="$(date +'%Y/%m')"
for i in 1 2 3; do
  d="$(date +'%Y/%m' --date=$i' month ago')"
  months="$d $months"
done

echo "Reload: $months"
echo "Dir: $conf_LOGDIR"
echo "Pkg: $(rpm -q python3-mirrors-countme)"
echo "RW: $conf_RW"
sleep 5


rawdb="/var/lib/countme/raw.db"
totsdb="/var/lib/countme/totals.db"
totscsv="/var/lib/countme/totals.csv"

od="$(date -I)"

if $conf_RW; then
  if [ ! -d $od ] ; then
    echo "Moving old DB files to: $od"
    mkdir $od
    mv raw.db totals.db $od || true
    cp totals.csv $od || true
  else
    echo "Old dir already exists: $od"
    echo " ** Keeping DB files."
  fi
fi

# This is a quick way of doing "recent" countme-update-rawdb.sh runs:
imported=false
for month in $months; do
  echo "Importing: ${month}"
  for day in $(seq -w 31); do
    logfile="$conf_LOGDIR/${month}/${day}/$conf_LOGNAME"
    if [ -f ${logfile}* ]; then
      if $conf_RW; then
        imported=true
        countme-parse-access-log $conf_progress --sqlite ${rawdb} ${logfile}*
      else
	ls -asF ${logfile}*
      fi
    fi
  done
done

if $imported; then
bash countme-update-totals.sh --rawdb ${rawdb} --totals-db ${totsdb} --totals-csv ${totscsv} $conf_progress
else
echo "Nothing imported!"
fi


