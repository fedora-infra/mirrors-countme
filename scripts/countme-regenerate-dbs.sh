#! /bin/sh -e

cd /var/lib/countme/

conf_CENTOS=false

conf_JAMES=true

conf_DEL=false

conf_LOGDIR="/mnt/fedora_stats/combined-http"

if $conf_CENTOS; then
    conf_YEAR=2020
    conf_LOGNAME="mirrors.centos.org-access.log"
    suffix="-centos"
else
    conf_YEAR=2007
    conf_LOGNAME="mirrors.fedoraproject.org-access.log"
    suffix=""
fi
conf_MONTH=1
conf_DAY=1

if $conf_JAMES; then
    _james=/home/fedora/james/mirrors-countme
    cmd_cpa="nice python3.11 $_james/mirrors_countme/scripts/countme_parse_access_log.py"
    cmd_cut="                $_james/scripts/countme-update-totals.sh"
    cmd_ctr="     python3.11 $_james/mirrors_countme/scripts/countme_trim_raw.py"
else
    cmd_cpa=countme-parse-access-log
    cmd_cut=countme-update-totals.sh
    cmd_ctr=countme-trim-raw
fi

conf_progress=--progress
# conf_progress=

if [ "x$(whoami)" != "xcountme" ]; then
    echo "Need to be run as countme."
    exit 1
fi

_cur_year="$(date +'%Y')"
#  Reload all the data from $conf_YEAR onwards. This will take some time.
# Or just reload a single year...

oneyear=false
if [ "x$1" != "x" ]; then
    conf_YEAR="$1"
    oneyear=true
    conf_DEL=false
fi

if [ "x$2" != "x" ]; then
    conf_MONTH="$2"
    oneyear=true
    conf_DEL=false
fi

echo "Regenerating new DB files as:"
echo "  Dir      : /var/lib/countme"
echo "  RAW DB   : raw-new$suffix.db"
echo "  TOTALS DB: totals-new$suffix.db"
if $oneyear; then
    echo "Reload: $conf_YEAR"
else
    echo "Reload: $conf_YEAR-$_cur_year"
fi
echo "Dir: $conf_LOGDIR"
echo "Pkg: $(rpm -q python3-mirrors-countme)"
if $conf_JAMES; then
    echo " ** Running directly from $_james"
fi


rawdb="/var/lib/countme/raw-new$suffix.db"
totsdb="/var/lib/countme/totals-new$suffix.db"

if $conf_DEL; then
    if [ -f $rawdb ]; then
        echo ""
        echo " ***"
        echo "Note: $rawdb already exists!!!"
        echo " ***: DELETEing it and starting from scratch in 5 seconds."
        echo " ***"
        echo ""
        sleep 5
    fi

    rm -f $rawdb
    rm -f $totsdb
else
    if [ -f $rawdb ]; then
        echo "Note: $rawdb already exists, continuing anyway."
    fi
fi

function superVacuum {
    rm -f ${rawdb}.dump.sql.tmp
    sqlite3 ${rawdb} '.dump' > ${rawdb}.dump.sql.tmp
    rm -f ${rawdb}.restore.tmp
    sqlite3 ${rawdb}.restore.tmp < ${rawdb}.dump.sql.tmp
    mv ${rawdb}.restore.tmp ${rawdb}
    rm -f ${rawdb}.dump.sql.tmp
}

num=$conf_YEAR
while [ $num -le $_cur_year ]; do

    if [ -f ${rawdb} ]; then
        # sqlite3 ${rawdb} 'VACUUM;'
        # Do a super VACUUM every year, including to start with...
        # echo "Super VACUUM"
        # superVacuum
        echo ""
    fi


    for month in $(seq -w 12); do
        if [ $conf_MONTH -gt $month ]; then
            echo "Skip: $num/$month"
        else
            conf_MONTH=1
            ran=false
            for day in $(seq -w 31); do
                if [ $conf_DAY -gt $day ]; then
                    echo "Skip: $num/$month/$day"
                else
                    fn="$conf_LOGDIR/$num/$month/$day/$conf_LOGNAME"
                    if [ -f ${fn}* ]; then
                        ran=true
                        if [ "x$conf_progress" = "x" ]; then
                            echo "Day: $num/$month/$day"
                        fi
                        $cmd_cpa  $conf_progress --sqlite ${rawdb} ${fn}*
                    fi
                fi
            done

            if $ran; then
                echo "Doing monthly totals/cleanup: $num/$month"
                $cmd_cut --rawdb ${rawdb} --totals-db ${totsdb} $conf_progress
                $cmd_ctr --rw ${rawdb} 1
                echo "Super VACUUM"
                superVacuum
                # sqlite3 ${rawdb} 'VACUUM;'
            fi
        fi
    done

    if $oneyear; then
        num="$(( $_cur_year + 1 ))"
    else
        num="$(( $num + 1 ))"
    fi

done
