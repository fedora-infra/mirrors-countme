#!/bin/bash

DISTROS=('Red Hat Enterprise'
	 'CentOS Linux'
	 'CentOS Stream'
	 'Oracle Linux'
	 'AlmaLinux'
	 'Rocky'
	 );
WEEKS=2

if [ "x$1" = "x--help" -o "x$1" = "xhelp" ]; then
	echo "$0: [WEEKS]"
	echo "  Show CSV data in a more usable form for N weeks ago. Default: 2"
	echo "  0/1 works but may be incomplete."
    exit 0
fi

if [ "x$1" != "x" ]; then
	WEEKS="$1"
fi

DAYS=$((7 * $WEEKS))
DATE=$( date -d "last monday - ${DAYS} days" -I )

ARCHES=('x86_64' 'aarch64' 'ppc64le' 's390x')
RELEASES=('epel-8' 'epel-9')
RELEASE3=('centos-baseos-9-stream')
FILE=/var/lib/countme/totals.csv 
## Countme minimum age
AGE=2

IFS=""

echo "===== Fedora  Base Stats ====="
for arch in ${ARCHES[@]}; do
    grep "${DATE}.*fedora-3.,${arch}" ${FILE} | awk -F, -vW=${DATE} -vR="fedora-3." -vA=${arch} -vX=${AGE} 'BEGIN{x=0}; ($8>=X){x=x+$3}; END{printf("%-12s %-10s %-10s %9d\n",W,R,A,x);}';
done

echo "===== EPEL Base Stats ====="

grep "${DATE}" /var/www/html/csv-reports/mirrors/mirrorsdata-all.csv | awk -F, -vW=${DATE} -vR="epel-7" -vA="all" '{x=$5; printf("%-12s %-10s %-10s %9d ( %9d )\n",W,R,A,x,(x*1.95));}'    
grep "${DATE}" /var/www/html/csv-reports/mirrors/mirrorsdata-all.csv | awk -F, -vW=${DATE} -vR="epel-8" -vA="all" '{x=$72; printf("%-12s %-10s %-10s %9d ( %9d )\n",W,R,A,x,(x*1.95));}'    
grep "${DATE}" /var/www/html/csv-reports/mirrors/mirrorsdata-all.csv | awk -F, -vW=${DATE} -vR="epel-9" -vA="all" '{x=$73; printf("%-12s %-10s %-10s %9d ( %9d )\n",W,R,A,x,(x*1.3));}'    
for release in ${RELEASES[@]}; do
    for arch in ${ARCHES[@]}; do
        grep "${DATE}.*${release},${arch}" ${FILE} | awk -F, -vW=${DATE} -vR=${release} -vA=${arch} -vX=${AGE} 'BEGIN{x=0}; ($8>=X){x=x+$3}; END{printf("%-12s %-10s %-10s %9d\n",W,R,A,x);}';
    done
done

echo "===== EPEL OS Stats ======"
for distro in ${DISTROS[@]}; do
    for release in ${RELEASES[@]}; do
	for arch in ${ARCHES[@]}; do
	    grep "${DATE}.*${distro}.*${release},${arch}" ${FILE} | awk -F, -vW=${DATE} -vD=${distro} -vR=${release} -vA=${arch} -vX=${AGE} 'BEGIN{x=0}; ($8>=X){x=x+$3}; END{printf("%-12s %-18s %-10s %-10s %9d\n",W,D,R,A,x);}';
	done
    done
done

echo "===== CS9 Base Stats ====="
FILE=/var/lib/countme/totals-centos.csv 
for distro in 'CentOS Stream'; do
    for release in ${RELEASE3[@]}; do
	for arch in ${ARCHES[@]}; do
	    grep "${DATE}.*${distro}.*${release},${arch}" ${FILE} | awk -F, -vW=${DATE} -vD=${distro} -vR=${release} -vA=${arch} -vX=${AGE} 'BEGIN{x=0}; ($8>=X){x=x+$3}; END{printf("%-12s %-18s %-10s %-10s %9d\n",W,D,R,A,x);}';
	done
    done
done
