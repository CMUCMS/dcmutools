#!/bin/bash

CWD=$(dirname $0)
TARG=$1

TEMPFILE=tmp_$(date +%s)
TEMPFILE2=tmp2_$(date +%s)

empty=""
for lfn in $(ls $TARG/susyEvents*.root); do
    pfn=$(readlink $lfn)
    if [ ! -e $pfn -o "$(du $pfn | awk '{print $1}')" -eq 0 ]; then
        empty=$emtpy"$lfn "
    fi
done

if [ -n "$empty" ]; then
    echo "Remove empty files [Y/n]?"
    while read response; do
        case $response in
            Y)
                for lfn in $empty; do
                    echo $lfn
                    dsrm $lfn
                    triglfn=$(echo $lfn | sed 's/susyEvents/susyTriggers/')
                    stat $triglfn > /dev/null 2>&1 && echo $triglfn && dsrm $triglfn
                done
                break
                ;;
            n)
                break
                ;;
            *)
                ;;
        esac
    done
fi

python $CWD/check_dataset.py $TARG > $TEMPFILE

nlines=$(awk '/Orphan/ {print $4}' < $TEMPFILE | wc -l)
if [ $nlines -gt 0 ]; then
    awk '/Orphan/ {print $4}' < $TEMPFILE
    echo "Remove orphaned trigger files [Y/n]?"
    while read response; do
        case $response in
            Y)
                awk '/Orphan/ {print $4}' < $TEMPFILE | while read suffix; do
                    echo "$TARG/susyTriggers_${suffix}.root"
                    dsrm $TARG/susyTriggers_${suffix}.root
                done
                break
                ;;
            n)
                break
                ;;
            *)
                ;;
        esac
    done
fi

nlines=$(awk '/Missing trigger/ {print $4}' < $TEMPFILE | wc -l)
if [ $nlines -gt 0 ]; then
    awk '/Missing trigger/ {print $4}' < $TEMPFILE
    echo "Remove event files without trigger information [Y/n]?"
    while read response; do
        case $response in
            Y)
                awk '/Missing trigger/ {print $4}' < $TEMPFILE | while read suffix; do
                    echo "$TARG/susyEvents_${suffix}.root"
                    dsrm $TARG/susyEvents_${suffix}.root
                done
                break
                ;;
            n)
                break
                ;;
            *)
                ;;
        esac
    done
fi

nlines=$(awk '/Duplicated/ {print $4}' < $TEMPFILE | wc -l)
if [ $nlines -gt 0 ]; then
    echo "Remove duplicates [Y/n/A]?"
    doDuplicates=0
    while read response; do
        case $response in
            Y)
                doDuplicates=1
                break
                ;;
            n)
                break
                ;;
            A)
                doDuplicates=2
                break
                ;;
            *)
                ;;
        esac
    done
    
    if [ $doDuplicates -gt 0 ]; then
        sed -n 's/Duplicated event files //p' < $TEMPFILE > $TEMPFILE2
        
        l=0
        while read line; do
            lists[$l]="$line"
            l=$(($l+1))
        done < $TEMPFILE2
        nlists=$l
        
        l=0
        while [ $l -lt $nlists ]; do
            duplicates=${lists[$l]}
        
            i=0
            for suffix in $duplicates; do
                lfn[$i]="$TARG/susyEvents_${suffix}.root"
                pfn[$i]=$(readlink ${lfn[$i]})
		if [ -n "${pfn[$i]}" -a -e ${pfn[$i]} ]; then
		    i=$(($i+1))
		else
		    echo "$suffix removed already"
		fi
            done
            if [ $i -le 1 ]; then
                l=$(($l+1))
                continue
            fi

            n=$i
        
            i=0
            while [ $i -lt $n ]; do
                echo "$i. $(ls -l ${pfn[$i]})"
                i=$(($i+1))
            done

            if [ $doDuplicates -eq 1 ]; then
                echo "Remove: (space separated list)"
                read response
        
                for i in $response; do
                    echo ${lfn[$i]}
                    dsrm ${lfn[$i]}
                    triglfn=$(echo ${lfn[$i]} | sed 's/susyEvents/susyTriggers/')
                    stat $triglfn > /dev/null 2>&1 && echo $triglfn && dsrm $triglfn
                done
                echo
            else
                message="Removing "
                i=0
                while [ $i -lt $(($n-1)) ]; do
                    message=$message$i
                    i=$(($i+1))
                done
                echo $message
                i=0
                while [ $i -lt $(($n-1)) ]; do
                    echo ${lfn[$i]}
                    dsrm ${lfn[$i]}
                    triglfn=$(echo ${lfn[$i]} | sed 's/susyEvents/susyTriggers/')
                    stat $triglfn > /dev/null 2>&1 && echo $triglfn && dsrm $triglfn
                    i=$(($i+1))
                done
            fi
        
            l=$(($l+1))
        done
    
        rm $TEMPFILE2
    fi
fi

rm $TEMPFILE

python $CWD/check_dataset.py $TARG