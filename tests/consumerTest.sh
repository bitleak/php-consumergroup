#!/bin/bash

if [ -f "consumerTestTempFile" ]; then
    rm consumerTestTempFile;
fi

php runExample.php 0 &
sleep 2

php runExample.php 1 &
sleep 0.5
echo "-" >> consumerTestTempFile
sleep 1.5

php runExample.php 2 &
sleep 0.5
echo "-" >> consumerTestTempFile
sleep 1.5

ps aux | grep "php runExample.php 2" | awk '{print $2}' | xargs kill -2
sleep 0.5
echo "-" >> consumerTestTempFile
sleep 1.5

ps aux | grep "php runExample.php 1" | awk '{print $2}' | xargs kill -2
sleep 0.5
echo "-" >> consumerTestTempFile
sleep 1.5

ps aux | grep "php runExample.php 0" | awk '{print $2}' | xargs kill -2

a=1
num=1
flag=true
offsets=(0 0 0 0 0 0 0 0 0 0 0 0 0)
count=0

while read line
do
    if [ "$line" == "-" ]
    then
        num=`expr $num + $a`
        if [ $num -eq 3 ]
        then
            a=-1
        fi
        count=0
        continue
    else
        count=`expr $count + 1`
        consumer=`echo $line | awk '{print $1}'`
        partition=`echo $line | awk '{print $2}'`
        offset=`echo $line | awk '{print $3}'`
        if [ ${offsets[$partition]} -eq 0 ]
        then
            offsets[$partition]=$offset
        else
            if [ `expr ${offsets[$partition]} + 1` -ne $offset ]
            then
                flag=false
                break
            else
                offsets[$partition]=$offset
            fi
        fi

        if [ $count -lt 50 ]
        then
            if [ `expr $partition % $num` -ne $consumer ]
            then
                flag=false
                break
            fi
        fi
    fi
done < consumerTestTempFile

if [ "$flag" = true ]
then
    echo "test success !"
else
    echo "test fail !"
fi

rm consumerTestTempFile
