#!/bin/bash

# Make sure umask is sane
umask 022

# Automatically mark variables and functions which are modified or created
# for export to the environment of subsequent commands.
set -o allexport

if [ "$(whoami)" != "hdfs" ]
then
    echo "Must be run as hdfs user!"
    exit 125
fi

# Set up a default search path.
PATH="/sbin:/usr/sbin:/bin:/usr/bin"
export PATH

scriptdir=`dirname $0`/..
HOMEDIR=${HOMEDIRPRIME:-`readlink -f $scriptdir`}

appdir=$HOMEDIR/..
APPDIR=${APPDIRPRIME:-`readlink -f $appdir`}

SRCDIR=${SRCDIRPRIME:-$HOMEDIR/src}
LIBDIR=${LIBDIRPRIME:-$HOMEDIR/lib}
CONFDIR=${SRCONFDIRPRIME:-$HOMEDIR/config}

# Update path so scripts under bin are available
PATH=$HOMEDIR/bin:$PATH

cd $HOMEDIR

## Generate classpath from libraries in $LIBDIR, and add $CONFDIR if required
OUR_CLASSPATH=$(find $LIBDIR -type f -name "*.jar" | paste -sd:)
if [ -d "$CONFDIR" ]; then
    OUR_CLASSPATH=$CONFDIR:$OUR_CLASSPATH
fi

# Remote Debug Java Opts
#JAVA_OPTS="-Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,address=7778,server=y,suspend=y"

# Memory-related Opts
#JAVA_OPTS="-XX:+HeapDumpOnOutOfMemoryError -verbose:gc"

# Start up size for memory allocation pool for java VM.
MS=512m

# Max size of memory allocation pool for java VM.
MX=2048m

#export HADOOP_USER_CLASSPATH_FIRST=true

JAVA_OPTS="${JAVA_OPTS} -Xms$MS -Xmx$MX -XX:+UseParNewGC -XX:+UseConcMarkSweepGC"
JAVA_OPTS="${JAVA_OPTS} -XX:-CMSConcurrentMTEnabled -XX:CMSInitiatingOccupancyFraction=70"
JAVA_OPTS="${JAVA_OPTS} -XX:+CMSParallelRemarkEnabled -XX:+DoEscapeAnalysis"

HADOOP_OPTS="${HADOOP_OPTS} -Dlog4j.configuration=log4j-production.properties"
#HADOOP_OPTS="${HADOOP_OPTS} -Dmapreduce.job.user.classpath.first=true"

export HADOOP_CLASSPATH="$OUR_CLASSPATH:/etc/hive/conf"
export HADOOP_OPTS="${JAVA_OPTS} ${HADOOP_OPTS}"
export YARN_OPTS="${JAVA_OPTS} ${HADOOP_OPTS}"

command="yarn jar $LIBDIR/hive-auto-partitioner.jar ch.daplab.hivepartition.HivePartitionsSynchCli"

echo $command $@
$command $@
