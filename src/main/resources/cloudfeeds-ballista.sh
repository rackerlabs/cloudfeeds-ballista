#!/bin/bash
#
# cloudfeeds-ballista:    Script for running the cloudfeeds ballista util
#
#

#################################
# Default configuration options #
#################################

# user to run the service as
USER=ballista
# name of the service
NAME=cloudfeeds-ballista
# configuration directory
CONFIG_DIRECTORY="/etc/${NAME}"
# configuration file
CONFIG_FILE="${CONFIG_DIRECTORY}/${NAME}.conf"
# log configuration file
LOG_CONFIG_FILE="${CONFIG_DIRECTORY}/logback.xml"
# default log locatiojn
LOG_PATH="/var/log/${NAME}"
# pid file location
PID_FILE="/var/run/${NAME}.pid"
# default run options
RUN_OPTS=""
# default java options
JAVA_OPTS="-Dconfig.file=${CONFIG_FILE} -Dlogback.configurationFile=${LOG_CONFIG_FILE}"
# home directory for the service
EXEC_JAR="$(dirname $0)/../lib/${NAME}.jar"

# Override the defaults via /etc/sysconfig/${NAME}
if [ -f "/etc/sysconfig/${NAME}" ]; then
  . /etc/sysconfig/${NAME}
fi

#################
# Sanity checks #
#################

# check for the configuration directory
if [ ! -d "${CONFIG_DIRECTORY}" ]; then
  echo "Unable to find ${CONFIG_DIRECTORY}."
  exit 1
fi

# check for the logging directory
if [ ! -d "${LOG_PATH}" ]; then
  echo "Unable to log to ${LOG_PATH}."
  exit 1
fi

/usr/bin/java $JAVA_OPTS -jar $EXEC_JAR $RUN_OPTS $*

rc=$?

if [ $rc -eq 0 ]; then
    # SUCCESS
    date --iso-8601=seconds --utc > $LOG_PATH/last_success.txt
fi
