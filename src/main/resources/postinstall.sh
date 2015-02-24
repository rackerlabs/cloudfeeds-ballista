#!/bin/sh
USERNAME=ballista
GROUP=ballista
LOG_DIR=/var/log/cloudfeeds-ballista
OUTPUT_DIR=/var/lib/cloudfeeds-ballista
test -d $LOG_DIR || ( mkdir $LOG_DIR && chown $USERNAME:$GROUP $LOG_DIR )
test -d $OUTPUT_DIR || (mkdir $OUTPUT_DIR && chown $USERNAME:$GROUP $OUTPUT_DIR)