#!/bin/sh
USERNAME=ballista
GROUP=ballista
LOG_DIR=/var/log/cloudfeeds-ballista
test -d $LOG_DIR || ( mkdir $LOG_DIR && chown $USERNAME:$GROUP $LOG_DIR )
