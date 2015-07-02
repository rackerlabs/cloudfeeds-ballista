#!/bin/bash
#
# cloudfeeds-ballista_start:    Script for kicking off cloudfeeds-ballista.sh and creating a success file on exit 0
#
#

# path where the success file is to be stored on successful execution
SUCCESS_FILE_PATH="/var/log/cloudfeeds-ballista"

`dirname $0`/cloudfeeds-ballista.sh $*

rc=$?

if [ $rc -eq 0 ]; then
    # SUCCESS
    date --iso-8601=seconds --utc > $SUCCESS_FILE_PATH/last_success.txt
fi

exit $rc
