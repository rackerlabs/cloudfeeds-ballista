#!/bin/sh
USERNAME=ballista
GROUP=ballista
HOME_DIR=/usr/share/cloudfeeds-ballista
getent group $GROUP >/dev/null || groupadd -r $GROUP
getent passwd $USERNAME >/dev/null || useradd -r -g $USERNAME -s /sbin/nologin -d $HOME_DIR -c "Rackspace Cloud Feeds Ballista" $USERNAME
