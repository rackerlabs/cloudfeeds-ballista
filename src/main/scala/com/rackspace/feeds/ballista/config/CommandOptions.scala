package com.rackspace.feeds.ballista.config

import org.joda.time.DateTime

case class CommandOptions(runDate: DateTime = DateTime.now.minusDays(1).withTimeAtStartOfDay(),
                          dbNames: Set[String] = AppConfig.export.from.dbs.dbConfigMap.keySet,
                          overwrite: Boolean = false)