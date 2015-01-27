package com.rackspace.feeds.ballista

import com.rackspace.feeds.ballista.config.AppConfig
import com.rackspace.feeds.ballista.service.DefaultExportSvc

object AppMain {

  def main(args: Array[String]) {

    AppConfig.export.from.dbs.dbConfigMap.keySet.foreach(
      new DefaultExportSvc(_).export()
    )

  }
}
