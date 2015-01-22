package com.rackspace.feeds.ballista

import com.rackspace.feeds.ballista.config.AppConfig
import com.rackspace.feeds.ballista.service.ExportDataSvc
import com.rackspace.feeds.ballista.util.DataSourceRepository

object AppMain {

  def main(args: Array[String]) {

    AppConfig.validate()

    val fileName = "preferences-data-" + System.currentTimeMillis() + ".csv"

    ExportDataSvc.export(DataSourceRepository("prefs"), fileName)

  }
}
