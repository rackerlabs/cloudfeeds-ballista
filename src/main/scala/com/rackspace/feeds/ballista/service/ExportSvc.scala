package com.rackspace.feeds.ballista.service

import javax.sql.DataSource

trait ExportSvc {

  def dataExport: DataExport
  def fsClient: FSClient

  def export(dataSource: DataSource, query: String, outputFilePath: String): Long = {

    val outputStream = fsClient.getOutputStream(outputFilePath)

    dataExport.export(dataSource, query, outputStream)
  }
}
