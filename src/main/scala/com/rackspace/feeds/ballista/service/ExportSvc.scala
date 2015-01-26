package com.rackspace.feeds.ballista.service

import javax.sql.DataSource

trait ExportSvc {

  val DATE_FORMAT: String = "yyyy-MM-dd"

  def dataExport: DataExport
  def fsClient: FSClient

  def export(dataSource: DataSource, query: String, outputFilePath: String, overwriteFile: Boolean ): Long = {

    val outputStream = fsClient.getOutputStream(outputFilePath, overwriteFile)
    
    dataExport.export(dataSource, query, outputStream)
  }
}
