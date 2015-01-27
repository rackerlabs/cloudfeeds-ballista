package com.rackspace.feeds.ballista.service

import javax.sql.DataSource

trait ExportSvc {

  def dataExport: DataExport
  def fsClient: FSClient
  def dataSource: DataSource
  
  def export(query: String, outputFilePath: String, overwriteFile: Boolean ): Long = {

    val outputStream = fsClient.getOutputStream(outputFilePath, overwriteFile)
    
    dataExport.export(dataSource, query, outputStream)
  }
}
