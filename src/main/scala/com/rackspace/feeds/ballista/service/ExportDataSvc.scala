package com.rackspace.feeds.ballista.service

import java.io.{File, OutputStream}
import javax.sql.DataSource

import com.rackspace.feeds.ballista.config.AppConfig
import com.rackspace.feeds.ballista.util.DataSourceUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.postgresql.PGConnection
import org.slf4j.LoggerFactory

object ExportDataSvc {

  val logger = LoggerFactory.getLogger(getClass)

  private val conf = new Configuration()
  conf.addResource(new Path(AppConfig.export.to.hdfs.coreSitePath))
  conf.addResource(new Path(AppConfig.export.to.hdfs.hdfsSitePath))

  private val fileSystem = FileSystem.get(conf)

  private def getHDFSOutputStream(filepath: String) = {
    val file = new File(filepath)
    
    //overrides existing file
    fileSystem.create(new Path(file.getName), true)
  }

  def export(dataSource: DataSource, filepath: String) = {
    logger.info("Exporting to HDFS -> filename:" + filepath)
    
    val pgConnection: PGConnection = DataSourceUtil.getPGConnection(dataSource)
    val outputStream: OutputStream = getHDFSOutputStream(filepath)
    
    try {

      pgConnection.getCopyAPI
        .copyOut("COPY (SELECT * FROM preferences limit 10) TO STDOUT WITH DELIMITER ','", outputStream)

    } finally {
      outputStream.close()
    }
  }


}
