package com.rackspace.feeds.ballista.service

import java.io.{File, OutputStream}

import com.rackspace.feeds.ballista.config.AppConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HDFSClient extends FSClient {

  private val conf = new Configuration()
  conf.addResource(new Path(AppConfig.export.to.hdfs.coreSitePath))
  conf.addResource(new Path(AppConfig.export.to.hdfs.hdfsSitePath))

  private val fileSystem = FileSystem.get(conf)


  override def getOutputStream(filePath: String, overwrite: Boolean): OutputStream = {
    val file = new File(filePath)

    fileSystem.create(new Path(filePath), overwrite)
  }
}
