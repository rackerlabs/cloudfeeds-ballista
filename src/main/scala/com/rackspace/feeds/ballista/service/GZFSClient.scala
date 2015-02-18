package com.rackspace.feeds.ballista.service

import java.io.{FileOutputStream, File, OutputStream}
import java.util.zip.GZIPOutputStream

import org.slf4j.LoggerFactory

class GZFSClient extends FSClient {

  val logger = LoggerFactory.getLogger(getClass)
  
  override def getOutputStream(outputFilePath: String): OutputStream = {

    val fos: FileOutputStream = new FileOutputStream(outputFilePath)
    new GZIPOutputStream(fos);
  }

}
