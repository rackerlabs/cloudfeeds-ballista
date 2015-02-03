package com.rackspace.feeds.ballista.service

import java.io.OutputStream

trait FSClient {

  /**
   * Gets output stream for the provided filePath
   *
   * @param filePath
   * @param overwrite
   * @return output stream
   */
  def getOutputStream(filePath: String, overwrite: Boolean): OutputStream
}
