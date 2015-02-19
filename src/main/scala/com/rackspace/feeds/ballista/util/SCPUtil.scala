package com.rackspace.feeds.ballista.util

import java.io._

import com.jcraft.jsch.{Channel, ChannelExec, Session}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory

import scala.io.Source

class SCPUtil {

  val DefaultBufSize = 1024
  val logger = LoggerFactory.getLogger(getClass)

  /**
   * Using $sessionInfo, scp's the file $localFilePath to $remoteFileLocation/$subDir/$remoteFileName 
   *  
   * @param sessionInfo
   * @param localFilePath
   * @param remoteFileLocation
   */
  def scp(sessionInfo: SCPSessionInfo, localFilePath: String, remoteFileName: String, remoteFileLocation: String, subDir: String) = {

    val remoteFilePath = if (StringUtils.isNotEmpty(subDir))
      s"$remoteFileLocation/$subDir/$remoteFileName"
    else 
      s"$remoteFileLocation/$remoteFileName"
    
    logger.info(s"SCP start: local file [$localFilePath] to remote file [$remoteFilePath]")
    
    var remoteOutputStream: OutputStream = null
    var remoteInputStream: InputStream = null

    try {
      val session: Session = sessionInfo.createSession()
      session.connect()

      val channel: Channel = openChannelAndSetCommand(remoteFileLocation, session, subDir)

      remoteOutputStream = channel.getOutputStream
      remoteInputStream = channel.getInputStream

      channel.connect()

      if (isValidTransfer(remoteInputStream)) {
        logger.debug("channel connection successful")
      }

      sendFileSize(remoteFileName, localFilePath, remoteOutputStream, remoteInputStream, subDir)
      sendContent(localFilePath, remoteOutputStream, remoteInputStream)
      remoteOutputStream.close()

      channel.disconnect()
      session.disconnect()

    } finally {
      IOUtils.closeQuietly(remoteOutputStream)
      IOUtils.closeQuietly(remoteInputStream)
    }
    logger.info(s"SCP end: local file [$localFilePath] to remote file [$remoteFilePath]")
  }


  private def openChannelAndSetCommand(remoteFileLocation: String, session: Session, subDir: String): Channel = {

    /**
     *  scp -t is meant for running scp in sink mode on the remote node. These options are for internal
     *  usage only and aren't documented. I found this link very helpful.
     *  (https://blogs.oracle.com/janp/entry/how_the_scp_protocol_works)
     *  
     *  Jsch library internally parses this command into a String array. So be careful 
     *  to not include any extra spaces inbetween.
     *  
     *  Adding r flag if directory creation is involved
     */
    val recursiveCopyFlag = if (StringUtils.isNotEmpty(subDir)) "r" else ""
    
    val command = s"scp -${recursiveCopyFlag}t $remoteFileLocation"
    val channel = session.openChannel("exec")

    logger.debug(s"scp command: [$command]")
    channel.asInstanceOf[ChannelExec].setCommand(command)
    channel
  }

  /**
   * This method sends the raw data to the consumer (scp in sink mode) by writing
   * to the $remoteOutputStream. The consumer reads exactly that much data as specified 
   * in the length field previously sent with raw protocol message.
   *
   * @param localFilePath
   * @param remoteOutputStream
   * @param remoteInputStream
   */
  private def sendContent(localFilePath: String, remoteOutputStream: OutputStream, remoteInputStream: InputStream): Unit = {
    val buf: Array[Byte] = new Array[Byte](DefaultBufSize)

    val inputStream = new FileInputStream(localFilePath)
    try {
      Iterator
        .continually (inputStream.read(buf, 0, buf.length))
        .takeWhile (-1 !=)
        .foreach (numberOfBytesRead => remoteOutputStream.write(buf, 0, numberOfBytesRead))
    } finally {
      inputStream.close()
    }

    buf(0) = 0.toByte
    remoteOutputStream.write(buf, 0, 1)
    remoteOutputStream.flush()

    if (isValidTransfer(remoteInputStream)) {
      logger.debug("file content sent successfully")
    }
  }

  /**
   * Sending raw protocol message to scp in sink mode indicating the filename, mode, length
   * in the below format
   *
   * Cmmmm <length> <filename>
   *  a single file copy, mmmmm is mode(as the one used in chmod command). Example: C0644 299 remote_file_name
   *  C indicates file copy (D indicates directory)
   *
   * Note: filename should not contain "/"
   *  
   * after C message the data is expected (unless the file is empty)
   * after D message either C or E is expected. This means that it's correct to copy an empty directory providing that user used -r option.
   *
   * @param localFilePath
   * @param out
   * @param in
   */
  private def sendFileSize(remoteFileName: String, localFilePath: String, out: OutputStream, in: InputStream, subDir: String) {

    val localFile = new File(localFilePath)
    val localFileSize = localFile.length()

    val streamCommand = new StringBuilder
    if (StringUtils.isNotEmpty(subDir))
      streamCommand.append(s"D0755 0 $subDir\n")
    streamCommand.append(s"C0644 $localFileSize $remoteFileName\n")

    out.write(streamCommand.toString().getBytes)
    out.flush()

    if (isValidTransfer(in)) {
      logger.debug(s"[$streamCommand] message sent successfully")
    }
  }

  /**
   * This method returns true for a valid transfer and throws RuntimeException if not.
   *
   * The consumer(scp in sink mode(-t)) can reply in 3 different messages; 
   * binary 0 (OK), 1 (warning) or 2 (fatal error; will end the connection). 
   *
   * Messages 1 and 2 can be followed by a the error message and then followed 
   * by a new line character.
   *
   * @param inputStream
   * @return true/false indicating a valid transfer
   */
  private def isValidTransfer(inputStream: InputStream): Boolean = {
    inputStream.read match {
      case 0 => true
      case anyOtherValue => {
        val message = Source.fromInputStream(inputStream).getLines().mkString
        val errorMessage = s"scp -t responded with $anyOtherValue message code followed by error message [$message]"
        logger.error(errorMessage)
        throw new RuntimeException(errorMessage)
      }
    }
  }

}

