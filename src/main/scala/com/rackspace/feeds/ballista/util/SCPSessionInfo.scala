package com.rackspace.feeds.ballista.util

import java.util.Properties

import com.jcraft.jsch.{UserInfo, Session, JSch}


class SCPUserInfo(val password: String) extends UserInfo {
  override def getPassphrase: String = null

  override def promptPassword(message: String): Boolean = password != null

  override def promptYesNo(message: String): Boolean = false

  override def showMessage(message: String): Unit = {}

  override def getPassword: String = password

  override def promptPassphrase(message: String): Boolean = false
}

class SCPSessionInfo(userName: String, password: String, host: String, port: Int, pKeyFilePath: String, pKeyPassPhrase: String) {

  private def getConfig() = {
    val props: Properties = new java.util.Properties
    props.put("StrictHostKeyChecking", "no")

    props
  }

  def createSession() = {

    val jsch: JSch = new JSch
    if (pKeyFilePath != null && pKeyFilePath.length > 0)
      jsch.addIdentity(pKeyFilePath, pKeyPassPhrase)

    val session: Session = jsch.getSession(userName, host, port)

    session.setConfig(getConfig())
    session.setUserInfo(new SCPUserInfo(password))

    session
  }

}

object SCPSessionInfo {
  def apply(userName: String, password: String, host: String, port: Int, pKeyFilePath: String, pKeyPassPhrase: String) = {
    new SCPSessionInfo(userName, password, host, port, pKeyFilePath, pKeyPassPhrase)
  }
}
