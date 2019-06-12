package com.vova.conf

import com.typesafe.config.{Config, ConfigFactory}

object Conf {
  private lazy val buildEnv: String = System.getenv("BUILD_ENV")

  private def configure: Config = buildEnv match {
    case "local" => ConfigFactory.load("local.conf")
    case _ => ConfigFactory.load("prod.conf")
  }
  private val conf: Config = ConfigFactory.systemEnvironment()
    .withFallback(configure)

  def getString(path: String): String = {
    conf.getString(path)
  }
}
