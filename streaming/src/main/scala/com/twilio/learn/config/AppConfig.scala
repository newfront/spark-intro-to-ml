package com.twilio.learn.config

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object AppConfig {
  private val mapper = new ObjectMapper(new YAMLFactory)
  mapper.registerModule(DefaultScalaModule)

  def parse(configPath: String): AppConfig = {
    val path = if (configPath.nonEmpty) configPath else "src/main/test/resources/app.yaml"
    mapper.readValue(new File(path), classOf[AppConfig])
  }
}

@SerialVersionUID(100L)
case class AppConfig(
  sparkAppConfig: SparkAppConfig
) extends Serializable

@SerialVersionUID(100L)
case class SparkAppConfig(
  appName: String,
  core: Map[String, String]
) extends Serializable

