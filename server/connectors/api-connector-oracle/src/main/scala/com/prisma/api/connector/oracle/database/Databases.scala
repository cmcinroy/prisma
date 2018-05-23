package com.prisma.api.connector.oracle.database

import com.prisma.config.DatabaseConfig
import com.typesafe.config.{Config, ConfigFactory}
import slick.jdbc.OracleProfile.api._
import slick.jdbc.OracleProfile.backend.DatabaseDef

case class Databases(master: DatabaseDef, readOnly: DatabaseDef)

object Databases {
  private lazy val dbDriver = new oracle.jdbc.OracleDriver

  // Oracle db used for all Prisma schemas (must be in sync with the deploy connector)
  val defaultDatabase = "prisma"

  // Schema to use in the database
  // val schema = "public" // default schema

  def initialize(dbConfig: DatabaseConfig): Databases = {
    val config   = typeSafeConfigFromDatabaseConfig(dbConfig)
    val masterDb = Database.forConfig("database", config, driver = dbDriver)
    val dbs = Databases(
      master = masterDb,
      readOnly = masterDb
    )

    dbs
  }

  def typeSafeConfigFromDatabaseConfig(dbConfig: DatabaseConfig): Config = {
    val pooled = if (dbConfig.pooled) "" else "connectionPool = disabled"

    val database = dbConfig.database.getOrElse(defaultDatabase)

    ConfigFactory
      .parseString(s"""
        |database {
        |  dataSourceClass = "slick.jdbc.DriverDataSource"
        |  properties {
        |    url = "jdbc:oracle://${dbConfig.host}:${dbConfig.port}/$database"
        |    user = "${dbConfig.user}"
        |    password = "${dbConfig.password.getOrElse("")}"
        |  }
        |  numThreads = ${dbConfig.connectionLimit.getOrElse(10)}
        |  connectionTimeout = 5000
        |  $pooled
        |}
      """.stripMargin)
      .resolve
  }
}
