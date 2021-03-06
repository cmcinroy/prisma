package com.prisma.connectors.utils

import com.prisma.api.connector.ApiConnector
import com.prisma.api.connector.mysql.MySqlApiConnector
import com.prisma.api.connector.oracle.{PassiveOracleApiConnector, OracleApiConnector}
import com.prisma.api.connector.postgresql.{PassivePostgresApiConnector, PostgresApiConnector}
import com.prisma.config.PrismaConfig
import com.prisma.deploy.connector.DeployConnector
import com.prisma.deploy.connector.mysql.MySqlDeployConnector
import com.prisma.deploy.connector.oracle.{PassiveOracleDeployConnector, OracleDeployConnector}
import com.prisma.deploy.connector.postgresql.{PassivePostgresDeployConnector, PostgresDeployConnector}

import scala.concurrent.ExecutionContext

object ConnectorUtils {
  def loadApiConnector(config: PrismaConfig)(implicit ec: ExecutionContext): ApiConnector = {
    val databaseConfig = config.databases.head
    (databaseConfig.connector, databaseConfig.active) match {
      case ("mysql", true)     => MySqlApiConnector(databaseConfig)
      case ("mysql", false)    => sys.error("There is not passive mysql deploy connector yet!")
      case ("oracle", true)  => OracleApiConnector(databaseConfig)
      case ("oracle", false) => PassiveOracleApiConnector(databaseConfig)
      case ("postgres", true)  => PostgresApiConnector(databaseConfig)
      case ("postgres", false) => PassivePostgresApiConnector(databaseConfig)
      case (conn, _)           => sys.error(s"Unknown connector $conn")
    }
  }

  def loadDeployConnector(config: PrismaConfig)(implicit ec: ExecutionContext): DeployConnector = {
    val databaseConfig = config.databases.head
    (databaseConfig.connector, databaseConfig.active) match {
      case ("mysql", true)     => MySqlDeployConnector(databaseConfig)
      case ("mysql", false)    => sys.error("There is not passive mysql deploy connector yet!")
      case ("oracle", true)  => OracleDeployConnector(databaseConfig)
      case ("oracle", false) => new PassiveOracleDeployConnector(databaseConfig)
      case ("postgres", true)  => PostgresDeployConnector(databaseConfig)
      case ("postgres", false) => new PassivePostgresDeployConnector(databaseConfig)
      case (conn, _)           => sys.error(s"Unknown connector $conn")
    }
  }
}
