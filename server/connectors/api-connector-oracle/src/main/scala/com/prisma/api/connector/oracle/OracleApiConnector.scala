package com.prisma.api.connector.oracle

import com.prisma.api.connector.{ApiConnector, NodeQueryCapability}
import com.prisma.api.connector.oracle.database.{Databases, OracleDataResolver}
import com.prisma.api.connector.oracle.impl.OracleDatabaseMutactionExecutor
import com.prisma.config.DatabaseConfig
import com.prisma.shared.models.{Project, ProjectIdEncoder}

import scala.concurrent.{ExecutionContext, Future}

case class OracleApiConnector(config: DatabaseConfig)(implicit ec: ExecutionContext) extends ApiConnector {
  lazy val databases = Databases.initialize(config)

  override def initialize() = {
    databases
    Future.unit
  }

  override def shutdown() = {
    for {
      _ <- databases.master.shutdown
      _ <- databases.readOnly.shutdown
    } yield ()
  }

  override def databaseMutactionExecutor: OracleDatabaseMutactionExecutor = OracleDatabaseMutactionExecutor(databases.master)
  override def dataResolver(project: Project)                               = OracleDataResolver(project, databases.readOnly, schemaName = None)
  override def masterDataResolver(project: Project)                         = OracleDataResolver(project, databases.master, schemaName = None)
  override def projectIdEncoder: ProjectIdEncoder                           = ProjectIdEncoder('$')
  override def capabilities                                                 = Vector(NodeQueryCapability)
}
