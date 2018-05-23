package com.prisma.api.connector.oracle

import com.prisma.api.connector.oracle.database.OracleApiDatabaseMutationBuilder
import com.prisma.api.connector.{DatabaseMutactionResult, UnitDatabaseMutactionResult}
import com.prisma.api.schema.UserFacingError
import slick.dbio.{DBIO, DBIOAction, Effect, NoStream}

trait DatabaseMutactionInterpreter {
  private val unitResult = DBIO.successful(UnitDatabaseMutactionResult)

  def newAction(mutationBuilder: OracleApiDatabaseMutationBuilder): DBIO[DatabaseMutactionResult] = action(mutationBuilder).andThen(unitResult)

  def action(mutationBuilder: OracleApiDatabaseMutationBuilder): DBIOAction[Any, NoStream, Effect.All]

  def errorMapper: PartialFunction[Throwable, UserFacingError] = PartialFunction.empty
}
