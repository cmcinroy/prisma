package com.prisma.api.connector.oracle.impl

import com.prisma.api.connector._
import com.prisma.api.connector.oracle.DatabaseMutactionInterpreter
import com.prisma.api.connector.oracle.database.OracleApiDatabaseMutationBuilder
import slick.jdbc.OracleProfile.api._
import slick.jdbc.TransactionIsolation

import scala.concurrent.{ExecutionContext, Future}

case class OracleDatabaseMutactionExecutor(clientDb: Database)(implicit ec: ExecutionContext) extends DatabaseMutactionExecutor {

  override def execute(mutactions: Vector[DatabaseMutaction], runTransactionally: Boolean): Future[Vector[DatabaseMutactionResult]] = {
    val interpreters        = mutactions.map(interpreterFor)
    val combinedErrorMapper = interpreters.map(_.errorMapper).reduceLeft(_ orElse _)
    val mutationBuilder     = OracleApiDatabaseMutationBuilder(schemaName = mutactions.head.project.id, schema = mutactions.head.project.schema)

    val singleAction = runTransactionally match {
      case true  => DBIO.sequence(interpreters.map(_.newAction(mutationBuilder))).transactionally
      case false => DBIO.sequence(interpreters.map(_.newAction(mutationBuilder)))
    }

    clientDb
      .run(singleAction.withTransactionIsolation(TransactionIsolation.ReadCommitted))
      .recover { case error => throw combinedErrorMapper.lift(error).getOrElse(error) }
  }

  def interpreterFor(mutaction: DatabaseMutaction): DatabaseMutactionInterpreter = mutaction match {
    case m: AddDataItemToManyRelationByPath   => AddDataItemToManyRelationByPathInterpreter(m)
    case m: CascadingDeleteRelationMutactions => CascadingDeleteRelationMutactionsInterpreter(m)
    case m: CreateDataItem                    => CreateDataItemInterpreter(m)
    case m: DeleteDataItem                    => DeleteDataItemInterpreter(m)
    case m: DeleteDataItemNested              => DeleteDataItemNestedInterpreter(m)
    case m: DeleteDataItems                   => DeleteDataItemsInterpreter(m)
    case m: DeleteManyRelationChecks          => DeleteManyRelationChecksInterpreter(m)
    case m: DeleteRelationCheck               => DeleteRelationCheckInterpreter(m)
    case m: NestedConnectRelation             => NestedConnectRelationInterpreter(m)
    case m: NestedCreateRelation              => NestedCreateRelationInterpreter(m)
    case m: NestedDisconnectRelation          => NestedDisconnectRelationInterpreter(m)
    case m: ResetDataMutaction                => ResetDataInterpreter(m)
    case m: UpdateDataItem                    => UpdateDataItemInterpreter(m)
    case m: NestedUpdateDataItem              => UpdateDataItemInterpreter(m)
    case m: UpdateDataItems                   => UpdateDataItemsInterpreter(m)
    case m: UpsertDataItem                    => UpsertDataItemInterpreter(m)
    case m: UpsertDataItemIfInRelationWith    => UpsertDataItemIfInRelationWithInterpreter(m)
    case m: VerifyConnection                  => VerifyConnectionInterpreter(m)
    case m: VerifyWhere                       => VerifyWhereInterpreter(m)
    case m: CreateDataItemsImport             => CreateDataItemsImportInterpreter(m)
    case m: CreateRelationRowsImport          => CreateRelationRowsImportInterpreter(m)
    case m: PushScalarListsImport             => PushScalarListsImportInterpreter(m)
  }
}
