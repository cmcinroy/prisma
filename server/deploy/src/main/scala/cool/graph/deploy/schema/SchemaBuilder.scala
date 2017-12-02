package cool.graph.deploy.schema

import akka.actor.ActorSystem
import cool.graph.deploy.DeployDependencies
import cool.graph.deploy.database.persistence.{MigrationPersistence, ProjectPersistence}
import cool.graph.deploy.migration.{DesiredProjectInferer, MigrationStepsProposer, RenameInferer}
import cool.graph.deploy.schema.fields.{AddProjectField, DeployField}
import cool.graph.deploy.schema.mutations._
import cool.graph.deploy.schema.types.{MigrationStepType, MigrationType, ProjectType, SchemaErrorType}
import cool.graph.shared.models.{Client, Migration, Project}
import cool.graph.utils.future.FutureUtils.FutureOpt
import sangria.relay.Mutation
import sangria.schema._
import slick.jdbc.MySQLProfile.backend.DatabaseDef

import scala.concurrent.Future

case class SystemUserContext(client: Client)

trait SchemaBuilder {
  def apply(userContext: SystemUserContext): Schema[SystemUserContext, Unit]
}

object SchemaBuilder {
  def apply()(implicit system: ActorSystem, dependencies: DeployDependencies): SchemaBuilder =
    new SchemaBuilder {
      override def apply(userContext: SystemUserContext) = {
        SchemaBuilderImpl(userContext).build()
      }
    }
}

case class SchemaBuilderImpl(
    userContext: SystemUserContext
)(implicit system: ActorSystem, dependencies: DeployDependencies) {
  import system.dispatcher

  val internalDb: DatabaseDef                        = dependencies.internalDb
  val clientDb: DatabaseDef                          = dependencies.clientDb
  val projectPersistence: ProjectPersistence         = dependencies.projectPersistence
  val migrationPersistence: MigrationPersistence     = dependencies.migrationPersistence
  val desiredProjectInferer: DesiredProjectInferer   = DesiredProjectInferer()
  val migrationStepsProposer: MigrationStepsProposer = MigrationStepsProposer()
  val renameInferer: RenameInferer                   = RenameInferer

  def build(): Schema[SystemUserContext, Unit] = {
    val Query = ObjectType[SystemUserContext, Unit](
      "Query",
      getQueryFields.toList
    )

    val Mutation = ObjectType(
      "Mutation",
      getMutationFields.toList
    )

    Schema(Query, Some(Mutation), additionalTypes = MigrationStepType.allTypes)
  }

  def getQueryFields: Vector[Field[SystemUserContext, Unit]] = Vector(
    migrationStatusField,
    listProjectsField,
    listMigrationsField
  )

  def getMutationFields: Vector[Field[SystemUserContext, Unit]] = Vector(
    deployField,
    addProjectField
  )

  val migrationStatusField: Field[SystemUserContext, Unit] = Field(
    "migrationStatus",
    MigrationType.Type,
    arguments = List(Argument("projectId", StringType, description = "The project id.")),
    description =
      Some("Shows the status of the next migration in line to be applied to the project. If no such migration exists, it shows the last applied migration."),
    resolve = (ctx) => {
      val projectId = ctx.args.arg[String]("projectId")
      for {
        migration <- FutureOpt(migrationPersistence.getNextMigration(projectId)).fallbackTo(migrationPersistence.getLastMigration(projectId))
      } yield {
        migration.get
      }
    }
  )

  // todo revision is not loaded at the moment, always 0
  val listProjectsField: Field[SystemUserContext, Unit] = Field(
    "listProjects",
    ListType(ProjectType.Type),
    description = Some("Shows all projects the caller has access to."),
    resolve = (ctx) => {
      projectPersistence.loadAll()
    }
  )

  // todo remove if not used anymore
  val listMigrationsField: Field[SystemUserContext, Unit] = Field(
    "listMigrations",
    ListType(MigrationType.Type),
    arguments = List(Argument("projectId", StringType, description = "The project id.")),
    description = Some("Shows all migrations for the project. Debug query, will likely be removed in the future."),
    resolve = (ctx) => {
      val projectId = ctx.args.arg[String]("projectId")
      migrationPersistence.loadAll(projectId)
    }
  )

  def viewerField(): Field[SystemUserContext, Unit] = {
//    Field(
//      "viewer",
//      fieldType = viewerType,
//      resolve = _ => ViewerModel()
//    )
    ???
  }

  def deployField: Field[SystemUserContext, Unit] = {
    import DeployField.fromInput
    Mutation.fieldWithClientMutationId[SystemUserContext, Unit, DeployMutationPayload, DeployMutationInput](
      fieldName = "deploy",
      typeName = "Deploy",
      inputFields = DeployField.inputFields,
      outputFields = sangria.schema.fields[SystemUserContext, DeployMutationPayload](
        Field("project", OptionType(ProjectType.Type), resolve = (ctx: Context[SystemUserContext, DeployMutationPayload]) => ctx.value.project),
        Field("errors", ListType(SchemaErrorType.Type), resolve = (ctx: Context[SystemUserContext, DeployMutationPayload]) => ctx.value.errors),
        Field("migration", MigrationType.Type, resolve = (ctx: Context[SystemUserContext, DeployMutationPayload]) => ctx.value.migration)
      ),
      mutateAndGetPayload = (args, ctx) =>
        handleMutationResult {
          for {
            project <- getProjectOrThrow(args.projectId)
            result <- DeployMutation(
                       args = args,
                       project = project,
                       desiredProjectInferer = desiredProjectInferer,
                       migrationStepsProposer = migrationStepsProposer,
                       renameInferer = renameInferer,
                       migrationPersistence = migrationPersistence
                     ).execute
          } yield result
      }
    )
  }

  def addProjectField: Field[SystemUserContext, Unit] = {
    import AddProjectField.fromInput
    Mutation.fieldWithClientMutationId[SystemUserContext, Unit, AddProjectMutationPayload, AddProjectInput](
      fieldName = "addProject",
      typeName = "AddProject",
      inputFields = AddProjectField.inputFields,
      outputFields = sangria.schema.fields[SystemUserContext, AddProjectMutationPayload](
        Field("project", OptionType(ProjectType.Type), resolve = (ctx: Context[SystemUserContext, AddProjectMutationPayload]) => ctx.value.project)
      ),
      mutateAndGetPayload = (args, ctx) =>
        handleMutationResult {
          AddProjectMutation(
            args = args,
            client = ctx.ctx.client,
            projectPersistence = projectPersistence,
            migrationPersistence = migrationPersistence,
            clientDb = clientDb
          ).execute
      }
    )
  }

  private def handleMutationResult[T](result: Future[MutationResult[T]]): Future[T] = {
    result.map {
      case MutationSuccess(x) => x
      case error              => sys.error(s"The mutation failed with the error: $error")
    }
  }

  private def getProjectOrThrow(projectId: String): Future[Project] = {
    projectPersistence.load(projectId).map { projectOpt =>
      projectOpt.getOrElse(throw InvalidProjectId(projectId))
    }
  }
}
