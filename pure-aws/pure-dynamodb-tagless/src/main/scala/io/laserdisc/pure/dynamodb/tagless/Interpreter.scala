package io.laserdisc.pure.dynamodb.tagless

// Library imports
import cats.data.Kleisli
import cats.effect.{ Async, Blocker, ContextShift, Resource }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder

import java.util.concurrent.CompletionException

// Types referenced
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  BatchExecuteStatementRequest,
  BatchGetItemRequest,
  BatchWriteItemRequest,
  CreateBackupRequest,
  CreateGlobalTableRequest,
  CreateTableRequest,
  DeleteBackupRequest,
  DeleteItemRequest,
  DeleteTableRequest,
  DescribeBackupRequest,
  DescribeContinuousBackupsRequest,
  DescribeContributorInsightsRequest,
  DescribeEndpointsRequest,
  DescribeExportRequest,
  DescribeGlobalTableRequest,
  DescribeGlobalTableSettingsRequest,
  DescribeKinesisStreamingDestinationRequest,
  DescribeLimitsRequest,
  DescribeTableReplicaAutoScalingRequest,
  DescribeTableRequest,
  DescribeTimeToLiveRequest,
  DisableKinesisStreamingDestinationRequest,
  EnableKinesisStreamingDestinationRequest,
  ExecuteStatementRequest,
  ExecuteTransactionRequest,
  ExportTableToPointInTimeRequest,
  GetItemRequest,
  ListBackupsRequest,
  ListContributorInsightsRequest,
  ListExportsRequest,
  ListGlobalTablesRequest,
  ListTablesRequest,
  ListTagsOfResourceRequest,
  PutItemRequest,
  QueryRequest,
  RestoreTableFromBackupRequest,
  RestoreTableToPointInTimeRequest,
  ScanRequest,
  TagResourceRequest,
  TransactGetItemsRequest,
  TransactWriteItemsRequest,
  UntagResourceRequest,
  UpdateContinuousBackupsRequest,
  UpdateContributorInsightsRequest,
  UpdateGlobalTableRequest,
  UpdateGlobalTableSettingsRequest,
  UpdateItemRequest,
  UpdateTableReplicaAutoScalingRequest,
  UpdateTableRequest,
  UpdateTimeToLiveRequest
}

import java.util.concurrent.CompletableFuture

object Interpreter {

  @deprecated("Use Interpreter[M]. blocker is not needed anymore", "3.2.0")
  def apply[M[_]](b: Blocker)(
    implicit am: Async[M],
    cs: ContextShift[M]
  ): Interpreter[M] =
    new Interpreter[M] {
      val asyncM        = am
      val contextShiftM = cs
    }

  def apply[M[_]](
    implicit am: Async[M],
    cs: ContextShift[M]
  ): Interpreter[M] =
    new Interpreter[M] {
      val asyncM        = am
      val contextShiftM = cs
    }

}

// Family of interpreters into Kleisli arrows for some monad M.
trait Interpreter[M[_]] { outer =>

  implicit val asyncM: Async[M]

  // to support shifting blocking operations to another pool.
  val contextShiftM: ContextShift[M]

  lazy val DynamoDbAsyncClientInterpreter: DynamoDbAsyncClientInterpreter =
    new DynamoDbAsyncClientInterpreter {}
  // Some methods are common to all interpreters and can be overridden to change behavior globally.

  def primitive[J, A](f: J => A): Kleisli[M, J, A] = Kleisli(a => primitive1(f(a)))

  def primitive1[J, A](f: =>A): M[A] = asyncM.delay(f)

  def eff[J, A](fut: J => CompletableFuture[A]): Kleisli[M, J, A] = Kleisli(a => eff1(fut(a)))

  def eff1[J, A](fut: =>CompletableFuture[A]): M[A] =
    asyncM.guarantee(
      asyncM
        .async[A] { cb =>
          fut.handle[Unit] { (a, x) =>
            if (a == null)
              x match {
                case t: CompletionException => cb(Left(t.getCause))
                case t                      => cb(Left(t))
              }
            else
              cb(Right(a))
          }
          ()
        }
    )(contextShiftM.shift)

  // Interpreters
  trait DynamoDbAsyncClientInterpreter
      extends DynamoDbAsyncClientOp[Kleisli[M, DynamoDbAsyncClient, *]] {

    // domain-specific operations are implemented in terms of `primitive`
    override def batchExecuteStatement(a: BatchExecuteStatementRequest) =
      eff(_.batchExecuteStatement(a))
    override def batchGetItem(a: BatchGetItemRequest) = eff(_.batchGetItem(a))
    override def batchGetItemPaginator(a: BatchGetItemRequest) =
      primitive(_.batchGetItemPaginator(a))
    override def batchWriteItem(a: BatchWriteItemRequest)       = eff(_.batchWriteItem(a))
    override def close                                          = primitive(_.close)
    override def createBackup(a: CreateBackupRequest)           = eff(_.createBackup(a))
    override def createGlobalTable(a: CreateGlobalTableRequest) = eff(_.createGlobalTable(a))
    override def createTable(a: CreateTableRequest)             = eff(_.createTable(a))
    override def deleteBackup(a: DeleteBackupRequest)           = eff(_.deleteBackup(a))
    override def deleteItem(a: DeleteItemRequest)               = eff(_.deleteItem(a))
    override def deleteTable(a: DeleteTableRequest)             = eff(_.deleteTable(a))
    override def describeBackup(a: DescribeBackupRequest)       = eff(_.describeBackup(a))
    override def describeContinuousBackups(a: DescribeContinuousBackupsRequest) =
      eff(_.describeContinuousBackups(a))
    override def describeContributorInsights(a: DescribeContributorInsightsRequest) =
      eff(_.describeContributorInsights(a))
    override def describeEndpoints                                  = eff(_.describeEndpoints)
    override def describeEndpoints(a: DescribeEndpointsRequest)     = eff(_.describeEndpoints(a))
    override def describeExport(a: DescribeExportRequest)           = eff(_.describeExport(a))
    override def describeGlobalTable(a: DescribeGlobalTableRequest) = eff(_.describeGlobalTable(a))
    override def describeGlobalTableSettings(a: DescribeGlobalTableSettingsRequest) =
      eff(_.describeGlobalTableSettings(a))
    override def describeKinesisStreamingDestination(
      a: DescribeKinesisStreamingDestinationRequest
    )                                                     = eff(_.describeKinesisStreamingDestination(a))
    override def describeLimits                           = eff(_.describeLimits)
    override def describeLimits(a: DescribeLimitsRequest) = eff(_.describeLimits(a))
    override def describeTable(a: DescribeTableRequest)   = eff(_.describeTable(a))
    override def describeTableReplicaAutoScaling(a: DescribeTableReplicaAutoScalingRequest) =
      eff(_.describeTableReplicaAutoScaling(a))
    override def describeTimeToLive(a: DescribeTimeToLiveRequest) = eff(_.describeTimeToLive(a))
    override def disableKinesisStreamingDestination(a: DisableKinesisStreamingDestinationRequest) =
      eff(_.disableKinesisStreamingDestination(a))
    override def enableKinesisStreamingDestination(a: EnableKinesisStreamingDestinationRequest) =
      eff(_.enableKinesisStreamingDestination(a))
    override def executeStatement(a: ExecuteStatementRequest)     = eff(_.executeStatement(a))
    override def executeTransaction(a: ExecuteTransactionRequest) = eff(_.executeTransaction(a))
    override def exportTableToPointInTime(a: ExportTableToPointInTimeRequest) =
      eff(_.exportTableToPointInTime(a))
    override def getItem(a: GetItemRequest)         = eff(_.getItem(a))
    override def listBackups                        = eff(_.listBackups)
    override def listBackups(a: ListBackupsRequest) = eff(_.listBackups(a))
    override def listContributorInsights(a: ListContributorInsightsRequest) =
      eff(_.listContributorInsights(a))
    override def listContributorInsightsPaginator(a: ListContributorInsightsRequest) =
      primitive(_.listContributorInsightsPaginator(a))
    override def listExports(a: ListExportsRequest)               = eff(_.listExports(a))
    override def listExportsPaginator(a: ListExportsRequest)      = primitive(_.listExportsPaginator(a))
    override def listGlobalTables                                 = eff(_.listGlobalTables)
    override def listGlobalTables(a: ListGlobalTablesRequest)     = eff(_.listGlobalTables(a))
    override def listTables                                       = eff(_.listTables)
    override def listTables(a: ListTablesRequest)                 = eff(_.listTables(a))
    override def listTablesPaginator                              = primitive(_.listTablesPaginator)
    override def listTablesPaginator(a: ListTablesRequest)        = primitive(_.listTablesPaginator(a))
    override def listTagsOfResource(a: ListTagsOfResourceRequest) = eff(_.listTagsOfResource(a))
    override def putItem(a: PutItemRequest)                       = eff(_.putItem(a))
    override def query(a: QueryRequest)                           = eff(_.query(a))
    override def queryPaginator(a: QueryRequest)                  = primitive(_.queryPaginator(a))
    override def restoreTableFromBackup(a: RestoreTableFromBackupRequest) =
      eff(_.restoreTableFromBackup(a))
    override def restoreTableToPointInTime(a: RestoreTableToPointInTimeRequest) =
      eff(_.restoreTableToPointInTime(a))
    override def scan(a: ScanRequest)                             = eff(_.scan(a))
    override def scanPaginator(a: ScanRequest)                    = primitive(_.scanPaginator(a))
    override def serviceName                                      = primitive(_.serviceName)
    override def tagResource(a: TagResourceRequest)               = eff(_.tagResource(a))
    override def transactGetItems(a: TransactGetItemsRequest)     = eff(_.transactGetItems(a))
    override def transactWriteItems(a: TransactWriteItemsRequest) = eff(_.transactWriteItems(a))
    override def untagResource(a: UntagResourceRequest)           = eff(_.untagResource(a))
    override def updateContinuousBackups(a: UpdateContinuousBackupsRequest) =
      eff(_.updateContinuousBackups(a))
    override def updateContributorInsights(a: UpdateContributorInsightsRequest) =
      eff(_.updateContributorInsights(a))
    override def updateGlobalTable(a: UpdateGlobalTableRequest) = eff(_.updateGlobalTable(a))
    override def updateGlobalTableSettings(a: UpdateGlobalTableSettingsRequest) =
      eff(_.updateGlobalTableSettings(a))
    override def updateItem(a: UpdateItemRequest)   = eff(_.updateItem(a))
    override def updateTable(a: UpdateTableRequest) = eff(_.updateTable(a))
    override def updateTableReplicaAutoScaling(a: UpdateTableReplicaAutoScalingRequest) =
      eff(_.updateTableReplicaAutoScaling(a))
    override def updateTimeToLive(a: UpdateTimeToLiveRequest) = eff(_.updateTimeToLive(a))
    override def waiter                                       = primitive(_.waiter)
    def lens[E](f: E => DynamoDbAsyncClient): DynamoDbAsyncClientOp[Kleisli[M, E, *]] =
      new DynamoDbAsyncClientOp[Kleisli[M, E, *]] {
        override def batchExecuteStatement(a: BatchExecuteStatementRequest) =
          Kleisli(e => eff1(f(e).batchExecuteStatement(a)))
        override def batchGetItem(a: BatchGetItemRequest) = Kleisli(e => eff1(f(e).batchGetItem(a)))
        override def batchGetItemPaginator(a: BatchGetItemRequest) =
          Kleisli(e => primitive1(f(e).batchGetItemPaginator(a)))
        override def batchWriteItem(a: BatchWriteItemRequest) =
          Kleisli(e => eff1(f(e).batchWriteItem(a)))
        override def close                                = Kleisli(e => primitive1(f(e).close))
        override def createBackup(a: CreateBackupRequest) = Kleisli(e => eff1(f(e).createBackup(a)))
        override def createGlobalTable(a: CreateGlobalTableRequest) =
          Kleisli(e => eff1(f(e).createGlobalTable(a)))
        override def createTable(a: CreateTableRequest)   = Kleisli(e => eff1(f(e).createTable(a)))
        override def deleteBackup(a: DeleteBackupRequest) = Kleisli(e => eff1(f(e).deleteBackup(a)))
        override def deleteItem(a: DeleteItemRequest)     = Kleisli(e => eff1(f(e).deleteItem(a)))
        override def deleteTable(a: DeleteTableRequest)   = Kleisli(e => eff1(f(e).deleteTable(a)))
        override def describeBackup(a: DescribeBackupRequest) =
          Kleisli(e => eff1(f(e).describeBackup(a)))
        override def describeContinuousBackups(a: DescribeContinuousBackupsRequest) =
          Kleisli(e => eff1(f(e).describeContinuousBackups(a)))
        override def describeContributorInsights(a: DescribeContributorInsightsRequest) =
          Kleisli(e => eff1(f(e).describeContributorInsights(a)))
        override def describeEndpoints = Kleisli(e => eff1(f(e).describeEndpoints))
        override def describeEndpoints(a: DescribeEndpointsRequest) =
          Kleisli(e => eff1(f(e).describeEndpoints(a)))
        override def describeExport(a: DescribeExportRequest) =
          Kleisli(e => eff1(f(e).describeExport(a)))
        override def describeGlobalTable(a: DescribeGlobalTableRequest) =
          Kleisli(e => eff1(f(e).describeGlobalTable(a)))
        override def describeGlobalTableSettings(a: DescribeGlobalTableSettingsRequest) =
          Kleisli(e => eff1(f(e).describeGlobalTableSettings(a)))
        override def describeKinesisStreamingDestination(
          a: DescribeKinesisStreamingDestinationRequest
        )                           = Kleisli(e => eff1(f(e).describeKinesisStreamingDestination(a)))
        override def describeLimits = Kleisli(e => eff1(f(e).describeLimits))
        override def describeLimits(a: DescribeLimitsRequest) =
          Kleisli(e => eff1(f(e).describeLimits(a)))
        override def describeTable(a: DescribeTableRequest) =
          Kleisli(e => eff1(f(e).describeTable(a)))
        override def describeTableReplicaAutoScaling(a: DescribeTableReplicaAutoScalingRequest) =
          Kleisli(e => eff1(f(e).describeTableReplicaAutoScaling(a)))
        override def describeTimeToLive(a: DescribeTimeToLiveRequest) =
          Kleisli(e => eff1(f(e).describeTimeToLive(a)))
        override def disableKinesisStreamingDestination(
          a: DisableKinesisStreamingDestinationRequest
        ) = Kleisli(e => eff1(f(e).disableKinesisStreamingDestination(a)))
        override def enableKinesisStreamingDestination(
          a: EnableKinesisStreamingDestinationRequest
        ) = Kleisli(e => eff1(f(e).enableKinesisStreamingDestination(a)))
        override def executeStatement(a: ExecuteStatementRequest) =
          Kleisli(e => eff1(f(e).executeStatement(a)))
        override def executeTransaction(a: ExecuteTransactionRequest) =
          Kleisli(e => eff1(f(e).executeTransaction(a)))
        override def exportTableToPointInTime(a: ExportTableToPointInTimeRequest) =
          Kleisli(e => eff1(f(e).exportTableToPointInTime(a)))
        override def getItem(a: GetItemRequest)         = Kleisli(e => eff1(f(e).getItem(a)))
        override def listBackups                        = Kleisli(e => eff1(f(e).listBackups))
        override def listBackups(a: ListBackupsRequest) = Kleisli(e => eff1(f(e).listBackups(a)))
        override def listContributorInsights(a: ListContributorInsightsRequest) =
          Kleisli(e => eff1(f(e).listContributorInsights(a)))
        override def listContributorInsightsPaginator(a: ListContributorInsightsRequest) =
          Kleisli(e => primitive1(f(e).listContributorInsightsPaginator(a)))
        override def listExports(a: ListExportsRequest) = Kleisli(e => eff1(f(e).listExports(a)))
        override def listExportsPaginator(a: ListExportsRequest) =
          Kleisli(e => primitive1(f(e).listExportsPaginator(a)))
        override def listGlobalTables = Kleisli(e => eff1(f(e).listGlobalTables))
        override def listGlobalTables(a: ListGlobalTablesRequest) =
          Kleisli(e => eff1(f(e).listGlobalTables(a)))
        override def listTables                       = Kleisli(e => eff1(f(e).listTables))
        override def listTables(a: ListTablesRequest) = Kleisli(e => eff1(f(e).listTables(a)))
        override def listTablesPaginator              = Kleisli(e => primitive1(f(e).listTablesPaginator))
        override def listTablesPaginator(a: ListTablesRequest) =
          Kleisli(e => primitive1(f(e).listTablesPaginator(a)))
        override def listTagsOfResource(a: ListTagsOfResourceRequest) =
          Kleisli(e => eff1(f(e).listTagsOfResource(a)))
        override def putItem(a: PutItemRequest) = Kleisli(e => eff1(f(e).putItem(a)))
        override def query(a: QueryRequest)     = Kleisli(e => eff1(f(e).query(a)))
        override def queryPaginator(a: QueryRequest) =
          Kleisli(e => primitive1(f(e).queryPaginator(a)))
        override def restoreTableFromBackup(a: RestoreTableFromBackupRequest) =
          Kleisli(e => eff1(f(e).restoreTableFromBackup(a)))
        override def restoreTableToPointInTime(a: RestoreTableToPointInTimeRequest) =
          Kleisli(e => eff1(f(e).restoreTableToPointInTime(a)))
        override def scan(a: ScanRequest)               = Kleisli(e => eff1(f(e).scan(a)))
        override def scanPaginator(a: ScanRequest)      = Kleisli(e => primitive1(f(e).scanPaginator(a)))
        override def serviceName                        = Kleisli(e => primitive1(f(e).serviceName))
        override def tagResource(a: TagResourceRequest) = Kleisli(e => eff1(f(e).tagResource(a)))
        override def transactGetItems(a: TransactGetItemsRequest) =
          Kleisli(e => eff1(f(e).transactGetItems(a)))
        override def transactWriteItems(a: TransactWriteItemsRequest) =
          Kleisli(e => eff1(f(e).transactWriteItems(a)))
        override def untagResource(a: UntagResourceRequest) =
          Kleisli(e => eff1(f(e).untagResource(a)))
        override def updateContinuousBackups(a: UpdateContinuousBackupsRequest) =
          Kleisli(e => eff1(f(e).updateContinuousBackups(a)))
        override def updateContributorInsights(a: UpdateContributorInsightsRequest) =
          Kleisli(e => eff1(f(e).updateContributorInsights(a)))
        override def updateGlobalTable(a: UpdateGlobalTableRequest) =
          Kleisli(e => eff1(f(e).updateGlobalTable(a)))
        override def updateGlobalTableSettings(a: UpdateGlobalTableSettingsRequest) =
          Kleisli(e => eff1(f(e).updateGlobalTableSettings(a)))
        override def updateItem(a: UpdateItemRequest)   = Kleisli(e => eff1(f(e).updateItem(a)))
        override def updateTable(a: UpdateTableRequest) = Kleisli(e => eff1(f(e).updateTable(a)))
        override def updateTableReplicaAutoScaling(a: UpdateTableReplicaAutoScalingRequest) =
          Kleisli(e => eff1(f(e).updateTableReplicaAutoScaling(a)))
        override def updateTimeToLive(a: UpdateTimeToLiveRequest) =
          Kleisli(e => eff1(f(e).updateTimeToLive(a)))
        override def waiter = Kleisli(e => primitive1(f(e).waiter))
      }
  }

  def DynamoDbAsyncClientResource(
    builder: DynamoDbAsyncClientBuilder
  ): Resource[M, DynamoDbAsyncClient] = Resource.fromAutoCloseable(asyncM.delay(builder.build()))
  def DynamoDbAsyncClientOpResource(builder: DynamoDbAsyncClientBuilder) =
    DynamoDbAsyncClientResource(builder).map(create)
  def create(client: DynamoDbAsyncClient): DynamoDbAsyncClientOp[M] = new DynamoDbAsyncClientOp[M] {

    // domain-specific operations are implemented in terms of `primitive`
    override def batchExecuteStatement(a: BatchExecuteStatementRequest) =
      eff1(client.batchExecuteStatement(a))
    override def batchGetItem(a: BatchGetItemRequest) = eff1(client.batchGetItem(a))
    override def batchGetItemPaginator(a: BatchGetItemRequest) =
      primitive1(client.batchGetItemPaginator(a))
    override def batchWriteItem(a: BatchWriteItemRequest)       = eff1(client.batchWriteItem(a))
    override def close                                          = primitive1(client.close)
    override def createBackup(a: CreateBackupRequest)           = eff1(client.createBackup(a))
    override def createGlobalTable(a: CreateGlobalTableRequest) = eff1(client.createGlobalTable(a))
    override def createTable(a: CreateTableRequest)             = eff1(client.createTable(a))
    override def deleteBackup(a: DeleteBackupRequest)           = eff1(client.deleteBackup(a))
    override def deleteItem(a: DeleteItemRequest)               = eff1(client.deleteItem(a))
    override def deleteTable(a: DeleteTableRequest)             = eff1(client.deleteTable(a))
    override def describeBackup(a: DescribeBackupRequest)       = eff1(client.describeBackup(a))
    override def describeContinuousBackups(a: DescribeContinuousBackupsRequest) =
      eff1(client.describeContinuousBackups(a))
    override def describeContributorInsights(a: DescribeContributorInsightsRequest) =
      eff1(client.describeContributorInsights(a))
    override def describeEndpoints                              = eff1(client.describeEndpoints)
    override def describeEndpoints(a: DescribeEndpointsRequest) = eff1(client.describeEndpoints(a))
    override def describeExport(a: DescribeExportRequest)       = eff1(client.describeExport(a))
    override def describeGlobalTable(a: DescribeGlobalTableRequest) =
      eff1(client.describeGlobalTable(a))
    override def describeGlobalTableSettings(a: DescribeGlobalTableSettingsRequest) =
      eff1(client.describeGlobalTableSettings(a))
    override def describeKinesisStreamingDestination(
      a: DescribeKinesisStreamingDestinationRequest
    )                                                     = eff1(client.describeKinesisStreamingDestination(a))
    override def describeLimits                           = eff1(client.describeLimits)
    override def describeLimits(a: DescribeLimitsRequest) = eff1(client.describeLimits(a))
    override def describeTable(a: DescribeTableRequest)   = eff1(client.describeTable(a))
    override def describeTableReplicaAutoScaling(a: DescribeTableReplicaAutoScalingRequest) =
      eff1(client.describeTableReplicaAutoScaling(a))
    override def describeTimeToLive(a: DescribeTimeToLiveRequest) =
      eff1(client.describeTimeToLive(a))
    override def disableKinesisStreamingDestination(a: DisableKinesisStreamingDestinationRequest) =
      eff1(client.disableKinesisStreamingDestination(a))
    override def enableKinesisStreamingDestination(a: EnableKinesisStreamingDestinationRequest) =
      eff1(client.enableKinesisStreamingDestination(a))
    override def executeStatement(a: ExecuteStatementRequest) = eff1(client.executeStatement(a))
    override def executeTransaction(a: ExecuteTransactionRequest) =
      eff1(client.executeTransaction(a))
    override def exportTableToPointInTime(a: ExportTableToPointInTimeRequest) =
      eff1(client.exportTableToPointInTime(a))
    override def getItem(a: GetItemRequest)         = eff1(client.getItem(a))
    override def listBackups                        = eff1(client.listBackups)
    override def listBackups(a: ListBackupsRequest) = eff1(client.listBackups(a))
    override def listContributorInsights(a: ListContributorInsightsRequest) =
      eff1(client.listContributorInsights(a))
    override def listContributorInsightsPaginator(a: ListContributorInsightsRequest) =
      primitive1(client.listContributorInsightsPaginator(a))
    override def listExports(a: ListExportsRequest) = eff1(client.listExports(a))
    override def listExportsPaginator(a: ListExportsRequest) =
      primitive1(client.listExportsPaginator(a))
    override def listGlobalTables                             = eff1(client.listGlobalTables)
    override def listGlobalTables(a: ListGlobalTablesRequest) = eff1(client.listGlobalTables(a))
    override def listTables                                   = eff1(client.listTables)
    override def listTables(a: ListTablesRequest)             = eff1(client.listTables(a))
    override def listTablesPaginator                          = primitive1(client.listTablesPaginator)
    override def listTablesPaginator(a: ListTablesRequest) =
      primitive1(client.listTablesPaginator(a))
    override def listTagsOfResource(a: ListTagsOfResourceRequest) =
      eff1(client.listTagsOfResource(a))
    override def putItem(a: PutItemRequest)      = eff1(client.putItem(a))
    override def query(a: QueryRequest)          = eff1(client.query(a))
    override def queryPaginator(a: QueryRequest) = primitive1(client.queryPaginator(a))
    override def restoreTableFromBackup(a: RestoreTableFromBackupRequest) =
      eff1(client.restoreTableFromBackup(a))
    override def restoreTableToPointInTime(a: RestoreTableToPointInTimeRequest) =
      eff1(client.restoreTableToPointInTime(a))
    override def scan(a: ScanRequest)                         = eff1(client.scan(a))
    override def scanPaginator(a: ScanRequest)                = primitive1(client.scanPaginator(a))
    override def serviceName                                  = primitive1(client.serviceName)
    override def tagResource(a: TagResourceRequest)           = eff1(client.tagResource(a))
    override def transactGetItems(a: TransactGetItemsRequest) = eff1(client.transactGetItems(a))
    override def transactWriteItems(a: TransactWriteItemsRequest) =
      eff1(client.transactWriteItems(a))
    override def untagResource(a: UntagResourceRequest) = eff1(client.untagResource(a))
    override def updateContinuousBackups(a: UpdateContinuousBackupsRequest) =
      eff1(client.updateContinuousBackups(a))
    override def updateContributorInsights(a: UpdateContributorInsightsRequest) =
      eff1(client.updateContributorInsights(a))
    override def updateGlobalTable(a: UpdateGlobalTableRequest) = eff1(client.updateGlobalTable(a))
    override def updateGlobalTableSettings(a: UpdateGlobalTableSettingsRequest) =
      eff1(client.updateGlobalTableSettings(a))
    override def updateItem(a: UpdateItemRequest)   = eff1(client.updateItem(a))
    override def updateTable(a: UpdateTableRequest) = eff1(client.updateTable(a))
    override def updateTableReplicaAutoScaling(a: UpdateTableReplicaAutoScalingRequest) =
      eff1(client.updateTableReplicaAutoScaling(a))
    override def updateTimeToLive(a: UpdateTimeToLiveRequest) = eff1(client.updateTimeToLive(a))
    override def waiter                                       = primitive1(client.waiter)

  }

}
