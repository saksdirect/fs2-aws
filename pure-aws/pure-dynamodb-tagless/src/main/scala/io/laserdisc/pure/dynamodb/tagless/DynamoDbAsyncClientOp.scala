package io.laserdisc.pure.dynamodb.tagless

import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.paginators._
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter

trait DynamoDbAsyncClientOp[F[_]] {
  // DynamoDbAsyncClient
  def batchExecuteStatement(a: BatchExecuteStatementRequest): F[BatchExecuteStatementResponse]
  def batchGetItem(a: BatchGetItemRequest): F[BatchGetItemResponse]
  def batchGetItemPaginator(a: BatchGetItemRequest): F[BatchGetItemPublisher]
  def batchWriteItem(a: BatchWriteItemRequest): F[BatchWriteItemResponse]
  def close: F[Unit]
  def createBackup(a: CreateBackupRequest): F[CreateBackupResponse]
  def createGlobalTable(a: CreateGlobalTableRequest): F[CreateGlobalTableResponse]
  def createTable(a: CreateTableRequest): F[CreateTableResponse]
  def deleteBackup(a: DeleteBackupRequest): F[DeleteBackupResponse]
  def deleteItem(a: DeleteItemRequest): F[DeleteItemResponse]
  def deleteTable(a: DeleteTableRequest): F[DeleteTableResponse]
  def describeBackup(a: DescribeBackupRequest): F[DescribeBackupResponse]
  def describeContinuousBackups(
    a: DescribeContinuousBackupsRequest
  ): F[DescribeContinuousBackupsResponse]
  def describeContributorInsights(
    a: DescribeContributorInsightsRequest
  ): F[DescribeContributorInsightsResponse]
  def describeEndpoints: F[DescribeEndpointsResponse]
  def describeEndpoints(a: DescribeEndpointsRequest): F[DescribeEndpointsResponse]
  def describeExport(a: DescribeExportRequest): F[DescribeExportResponse]
  def describeGlobalTable(a: DescribeGlobalTableRequest): F[DescribeGlobalTableResponse]
  def describeGlobalTableSettings(
    a: DescribeGlobalTableSettingsRequest
  ): F[DescribeGlobalTableSettingsResponse]
  def describeKinesisStreamingDestination(
    a: DescribeKinesisStreamingDestinationRequest
  ): F[DescribeKinesisStreamingDestinationResponse]
  def describeLimits: F[DescribeLimitsResponse]
  def describeLimits(a: DescribeLimitsRequest): F[DescribeLimitsResponse]
  def describeTable(a: DescribeTableRequest): F[DescribeTableResponse]
  def describeTableReplicaAutoScaling(
    a: DescribeTableReplicaAutoScalingRequest
  ): F[DescribeTableReplicaAutoScalingResponse]
  def describeTimeToLive(a: DescribeTimeToLiveRequest): F[DescribeTimeToLiveResponse]
  def disableKinesisStreamingDestination(
    a: DisableKinesisStreamingDestinationRequest
  ): F[DisableKinesisStreamingDestinationResponse]
  def enableKinesisStreamingDestination(
    a: EnableKinesisStreamingDestinationRequest
  ): F[EnableKinesisStreamingDestinationResponse]
  def executeStatement(a: ExecuteStatementRequest): F[ExecuteStatementResponse]
  def executeTransaction(a: ExecuteTransactionRequest): F[ExecuteTransactionResponse]
  def exportTableToPointInTime(
    a: ExportTableToPointInTimeRequest
  ): F[ExportTableToPointInTimeResponse]
  def getItem(a: GetItemRequest): F[GetItemResponse]
  def listBackups: F[ListBackupsResponse]
  def listBackups(a: ListBackupsRequest): F[ListBackupsResponse]
  def listContributorInsights(a: ListContributorInsightsRequest): F[ListContributorInsightsResponse]
  def listContributorInsightsPaginator(
    a: ListContributorInsightsRequest
  ): F[ListContributorInsightsPublisher]
  def listExports(a: ListExportsRequest): F[ListExportsResponse]
  def listExportsPaginator(a: ListExportsRequest): F[ListExportsPublisher]
  def listGlobalTables: F[ListGlobalTablesResponse]
  def listGlobalTables(a: ListGlobalTablesRequest): F[ListGlobalTablesResponse]
  def listTables: F[ListTablesResponse]
  def listTables(a: ListTablesRequest): F[ListTablesResponse]
  def listTablesPaginator: F[ListTablesPublisher]
  def listTablesPaginator(a: ListTablesRequest): F[ListTablesPublisher]
  def listTagsOfResource(a: ListTagsOfResourceRequest): F[ListTagsOfResourceResponse]
  def putItem(a: PutItemRequest): F[PutItemResponse]
  def query(a: QueryRequest): F[QueryResponse]
  def queryPaginator(a: QueryRequest): F[QueryPublisher]
  def restoreTableFromBackup(a: RestoreTableFromBackupRequest): F[RestoreTableFromBackupResponse]
  def restoreTableToPointInTime(
    a: RestoreTableToPointInTimeRequest
  ): F[RestoreTableToPointInTimeResponse]
  def scan(a: ScanRequest): F[ScanResponse]
  def scanPaginator(a: ScanRequest): F[ScanPublisher]
  def serviceName: F[String]
  def tagResource(a: TagResourceRequest): F[TagResourceResponse]
  def transactGetItems(a: TransactGetItemsRequest): F[TransactGetItemsResponse]
  def transactWriteItems(a: TransactWriteItemsRequest): F[TransactWriteItemsResponse]
  def untagResource(a: UntagResourceRequest): F[UntagResourceResponse]
  def updateContinuousBackups(a: UpdateContinuousBackupsRequest): F[UpdateContinuousBackupsResponse]
  def updateContributorInsights(
    a: UpdateContributorInsightsRequest
  ): F[UpdateContributorInsightsResponse]
  def updateGlobalTable(a: UpdateGlobalTableRequest): F[UpdateGlobalTableResponse]
  def updateGlobalTableSettings(
    a: UpdateGlobalTableSettingsRequest
  ): F[UpdateGlobalTableSettingsResponse]
  def updateItem(a: UpdateItemRequest): F[UpdateItemResponse]
  def updateTable(a: UpdateTableRequest): F[UpdateTableResponse]
  def updateTableReplicaAutoScaling(
    a: UpdateTableReplicaAutoScalingRequest
  ): F[UpdateTableReplicaAutoScalingResponse]
  def updateTimeToLive(a: UpdateTimeToLiveRequest): F[UpdateTimeToLiveResponse]
  def waiter: F[DynamoDbAsyncWaiter]

}
