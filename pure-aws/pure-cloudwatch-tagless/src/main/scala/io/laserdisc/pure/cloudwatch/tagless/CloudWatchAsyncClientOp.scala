package io.laserdisc.pure.cloudwatch.tagless

import software.amazon.awssdk.services.cloudwatch.model.{
  DeleteAlarmsRequest,
  DeleteAnomalyDetectorRequest,
  DeleteDashboardsRequest,
  DeleteInsightRulesRequest,
  DeleteMetricStreamRequest,
  DescribeAlarmHistoryRequest,
  DescribeAlarmsForMetricRequest,
  DescribeAlarmsRequest,
  DescribeAnomalyDetectorsRequest,
  DescribeInsightRulesRequest,
  DisableAlarmActionsRequest,
  DisableInsightRulesRequest,
  EnableAlarmActionsRequest,
  EnableInsightRulesRequest,
  GetDashboardRequest,
  GetInsightRuleReportRequest,
  GetMetricDataRequest,
  GetMetricStatisticsRequest,
  GetMetricStreamRequest,
  GetMetricWidgetImageRequest,
  ListDashboardsRequest,
  ListMetricStreamsRequest,
  ListMetricsRequest,
  ListTagsForResourceRequest,
  PutAnomalyDetectorRequest,
  PutCompositeAlarmRequest,
  PutDashboardRequest,
  PutInsightRuleRequest,
  PutMetricAlarmRequest,
  PutMetricDataRequest,
  PutMetricStreamRequest,
  SetAlarmStateRequest,
  StartMetricStreamsRequest,
  StopMetricStreamsRequest,
  TagResourceRequest,
  UntagResourceRequest,
  _
}
import software.amazon.awssdk.services.cloudwatch.paginators.{
  DescribeAlarmHistoryPublisher,
  DescribeAlarmsPublisher,
  DescribeInsightRulesPublisher,
  GetMetricDataPublisher,
  ListDashboardsPublisher,
  ListMetricStreamsPublisher,
  ListMetricsPublisher
}
import software.amazon.awssdk.services.cloudwatch.waiters.CloudWatchAsyncWaiter

trait CloudWatchAsyncClientOp[F[_]] {
  // CloudWatchAsyncClient
  def close: F[Unit]
  def deleteAlarms(a: DeleteAlarmsRequest): F[DeleteAlarmsResponse]
  def deleteAnomalyDetector(a: DeleteAnomalyDetectorRequest): F[DeleteAnomalyDetectorResponse]
  def deleteDashboards(a: DeleteDashboardsRequest): F[DeleteDashboardsResponse]
  def deleteInsightRules(a: DeleteInsightRulesRequest): F[DeleteInsightRulesResponse]
  def deleteMetricStream(a: DeleteMetricStreamRequest): F[DeleteMetricStreamResponse]
  def describeAlarmHistory: F[DescribeAlarmHistoryResponse]
  def describeAlarmHistory(a: DescribeAlarmHistoryRequest): F[DescribeAlarmHistoryResponse]
  def describeAlarmHistoryPaginator: F[DescribeAlarmHistoryPublisher]
  def describeAlarmHistoryPaginator(
    a: DescribeAlarmHistoryRequest
  ): F[DescribeAlarmHistoryPublisher]
  def describeAlarms: F[DescribeAlarmsResponse]
  def describeAlarms(a: DescribeAlarmsRequest): F[DescribeAlarmsResponse]
  def describeAlarmsForMetric(a: DescribeAlarmsForMetricRequest): F[DescribeAlarmsForMetricResponse]
  def describeAlarmsPaginator: F[DescribeAlarmsPublisher]
  def describeAlarmsPaginator(a: DescribeAlarmsRequest): F[DescribeAlarmsPublisher]
  def describeAnomalyDetectors(
    a: DescribeAnomalyDetectorsRequest
  ): F[DescribeAnomalyDetectorsResponse]
  def describeInsightRules(a: DescribeInsightRulesRequest): F[DescribeInsightRulesResponse]
  def describeInsightRulesPaginator(
    a: DescribeInsightRulesRequest
  ): F[DescribeInsightRulesPublisher]
  def disableAlarmActions(a: DisableAlarmActionsRequest): F[DisableAlarmActionsResponse]
  def disableInsightRules(a: DisableInsightRulesRequest): F[DisableInsightRulesResponse]
  def enableAlarmActions(a: EnableAlarmActionsRequest): F[EnableAlarmActionsResponse]
  def enableInsightRules(a: EnableInsightRulesRequest): F[EnableInsightRulesResponse]
  def getDashboard(a: GetDashboardRequest): F[GetDashboardResponse]
  def getInsightRuleReport(a: GetInsightRuleReportRequest): F[GetInsightRuleReportResponse]
  def getMetricData(a: GetMetricDataRequest): F[GetMetricDataResponse]
  def getMetricDataPaginator(a: GetMetricDataRequest): F[GetMetricDataPublisher]
  def getMetricStatistics(a: GetMetricStatisticsRequest): F[GetMetricStatisticsResponse]
  def getMetricStream(a: GetMetricStreamRequest): F[GetMetricStreamResponse]
  def getMetricWidgetImage(a: GetMetricWidgetImageRequest): F[GetMetricWidgetImageResponse]
  def listDashboards: F[ListDashboardsResponse]
  def listDashboards(a: ListDashboardsRequest): F[ListDashboardsResponse]
  def listDashboardsPaginator: F[ListDashboardsPublisher]
  def listDashboardsPaginator(a: ListDashboardsRequest): F[ListDashboardsPublisher]
  def listMetricStreams(a: ListMetricStreamsRequest): F[ListMetricStreamsResponse]
  def listMetricStreamsPaginator(a: ListMetricStreamsRequest): F[ListMetricStreamsPublisher]
  def listMetrics: F[ListMetricsResponse]
  def listMetrics(a: ListMetricsRequest): F[ListMetricsResponse]
  def listMetricsPaginator: F[ListMetricsPublisher]
  def listMetricsPaginator(a: ListMetricsRequest): F[ListMetricsPublisher]
  def listTagsForResource(a: ListTagsForResourceRequest): F[ListTagsForResourceResponse]
  def putAnomalyDetector(a: PutAnomalyDetectorRequest): F[PutAnomalyDetectorResponse]
  def putCompositeAlarm(a: PutCompositeAlarmRequest): F[PutCompositeAlarmResponse]
  def putDashboard(a: PutDashboardRequest): F[PutDashboardResponse]
  def putInsightRule(a: PutInsightRuleRequest): F[PutInsightRuleResponse]
  def putMetricAlarm(a: PutMetricAlarmRequest): F[PutMetricAlarmResponse]
  def putMetricData(a: PutMetricDataRequest): F[PutMetricDataResponse]
  def putMetricStream(a: PutMetricStreamRequest): F[PutMetricStreamResponse]
  def serviceName: F[String]
  def setAlarmState(a: SetAlarmStateRequest): F[SetAlarmStateResponse]
  def startMetricStreams(a: StartMetricStreamsRequest): F[StartMetricStreamsResponse]
  def stopMetricStreams(a: StopMetricStreamsRequest): F[StopMetricStreamsResponse]
  def tagResource(a: TagResourceRequest): F[TagResourceResponse]
  def untagResource(a: UntagResourceRequest): F[UntagResourceResponse]
  def waiter: F[CloudWatchAsyncWaiter]

}
