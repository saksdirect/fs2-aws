package io.laserdisc.pure.cloudwatch.tagless

// Library imports
import cats.data.Kleisli
import cats.effect.{ Async, Blocker, ContextShift, Resource }
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder

import java.util.concurrent.CompletionException

// Types referenced
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
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
  UntagResourceRequest
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

  lazy val CloudWatchAsyncClientInterpreter: CloudWatchAsyncClientInterpreter =
    new CloudWatchAsyncClientInterpreter {}
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
  trait CloudWatchAsyncClientInterpreter
      extends CloudWatchAsyncClientOp[Kleisli[M, CloudWatchAsyncClient, *]] {

    // domain-specific operations are implemented in terms of `primitive`
    override def close                                = primitive(_.close)
    override def deleteAlarms(a: DeleteAlarmsRequest) = eff(_.deleteAlarms(a))
    override def deleteAnomalyDetector(a: DeleteAnomalyDetectorRequest) =
      eff(_.deleteAnomalyDetector(a))
    override def deleteDashboards(a: DeleteDashboardsRequest)     = eff(_.deleteDashboards(a))
    override def deleteInsightRules(a: DeleteInsightRulesRequest) = eff(_.deleteInsightRules(a))
    override def deleteMetricStream(a: DeleteMetricStreamRequest) = eff(_.deleteMetricStream(a))
    override def describeAlarmHistory                             = eff(_.describeAlarmHistory)
    override def describeAlarmHistory(a: DescribeAlarmHistoryRequest) =
      eff(_.describeAlarmHistory(a))
    override def describeAlarmHistoryPaginator = primitive(_.describeAlarmHistoryPaginator)
    override def describeAlarmHistoryPaginator(a: DescribeAlarmHistoryRequest) =
      primitive(_.describeAlarmHistoryPaginator(a))
    override def describeAlarms                           = eff(_.describeAlarms)
    override def describeAlarms(a: DescribeAlarmsRequest) = eff(_.describeAlarms(a))
    override def describeAlarmsForMetric(a: DescribeAlarmsForMetricRequest) =
      eff(_.describeAlarmsForMetric(a))
    override def describeAlarmsPaginator = primitive(_.describeAlarmsPaginator)
    override def describeAlarmsPaginator(a: DescribeAlarmsRequest) =
      primitive(_.describeAlarmsPaginator(a))
    override def describeAnomalyDetectors(a: DescribeAnomalyDetectorsRequest) =
      eff(_.describeAnomalyDetectors(a))
    override def describeInsightRules(a: DescribeInsightRulesRequest) =
      eff(_.describeInsightRules(a))
    override def describeInsightRulesPaginator(a: DescribeInsightRulesRequest) =
      primitive(_.describeInsightRulesPaginator(a))
    override def disableAlarmActions(a: DisableAlarmActionsRequest) = eff(_.disableAlarmActions(a))
    override def disableInsightRules(a: DisableInsightRulesRequest) = eff(_.disableInsightRules(a))
    override def enableAlarmActions(a: EnableAlarmActionsRequest)   = eff(_.enableAlarmActions(a))
    override def enableInsightRules(a: EnableInsightRulesRequest)   = eff(_.enableInsightRules(a))
    override def getDashboard(a: GetDashboardRequest)               = eff(_.getDashboard(a))
    override def getInsightRuleReport(a: GetInsightRuleReportRequest) =
      eff(_.getInsightRuleReport(a))
    override def getMetricData(a: GetMetricDataRequest) = eff(_.getMetricData(a))
    override def getMetricDataPaginator(a: GetMetricDataRequest) =
      primitive(_.getMetricDataPaginator(a))
    override def getMetricStatistics(a: GetMetricStatisticsRequest) = eff(_.getMetricStatistics(a))
    override def getMetricStream(a: GetMetricStreamRequest)         = eff(_.getMetricStream(a))
    override def getMetricWidgetImage(a: GetMetricWidgetImageRequest) =
      eff(_.getMetricWidgetImage(a))
    override def listDashboards                           = eff(_.listDashboards)
    override def listDashboards(a: ListDashboardsRequest) = eff(_.listDashboards(a))
    override def listDashboardsPaginator                  = primitive(_.listDashboardsPaginator)
    override def listDashboardsPaginator(a: ListDashboardsRequest) =
      primitive(_.listDashboardsPaginator(a))
    override def listMetricStreams(a: ListMetricStreamsRequest) = eff(_.listMetricStreams(a))
    override def listMetricStreamsPaginator(a: ListMetricStreamsRequest) =
      primitive(_.listMetricStreamsPaginator(a))
    override def listMetrics                                        = eff(_.listMetrics)
    override def listMetrics(a: ListMetricsRequest)                 = eff(_.listMetrics(a))
    override def listMetricsPaginator                               = primitive(_.listMetricsPaginator)
    override def listMetricsPaginator(a: ListMetricsRequest)        = primitive(_.listMetricsPaginator(a))
    override def listTagsForResource(a: ListTagsForResourceRequest) = eff(_.listTagsForResource(a))
    override def putAnomalyDetector(a: PutAnomalyDetectorRequest)   = eff(_.putAnomalyDetector(a))
    override def putCompositeAlarm(a: PutCompositeAlarmRequest)     = eff(_.putCompositeAlarm(a))
    override def putDashboard(a: PutDashboardRequest)               = eff(_.putDashboard(a))
    override def putInsightRule(a: PutInsightRuleRequest)           = eff(_.putInsightRule(a))
    override def putMetricAlarm(a: PutMetricAlarmRequest)           = eff(_.putMetricAlarm(a))
    override def putMetricData(a: PutMetricDataRequest)             = eff(_.putMetricData(a))
    override def putMetricStream(a: PutMetricStreamRequest)         = eff(_.putMetricStream(a))
    override def serviceName                                        = primitive(_.serviceName)
    override def setAlarmState(a: SetAlarmStateRequest)             = eff(_.setAlarmState(a))
    override def startMetricStreams(a: StartMetricStreamsRequest)   = eff(_.startMetricStreams(a))
    override def stopMetricStreams(a: StopMetricStreamsRequest)     = eff(_.stopMetricStreams(a))
    override def tagResource(a: TagResourceRequest)                 = eff(_.tagResource(a))
    override def untagResource(a: UntagResourceRequest)             = eff(_.untagResource(a))
    override def waiter                                             = primitive(_.waiter)
    def lens[E](f: E => CloudWatchAsyncClient): CloudWatchAsyncClientOp[Kleisli[M, E, *]] =
      new CloudWatchAsyncClientOp[Kleisli[M, E, *]] {
        override def close                                = Kleisli(e => primitive1(f(e).close))
        override def deleteAlarms(a: DeleteAlarmsRequest) = Kleisli(e => eff1(f(e).deleteAlarms(a)))
        override def deleteAnomalyDetector(a: DeleteAnomalyDetectorRequest) =
          Kleisli(e => eff1(f(e).deleteAnomalyDetector(a)))
        override def deleteDashboards(a: DeleteDashboardsRequest) =
          Kleisli(e => eff1(f(e).deleteDashboards(a)))
        override def deleteInsightRules(a: DeleteInsightRulesRequest) =
          Kleisli(e => eff1(f(e).deleteInsightRules(a)))
        override def deleteMetricStream(a: DeleteMetricStreamRequest) =
          Kleisli(e => eff1(f(e).deleteMetricStream(a)))
        override def describeAlarmHistory = Kleisli(e => eff1(f(e).describeAlarmHistory))
        override def describeAlarmHistory(a: DescribeAlarmHistoryRequest) =
          Kleisli(e => eff1(f(e).describeAlarmHistory(a)))
        override def describeAlarmHistoryPaginator =
          Kleisli(e => primitive1(f(e).describeAlarmHistoryPaginator))
        override def describeAlarmHistoryPaginator(a: DescribeAlarmHistoryRequest) =
          Kleisli(e => primitive1(f(e).describeAlarmHistoryPaginator(a)))
        override def describeAlarms = Kleisli(e => eff1(f(e).describeAlarms))
        override def describeAlarms(a: DescribeAlarmsRequest) =
          Kleisli(e => eff1(f(e).describeAlarms(a)))
        override def describeAlarmsForMetric(a: DescribeAlarmsForMetricRequest) =
          Kleisli(e => eff1(f(e).describeAlarmsForMetric(a)))
        override def describeAlarmsPaginator =
          Kleisli(e => primitive1(f(e).describeAlarmsPaginator))
        override def describeAlarmsPaginator(a: DescribeAlarmsRequest) =
          Kleisli(e => primitive1(f(e).describeAlarmsPaginator(a)))
        override def describeAnomalyDetectors(a: DescribeAnomalyDetectorsRequest) =
          Kleisli(e => eff1(f(e).describeAnomalyDetectors(a)))
        override def describeInsightRules(a: DescribeInsightRulesRequest) =
          Kleisli(e => eff1(f(e).describeInsightRules(a)))
        override def describeInsightRulesPaginator(a: DescribeInsightRulesRequest) =
          Kleisli(e => primitive1(f(e).describeInsightRulesPaginator(a)))
        override def disableAlarmActions(a: DisableAlarmActionsRequest) =
          Kleisli(e => eff1(f(e).disableAlarmActions(a)))
        override def disableInsightRules(a: DisableInsightRulesRequest) =
          Kleisli(e => eff1(f(e).disableInsightRules(a)))
        override def enableAlarmActions(a: EnableAlarmActionsRequest) =
          Kleisli(e => eff1(f(e).enableAlarmActions(a)))
        override def enableInsightRules(a: EnableInsightRulesRequest) =
          Kleisli(e => eff1(f(e).enableInsightRules(a)))
        override def getDashboard(a: GetDashboardRequest) = Kleisli(e => eff1(f(e).getDashboard(a)))
        override def getInsightRuleReport(a: GetInsightRuleReportRequest) =
          Kleisli(e => eff1(f(e).getInsightRuleReport(a)))
        override def getMetricData(a: GetMetricDataRequest) =
          Kleisli(e => eff1(f(e).getMetricData(a)))
        override def getMetricDataPaginator(a: GetMetricDataRequest) =
          Kleisli(e => primitive1(f(e).getMetricDataPaginator(a)))
        override def getMetricStatistics(a: GetMetricStatisticsRequest) =
          Kleisli(e => eff1(f(e).getMetricStatistics(a)))
        override def getMetricStream(a: GetMetricStreamRequest) =
          Kleisli(e => eff1(f(e).getMetricStream(a)))
        override def getMetricWidgetImage(a: GetMetricWidgetImageRequest) =
          Kleisli(e => eff1(f(e).getMetricWidgetImage(a)))
        override def listDashboards = Kleisli(e => eff1(f(e).listDashboards))
        override def listDashboards(a: ListDashboardsRequest) =
          Kleisli(e => eff1(f(e).listDashboards(a)))
        override def listDashboardsPaginator =
          Kleisli(e => primitive1(f(e).listDashboardsPaginator))
        override def listDashboardsPaginator(a: ListDashboardsRequest) =
          Kleisli(e => primitive1(f(e).listDashboardsPaginator(a)))
        override def listMetricStreams(a: ListMetricStreamsRequest) =
          Kleisli(e => eff1(f(e).listMetricStreams(a)))
        override def listMetricStreamsPaginator(a: ListMetricStreamsRequest) =
          Kleisli(e => primitive1(f(e).listMetricStreamsPaginator(a)))
        override def listMetrics                        = Kleisli(e => eff1(f(e).listMetrics))
        override def listMetrics(a: ListMetricsRequest) = Kleisli(e => eff1(f(e).listMetrics(a)))
        override def listMetricsPaginator               = Kleisli(e => primitive1(f(e).listMetricsPaginator))
        override def listMetricsPaginator(a: ListMetricsRequest) =
          Kleisli(e => primitive1(f(e).listMetricsPaginator(a)))
        override def listTagsForResource(a: ListTagsForResourceRequest) =
          Kleisli(e => eff1(f(e).listTagsForResource(a)))
        override def putAnomalyDetector(a: PutAnomalyDetectorRequest) =
          Kleisli(e => eff1(f(e).putAnomalyDetector(a)))
        override def putCompositeAlarm(a: PutCompositeAlarmRequest) =
          Kleisli(e => eff1(f(e).putCompositeAlarm(a)))
        override def putDashboard(a: PutDashboardRequest) = Kleisli(e => eff1(f(e).putDashboard(a)))
        override def putInsightRule(a: PutInsightRuleRequest) =
          Kleisli(e => eff1(f(e).putInsightRule(a)))
        override def putMetricAlarm(a: PutMetricAlarmRequest) =
          Kleisli(e => eff1(f(e).putMetricAlarm(a)))
        override def putMetricData(a: PutMetricDataRequest) =
          Kleisli(e => eff1(f(e).putMetricData(a)))
        override def putMetricStream(a: PutMetricStreamRequest) =
          Kleisli(e => eff1(f(e).putMetricStream(a)))
        override def serviceName = Kleisli(e => primitive1(f(e).serviceName))
        override def setAlarmState(a: SetAlarmStateRequest) =
          Kleisli(e => eff1(f(e).setAlarmState(a)))
        override def startMetricStreams(a: StartMetricStreamsRequest) =
          Kleisli(e => eff1(f(e).startMetricStreams(a)))
        override def stopMetricStreams(a: StopMetricStreamsRequest) =
          Kleisli(e => eff1(f(e).stopMetricStreams(a)))
        override def tagResource(a: TagResourceRequest) = Kleisli(e => eff1(f(e).tagResource(a)))
        override def untagResource(a: UntagResourceRequest) =
          Kleisli(e => eff1(f(e).untagResource(a)))
        override def waiter = Kleisli(e => primitive1(f(e).waiter))
      }
  }

  def CloudWatchAsyncClientResource(
    builder: CloudWatchAsyncClientBuilder
  ): Resource[M, CloudWatchAsyncClient] = Resource.fromAutoCloseable(asyncM.delay(builder.build()))
  def CloudWatchAsyncClientOpResource(builder: CloudWatchAsyncClientBuilder) =
    CloudWatchAsyncClientResource(builder).map(create)
  def create(client: CloudWatchAsyncClient): CloudWatchAsyncClientOp[M] =
    new CloudWatchAsyncClientOp[M] {

      // domain-specific operations are implemented in terms of `primitive`
      override def close                                = primitive1(client.close)
      override def deleteAlarms(a: DeleteAlarmsRequest) = eff1(client.deleteAlarms(a))
      override def deleteAnomalyDetector(a: DeleteAnomalyDetectorRequest) =
        eff1(client.deleteAnomalyDetector(a))
      override def deleteDashboards(a: DeleteDashboardsRequest) = eff1(client.deleteDashboards(a))
      override def deleteInsightRules(a: DeleteInsightRulesRequest) =
        eff1(client.deleteInsightRules(a))
      override def deleteMetricStream(a: DeleteMetricStreamRequest) =
        eff1(client.deleteMetricStream(a))
      override def describeAlarmHistory = eff1(client.describeAlarmHistory)
      override def describeAlarmHistory(a: DescribeAlarmHistoryRequest) =
        eff1(client.describeAlarmHistory(a))
      override def describeAlarmHistoryPaginator = primitive1(client.describeAlarmHistoryPaginator)
      override def describeAlarmHistoryPaginator(a: DescribeAlarmHistoryRequest) =
        primitive1(client.describeAlarmHistoryPaginator(a))
      override def describeAlarms                           = eff1(client.describeAlarms)
      override def describeAlarms(a: DescribeAlarmsRequest) = eff1(client.describeAlarms(a))
      override def describeAlarmsForMetric(a: DescribeAlarmsForMetricRequest) =
        eff1(client.describeAlarmsForMetric(a))
      override def describeAlarmsPaginator = primitive1(client.describeAlarmsPaginator)
      override def describeAlarmsPaginator(a: DescribeAlarmsRequest) =
        primitive1(client.describeAlarmsPaginator(a))
      override def describeAnomalyDetectors(a: DescribeAnomalyDetectorsRequest) =
        eff1(client.describeAnomalyDetectors(a))
      override def describeInsightRules(a: DescribeInsightRulesRequest) =
        eff1(client.describeInsightRules(a))
      override def describeInsightRulesPaginator(a: DescribeInsightRulesRequest) =
        primitive1(client.describeInsightRulesPaginator(a))
      override def disableAlarmActions(a: DisableAlarmActionsRequest) =
        eff1(client.disableAlarmActions(a))
      override def disableInsightRules(a: DisableInsightRulesRequest) =
        eff1(client.disableInsightRules(a))
      override def enableAlarmActions(a: EnableAlarmActionsRequest) =
        eff1(client.enableAlarmActions(a))
      override def enableInsightRules(a: EnableInsightRulesRequest) =
        eff1(client.enableInsightRules(a))
      override def getDashboard(a: GetDashboardRequest) = eff1(client.getDashboard(a))
      override def getInsightRuleReport(a: GetInsightRuleReportRequest) =
        eff1(client.getInsightRuleReport(a))
      override def getMetricData(a: GetMetricDataRequest) = eff1(client.getMetricData(a))
      override def getMetricDataPaginator(a: GetMetricDataRequest) =
        primitive1(client.getMetricDataPaginator(a))
      override def getMetricStatistics(a: GetMetricStatisticsRequest) =
        eff1(client.getMetricStatistics(a))
      override def getMetricStream(a: GetMetricStreamRequest) = eff1(client.getMetricStream(a))
      override def getMetricWidgetImage(a: GetMetricWidgetImageRequest) =
        eff1(client.getMetricWidgetImage(a))
      override def listDashboards                           = eff1(client.listDashboards)
      override def listDashboards(a: ListDashboardsRequest) = eff1(client.listDashboards(a))
      override def listDashboardsPaginator                  = primitive1(client.listDashboardsPaginator)
      override def listDashboardsPaginator(a: ListDashboardsRequest) =
        primitive1(client.listDashboardsPaginator(a))
      override def listMetricStreams(a: ListMetricStreamsRequest) =
        eff1(client.listMetricStreams(a))
      override def listMetricStreamsPaginator(a: ListMetricStreamsRequest) =
        primitive1(client.listMetricStreamsPaginator(a))
      override def listMetrics                        = eff1(client.listMetrics)
      override def listMetrics(a: ListMetricsRequest) = eff1(client.listMetrics(a))
      override def listMetricsPaginator               = primitive1(client.listMetricsPaginator)
      override def listMetricsPaginator(a: ListMetricsRequest) =
        primitive1(client.listMetricsPaginator(a))
      override def listTagsForResource(a: ListTagsForResourceRequest) =
        eff1(client.listTagsForResource(a))
      override def putAnomalyDetector(a: PutAnomalyDetectorRequest) =
        eff1(client.putAnomalyDetector(a))
      override def putCompositeAlarm(a: PutCompositeAlarmRequest) =
        eff1(client.putCompositeAlarm(a))
      override def putDashboard(a: PutDashboardRequest)       = eff1(client.putDashboard(a))
      override def putInsightRule(a: PutInsightRuleRequest)   = eff1(client.putInsightRule(a))
      override def putMetricAlarm(a: PutMetricAlarmRequest)   = eff1(client.putMetricAlarm(a))
      override def putMetricData(a: PutMetricDataRequest)     = eff1(client.putMetricData(a))
      override def putMetricStream(a: PutMetricStreamRequest) = eff1(client.putMetricStream(a))
      override def serviceName                                = primitive1(client.serviceName)
      override def setAlarmState(a: SetAlarmStateRequest)     = eff1(client.setAlarmState(a))
      override def startMetricStreams(a: StartMetricStreamsRequest) =
        eff1(client.startMetricStreams(a))
      override def stopMetricStreams(a: StopMetricStreamsRequest) =
        eff1(client.stopMetricStreams(a))
      override def tagResource(a: TagResourceRequest)     = eff1(client.tagResource(a))
      override def untagResource(a: UntagResourceRequest) = eff1(client.untagResource(a))
      override def waiter                                 = primitive1(client.waiter)

    }

}
