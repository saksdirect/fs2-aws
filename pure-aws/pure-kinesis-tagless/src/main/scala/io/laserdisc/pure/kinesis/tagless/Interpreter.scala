package io.laserdisc.pure.kinesis.tagless

// Library imports
import cats.data.Kleisli
import cats.effect.{ Async, Blocker, ContextShift, Resource }
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder

import java.util.concurrent.CompletionException

// Types referenced
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model._

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

  lazy val KinesisAsyncClientInterpreter: KinesisAsyncClientInterpreter =
    new KinesisAsyncClientInterpreter {}
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
  trait KinesisAsyncClientInterpreter
      extends KinesisAsyncClientOp[Kleisli[M, KinesisAsyncClient, *]] {

    // domain-specific operations are implemented in terms of `primitive`
    override def addTagsToStream(a: AddTagsToStreamRequest) = eff(_.addTagsToStream(a))
    override def close                                      = primitive(_.close)
    override def createStream(a: CreateStreamRequest)       = eff(_.createStream(a))
    override def decreaseStreamRetentionPeriod(a: DecreaseStreamRetentionPeriodRequest) =
      eff(_.decreaseStreamRetentionPeriod(a))
    override def deleteStream(a: DeleteStreamRequest) = eff(_.deleteStream(a))
    override def deregisterStreamConsumer(a: DeregisterStreamConsumerRequest) =
      eff(_.deregisterStreamConsumer(a))
    override def describeLimits                           = eff(_.describeLimits)
    override def describeLimits(a: DescribeLimitsRequest) = eff(_.describeLimits(a))
    override def describeStream(a: DescribeStreamRequest) = eff(_.describeStream(a))
    override def describeStreamConsumer(a: DescribeStreamConsumerRequest) =
      eff(_.describeStreamConsumer(a))
    override def describeStreamSummary(a: DescribeStreamSummaryRequest) =
      eff(_.describeStreamSummary(a))
    override def disableEnhancedMonitoring(a: DisableEnhancedMonitoringRequest) =
      eff(_.disableEnhancedMonitoring(a))
    override def enableEnhancedMonitoring(a: EnableEnhancedMonitoringRequest) =
      eff(_.enableEnhancedMonitoring(a))
    override def getRecords(a: GetRecordsRequest)             = eff(_.getRecords(a))
    override def getShardIterator(a: GetShardIteratorRequest) = eff(_.getShardIterator(a))
    override def increaseStreamRetentionPeriod(a: IncreaseStreamRetentionPeriodRequest) =
      eff(_.increaseStreamRetentionPeriod(a))
    override def listShards(a: ListShardsRequest)                   = eff(_.listShards(a))
    override def listStreamConsumers(a: ListStreamConsumersRequest) = eff(_.listStreamConsumers(a))
    override def listStreamConsumersPaginator(a: ListStreamConsumersRequest) =
      primitive(_.listStreamConsumersPaginator(a))
    override def listStreams                                    = eff(_.listStreams)
    override def listStreams(a: ListStreamsRequest)             = eff(_.listStreams(a))
    override def listTagsForStream(a: ListTagsForStreamRequest) = eff(_.listTagsForStream(a))
    override def mergeShards(a: MergeShardsRequest)             = eff(_.mergeShards(a))
    override def putRecord(a: PutRecordRequest)                 = eff(_.putRecord(a))
    override def putRecords(a: PutRecordsRequest)               = eff(_.putRecords(a))
    override def registerStreamConsumer(a: RegisterStreamConsumerRequest) =
      eff(_.registerStreamConsumer(a))
    override def removeTagsFromStream(a: RemoveTagsFromStreamRequest) =
      eff(_.removeTagsFromStream(a))
    override def serviceName                      = primitive(_.serviceName)
    override def splitShard(a: SplitShardRequest) = eff(_.splitShard(a))
    override def startStreamEncryption(a: StartStreamEncryptionRequest) =
      eff(_.startStreamEncryption(a))
    override def stopStreamEncryption(a: StopStreamEncryptionRequest) =
      eff(_.stopStreamEncryption(a))
    override def subscribeToShard(a: SubscribeToShardRequest, b: SubscribeToShardResponseHandler) =
      eff(_.subscribeToShard(a, b))
    override def updateShardCount(a: UpdateShardCountRequest) = eff(_.updateShardCount(a))
    override def waiter                                       = primitive(_.waiter)
    def lens[E](f: E => KinesisAsyncClient): KinesisAsyncClientOp[Kleisli[M, E, *]] =
      new KinesisAsyncClientOp[Kleisli[M, E, *]] {
        override def addTagsToStream(a: AddTagsToStreamRequest) =
          Kleisli(e => eff1(f(e).addTagsToStream(a)))
        override def close                                = Kleisli(e => primitive1(f(e).close))
        override def createStream(a: CreateStreamRequest) = Kleisli(e => eff1(f(e).createStream(a)))
        override def decreaseStreamRetentionPeriod(a: DecreaseStreamRetentionPeriodRequest) =
          Kleisli(e => eff1(f(e).decreaseStreamRetentionPeriod(a)))
        override def deleteStream(a: DeleteStreamRequest) = Kleisli(e => eff1(f(e).deleteStream(a)))
        override def deregisterStreamConsumer(a: DeregisterStreamConsumerRequest) =
          Kleisli(e => eff1(f(e).deregisterStreamConsumer(a)))
        override def describeLimits = Kleisli(e => eff1(f(e).describeLimits))
        override def describeLimits(a: DescribeLimitsRequest) =
          Kleisli(e => eff1(f(e).describeLimits(a)))
        override def describeStream(a: DescribeStreamRequest) =
          Kleisli(e => eff1(f(e).describeStream(a)))
        override def describeStreamConsumer(a: DescribeStreamConsumerRequest) =
          Kleisli(e => eff1(f(e).describeStreamConsumer(a)))
        override def describeStreamSummary(a: DescribeStreamSummaryRequest) =
          Kleisli(e => eff1(f(e).describeStreamSummary(a)))
        override def disableEnhancedMonitoring(a: DisableEnhancedMonitoringRequest) =
          Kleisli(e => eff1(f(e).disableEnhancedMonitoring(a)))
        override def enableEnhancedMonitoring(a: EnableEnhancedMonitoringRequest) =
          Kleisli(e => eff1(f(e).enableEnhancedMonitoring(a)))
        override def getRecords(a: GetRecordsRequest) = Kleisli(e => eff1(f(e).getRecords(a)))
        override def getShardIterator(a: GetShardIteratorRequest) =
          Kleisli(e => eff1(f(e).getShardIterator(a)))
        override def increaseStreamRetentionPeriod(a: IncreaseStreamRetentionPeriodRequest) =
          Kleisli(e => eff1(f(e).increaseStreamRetentionPeriod(a)))
        override def listShards(a: ListShardsRequest) = Kleisli(e => eff1(f(e).listShards(a)))
        override def listStreamConsumers(a: ListStreamConsumersRequest) =
          Kleisli(e => eff1(f(e).listStreamConsumers(a)))
        override def listStreamConsumersPaginator(a: ListStreamConsumersRequest) =
          Kleisli(e => primitive1(f(e).listStreamConsumersPaginator(a)))
        override def listStreams                        = Kleisli(e => eff1(f(e).listStreams))
        override def listStreams(a: ListStreamsRequest) = Kleisli(e => eff1(f(e).listStreams(a)))
        override def listTagsForStream(a: ListTagsForStreamRequest) =
          Kleisli(e => eff1(f(e).listTagsForStream(a)))
        override def mergeShards(a: MergeShardsRequest) = Kleisli(e => eff1(f(e).mergeShards(a)))
        override def putRecord(a: PutRecordRequest)     = Kleisli(e => eff1(f(e).putRecord(a)))
        override def putRecords(a: PutRecordsRequest)   = Kleisli(e => eff1(f(e).putRecords(a)))
        override def registerStreamConsumer(a: RegisterStreamConsumerRequest) =
          Kleisli(e => eff1(f(e).registerStreamConsumer(a)))
        override def removeTagsFromStream(a: RemoveTagsFromStreamRequest) =
          Kleisli(e => eff1(f(e).removeTagsFromStream(a)))
        override def serviceName                      = Kleisli(e => primitive1(f(e).serviceName))
        override def splitShard(a: SplitShardRequest) = Kleisli(e => eff1(f(e).splitShard(a)))
        override def startStreamEncryption(a: StartStreamEncryptionRequest) =
          Kleisli(e => eff1(f(e).startStreamEncryption(a)))
        override def stopStreamEncryption(a: StopStreamEncryptionRequest) =
          Kleisli(e => eff1(f(e).stopStreamEncryption(a)))
        override def subscribeToShard(
          a: SubscribeToShardRequest,
          b: SubscribeToShardResponseHandler
        ) = Kleisli(e => eff1(f(e).subscribeToShard(a, b)))
        override def updateShardCount(a: UpdateShardCountRequest) =
          Kleisli(e => eff1(f(e).updateShardCount(a)))
        override def waiter = Kleisli(e => primitive1(f(e).waiter))
      }
  }

  def KinesisAsyncClientResource(
    builder: KinesisAsyncClientBuilder
  ): Resource[M, KinesisAsyncClient] = Resource.fromAutoCloseable(asyncM.delay(builder.build()))
  def KinesisAsyncClientOpResource(builder: KinesisAsyncClientBuilder) =
    KinesisAsyncClientResource(builder).map(create)
  def create(client: KinesisAsyncClient): KinesisAsyncClientOp[M] = new KinesisAsyncClientOp[M] {

    // domain-specific operations are implemented in terms of `primitive`
    override def addTagsToStream(a: AddTagsToStreamRequest) = eff1(client.addTagsToStream(a))
    override def close                                      = primitive1(client.close)
    override def createStream(a: CreateStreamRequest)       = eff1(client.createStream(a))
    override def decreaseStreamRetentionPeriod(a: DecreaseStreamRetentionPeriodRequest) =
      eff1(client.decreaseStreamRetentionPeriod(a))
    override def deleteStream(a: DeleteStreamRequest) = eff1(client.deleteStream(a))
    override def deregisterStreamConsumer(a: DeregisterStreamConsumerRequest) =
      eff1(client.deregisterStreamConsumer(a))
    override def describeLimits                           = eff1(client.describeLimits)
    override def describeLimits(a: DescribeLimitsRequest) = eff1(client.describeLimits(a))
    override def describeStream(a: DescribeStreamRequest) = eff1(client.describeStream(a))
    override def describeStreamConsumer(a: DescribeStreamConsumerRequest) =
      eff1(client.describeStreamConsumer(a))
    override def describeStreamSummary(a: DescribeStreamSummaryRequest) =
      eff1(client.describeStreamSummary(a))
    override def disableEnhancedMonitoring(a: DisableEnhancedMonitoringRequest) =
      eff1(client.disableEnhancedMonitoring(a))
    override def enableEnhancedMonitoring(a: EnableEnhancedMonitoringRequest) =
      eff1(client.enableEnhancedMonitoring(a))
    override def getRecords(a: GetRecordsRequest)             = eff1(client.getRecords(a))
    override def getShardIterator(a: GetShardIteratorRequest) = eff1(client.getShardIterator(a))
    override def increaseStreamRetentionPeriod(a: IncreaseStreamRetentionPeriodRequest) =
      eff1(client.increaseStreamRetentionPeriod(a))
    override def listShards(a: ListShardsRequest) = eff1(client.listShards(a))
    override def listStreamConsumers(a: ListStreamConsumersRequest) =
      eff1(client.listStreamConsumers(a))
    override def listStreamConsumersPaginator(a: ListStreamConsumersRequest) =
      primitive1(client.listStreamConsumersPaginator(a))
    override def listStreams                                    = eff1(client.listStreams)
    override def listStreams(a: ListStreamsRequest)             = eff1(client.listStreams(a))
    override def listTagsForStream(a: ListTagsForStreamRequest) = eff1(client.listTagsForStream(a))
    override def mergeShards(a: MergeShardsRequest)             = eff1(client.mergeShards(a))
    override def putRecord(a: PutRecordRequest)                 = eff1(client.putRecord(a))
    override def putRecords(a: PutRecordsRequest)               = eff1(client.putRecords(a))
    override def registerStreamConsumer(a: RegisterStreamConsumerRequest) =
      eff1(client.registerStreamConsumer(a))
    override def removeTagsFromStream(a: RemoveTagsFromStreamRequest) =
      eff1(client.removeTagsFromStream(a))
    override def serviceName                      = primitive1(client.serviceName)
    override def splitShard(a: SplitShardRequest) = eff1(client.splitShard(a))
    override def startStreamEncryption(a: StartStreamEncryptionRequest) =
      eff1(client.startStreamEncryption(a))
    override def stopStreamEncryption(a: StopStreamEncryptionRequest) =
      eff1(client.stopStreamEncryption(a))
    override def subscribeToShard(a: SubscribeToShardRequest, b: SubscribeToShardResponseHandler) =
      eff1(client.subscribeToShard(a, b))
    override def updateShardCount(a: UpdateShardCountRequest) = eff1(client.updateShardCount(a))
    override def waiter                                       = primitive1(client.waiter)

  }

}
