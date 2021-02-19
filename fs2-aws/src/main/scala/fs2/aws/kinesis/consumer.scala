package fs2.aws.kinesis

import cats.effect.concurrent.Deferred
import cats.effect.{ Blocker, ConcurrentEffect, ContextShift, IO, Sync, Timer }
import cats.implicits._
import fs2.aws.core
import fs2.concurrent.Queue
import fs2.{ Chunk, Pipe, RaiseThrowable, Stream }
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.kinesis.common.{ ConfigsBuilder, InitialPositionInStreamExtended }
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig

import java.util.UUID

object consumer {
  def mkDefaultKinesisClient(settings: KinesisConsumerSettings): KinesisAsyncClient = {
    val builder = KinesisAsyncClient
      .builder()
    settings.endpoint.foreach(builder.endpointOverride)
    builder
      .region(settings.region)
      .credentialsProvider(
        settings.stsAssumeRole
          .map { stsSettings =>
            val assumeRoleRequest = AssumeRoleRequest
              .builder()
              .roleArn(stsSettings.roleArn)
              .roleSessionName(stsSettings.roleSessionName)
            stsSettings.externalId.foreach(assumeRoleRequest.externalId)
            stsSettings.durationSeconds.foreach(d => assumeRoleRequest.durationSeconds(d))
            StsAssumeRoleCredentialsProvider
              .builder()
              .stsClient(StsClient.builder.build())
              .refreshRequest(
                assumeRoleRequest.build()
              )
              .build()
          }
          .getOrElse(DefaultCredentialsProvider.create())
      )
      .httpClientBuilder(
        NettyNioAsyncHttpClient
          .builder()
          .maxConcurrency(settings.maxConcurrency)
      )
      .build()
  }

  private def defaultScheduler(
    recordProcessorFactory: ShardRecordProcessorFactory,
    settings: KinesisConsumerSettings,
    kinesisClient: KinesisAsyncClient
  ): Scheduler = {

    val dynamoClientBuilder = DynamoDbAsyncClient.builder()
    settings.endpoint.foreach(dynamoClientBuilder.endpointOverride)
    val dynamoClient: DynamoDbAsyncClient =
      dynamoClientBuilder.region(settings.region).build()

    val cloudWatchClientBuilder = CloudWatchAsyncClient.builder()
    settings.endpoint.foreach(cloudWatchClientBuilder.endpointOverride)
    val cloudWatchClient: CloudWatchAsyncClient =
      cloudWatchClientBuilder.region(settings.region).build()

    val configsBuilder: ConfigsBuilder = new ConfigsBuilder(
      settings.streamName,
      settings.appName,
      kinesisClient,
      dynamoClient,
      cloudWatchClient,
      UUID.randomUUID().toString,
      recordProcessorFactory
    )

    val retrievalConfig = configsBuilder.retrievalConfig()
    retrievalConfig.retrievalSpecificConfig(
      settings.retrievalMode match {
        case FanOut  => new FanOutConfig(kinesisClient)
        case Polling => new PollingConfig(settings.streamName, kinesisClient)
      }
    )
    retrievalConfig.initialPositionInStreamExtended(
      settings.initialPositionInStream match {
        case Left(position) =>
          InitialPositionInStreamExtended.newInitialPosition(position)

        case Right(date) =>
          InitialPositionInStreamExtended.newInitialPositionAtTimestamp(date)
      }
    )

    new Scheduler(
      configsBuilder.checkpointConfig(),
      configsBuilder.coordinatorConfig(),
      configsBuilder.leaseManagementConfig(),
      configsBuilder.lifecycleConfig(),
      configsBuilder.metricsConfig(),
      configsBuilder.processorConfig(),
      retrievalConfig
    )
  }

  /** Intialize a worker and start streaming records from a Kinesis stream
    * On stream finish (due to error or other), worker will be shutdown
    *
    * @tparam F effect type of the fs2 stream
    * @param appName    name of the Kinesis application. Used by KCL when resharding
    * @param streamName name of the Kinesis stream to consume from
    * @return an infinite fs2 Stream that emits Kinesis Records
    */
  def readFromKinesisStream[F[_]: ConcurrentEffect: ContextShift](
    appName: String,
    streamName: String
  )(implicit rt: RaiseThrowable[F]): Stream[F, CommittableRecord] =
    KinesisConsumerSettings(streamName, appName) match {
      case Right(config) => readFromKinesisStream(config)
      case Left(err)     => Stream.raiseError(err)
    }

  /** Initialize a worker and start streaming records from a Kinesis stream
    * On stream finish (due to error or other), worker will be shutdown
    *
    * @tparam F effect type of the fs2 stream
    * @param consumerConfig configuration parameters for the KCL
    * @return an infinite fs2 Stream that emits Kinesis Records
    */
  def readFromKinesisStream[F[_]: ConcurrentEffect: ContextShift](
    consumerConfig: KinesisConsumerSettings
  ): Stream[F, CommittableRecord] =
    readFromKinesisStream(
      consumerConfig,
      defaultScheduler(_, consumerConfig, mkDefaultKinesisClient(consumerConfig))
    )

  /** Intialize a worker and start streaming records from a Kinesis stream
    * On stream finish (due to error or other), worker will be shutdown
    *
    * @tparam F effect type of the fs2 stream
    * @param appName    name of the Kinesis application. Used by KCL when resharding
    * @param streamName name of the Kinesis stream to consume from
    * @param kinesisClient preconfigured kineiss klient, usefull when you need STS access
    * @return an infinite fs2 Stream that emits Kinesis Records
    */
  def readFromKinesisStream[F[_]: ConcurrentEffect: ContextShift](
    appName: String,
    streamName: String,
    kinesisClient: KinesisAsyncClient
  )(implicit rt: RaiseThrowable[F]): Stream[F, CommittableRecord] =
    KinesisConsumerSettings(streamName, appName) match {
      case Right(config) => readFromKinesisStream(config, kinesisClient)
      case Left(err)     => Stream.raiseError(err)
    }

  /** Initialize a worker and start streaming records from a Kinesis stream
    * On stream finish (due to error or other), worker will be shutdown
    *
    * @tparam F effect type of the fs2 stream
    * @param consumerConfig configuration parameters for the KCL
    * @param kinesisClient preconfigured kineiss klient, usefull when you need STS access
    * @return an infinite fs2 Stream that emits Kinesis Records
    */
  def readFromKinesisStream[F[_]: ConcurrentEffect: ContextShift](
    consumerConfig: KinesisConsumerSettings,
    kinesisClient: KinesisAsyncClient
  ): Stream[F, CommittableRecord] =
    readFromKinesisStream(consumerConfig, defaultScheduler(_, consumerConfig, kinesisClient))

  /** Initialize a worker and start streaming records from a Kinesis stream
    * On stream finish (due to error or other), worker will be shutdown
    *
    * @tparam F effect type of the fs2 stream
    * @param consumerConfig configuration parameters for the KCL
    * @return an infinite fs2 Stream that emits Kinesis Records Chunks
    */
  def readChunkedFromKinesisStream[F[_]: ConcurrentEffect: ContextShift](
    consumerConfig: KinesisConsumerSettings
  ): Stream[F, Chunk[CommittableRecord]] =
    readChunksFromKinesisStream(
      consumerConfig,
      defaultScheduler(_, consumerConfig, mkDefaultKinesisClient(consumerConfig))
    )

  /** Initialize a worker and start streaming records from a Kinesis stream
    * On stream finish (due to error or other), worker will be shutdown
    *
    * @tparam F effect type of the fs2 stream
    * @param consumerConfig configuration parameters for the KCL
    * @param kinesisClient preconfigured kineiss klient, usefull when you need STS access
    * @return an infinite fs2 Stream that emits Kinesis Records Chunks
    */
  def readChunkedFromKinesisStream[F[_]: ConcurrentEffect: ContextShift](
    consumerConfig: KinesisConsumerSettings,
    kinesisClient: KinesisAsyncClient
  ): Stream[F, Chunk[CommittableRecord]] =
    readChunksFromKinesisStream(consumerConfig, defaultScheduler(_, consumerConfig, kinesisClient))

  /** Intialize a worker and start streaming records from a Kinesis stream
    * On stream finish (due to error or other), worker will be shutdown
    *
    * @tparam F effect type of the fs2 stream
    * @param schedulerFactory function to create a Worker from a IRecordProcessorFactory
    * @param streamConfig  configuration for the internal stream
    * @return an infinite fs2 Stream that emits Kinesis Records
    */
  private[kinesis] def readFromKinesisStream[F[_]: ConcurrentEffect: ContextShift](
    streamConfig: KinesisConsumerSettings,
    schedulerFactory: ShardRecordProcessorFactory => Scheduler
  ): Stream[F, CommittableRecord] =
    readChunksFromKinesisStream[F](streamConfig, schedulerFactory).flatMap(fs2.Stream.chunk)

  private[kinesis] def readChunksFromKinesisStream[F[_]: ConcurrentEffect: ContextShift](
    streamConfig: KinesisConsumerSettings,
    schedulerFactory: ShardRecordProcessorFactory => Scheduler
  ): Stream[F, Chunk[CommittableRecord]] = {
    // Initialize a KCL scheduler which appends to the internal stream queue on message receipt
    def instantiateWorker(queue: Queue[F, Chunk[CommittableRecord]]): fs2.Stream[F, Scheduler] =
      Stream.emit(
        schedulerFactory(() =>
          new ChunkedRecordProcessor(records =>
            ConcurrentEffect[F].runAsync(queue.enqueue1(records))(_ => IO.unit).unsafeRunSync()
          )
        )
      )

    // Instantiate a new bounded queue and concurrently run the queue populator
    // Expose the elements by dequeuing the internal buffer
    for {
      buffer    <- Stream.eval(Queue.bounded[F, Chunk[CommittableRecord]](streamConfig.bufferSize))
      scheduler <- instantiateWorker(buffer)
      switch    <- Stream.eval(Deferred[F, Unit])
      enqueue = Stream
        .eval(
          Blocker[F].use(blocker => blocker.delay(scheduler.run()))
        )
        .onFinalize(switch.complete(()))
      dequeue = buffer.dequeue
        .interruptWhen(switch.get.attempt)
      stream <- dequeue.concurrently(enqueue).onFinalize(Sync[F].delay(scheduler.shutdown()))
    } yield stream
  }

  /** Pipe to checkpoint records in Kinesis, marking them as processed
    * Groups records by shard id, so that each shard is subject to its own clustering of records
    * After accumulating maxBatchSize or reaching maxBatchWait for a respective shard, the latest record is checkpointed
    * By design, all records prior to the checkpointed record are also checkpointed in Kinesis
    *
    * @tparam F effect type of the fs2 stream
    * @param checkpointSettings configure maxBatchSize and maxBatchWait time before triggering a checkpoint
    * @return a stream of Record types representing checkpointed messages
    */
  def checkpointRecords[F[_]: ConcurrentEffect: Timer](
    checkpointSettings: KinesisCheckpointSettings = KinesisCheckpointSettings.defaultInstance
  ): Pipe[F, CommittableRecord, KinesisClientRecord] = {
    def checkpoint(
      checkpointSettings: KinesisCheckpointSettings
    ): Pipe[F, CommittableRecord, KinesisClientRecord] =
      _.groupWithin(checkpointSettings.maxBatchSize, checkpointSettings.maxBatchWait)
        .collect { case chunk if chunk.size > 0 => chunk.toList.max }
        .flatMap(cr => fs2.Stream.eval_(cr.checkpoint.as(cr.record)))

    def bypass: Pipe[F, CommittableRecord, KinesisClientRecord] = _.map(r => r.record)

    _.through(core.groupBy(r => Sync[F].pure(r.shardId))).map {
      case (_, st) =>
        st.broadcastThrough(checkpoint(checkpointSettings), bypass)
    }.parJoinUnbounded
  }

  /** Sink to checkpoint records in Kinesis, marking them as processed
    * Groups records by shard id, so that each shard is subject to its own clustering of records
    * After accumulating maxBatchSize or reaching maxBatchWait for a respective shard, the latest record is checkpointed
    * By design, all records prior to the checkpointed record are also checkpointed in Kinesis
    *
    * @tparam F effect type of the fs2 stream
    * @param checkpointSettings configure maxBatchSize and maxBatchWait time before triggering a checkpoint
    * @return a Sink that accepts a stream of CommittableRecords
    */
  def checkpointRecords_[F[_]](
    checkpointSettings: KinesisCheckpointSettings = KinesisCheckpointSettings.defaultInstance
  )(implicit F: ConcurrentEffect[F], timer: Timer[F]): Pipe[F, CommittableRecord, Unit] =
    _.through(checkpointRecords(checkpointSettings)).void
}
