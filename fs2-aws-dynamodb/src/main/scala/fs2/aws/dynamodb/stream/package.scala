package fs2.aws.dynamodb

import cats.effect._
import cats.implicits._
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder, AmazonDynamoDBStreams, AmazonDynamoDBStreamsClientBuilder}
import com.amazonaws.services.dynamodbv2.streamsadapter.{AmazonDynamoDBStreamsAdapterClient, StreamsWorkerFactory}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.model.Record
import fs2.aws.core
import fs2.concurrent.Queue
import fs2.{Pipe, Stream}

//This code is almost the same as for Kinesis, except that it based on KCL v1, because DynamoDB streams are not migrated to V2
// this is why a lot of copy-paste

package object stream {
  private def defaultWorker(recordProcessorFactory: IRecordProcessorFactory)(
    workerConfiguration: KinesisClientLibConfiguration,
    dynamoDBStreamsClient: AmazonDynamoDBStreams,
    dynamoDBClient: AmazonDynamoDB,
    cloudWatchClient: AmazonCloudWatch
  ): Worker = {
    val adapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreamsClient)
    StreamsWorkerFactory.createDynamoDbStreamsWorker(
      recordProcessorFactory,
      workerConfiguration,
      adapterClient,
      dynamoDBClient,
      cloudWatchClient
    )
  }

  /** Intialize a worker and start streaming records from a Kinesis stream
    * On stream finish (due to error or other), worker will be shutdown
    *
    *  @tparam F effect type of the fs2 stream
    *  @param appName name of the Kinesis application. Used by KCL when resharding
    *  @param streamName name of the Kinesis stream to consume from
    *  @return an infinite fs2 Stream that emits Kinesis Records
    */
  def readFromDynamDBStream[F[_]: ConcurrentEffect: ContextShift](
    appName: String,
    streamName: String
  ): fs2.Stream[F, CommittableRecord] = {
    val workerConfig = new KinesisClientLibConfiguration(
      appName,
      streamName,
      DefaultAWSCredentialsProviderChain.getInstance(),
      s"${
        import scala.sys.process._
        "hostname".!!.trim()
      }:${java.util.UUID.randomUUID()}"
    ).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)

    readFromDynamoDBStream(workerConfig)
  }

  /** Intialize a worker and start streaming records from a Kinesis stream
    * On stream finish (due to error or other), worker will be shutdown
    *
    *  @tparam F effect type of the fs2 stream
    *  @param streamConfig configuration for the internal stream
    *  @return an infinite fs2 Stream that emits Kinesis Records
    */
  def readFromDynamoDBStream[F[_]: ConcurrentEffect: ContextShift](
    workerConfiguration: KinesisClientLibConfiguration,
    dynamoDBStreamsClient: AmazonDynamoDBStreams =
      AmazonDynamoDBStreamsClientBuilder.standard().withRegion(Regions.US_EAST_1).build(),
    dynamoDBClient: AmazonDynamoDB =
      AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build(),
    cloudWatchClient: AmazonCloudWatch =
      AmazonCloudWatchClientBuilder.standard().withRegion(Regions.US_EAST_1).build(),
    streamConfig: KinesisStreamSettings = KinesisStreamSettings.defaultInstance
  ): fs2.Stream[F, CommittableRecord] =
    readFromDynamoDBStream(
      defaultWorker(_)(
        workerConfiguration,
        dynamoDBStreamsClient,
        dynamoDBClient,
        cloudWatchClient
      ),
      streamConfig
    )

  /** Intialize a worker and start streaming records from a Kinesis stream
    * On stream finish (due to error or other), worker will be shutdown
    *
    *  @tparam F effect type of the fs2 stream
    *  @param workerFactory function to create a Worker from a IRecordProcessorFactory
    *  @param streamConfig configuration for the internal stream
    *  @return an infinite fs2 Stream that emits Kinesis Records
    */
  private[aws] def readFromDynamoDBStream[F[_]: ConcurrentEffect: ContextShift](
    workerFactory: =>IRecordProcessorFactory => Worker,
    streamConfig: KinesisStreamSettings
  ): fs2.Stream[F, CommittableRecord] = {

    // Initialize a KCL worker which appends to the internal stream queue on message receipt
    def instantiateWorker(queue: Queue[F, CommittableRecord]): Stream[F, Worker] = Stream.emit {
      workerFactory(() =>
        new RecordProcessor(
          record => Effect[F].runAsync(queue.enqueue1(record))(_ => IO.unit).unsafeRunSync,
          streamConfig.terminateGracePeriod
        )
      )
    }
    // Instantiate a new bounded queue and concurrently run the queue populator
    // Expose the elements by dequeuing the internal buffer
    for {
      buffer <- Stream.eval(Queue.bounded[F, CommittableRecord](streamConfig.bufferSize))
      worker <- instantiateWorker(buffer)
      stream <- buffer.dequeue concurrently Stream.eval(
                 Blocker[F].use(blocker => blocker.delay(worker.run()))
               ) onFinalize Sync[
                 F
               ].delay(worker.shutdown())
    } yield stream
  }

  /** Pipe to checkpoint records in Kinesis, marking them as processed
    * Groups records by shard id, so that each shard is subject to its own clustering of records
    * After accumulating maxBatchSize or reaching maxBatchWait for a respective shard, the latest record is checkpointed
    * By design, all records prior to the checkpointed record are also checkpointed in Kinesis
    *
    *  @tparam F effect type of the fs2 stream
    *  @param checkpointSettings configure maxBatchSize and maxBatchWait time before triggering a checkpoint
    *  @return a stream of Record types representing checkpointed messages
    */
  def checkpointRecords[F[_]](
    checkpointSettings: KinesisCheckpointSettings = KinesisCheckpointSettings.defaultInstance,
    parallelism: Int = 10
  )(
    implicit F: ConcurrentEffect[F],
    timer: Timer[F]
  ): Pipe[F, CommittableRecord, Record] = {
    def checkpoint(checkpointSettings: KinesisCheckpointSettings, parallelism: Int)(
      implicit F: ConcurrentEffect[F],
      timer: Timer[F]
    ): Pipe[F, CommittableRecord, Record] =
      _.groupWithin(checkpointSettings.maxBatchSize, checkpointSettings.maxBatchWait)
        .collect { case chunk if chunk.size > 0 => chunk.toList.max }
        .flatMap { cr =>
          fs2.Stream.eval_(
            F.async[Record] { cb =>
              cr.checkpoint()
              cb(Right(cr.record))
            }
          )
        }

    def bypass: Pipe[F, CommittableRecord, Record] = _.map(r => r.record)

    _.through(core.groupBy(r => F.delay(r.shardId))).map {
      case (_, st) =>
        st.broadcastThrough(checkpoint(checkpointSettings, parallelism), bypass)
    }.parJoinUnbounded
  }

  /** Sink to checkpoint records in Kinesis, marking them as processed
    * Groups records by shard id, so that each shard is subject to its own clustering of records
    * After accumulating maxBatchSize or reaching maxBatchWait for a respective shard, the latest record is checkpointed
    * By design, all records prior to the checkpointed record are also checkpointed in Kinesis
    *
    *  @tparam F effect type of the fs2 stream
    *  @param checkpointSettings configure maxBatchSize and maxBatchWait time before triggering a checkpoint
    *  @return a Sink that accepts a stream of CommittableRecords
    */
  def checkpointRecords_[F[_]](
    checkpointSettings: KinesisCheckpointSettings = KinesisCheckpointSettings.defaultInstance
  )(implicit F: ConcurrentEffect[F], timer: Timer[F]): Pipe[F, CommittableRecord, Unit] =
    _.through(checkpointRecords(checkpointSettings))
      .map(_ => ())
}
