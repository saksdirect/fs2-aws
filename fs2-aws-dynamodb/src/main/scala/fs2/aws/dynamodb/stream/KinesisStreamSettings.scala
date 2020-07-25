package fs2.aws.dynamodb.stream

import scala.concurrent.duration._

/** Settings for configuring the Kinesis consumer stream
  *
  *  @param bufferSize size of the internal buffer used when reading messages from Kinesis
  */
class KinesisStreamSettings private (
  val bufferSize: Int,
  val terminateGracePeriod: FiniteDuration
)

/** Settings for configuring the Kinesis checkpointer pipe
  *
  *  @param maxBatchSize the maximum number of records to aggregate before checkpointing the cluster of records. Passing 1 means checkpoint on every record
  *  @param maxBatchWait the maximum amount of time to wait before checkpointing the cluster of records
  */
class KinesisCheckpointSettings private (
  val maxBatchSize: Int,
  val maxBatchWait: FiniteDuration
)

object KinesisStreamSettings {
  val defaultInstance: KinesisStreamSettings = new KinesisStreamSettings(10, 10.seconds)

  def apply(
    bufferSize: Int,
    terminateGracePeriod: FiniteDuration
  ): Either[Throwable, KinesisStreamSettings] =
    (bufferSize, terminateGracePeriod) match {
      case (bs, _) if bs < 1 => Left(new RuntimeException("Must be greater than 0"))
      case (bs, period)      => Right(new KinesisStreamSettings(bufferSize, period))
    }
}

object KinesisCheckpointSettings {
  val defaultInstance = new KinesisCheckpointSettings(1000, 10.seconds)

  def apply(
    maxBatchSize: Int,
    maxBatchWait: FiniteDuration
  ): Either[Throwable, KinesisCheckpointSettings] =
    (maxBatchSize, maxBatchWait) match {
      case (s, _) if s <= 0 =>
        Left(new RuntimeException("Must be greater than 0"))
      case (_, w) if w <= 0.milliseconds =>
        Left(
          new RuntimeException(
            "Must be greater than 0 milliseconds. To checkpoint immediately, pass 1 to the max batch size."
          )
        )
      case (s, w) =>
        Right(new KinesisCheckpointSettings(s, w))
    }
}
