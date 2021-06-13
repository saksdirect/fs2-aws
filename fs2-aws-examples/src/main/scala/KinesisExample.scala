import cats.NonEmptyParallel
import cats.effect.{
  Blocker,
  Concurrent,
  ConcurrentEffect,
  ContextShift,
  ExitCode,
  IO,
  IOApp,
  Resource,
  Sync,
  Timer
}
import fs2.Stream
import fs2.aws.kinesis.{ Kinesis, KinesisConsumerSettings }
import io.laserdisc.pure.cloudwatch.tagless.{ Interpreter => CloudwatchInterpreter }
import io.laserdisc.pure.dynamodb.tagless.{ Interpreter => DynamoDbInterpreter }
import io.laserdisc.pure.kinesis.tagless.{ KinesisAsyncClientOp, Interpreter => KinesisInterpreter }
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import software.amazon.awssdk.services.kinesis.model.{ CreateStreamRequest, DeleteStreamRequest }
import cats.implicits._
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import fs2.aws.internal.KinesisProducerClientImpl
import fs2.aws.kinesis.publisher.writeToKinesis

import java.nio.ByteBuffer
import syntax._

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global

object KinesisExample extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val appConfig = KinesisAppConfig.localstackConfig
    kAlgebraResource[IO](
      appConfig.kinesisSdkBuilder,
      appConfig.dynamoSdkBuilder,
      appConfig.cloudwatchSdkBuilder,
      appConfig.streamName
    ).use(kinesis =>
      program[IO](kinesis, appConfig.consumerConfig, appConfig.producerConfig).as(ExitCode.Success)
    )
  }
  private def kAlgebraResource[F[_]: ConcurrentEffect: ContextShift: Timer](
    kac: KinesisAsyncClientBuilder,
    dac: DynamoDbAsyncClientBuilder,
    cac: CloudWatchAsyncClientBuilder,
    streamName: String
  ) =
    for {
      b                  <- Blocker[F]
      i                  = KinesisInterpreter[F]
      k                  <- i.KinesisAsyncClientResource(kac)
      d                  <- DynamoDbInterpreter[F].DynamoDbAsyncClientResource(dac)
      c                  <- CloudwatchInterpreter[F].CloudWatchAsyncClientResource(cac)
      kinesisInterpreter = i.create(k)
      _                  <- disposableStream(kinesisInterpreter, streamName)
    } yield Kinesis.create[F](k, d, c, b)

  def program[F[_]: Concurrent: Timer: NonEmptyParallel](
    kinesis: Kinesis[F],
    consumerSettings: KinesisConsumerSettings,
    producerConfiguration: KinesisProducerConfiguration
  ): F[Unit] =
    (
      Stream
        .awakeEvery[F](5 seconds)
        .map(_.toString())
        .evalTap(d => Sync[F].delay(println(s"Producing $d")))
        .map(d => (d, ByteBuffer.wrap(d.getBytes)))
        .through(
          writeToKinesis[F](
            consumerSettings.streamName,
            producer = new KinesisProducerClientImpl[F](Some(producerConfiguration))
          )
        )
        .compile
        .drain,
      kinesis
        .readFromKinesisStream(consumerSettings)
        .map(cr => StandardCharsets.UTF_8.decode(cr.record.data()).toString)
        .evalTap(cr => Sync[F].delay(println(s"Consuming $cr")))
        .compile
        .drain
    ).parMapN { case (_, _) => () }

  private def disposableStream[F[_]: Sync](
    interpreter: KinesisAsyncClientOp[F],
    streamName: String
  ) =
    Resource.make(
      interpreter.createStream(
        CreateStreamRequest.builder().streamName(streamName).shardCount(1).build()
      )
    )(_ =>
      interpreter
        .deleteStream(DeleteStreamRequest.builder().streamName(streamName).build())
        .void
    )

}
