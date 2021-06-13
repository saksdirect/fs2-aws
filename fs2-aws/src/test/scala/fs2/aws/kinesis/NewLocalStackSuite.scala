package fs2.aws.kinesis

import cats.effect.{ Blocker, ContextShift, IO, Resource, Timer }
import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import fs2.Stream
import fs2.aws.internal.KinesisProducerClientImpl
import fs2.aws.kinesis.publisher.writeToKinesis
import io.laserdisc.pure.cloudwatch.tagless.{ Interpreter => CloudwatchInterpreter }
import io.laserdisc.pure.dynamodb.tagless.{ Interpreter => DynamoDbInterpreter }
import io.laserdisc.pure.kinesis.tagless.{ Interpreter => KinesisInterpreter }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Minutes, Second, Span }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.{
  CloudWatchAsyncClient,
  CloudWatchAsyncClientBuilder
}
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbAsyncClientBuilder }
import software.amazon.awssdk.services.kinesis.model.{
  CreateStreamRequest,
  DeleteStreamRequest,
  DescribeStreamRequest,
  UpdateShardCountRequest
}
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import software.amazon.kinesis.common.InitialPositionInStream

import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class NewLocalStackSuite extends AnyFlatSpec with Matchers with ScalaFutures {

  implicit val ec: ExecutionContext             = ExecutionContext.global
  implicit val timer: Timer[IO]                 = IO.timer(ec)
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ec)

  // this is required to make the KCL work with LocalStack
  System.setProperty("aws.cborEnabled", "false")
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Minutes)), interval = scaled(Span(1, Second)))

  val streamName = "test"

  val partitionKey = "test"

  val consumerConfig = KinesisConsumerSettings(
    streamName,
    "test-app",
    initialPositionInStream = Left(InitialPositionInStream.TRIM_HORIZON),
    endpoint = Some("http://localhost:4566"),
    retrievalMode = Polling
  )

  val credentials =
    new BasicAWSCredentials("dummy", "dummy")

  val producerConfig = new KinesisProducerConfiguration()
    .setCredentialsProvider(new AWSStaticCredentialsProvider(credentials))
    .setKinesisEndpoint("localhost")
    .setKinesisPort(4566)
    .setCloudwatchEndpoint("localhost")
    .setCloudwatchPort(4566)
    .setVerifyCertificate(false)
    .setRegion("us-east-1")

  val cp = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))

  val kac = KinesisAsyncClient
    .builder()
    .credentialsProvider(cp)
    .region(Region.US_EAST_1)
    .endpointOverride(URI.create("http://localhost:4566"))
  val dac = DynamoDbAsyncClient
    .builder()
    .credentialsProvider(cp)
    .region(Region.US_EAST_1)
    .endpointOverride(URI.create("http://localhost:4566"))
  val cac =
    CloudWatchAsyncClient
      .builder()
      .credentialsProvider(cp)
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create("http://localhost:4566"))

  "The Kinesis producer and consumer" should "be able to produce to and consume from LocalStack" in {

    val data = List("foo", "bar", "baz")

    val test = kAlgebraResource(kac, dac, cac).use {
      case (_, kAlgebra) =>
        for {
          _ <- Stream
                .emits(data)
                .map(d => (partitionKey, ByteBuffer.wrap(d.getBytes)))
                .through(
                  writeToKinesis[IO](
                    streamName,
                    producer = new KinesisProducerClientImpl[IO](Some(producerConfig))
                  )
                )
                .compile
                .drain
          record <- kAlgebra
                     .readFromKinesisStream(consumerConfig)
                     .take(data.length)
                     .compile
                     .toList
        } yield record

    }
    val records = test.unsafeToFuture().futureValue

    val actualPartitionKeys = records.map(_.record.partitionKey())
    val actualData          = records.map(deserialiseData)
    actualPartitionKeys shouldBe (1 to data.length).map(_ => partitionKey)
    actualData          shouldBe data
  }

  "The Kinesis fs2 Stream" should "seamlessly consume from re-sharded stream " ignore {

    val data = List("foo", "bar", "baz")
    val sn   = "resharding_test"
    val consumerConfig = KinesisConsumerSettings(
      sn,
      "test-app",
      initialPositionInStream = Left(InitialPositionInStream.TRIM_HORIZON),
      endpoint = Some("http://localhost:4566"),
      retrievalMode = Polling
    )
    val resource = for {
      r @ (kAsyncInterpreter, _) <- kAlgebraResource(kac, dac, cac)
      _ <- Resource.make(
            kAsyncInterpreter.createStream(
              CreateStreamRequest.builder().streamName(sn).shardCount(1).build()
            )
          )(_ =>
            kAsyncInterpreter
              .deleteStream(DeleteStreamRequest.builder().streamName(sn).build())
              .void
          )
    } yield r

    val producer = new KinesisProducerClientImpl[IO](Some(producerConfig))
    val test = resource.use {
      case (kAsyncInterpreter, kStreamInterpreter) =>
        for {

          _ <- Stream
                .emits(data)
                .map(d => (d, ByteBuffer.wrap(d.getBytes)))
                .through(writeToKinesis[IO](sn, producer = producer))
                .compile
                .drain
          _ <- kAsyncInterpreter.updateShardCount(
                UpdateShardCountRequest.builder().streamName(sn).targetShardCount(2).build()
              )
          _ <- fs2.Stream
                .retry(
                  kAsyncInterpreter
                    .describeStream(
                      DescribeStreamRequest.builder().streamName(sn).build()
                    )
                    .flatMap { r =>
                      if (r.streamDescription().shards().size() != 2)
                        IO.raiseError(new RuntimeException("Expected 2 shards"))
                      else IO.unit
                    },
                  2 seconds,
                  _.*(2),
                  5,
                  _.getMessage == "Expected 2 shards"
                )
                .compile
                .drain
          _ <- Stream
                .emits(List("foo1", "bar1", "baz1"))
                .map(d => (d, ByteBuffer.wrap(d.getBytes)))
                .through(writeToKinesis[IO](sn, producer = producer))
                .compile
                .drain
          record <- kStreamInterpreter
                     .readFromKinesisStream(consumerConfig)
                     .take(data.length * 2)
                     .compile
                     .toList

        } yield record

    }
    val records = test.unsafeToFuture().futureValue

    val actualPartitionKeys = records.map(_.record.partitionKey())
    val actualData          = records.map(deserialiseData)
    actualPartitionKeys shouldBe (1 to data.length).map(_ => partitionKey)
    actualData          shouldBe data
  }

  private def deserialiseData(committable: CommittableRecord) =
    StandardCharsets.UTF_8.decode(committable.record.data()).toString

  private def kAlgebraResource(
    kac: KinesisAsyncClientBuilder,
    dac: DynamoDbAsyncClientBuilder,
    cac: CloudWatchAsyncClientBuilder
  ) =
    for {
      b                  <- Blocker[IO]
      i                  = KinesisInterpreter[IO]
      k                  <- i.KinesisAsyncClientResource(kac)
      d                  <- DynamoDbInterpreter[IO].DynamoDbAsyncClientResource(dac)
      c                  <- CloudwatchInterpreter[IO].CloudWatchAsyncClientResource(cac)
      kinesisInterpreter = i.create(k)
      kAlgebra           = Kinesis.create[IO](k, d, c, b)
    } yield kinesisInterpreter -> kAlgebra

}
