package fs2.aws.kinesis

import cats.effect.{ ContextShift, IO, Timer }
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import fs2.Stream
import fs2.aws.internal.KinesisProducerClientImpl
import fs2.aws.kinesis.consumer.readFromKinesisStream
import fs2.aws.kinesis.publisher.writeToKinesis
import org.scalatest.Ignore
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Minutes, Second, Span }
import software.amazon.kinesis.common.InitialPositionInStream

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

@nowarn
@Ignore
class LocalStackSuite extends AnyFlatSpec with Matchers with ScalaFutures {

  implicit val ec: ExecutionContext             = ExecutionContext.global
  implicit val timer: Timer[IO]                 = IO.timer(ec)
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ec)

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Minutes)), interval = scaled(Span(1, Second)))

  "The Kinesis producer and consumer" should "be able to produce to and consume from LocalStack" in {

    // this is required to make the KCL work with LocalStack
    System.setProperty("aws.cborEnabled", "false")

    val streamName = "test"

    val consumerConfig = KinesisConsumerSettings(
      streamName,
      "test-app",
      initialPositionInStream = Left(InitialPositionInStream.TRIM_HORIZON),
      endpoint = Some("http://localhost:4566"),
      retrievalMode = Polling
    )

    val producerConfig = new KinesisProducerConfiguration()
      .setCredentialsProvider(new DefaultAWSCredentialsProviderChain())
      .setKinesisEndpoint("localhost")
      .setKinesisPort(4566)
      .setCloudwatchEndpoint("localhost")
      .setCloudwatchPort(4566)
      .setVerifyCertificate(false)
      .setRegion("us-east-1")

    val partitionKey = "test"
    val data         = List("foo", "bar", "baz")

    val test = for {
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
      record <- readFromKinesisStream[IO](consumerConfig).take(data.length).compile.toList
    } yield record

    val records = test.unsafeToFuture().futureValue

    val actualPartitionKeys = records.map(_.record.partitionKey())
    val actualData          = records.map(deserialiseData)
    actualPartitionKeys shouldBe (1 to data.length).map(_ => partitionKey)
    actualData          shouldBe data
  }

  private def deserialiseData(committable: CommittableRecord) =
    StandardCharsets.UTF_8.decode(committable.record.data()).toString

}
