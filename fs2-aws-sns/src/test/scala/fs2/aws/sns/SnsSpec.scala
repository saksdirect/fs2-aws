package fs2.aws.sns

import cats.effect.{ ContextShift, IO, Resource, Timer }
import fs2.aws.sns.sns.SNS
import io.laserdisc.pure.sns.tagless.{ Interpreter, SnsAsyncClientOp }
import io.laserdisc.pure.sqs.tagless
import io.laserdisc.pure.sqs.tagless.SqsAsyncClientOp
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sns.SnsAsyncClient
import software.amazon.awssdk.services.sns.model._
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._
import java.net.URI
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex
import cats.implicits._

class SnsSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  implicit val ec: ExecutionContext             = ExecutionContext.global
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ec)
  implicit val ioTimer: Timer[IO]               = IO.timer(ec)

  val awsClientsResource: Resource[IO, (SnsAsyncClientOp[IO], SqsAsyncClientOp[IO])] =
    (mkSNSClient(4566), mkSQSClient(4566)).mapN { case (s1, s2) => s1 -> s2 }
  var topicArn: String = _
  var queueUrl: String = _
  val pattern: Regex   = new Regex("\"Message\": \"[0-9]\"")

  override def beforeAll(): Unit =
    awsClientsResource
      .use {
        case (sns, sqs) =>
          for {
            topic <- sns
                      .createTopic(CreateTopicRequest.builder().name("topic").build())
                      .map(_.topicArn())

            queueUrlV <- sqs
                          .createQueue(CreateQueueRequest.builder().queueName("names").build())
                          .map(_.queueUrl())
          } yield {
            topicArn = topic
            queueUrl = queueUrlV
          }
      }
      .unsafeRunSync()

  override def afterAll(): Unit =
    awsClientsResource
      .use {
        case (sns, sqs) =>
          for {
            _ <- sns.deleteTopic(DeleteTopicRequest.builder().topicArn(topicArn).build())
            _ <- sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build())
          } yield {}
      }
      .unsafeRunSync()

  "SNS" should {
    "publish messages" in {
      val messages =
        awsClientsResource
          .use {
            case (sns, sqs) =>
              (for {
                sns_ <- fs2.Stream.eval(SNS.create[IO](sns))
                sqsArn <- fs2.Stream.eval(
                           sqs
                             .getQueueAttributes(
                               GetQueueAttributesRequest
                                 .builder()
                                 .queueUrl(queueUrl)
                                 .attributeNames(QueueAttributeName.QUEUE_ARN)
                                 .build()
                             )
                         )

                _ <- fs2.Stream.eval(
                      sns.subscribe(
                        SubscribeRequest
                          .builder()
                          .protocol("sqs")
                          .endpoint(sqsArn.attributes().get(QueueAttributeName.QUEUE_ARN))
                          .topicArn(topicArn)
                          .build()
                      )
                    )
                _ <- fs2
                      .Stream("1", "2", "3", "4", "5")
                      .covary[IO]
                      .through(sns_.publish(topicArn))

                sqsMessages <- fs2.Stream
                                .eval(
                                  sqs
                                    .receiveMessage(
                                      ReceiveMessageRequest
                                        .builder()
                                        .queueUrl(queueUrl)
                                        .build()
                                    )
                                )
                                .delayBy(3.seconds)
              } yield {
                sqsMessages
                  .messages()
                  .asScala
                  .map(x => pattern.findAllIn(x.body()).mkString(","))
                  .map(x => new Regex("\\d").findAllIn(x).mkString(","))
              }).take(5).compile.toList
          }
          .unsafeRunSync()

      messages.flatten should contain theSameElementsAs (List("1", "2", "3", "4", "5"))
    }
  }

  def mkSNSClient(snsPort: Int): Resource[IO, SnsAsyncClientOp[IO]] = {
    val credentials =
      AwsBasicCredentials.create("accesskey", "secretkey")
    Interpreter[IO].SnsAsyncClientOpResource(
      SnsAsyncClient
        .builder()
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .endpointOverride(URI.create(s"http://localhost:$snsPort"))
        .region(Region.US_EAST_1)
    )
  }

  def mkSQSClient(sqsPort: Int): Resource[IO, SqsAsyncClientOp[IO]] = {
    val credentials =
      AwsBasicCredentials.create("accesskey", "secretkey")
    tagless
      .Interpreter[IO]
      .SqsAsyncClientOpResource(
        SqsAsyncClient
          .builder()
          .credentialsProvider(StaticCredentialsProvider.create(credentials))
          .endpointOverride(URI.create(s"http://localhost:$sqsPort"))
          .region(Region.US_EAST_1)
      )
  }
}
