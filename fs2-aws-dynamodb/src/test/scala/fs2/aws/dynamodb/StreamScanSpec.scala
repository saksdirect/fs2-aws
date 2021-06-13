package fs2.aws.dynamodb

import cats.effect.{ ContextShift, IO, Resource, Timer }
import io.laserdisc.pure.dynamodb.tagless.{ DynamoDbAsyncClientOp, Interpreter => DDBInterpreter }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Minutes, Second, Span }
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  BatchWriteItemRequest,
  BillingMode,
  CreateTableRequest,
  DeleteTableRequest,
  KeySchemaElement,
  KeyType,
  PutRequest,
  ScalarAttributeType,
  ScanRequest,
  WriteRequest
}

import java.net.URI
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class StreamScanSpec extends AnyWordSpec with Matchers with ScalaFutures {
  implicit val ec: ExecutionContext             = ExecutionContext.global
  implicit val timer: Timer[IO]                 = IO.timer(ec)
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ec)

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Minutes)), interval = scaled(Span(1, Second)))

  "DynamoDB scan" should {
    "produce fs2 Stream form DDB scan operation" in {
      resourcesF
        .use {
          case (tableName, ddb) =>
            for {
              _ <- ddb.batchWriteItem(
                    BatchWriteItemRequest
                      .builder()
                      .requestItems(
                        Map(
                          tableName -> List("Dmytro", "Barry", "Ryan", "Vlad")
                            .map(mkWriteRequest)
                            .asJava
                        ).asJava
                      )
                      .build()
                  )
              scanned <- StreamScan[IO](ddb)
                          .scanDynamoDB(ScanRequest.builder().tableName(tableName).build(), 3)
                          .flatMap(fs2.Stream.chunk)
                          .compile
                          .toList
            } yield scanned.map(_.get("name").s()) should contain theSameElementsAs List(
              "Dmytro",
              "Barry",
              "Ryan",
              "Vlad"
            )
        }
        .unsafeToFuture()
        .futureValue
    }
  }
  def resourcesF: Resource[IO, (String, DynamoDbAsyncClientOp[IO])] = {
    val credentials = AwsBasicCredentials.create("accesskey", "secretkey")
    val port        = 4566
    for {
      ddb <- DDBInterpreter[IO].DynamoDbAsyncClientOpResource(
              DynamoDbAsyncClient
                .builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .endpointOverride(URI.create(s"http://localhost:$port"))
                .region(Region.US_EAST_1)
            )
      tableName = "scan_test"
      _ <- Resource.make(
            ddb.createTable(
              CreateTableRequest
                .builder()
                .tableName(tableName)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(
                  AttributeDefinition
                    .builder()
                    .attributeName("name")
                    .attributeType(ScalarAttributeType.S)
                    .build()
                )
                .keySchema(
                  KeySchemaElement.builder().attributeName("name").keyType(KeyType.HASH).build()
                )
                .build()
            )
          )(_ => ddb.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).void)

    } yield tableName -> ddb
  }

  def mkWriteRequest(name: String) =
    WriteRequest
      .builder()
      .putRequest(
        PutRequest
          .builder()
          .item(
            Map("name" -> AttributeValue.builder().s(name).build()).asJava
          )
          .build()
      )
      .build()
}
