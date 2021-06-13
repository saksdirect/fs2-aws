import cats.data.Kleisli
import cats.effect.{ ExitCode, IO, IOApp, Sync }
import cats.implicits._
import fs2.{ Chunk, Pipe }
import fs2.aws.dynamodb.StreamScan
import io.laserdisc.pure.dynamodb.tagless.{ DynamoDbAsyncClientOp, Interpreter }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import java.util.{ Map => JMap }
import scala.jdk.CollectionConverters._

/**
  * Following program demonstrates parallel scan and subsequent write to DDB table. This is efficient way to copy one DDB table to another
  * It is also possible to distribute workers across different instances assigning specific segments range to each worker
  */
class DDBReader[F[_]](ss: StreamScan[F]) {
  def read(
    segment: Int,
    totalSegments: Int,
    ddbName: String,
    pageSize: Int
  ): fs2.Stream[F, Chunk[JMap[String, AttributeValue]]] =
    ss.scanDynamoDB(
      ScanRequest
        .builder()
        .segment(segment)
        .totalSegments(totalSegments)
        .tableName(ddbName)
        .limit(100)
        .build(),
      pageSize
    )
}

class DDBWriter[F[_]: Sync](ddb: DynamoDbAsyncClientOp[F]) {
  private def writeRequests(chunk: Seq[JMap[String, AttributeValue]]): Seq[WriteRequest] =
    chunk.toList
      .map(m => WriteRequest.builder().putRequest(PutRequest.builder().item(m).build()).build())

  private def writeUntilExhausted(
    destDDB: String,
    requests: Seq[WriteRequest]
  ): F[Unit] = {
    def go(batch: Seq[WriteRequest]): F[Unit] =
      ddb
        .batchWriteItem(
          BatchWriteItemRequest
            .builder()
            .requestItems(Map(destDDB -> batch.asJava).asJava)
            .build()
        )
        .flatMap { r =>
          val unprocessed = r.unprocessedItems().values.asScala.flatMap(_.asScala).toList
          unprocessed match {
            case Nil => Sync[F].unit
            case u   => go(u)
          }
        }
    go(requests)
  }
  def write(ddbName: String): Pipe[F, Chunk[JMap[String, AttributeValue]], Unit] =
    _.evalMap(ch =>
      ch.toList
        .grouped(25) //Dynamodb Batch Write limit
        .toList
        .map(size25 => writeUntilExhausted(ddbName, writeRequests(size25)))
        .sequence
        .void
    )
}

case class DDBEnvironment[F[_]](
  ddbReader: DDBReader[F],
  ddbWriter: DDBWriter[F],
  srcDDB: String,
  destDDB: String,
  scanPageSize: Int
)
object DynamoParallelScan extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    (for {
      ddb <- Interpreter[IO]
              .DynamoDbAsyncClientOpResource(
                DynamoDbAsyncClient
                  .builder()
                  .region(Region.US_EAST_1)
              )
    } yield DDBEnvironment[IO](
      new DDBReader[IO](StreamScan[IO](ddb)),
      new DDBWriter[IO](ddb),
      "copyFromDDB",
      "copyToDDB",
      scanPageSize = 100
    )).use { env =>
      //Here the range of segments processed by this instance is hardcoded, in multi-worker environment the
      // worker id and total segments may be provided as program args
      // having this parameters it is easy to calculate worker specific segment range
      val totalSegments = 40
      (0 until totalSegments)
        .map(segment => program(segment, totalSegments))
        .toList
        .parSequence
        .run(env)
        .as(ExitCode.Success)

    }

  def program(segment: Int, totalSegments: Int): Kleisli[IO, DDBEnvironment[IO], Unit] =
    Kleisli[IO, DDBEnvironment[IO], Unit] { env =>
      env.ddbReader
        .read(segment, totalSegments, env.srcDDB, env.scanPageSize)
        .through(env.ddbWriter.write(env.destDDB))
        .compile
        .drain
    }

}
