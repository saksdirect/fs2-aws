import cats.effect.{ ExitCode, IO, IOApp }
import fs2.aws.dynamodb.stream._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import io.github.howardjohn.scanamo.CirceDynamoFormat._

object DynamoDBStreamer extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    for {
      implicit0(logger: SelfAwareStructuredLogger[IO]) <- Slf4jLogger.fromName[IO]("example")
      _ <- readFromDynamDBStream[IO](
              "dynamo_db_example",
              "arn:aws:dynamodb:us-east-1:023006903388:table/nightly-sync-events-Production/stream/2020-01-27T22:49:13.204"
            )
            .evalMap(cr => parsers.parseDynamoEvent[IO, Json](cr.record))
            .evalTap(msg => logger.info(s"received $msg"))
            .compile
            .drain
    } yield ExitCode.Success
}
