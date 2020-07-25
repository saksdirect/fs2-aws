package fs2.aws.dynamodb.stream

import java.util

import cats.effect.Sync
import cats.implicits._
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import org.scanamo.{ DynamoFormat, DynamoObject, DynamoReadError }

package object parsers {
  def parseDynamoEvent[F[_]: Sync, T](
    record: RecordAdapter
  )(implicit df: DynamoFormat[T]): F[DynamoDBBeforeAfter[T]] =
    (record.getInternalObject.getDynamodb.getStreamViewType, record.getInternalObject.getEventName) match {
      case ("NEW_IMAGE" | "NEW_AND_OLD_IMAGES", "INSERT") =>
        parseDynamoRecord(record.getInternalObject.getDynamodb.getNewImage)
          .map(after => Insert(after))
      case ("NEW_AND_OLD_IMAGES", "MODIFY") =>
        for {
          before <- parseDynamoRecord(record.getInternalObject.getDynamodb.getOldImage)
          after  <- parseDynamoRecord(record.getInternalObject.getDynamodb.getNewImage)
        } yield Update(before, after)
      case ("NEW_IMAGE", "MODIFY") =>
        for {
          after <- parseDynamoRecord(record.getInternalObject.getDynamodb.getNewImage)
        } yield Insert(after)
      case ("OLD_IMAGE" | "NEW_AND_OLD_IMAGES", "REMOVE") =>
        parseDynamoRecord(record.getInternalObject.getDynamodb.getOldImage)
          .map(after => Delete(after))
      case ("NEW_IMAGE", "REMOVE") =>
        Sync[F].pure(Unsupported("NEW_IMAGE is not supported with REMOVE"))
      case (viewType, operationType) =>
        Sync[F].raiseError(new RuntimeException(s"$viewType is not supported with $operationType"))
    }

  def parseDynamoRecord[F[_]: Sync, T](
    image: util.Map[String, AttributeValue]
  )(implicit df: DynamoFormat[T]): F[T] =
    df.read(
        DynamoObject(image).toAttributeValue
      )
      .leftMap(e => new RuntimeException(DynamoReadError.describe(e)))
      .liftTo[F]

}
