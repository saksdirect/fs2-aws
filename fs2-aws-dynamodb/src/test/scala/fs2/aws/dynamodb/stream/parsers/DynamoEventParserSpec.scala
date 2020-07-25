package fs2.aws.dynamodb.stream.parsers

import java.util

import cats.effect.IO
import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue,
  OperationType,
  Record,
  StreamRecord,
  StreamViewType
}
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import io.circe.Json
import org.scalatest.wordspec.AnyWordSpec
import io.github.howardjohn.scanamo.CirceDynamoFormat._
import org.scalatest.matchers.should.Matchers

class DynamoEventParserSpec extends AnyWordSpec with Matchers {
  "Dynamo Event Parser" should {
    "parse insert event type" in {
      val sr = new StreamRecord()
      sr.setStreamViewType(StreamViewType.NEW_IMAGE)
      val newImage = new util.HashMap[String, AttributeValue]()
      newImage.put("name", new AttributeValue().withS("Barry"))
      sr.setNewImage(newImage)
      val r = new Record()
      r.setEventName(OperationType.INSERT)
      r.withDynamodb(sr)
      parseDynamoEvent[IO, Json](new RecordAdapter(r)).unsafeRunSync() should be(
        Insert(Json.obj("name" -> Json.fromString("Barry")))
      )
    }

    "parse modify event type" in {
      val sr = new StreamRecord()
      sr.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
      val oldImage = new util.HashMap[String, AttributeValue]()
      oldImage.put("name", new AttributeValue().withS("Dmytro"))
      sr.setOldImage(oldImage)
      val newImage = new util.HashMap[String, AttributeValue]()
      newImage.put("name", new AttributeValue().withS("Barry"))
      sr.setNewImage(newImage)
      val r = new Record()
      r.setEventName(OperationType.MODIFY)
      r.withDynamodb(sr)
      parseDynamoEvent[IO, Json](new RecordAdapter(r)).unsafeRunSync() should be(
        Update(
          Json.obj("name" -> Json.fromString("Dmytro")),
          Json.obj("name" -> Json.fromString("Barry"))
        )
      )
    }

    "parse delete event type" in {
      val sr = new StreamRecord()
      sr.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
      val oldImage = new util.HashMap[String, AttributeValue]()
      oldImage.put("name", new AttributeValue().withS("Dmytro"))
      sr.setOldImage(oldImage)
      val r = new Record()
      r.setEventName(OperationType.REMOVE)
      r.withDynamodb(sr)
      parseDynamoEvent[IO, Json](new RecordAdapter(r)).unsafeRunSync() should be(
        Delete(
          Json.obj("name" -> Json.fromString("Dmytro"))
        )
      )
    }
    "parse modify event type with NewImage view only as Insert" in {
      val sr = new StreamRecord()
      sr.setStreamViewType(StreamViewType.NEW_IMAGE)
      val newImage = new util.HashMap[String, AttributeValue]()
      newImage.put("name", new AttributeValue().withS("Barry"))
      sr.setNewImage(newImage)
      val r = new Record()
      r.setEventName(OperationType.MODIFY)
      r.withDynamodb(sr)
      parseDynamoEvent[IO, Json](new RecordAdapter(r)).unsafeRunSync() should be(
        Insert(Json.obj("name" -> Json.fromString("Barry")))
      )
    }

    "do not support NEW_IMAGE view type with REMOVE operation type" in {
      val sr = new StreamRecord()
      sr.setStreamViewType(StreamViewType.NEW_IMAGE)
      val oldImage = new util.HashMap[String, AttributeValue]()
      oldImage.put("name", new AttributeValue().withS("Barry"))
      sr.setNewImage(oldImage)
      val r = new Record()
      r.setEventName(OperationType.REMOVE)
      r.withDynamodb(sr)
      parseDynamoEvent[IO, Json](new RecordAdapter(r)).unsafeRunSync() should be(
        Unsupported("NEW_IMAGE is not supported with REMOVE")
      )
    }
  }
}
