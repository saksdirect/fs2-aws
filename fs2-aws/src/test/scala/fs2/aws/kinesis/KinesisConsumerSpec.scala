package fs2
package aws
package kinesis

import java.nio.ByteBuffer
import java.time.Instant
import java.util.concurrent.Semaphore
import cats.effect.{ ContextShift, IO, Timer }
import cats.implicits._
import fs2.aws.kinesis.consumer.{ readFromKinesisStream, _ }
import org.mockito.Mockito._
import org.scalatest.{ BeforeAndAfterEach, Ignore }
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time._
import software.amazon.awssdk.regions.Region
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.{ ShardRecordProcessor, ShardRecordProcessorFactory }
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import org.mockito.ArgumentMatchers.any
import eu.timepit.refined.auto._

import scala.annotation.nowarn

@nowarn
@Ignore
class KinesisConsumerSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach
    with Eventually {

  implicit val ec: ExecutionContext             = ExecutionContext.global
  implicit val timer: Timer[IO]                 = IO.timer(ec)
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ec)

  implicit def sList2jList[A](sList: List[A]): java.util.List[A] = sList.asJava

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(5, Millis)))

  "KinesisWorker source" should "successfully read data from the Kinesis stream" in new WorkerContext
    with TestData {
    val res = (
      stream.take(1).compile.toList,
      IO.delay {
        semaphore.acquire()
        recordProcessor.initialize(initializationInput)
        recordProcessor.processRecords(recordsInput.build())
      }
    ).parMapN { case (msgs, _) => msgs }.unsafeRunSync()

    val commitableRecord = res.head
    commitableRecord.record.data() should be(record.data())
    commitableRecord.recordProcessorStartingSequenceNumber shouldBe initializationInput
      .extendedSequenceNumber()
    commitableRecord.shardId            shouldBe initializationInput.shardId()
    commitableRecord.millisBehindLatest shouldBe recordsInput.build().millisBehindLatest()
  }

  it should "Shutdown the worker if the stream is drained and has not failed" in new WorkerContext
    with TestData {
    (
      stream.take(1).compile.toList,
      IO.delay {
        semaphore.acquire()
        recordProcessor.initialize(initializationInput)
        recordProcessor.processRecords(recordsInput.build())
      }
    ).parMapN { case (_, _) => () }.unsafeRunSync()

    verify(mockScheduler, times(1)).shutdown()
  }

  it should "shutdown the worker if the stream terminates" in new WorkerContext(errorStream = true)
    with TestData {
    intercept[Exception] {
      (
        stream.take(1).compile.toList,
        IO.delay {
          semaphore.acquire()
          recordProcessor.initialize(initializationInput)
          recordProcessor.processRecords(recordsInput.build())
        }
      ).parMapN { case (_, _) => () }.unsafeRunSync()
    }

    verify(mockScheduler, times(1)).shutdown()
  }

  it should "not drop messages in case of back-pressure" in new WorkerContext with TestData {
    // Create and send 10 records (to match buffer size)
    val res = (
      stream.take(10).compile.toList,
      IO.delay {
        semaphore.acquire()
        recordProcessor.initialize(initializationInput)
        for (i <- 1 to 10) {
          val record = mock(classOf[KinesisClientRecord])
          when(record.sequenceNumber()).thenReturn(i.toString)
          recordProcessor.processRecords(recordsInput.records(List(record)).build())
        }
      }
    ).parMapN { case (msgs, _) => msgs }.unsafeRunSync()

    res should have size 10

    // Send a batch that exceeds the internal buffer size
    val res2 = (
      stream.take(50).compile.toList,
      IO.delay {
        semaphore.acquire()
        recordProcessor.initialize(initializationInput)
        for (i <- 1 to 50) {
          val record = mock(classOf[KinesisClientRecord])
          when(record.sequenceNumber()).thenReturn(i.toString)
          recordProcessor.processRecords(recordsInput.records(List(record)).build())
        }
      }
    ).parMapN { case (msgs, _) => msgs }.unsafeRunSync()

    res2 should have size 50
  }

  it should "not drop messages in case of back-pressure with multiple shard workers" in new WorkerContext
    with TestData {

    val res = (
      stream.take(10).compile.toList,
      IO.delay {
        semaphore.acquire()
        recordProcessor.initialize(initializationInput)
        for (i <- 1 to 5) {
          val record = mock(classOf[KinesisClientRecord])
          when(record.sequenceNumber()).thenReturn(i.toString)
          recordProcessor.processRecords(recordsInput.records(List(record)).build())
        }
      },
      IO.delay {
        semaphore.acquire()
        recordProcessor2.initialize(
          InitializationInput
            .builder()
            .shardId("shard2")
            .extendedSequenceNumber(ExtendedSequenceNumber.AT_TIMESTAMP)
            .build()
        )
        // Create and send 10 records (to match buffer size)
        for (i <- 1 to 5) {
          val record = mock(classOf[KinesisClientRecord])
          when(record.sequenceNumber()).thenReturn(i.toString)
          recordProcessor2.processRecords(recordsInput.records(List(record)).build())
        }
      }
    ).parMapN { case (msgs, _, _) => msgs }.unsafeRunSync()

    // Should process all 10 messages
    res should have size 10
  }

  it should "delay the end of shard checkpoint until all messages are drained" in new WorkerContext
    with TestData {
    val nRecords = 5
    val res: Seq[KinesisClientRecord] = (
      stream
        .take(nRecords)
        //emulate message processing latency to reproduce the situation when End of Shard arrives BEFORE
        // all in-flight records are done
        .parEvalMap(3)(msg => IO.sleep(200 millis) >> IO.pure(msg))
        .through(
          checkpointRecords[IO](
            KinesisCheckpointSettings(maxBatchSize = Int.MaxValue, maxBatchWait = 500.millis)
              .getOrElse(throw new Error())
          )
        )
        .compile
        .toList,
      IO.delay {
        semaphore.acquire()
        recordProcessor.initialize(initializationInput)
        (1 to nRecords).foreach { i =>
          val record = mock(classOf[KinesisClientRecord])
          when(record.sequenceNumber()).thenReturn(i.toString)
          recordProcessor.processRecords(
            recordsInput.isAtShardEnd(i == 5).records(List(record)).build()
          )
        }
        //Immediately publish end of shard event
        recordProcessor.shardEnded(
          ShardEndedInput.builder().checkpointer(checkpointer).build()
        )
      }
    ).parMapN { case (msgs, _) => msgs }.unsafeRunSync()

    res should have size 5
  }

  "KinesisWorker checkpoint pipe" should "checkpoint batch of records with same sequence number" in new KinesisWorkerCheckpointContext {
    val lastRecordSemaphore = new Semaphore(1)
    val input = (1 to 3) map { i =>
      val record = mock(classOf[KinesisClientRecord])
      when(record.sequenceNumber()).thenReturn("1")
      when(record.subSequenceNumber()).thenReturn(i.toLong)
      new CommittableRecord(
        "shard-1",
        mock(classOf[ExtendedSequenceNumber]),
        1L,
        record,
        recordProcessor,
        checkpointerShard1,
        lastRecordSemaphore
      )
    }

    startStream(input)

    eventually(timeout(1.second)) {
      verify(checkpointerShard1)
        .checkpoint(input.last.record.sequenceNumber(), input.last.record.subSequenceNumber())
    }
  }

  it should "checkpoint batch of records of different shards" in new KinesisWorkerCheckpointContext {
    val checkpointerShard2 = mock(classOf[ShardRecordProcessorCheckpointer])

    val lastRecordSemaphore = new Semaphore(1)
    val input = (1 to 6) map { i =>
      if (i <= 3) {
        val record = mock(classOf[KinesisClientRecord])
        when(record.sequenceNumber()).thenReturn(i.toString)
        new CommittableRecord(
          "shard-1",
          mock(classOf[ExtendedSequenceNumber]),
          i,
          record,
          recordProcessor,
          checkpointerShard1,
          lastRecordSemaphore
        )
      } else {
        val record = mock(classOf[KinesisClientRecord])
        when(record.sequenceNumber()).thenReturn(i.toString)
        new CommittableRecord(
          "shard-2",
          mock(classOf[ExtendedSequenceNumber]),
          i,
          record,
          recordProcessor,
          checkpointerShard2,
          lastRecordSemaphore
        )
      }
    }

    startStream(input)

    eventually(timeout(3.seconds)) {
      verify(checkpointerShard1)
        .checkpoint(input(2).record.sequenceNumber(), input(2).record.subSequenceNumber())
      verify(checkpointerShard2)
        .checkpoint(input.last.record.sequenceNumber(), input.last.record.subSequenceNumber())
    }

  }

  it should "checkpoint one last time if the RecordProcessor has reached the end of the shard" in new KinesisWorkerCheckpointContext {
    recordProcessor.shardEnded(ShardEndedInput.builder().checkpointer(checkpointerShard1).build())

    val input = (1 to 3) map { i =>
      val record = mock(classOf[KinesisClientRecord])
      when(record.sequenceNumber()).thenReturn("1")
      when(record.subSequenceNumber()).thenReturn(i.toLong)
      new CommittableRecord(
        "shard-1",
        mock(classOf[ExtendedSequenceNumber]),
        1L,
        record,
        recordProcessor,
        checkpointerShard1,
        recordProcessor.lastRecordSemaphore,
        i == 3
      )
    }

    startStream(input)

    verify(checkpointerShard1, times(1)).checkpoint()
  }

  it should "fail with Exception if checkpoint action fails" in new KinesisWorkerCheckpointContext {
    val checkpointer = mock(classOf[ShardRecordProcessorCheckpointer])

    val lastRecordSemaphore = new Semaphore(1)
    val record              = mock(classOf[KinesisClientRecord])
    when(record.sequenceNumber()).thenReturn("1")

    val input = new CommittableRecord(
      "shard-1",
      mock(classOf[ExtendedSequenceNumber]),
      1L,
      record,
      recordProcessor,
      checkpointer,
      lastRecordSemaphore
    )

    val failure = new RuntimeException("you have no power here")
    when(checkpointer.checkpoint(record.sequenceNumber, record.subSequenceNumber))
      .thenThrow(failure)

    the[RuntimeException] thrownBy fs2.Stream
      .emits(Seq(input))
      .through(checkpointRecords[IO](settings))
      .compile
      .toVector
      .unsafeRunSync() should have message "you have no power here"

    eventually(
      verify(checkpointer)
        .checkpoint(input.record.sequenceNumber(), input.record.subSequenceNumber())
    )
  }

  it should "bypass all items when checkpoint" in new KinesisWorkerCheckpointContext {
    val checkpointer = mock(classOf[ShardRecordProcessorCheckpointer])

    val lastRecordSemaphore = new Semaphore(1)
    val record              = mock(classOf[KinesisClientRecord])
    when(record.sequenceNumber()).thenReturn("1")

    val input = (1 to 100).map(idx =>
      new CommittableRecord(
        s"shard-1",
        mock(classOf[ExtendedSequenceNumber]),
        idx,
        record,
        recordProcessor,
        checkpointer,
        lastRecordSemaphore
      )
    )

    fs2.Stream
      .emits(input)
      .through(checkpointRecords[IO](settings))
      .compile
      .toVector
      .unsafeRunSync() should have size 100
  }

  abstract private class WorkerContext(errorStream: Boolean = false) {

    val semaphore = new Semaphore(1)
    semaphore.acquire()
    var output = List.newBuilder[CommittableRecord]

    protected val mockScheduler: Scheduler = mock(classOf[Scheduler])

    doAnswer(_ => println("running kinesis scheduler")).when(mockScheduler).run()

    doAnswer(_ => ()).when(mockScheduler).shutdown()

    var recordProcessorFactory: ShardRecordProcessorFactory = _
    var recordProcessor: ShardRecordProcessor               = _
    var recordProcessor2: ShardRecordProcessor              = _

    val builder = { x: ShardRecordProcessorFactory =>
      recordProcessorFactory = x
      recordProcessor = x.shardRecordProcessor()
      semaphore.release()
      recordProcessor2 = x.shardRecordProcessor()
      semaphore.release()
      mockScheduler
    }
    val config =
      KinesisConsumerSettings("testStream", "testApp", Region.US_EAST_1, 10)

    val stream: fs2.Stream[IO, CommittableRecord] =
      readFromKinesisStream[IO](config, builder).map(i =>
        if (errorStream) throw new Exception("boom") else i
      )
  }

  private trait TestData {
    @volatile var endOfShardSeen = false

    protected val checkpointer: ShardRecordProcessorCheckpointer = mock(
      classOf[ShardRecordProcessorCheckpointer]
    )

    doAnswer { _ =>
      endOfShardSeen = true
      null
    }.when(checkpointer).checkpoint()

    doAnswer { _ =>
      if (endOfShardSeen) throw new Exception("Checkpointing after End Of Shard")
      null
    }.when(checkpointer).checkpoint(any[String], any[Long])

    val initializationInput = {
      InitializationInput
        .builder()
        .shardId("shardId")
        .extendedSequenceNumber(ExtendedSequenceNumber.AT_TIMESTAMP)
        .build()
    }

    val record: KinesisClientRecord =
      KinesisClientRecord
        .builder()
        .approximateArrivalTimestamp(Instant.now())
        .partitionKey("partitionKey")
        .sequenceNumber("sequenceNum")
        .data(ByteBuffer.wrap("test".getBytes))
        .build()

    val recordsInput =
      ProcessRecordsInput
        .builder()
        .checkpointer(checkpointer)
        .millisBehindLatest(1L)
        .records(List(record).asJava)
  }

  private trait KinesisWorkerCheckpointContext {
    val recordProcessor    = new ChunkedRecordProcessor(_ => ())
    val checkpointerShard1 = mock(classOf[ShardRecordProcessorCheckpointer])
    val settings =
      KinesisCheckpointSettings(maxBatchSize = Int.MaxValue, maxBatchWait = 500.millis)
        .getOrElse(throw new Error())

    def startStream(input: Seq[CommittableRecord]): Seq[KinesisClientRecord] =
      fs2.Stream
        .emits(input)
        .through(checkpointRecords[IO](settings))
        .compile
        .toList
        .unsafeRunSync()
  }
}
