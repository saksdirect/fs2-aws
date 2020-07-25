package fs2.aws.dynamodb.stream

import java.util.Date
import java.util.concurrent.Semaphore

import cats.effect.{ ContextShift, IO, Timer }
import com.amazonaws.services.dynamodbv2.model
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, StreamRecord }
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{
  IRecordProcessor,
  IRecordProcessorFactory
}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{ ShutdownReason, Worker }
import com.amazonaws.services.kinesis.clientlibrary.types._
import com.amazonaws.services.kinesis.model.Record
import fs2.aws.dynamodb._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time._

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class DynamoDBConsumerSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach
    with Eventually {

  implicit val ec: ExecutionContext             = ExecutionContext.global
  implicit val timer: Timer[IO]                 = IO.timer(ec)
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ec)

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(5, Millis)))

  "KinesisWorker source" should "successfully read data from the Kinesis stream" in new WorkerContext
    with TestData {
    semaphore.acquire()
    recordProcessor.initialize(initializationInput)
    recordProcessor.processRecords(recordsInput)

    eventually(verify(mockWorker, times(1)).run())

    eventually(timeout(1.second)) {
      val commitableRecord = output.result().head
      commitableRecord.record.getData                        should be(record.getData)
      commitableRecord.recordProcessorStartingSequenceNumber shouldBe initializationInput.getExtendedSequenceNumber
      commitableRecord.shardId                               shouldBe initializationInput.getShardId
      commitableRecord.millisBehindLatest                    shouldBe recordsInput.getMillisBehindLatest
    }
    semaphore.release()
  }

  it should "not shutdown the worker if the stream is drained but has not failed" in new WorkerContext
    with TestData {
    semaphore.acquire()
    recordProcessor.initialize(initializationInput)
    recordProcessor.processRecords(recordsInput)

    eventually(verify(mockWorker, times(0)).shutdown())
    semaphore.release()
  }

  it should "shutdown the worker if the stream terminates" in new WorkerContext(errorStream = true)
    with TestData {
    semaphore.acquire()
    recordProcessor.initialize(initializationInput)
    recordProcessor.processRecords(recordsInput)

    eventually(verify(mockWorker, times(1)).shutdown())
  }

  it should "not drop messages in case of back-pressure" in new WorkerContext with TestData {
    semaphore.acquire()
    // Create and send 10 records (to match buffer size)
    for (i <- 1 to 10) {
      val record: Record = mock(classOf[RecordAdapter])
      when(record.getSequenceNumber).thenReturn(i.toString)
      recordProcessor.processRecords(recordsInput.withRecords(List(record).asJava))
    }

    // Should process all 10 messages
    eventually(output.result().size shouldBe (10))

    // Send a batch that exceeds the internal buffer size
    for (i <- 1 to 50) {
      val record: Record = mock(classOf[RecordAdapter])
      when(record.getSequenceNumber).thenReturn(i.toString)
      recordProcessor.processRecords(recordsInput.withRecords(List(record).asJava))
    }

    // Should have processed all 60 messages
    eventually(output.result().size shouldBe (60))

    eventually(verify(mockWorker, times(0)).shutdown())
    semaphore.release()
  }

  it should "not drop messages in case of back-pressure with multiple shard workers" in new WorkerContext
    with TestData {
    semaphore.acquire()
    recordProcessor.initialize(initializationInput)
    recordProcessor2.initialize(initializationInput.withShardId("shard2"))

    // Create and send 10 records (to match buffer size)
    for (i <- 1 to 5) {
      val record: Record = mock(classOf[RecordAdapter])
      when(record.getSequenceNumber).thenReturn(i.toString)
      recordProcessor.processRecords(recordsInput.withRecords(List(record).asJava))
      recordProcessor2.processRecords(recordsInput.withRecords(List(record).asJava))
    }

    // Should process all 10 messages
    eventually(output.result().size shouldBe (10))

    // Each shard is assigned its own worker thread, so we get messages
    // from each thread simultaneously.
    def simulateWorkerThread(rp: IRecordProcessor): Future[Unit] =
      Future {
        for (i <- 1 to 25) { // 10 is a buffer size
          val record: Record = mock(classOf[RecordAdapter])
          when(record.getSequenceNumber).thenReturn(i.toString)
          rp.processRecords(recordsInput.withRecords(List(record).asJava))
        }
      }

    simulateWorkerThread(recordProcessor)
    simulateWorkerThread(recordProcessor2)

    // Should have processed all 60 messages
    eventually(output.result().size shouldBe (60))
    semaphore.release()
  }

  "KinesisWorker checkpoint pipe" should "checkpoint batch of records with same sequence number" in new KinesisWorkerCheckpointContext {
    val input: immutable.IndexedSeq[CommittableRecord] = (1 to 3) map { i =>
      val record = mock(classOf[RecordAdapter])
      when(record.getSequenceNumber).thenReturn(i.toString)
      new CommittableRecord(
        "shard-1",
        mock(classOf[ExtendedSequenceNumber]),
        1L,
        record,
        recordProcessor,
        checkpointerShard1
      )
    }

    startStream(input)

    eventually(timeout(1.second)) {
      verify(checkpointerShard1).checkpoint(input.last.record)
    }
  }

  it should "checkpoint batch of records of different shards" in new KinesisWorkerCheckpointContext {
    val checkpointerShard2: IRecordProcessorCheckpointer =
      mock(classOf[IRecordProcessorCheckpointer])

    val input: immutable.IndexedSeq[CommittableRecord] = (1 to 6) map { i =>
      if (i <= 3) {
        val record = mock(classOf[RecordAdapter])
        when(record.getSequenceNumber).thenReturn(i.toString)
        new CommittableRecord(
          "shard-1",
          mock(classOf[ExtendedSequenceNumber]),
          i,
          record,
          recordProcessor,
          checkpointerShard1
        )
      } else {
        val record = mock(classOf[RecordAdapter])
        when(record.getSequenceNumber).thenReturn(i.toString)
        new CommittableRecord(
          "shard-2",
          mock(classOf[ExtendedSequenceNumber]),
          i,
          record,
          recordProcessor,
          checkpointerShard2
        )
      }
    }

    startStream(input)

    eventually(timeout(3.seconds)) {
      verify(checkpointerShard1).checkpoint(input(2).record)
      verify(checkpointerShard2).checkpoint(input.last.record)
    }

  }

  it should "not checkpoint the batch if the IRecordProcessor has been shutdown with ZOMBIE reason" in new KinesisWorkerCheckpointContext {
    recordProcessor.shutdown(
      new ShutdownInput()
        .withShutdownReason(ShutdownReason.ZOMBIE)
        .withCheckpointer(checkpointerShard1)
    )

    val input: immutable.IndexedSeq[CommittableRecord] = (1 to 3) map { i =>
      val record = mock(classOf[RecordAdapter])
      when(record.getSequenceNumber).thenReturn("1")
      new CommittableRecord(
        "shard-1",
        mock(classOf[ExtendedSequenceNumber]),
        1L,
        record,
        recordProcessor,
        checkpointerShard1
      )
    }

    startStream(input)

    verify(checkpointerShard1, times(0)).checkpoint()
  }

  it should "checkpoint one last time if the IRecordProcessor has been shutdown with TERMINATE reason" in new KinesisWorkerCheckpointContext {
    recordProcessor.shutdown(
      new ShutdownInput()
        .withShutdownReason(ShutdownReason.TERMINATE)
        .withCheckpointer(checkpointerShard1)
    )

    val input: immutable.IndexedSeq[CommittableRecord] = (1 to 3) map { i =>
      val record = mock(classOf[RecordAdapter])
      when(record.getSequenceNumber).thenReturn("1")
      new CommittableRecord(
        "shard-1",
        mock(classOf[ExtendedSequenceNumber]),
        1L,
        record,
        recordProcessor,
        checkpointerShard1
      )
    }

    startStream(input)

    verify(checkpointerShard1, times(1)).checkpoint()
  }

  it should "fail with Exception if checkpoint action fails" in new KinesisWorkerCheckpointContext {
    val checkpointer: IRecordProcessorCheckpointer = mock(classOf[IRecordProcessorCheckpointer])

    val record: RecordAdapter = mock(classOf[RecordAdapter])
    when(record.getSequenceNumber).thenReturn("1")

    val input = new CommittableRecord(
      "shard-1",
      mock(classOf[ExtendedSequenceNumber]),
      1L,
      record,
      recordProcessor,
      checkpointer
    )

    val failure = new RuntimeException("you have no power here")
    when(checkpointer.checkpoint(record)).thenThrow(failure)

    the[RuntimeException] thrownBy fs2.Stream
      .emits(Seq(input))
      .through(checkpointRecords[IO](settings))
      .compile
      .toVector
      .unsafeRunSync should have message "you have no power here"

    eventually(verify(checkpointer).checkpoint(input.record))
  }

  abstract private class WorkerContext(
    backpressureTimeout: FiniteDuration = 1.minute,
    errorStream: Boolean = false
  ) {

    val semaphore = new Semaphore(1)
    semaphore.acquire()
    var output = List.newBuilder[CommittableRecord]

    protected val mockWorker: Worker = mock(classOf[Worker])

    when(mockWorker.run()).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = ()
    })

    when(mockWorker.shutdown()).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = ()
    })

    var recordProcessorFactory: IRecordProcessorFactory = _
    var recordProcessor: IRecordProcessor               = _
    var recordProcessor2: IRecordProcessor              = _

    val builder: IRecordProcessorFactory => Worker = { x: IRecordProcessorFactory =>
      recordProcessorFactory = x
      recordProcessor = x.createProcessor()
      recordProcessor2 = x.createProcessor()
      semaphore.release()
      mockWorker
    }

    val config: KinesisStreamSettings = KinesisStreamSettings(bufferSize = 10, 10.seconds).right.get

    val stream: Unit =
      readFromDynamoDBStream[IO](builder, config)
        .through(_.evalMap(i => IO.delay(output += i)))
        .map(i => if (errorStream) throw new Exception("boom") else i)
        .compile
        .toVector
        .unsafeRunAsync(_ => ())
  }

  private trait TestData {
    protected val checkpointer: IRecordProcessorCheckpointer = mock(
      classOf[IRecordProcessorCheckpointer]
    )

    val initializationInput: InitializationInput = {
      new InitializationInput()
        .withShardId("shardId")
        .withExtendedSequenceNumber(ExtendedSequenceNumber.AT_TIMESTAMP)
    }
    val record: Record =
      new RecordAdapter(
        new model.Record()
          .withDynamodb(new StreamRecord().addNewImageEntry("name", new AttributeValue("Barry")))
      ).withApproximateArrivalTimestamp(new Date())
        .withEncryptionType("encryption")

    val recordsInput: ProcessRecordsInput =
      new ProcessRecordsInput()
        .withCheckpointer(checkpointer)
        .withMillisBehindLatest(1L)
        .withRecords(List(record).asJava)
  }

  private trait KinesisWorkerCheckpointContext {
    val recordProcessor = new RecordProcessor(_ => (), 1.seconds)
    val checkpointerShard1: IRecordProcessorCheckpointer = mock(
      classOf[IRecordProcessorCheckpointer]
    )
    val settings: KinesisCheckpointSettings =
      KinesisCheckpointSettings(maxBatchSize = 100, maxBatchWait = 500.millis).right.get

    def startStream(input: Seq[CommittableRecord]): Unit =
      fs2.Stream
        .emits(input)
        .through(checkpointRecords[IO](settings))
        .compile
        .toVector
        .unsafeRunAsync(_ => ())
  }
}
