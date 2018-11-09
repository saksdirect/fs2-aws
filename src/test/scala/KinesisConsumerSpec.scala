package fs2
package aws
package kinesis

import cats.effect.{IO, ContextShift, Timer}
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfterEach}
import org.scalatest.concurrent.Eventually
import org.mockito.Mockito._
import kinesis.kcl._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{
  IRecordProcessorFactory,
  IRecordProcessor
}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{Worker, ShutdownReason}
import com.amazonaws.services.kinesis.clientlibrary.types._
import com.amazonaws.services.kinesis.model.Record

import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.Mockito._

import java.util.Date
import java.nio.ByteBuffer
import java.util.concurrent.Semaphore
import scala.concurrent.Future
import scala.collection.JavaConverters._

class KinesisConsumerSpec extends FlatSpec with Matchers with BeforeAndAfterEach with Eventually {

  implicit val ec: ExecutionContext             = ExecutionContext.global
  implicit val timer: Timer[IO]                 = IO.timer(ec)
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ec)

  "KinesisStreamSettings" should "return a Left if a buffer of 0 is specified" in {
    new WorkerContext() {
      val c = KinesisStreamSettings(bufferSize = 0)
      c.isLeft shouldBe (true)
      c.left.get.getClass.getSimpleName shouldBe ("BufferSizeException")
      c.left.get.getMessage shouldBe ("Must be greater than 0")
    }
  }

  "KinesisCheckpointSettings" should "return a Left if a buffer of 0 is specified" in {
    new WorkerContext() {
      val s = KinesisCheckpointSettings(maxBatchSize = 0, maxBatchWait = 1.second)
      s.left.get.getClass.getSimpleName shouldBe ("MaxBatchSizeException")
      s.left.get.getMessage shouldBe ("Must be greater than 0")

      val s2 = KinesisCheckpointSettings(maxBatchSize = 1, maxBatchWait = 0.second)
      s2.left.get.getClass.getSimpleName shouldBe ("MaxBatchWaitException")
      s2.left.get.getMessage shouldBe ("Must be greater than 0 milliseconds. To checkpoint immediately, pass 1 to the max batch size.")
    }
  }

  "KinesisWorker source" should "successfully read data from the Kinesis stream" in new WorkerContext
  with TestData {
    semaphore.acquire()
    recordProcessor.initialize(initializationInput)
    recordProcessor.processRecords(recordsInput)

    eventually(verify(mockWorker, times(1)).run())

    eventually(timeout(1.second)) {
      val commitableRecord = output.head
      commitableRecord.record.getData should be(record.getData)
      commitableRecord.recordProcessorStartingSequenceNumber shouldBe initializationInput.getExtendedSequenceNumber
      commitableRecord.shardId shouldBe initializationInput.getShardId
      commitableRecord.millisBehindLatest shouldBe recordsInput.getMillisBehindLatest
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
      val record = mock(classOf[Record])
      when(record.getSequenceNumber).thenReturn(i.toString)
      recordProcessor.processRecords(recordsInput.withRecords(List(record).asJava))
    }

    // Should process all 10 messages
    eventually(output.size shouldBe (10))

    // Send a batch that exceeds the internal buffer size
    for (i <- 1 to 50) {
      val record = mock(classOf[Record])
      when(record.getSequenceNumber).thenReturn(i.toString)
      recordProcessor.processRecords(recordsInput.withRecords(List(record).asJava))
    }

    // Should have processed all 60 messages
    eventually(output.size shouldBe (60))

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
      val record = mock(classOf[Record])
      when(record.getSequenceNumber).thenReturn(i.toString)
      recordProcessor.processRecords(recordsInput.withRecords(List(record).asJava))
      recordProcessor2.processRecords(recordsInput.withRecords(List(record).asJava))
    }

    // Should process all 10 messages
    eventually(output.size shouldBe (10))

    // Each shard is assigned its own worker thread, so we get messages
    // from each thread simultaneously.
    def simulateWorkerThread(rp: IRecordProcessor): Future[Unit] = {
      Future {
        for (i <- 1 to 25) { // 10 is a buffer size
          val record = mock(classOf[Record])
          when(record.getSequenceNumber).thenReturn(i.toString)
          rp.processRecords(recordsInput.withRecords(List(record).asJava))
        }
      }
    }

    simulateWorkerThread(recordProcessor)
    simulateWorkerThread(recordProcessor2)

    // Should have processed all 60 messages
    eventually(output.size shouldBe (60))
    semaphore.release()
  }

  "KinesisWorker checkpoint pipe" should "checkpoint batch of records with same sequence number" in new KinesisWorkerCheckpointContext {
    val input = (1 to 3) map { i =>
      val record = mock(classOf[UserRecord])
      when(record.getSequenceNumber).thenReturn("1")
      when(record.getSubSequenceNumber).thenReturn(i.toLong)
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
    val checkpointerShard2 = mock(classOf[IRecordProcessorCheckpointer])

    val input = (1 to 6) map { i =>
      if (i <= 3) {
        val record = mock(classOf[UserRecord])
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
        val record = mock(classOf[UserRecord])
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

  it should "not checkpoint the batch if the IRecordProcessor has been shutdown" in new KinesisWorkerCheckpointContext {
    recordProcessor.shutdown(new ShutdownInput().withShutdownReason(ShutdownReason.TERMINATE))

    val input = (1 to 3) map { i =>
      val record = mock(classOf[UserRecord])
      when(record.getSequenceNumber).thenReturn("1")
      when(record.getSubSequenceNumber).thenReturn(i.toLong)
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

    verifyZeroInteractions(checkpointerShard1)
  }

  it should "fail with Exception if checkpoint action fails" in new KinesisWorkerCheckpointContext {
    val checkpointer = mock(classOf[IRecordProcessorCheckpointer])

    val record = mock(classOf[Record])
    when(record.getSequenceNumber).thenReturn("1")

    val input = new CommittableRecord(
      "shard-1",
      mock(classOf[ExtendedSequenceNumber]),
      1L,
      record,
      recordProcessor,
      checkpointer
    )

    val failure = new RuntimeException()
    when(checkpointer.checkpoint(record)).thenThrow(failure)

    fs2.Stream
      .emits(Seq(input))
      .through(checkpointRecords[IO](settings.right.get))
      .attempt
      .compile
      .toVector
      .unsafeRunSync
      .head
      .isLeft should be(true)

    eventually(verify(checkpointer).checkpoint(input.record))
  }

  "KinesisWorker checkpoint sink" should "checkpoint batch of records with same sequence number" in new KinesisWorkerCheckpointSinkContext {
    val input = (1 to 3) map { i =>
      val record = mock(classOf[UserRecord])
      when(record.getSequenceNumber).thenReturn("1")
      when(record.getSubSequenceNumber).thenReturn(i.toLong)
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

  it should "checkpoint batch of records of different shards" in new KinesisWorkerCheckpointSinkContext {
    val checkpointerShard2 = mock(classOf[IRecordProcessorCheckpointer])

    val input = (1 to 6) map { i =>
      if (i <= 3) {
        val record = mock(classOf[UserRecord])
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
        val record = mock(classOf[UserRecord])
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

  it should "not checkpoint the batch if the IRecordProcessor has been shutdown" in new KinesisWorkerCheckpointSinkContext {
    recordProcessor.shutdown(new ShutdownInput().withShutdownReason(ShutdownReason.TERMINATE))

    val input = (1 to 3) map { i =>
      val record = mock(classOf[UserRecord])
      when(record.getSequenceNumber).thenReturn("1")
      when(record.getSubSequenceNumber).thenReturn(i.toLong)
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

    verifyZeroInteractions(checkpointerShard1)
  }

  it should "fail with Exception if checkpoint action fails" in new KinesisWorkerCheckpointSinkContext {
    val checkpointer = mock(classOf[IRecordProcessorCheckpointer])

    val record = mock(classOf[Record])
    when(record.getSequenceNumber).thenReturn("1")

    val input = new CommittableRecord(
      "shard-1",
      mock(classOf[ExtendedSequenceNumber]),
      1L,
      record,
      recordProcessor,
      checkpointer
    )

    val failure = new RuntimeException()
    when(checkpointer.checkpoint(record)).thenThrow(failure)

    fs2.Stream
      .emits(Seq(input))
      .through(checkpointRecords[IO](settings))
      .attempt
      .compile
      .toVector
      .unsafeRunSync
      .head
      .isLeft should be(true)

    eventually(verify(checkpointer).checkpoint(input.record))
  }

  private abstract class WorkerContext(backpressureTimeout: FiniteDuration = 1.minute,
                                       errorStream: Boolean = false) {

    val semaphore = new Semaphore(1)
    semaphore.acquire()
    var output: List[CommittableRecord] = List()

    protected val mockWorker = mock(classOf[Worker])

    when(mockWorker.run()).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = ()
    })

    when(mockWorker.shutdown()).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = ()
    })

    var recordProcessorFactory: IRecordProcessorFactory = _
    var recordProcessor: IRecordProcessor               = _
    var recordProcessor2: IRecordProcessor              = _

    val builder = { x: IRecordProcessorFactory =>
      recordProcessorFactory = x
      recordProcessor = x.createProcessor()
      recordProcessor2 = x.createProcessor()
      semaphore.release()
      mockWorker
    }

    val config = KinesisStreamSettings(bufferSize = 10)

    val stream =
      readFromKinesisStream[IO](builder, config.right.get)
        .through(_.evalMap(i => IO(output = output :+ i)))
        .map(i => if (errorStream) throw new Exception("boom") else i)
        .compile
        .toVector
        .unsafeRunAsync(_ => ())
  }

  private trait TestData {
    protected val checkpointer = mock(classOf[IRecordProcessorCheckpointer])

    val initializationInput = {
      new InitializationInput()
        .withShardId("shardId")
        .withExtendedSequenceNumber(ExtendedSequenceNumber.AT_TIMESTAMP)
    }
    val record =
      new Record()
        .withApproximateArrivalTimestamp(new Date())
        .withEncryptionType("encryption")
        .withPartitionKey("partitionKey")
        .withSequenceNumber("sequenceNum")
        .withData(ByteBuffer.wrap("test".getBytes))
    val recordsInput =
      new ProcessRecordsInput()
        .withCheckpointer(checkpointer)
        .withMillisBehindLatest(1L)
        .withRecords(List(record).asJava)
  }

  private trait KinesisWorkerCheckpointContext {
    val recordProcessor    = new RecordProcessor(_ => ())
    val checkpointerShard1 = mock(classOf[IRecordProcessorCheckpointer])
    val settings =
      KinesisCheckpointSettings(maxBatchSize = 100, maxBatchWait = 500.millis)

    def startStream(input: Seq[CommittableRecord]) =
      fs2.Stream
        .emits(input)
        .through(checkpointRecords[IO](settings.right.get))
        .compile
        .toVector
        .unsafeRunAsync(_ => ())
  }

  private trait KinesisWorkerCheckpointSinkContext {
    val recordProcessor    = new RecordProcessor(_ => ())
    val checkpointerShard1 = mock(classOf[IRecordProcessorCheckpointer])
    val settings =
      KinesisCheckpointSettings(maxBatchSize = 100, maxBatchWait = 500.millis).right.get

    def startStream(input: Seq[CommittableRecord]) =
      fs2.Stream
        .emits(input)
        .through(checkpointRecords_[IO](settings))
        .compile
        .toVector
        .unsafeRunAsync(_ => ())
  }
}
