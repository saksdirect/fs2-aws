package fs2.aws.dynamodb.stream

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.kinesis.clientlibrary.interfaces._
import com.amazonaws.services.kinesis.clientlibrary.types._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

/** Concrete implementation of the AWS RecordProcessor interface.
  * Wraps incoming records into CommitableRecord types to allow for downstream
  * checkpointing
  *
  *  @constructor create a new instance with a callback function to perform on record receive
  *  @param cb callback function to run on record receive, passing the new CommittableRecord
  */
class RecordProcessor(
  cb: CommittableRecord => Unit,
  terminateGracePeriod: FiniteDuration
) extends v2.IRecordProcessor {
  private var shardId: String                                  = _
  private var extendedSequenceNumber: ExtendedSequenceNumber   = _
  var latestCheckpointer: Option[IRecordProcessorCheckpointer] = None
  var shutdown: Option[ShutdownReason]                         = None

  def isShutdown = shutdown.isDefined

  override def initialize(initializationInput: InitializationInput): Unit = {
    shardId = initializationInput.getShardId
    extendedSequenceNumber = initializationInput.getExtendedSequenceNumber
  }

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    latestCheckpointer = Some(processRecordsInput.getCheckpointer)
    processRecordsInput.getRecords.asScala.foreach { record =>
      cb(
        CommittableRecord(
          shardId,
          extendedSequenceNumber,
          processRecordsInput.getMillisBehindLatest,
          record.asInstanceOf[RecordAdapter],
          recordProcessor = this,
          processRecordsInput.getCheckpointer
        )
      )
    }
  }

  override def shutdown(shutdownInput: ShutdownInput): Unit = {
    shutdown = Some(shutdownInput.getShutdownReason)
    latestCheckpointer = Some(shutdownInput.getCheckpointer)
    shutdownInput.getShutdownReason match {
      case ShutdownReason.TERMINATE =>
        Thread.sleep(terminateGracePeriod.toMillis)
        latestCheckpointer.foreach(_.checkpoint())
      case ShutdownReason.ZOMBIE    => ()
      case ShutdownReason.REQUESTED => ()
    }
  }
}
