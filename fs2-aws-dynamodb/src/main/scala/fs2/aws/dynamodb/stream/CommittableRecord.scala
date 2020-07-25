package fs2.aws.dynamodb.stream

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer

/** A message type from Kinesis which has not yet been commited or checkpointed.
  *
  *  @constructor create a new commitable record with a name and age.
  *  @param shardId the unique identifier for the shard from which this record originated
  *  @param millisBehindLatest ms behind the latest record, used to detect if the consumer is lagging the producer
  *  @param record the original record document from Kinesis
  *  @param recordProcessor reference to the record processor that is responsible for processing this message
  *  @param checkpointer reference to the checkpointer used to commit this record
  */
case class CommittableRecord(
  shardId: String,
  recordProcessorStartingSequenceNumber: ExtendedSequenceNumber,
  millisBehindLatest: Long,
  record: RecordAdapter,
  recordProcessor: RecordProcessor,
  checkpointer: IRecordProcessorCheckpointer
) {
  val sequenceNumber: String = record.getSequenceNumber

  def canCheckpoint(): Boolean = !recordProcessor.isShutdown
  def checkpoint(): Unit       = checkpointer.checkpoint(record)
}

object CommittableRecord {

  implicit val orderBySequenceNumber: Ordering[CommittableRecord] =
    Ordering[String].on(cr ⇒ cr.sequenceNumber)
}
