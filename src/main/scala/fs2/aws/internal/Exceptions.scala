package fs2
package aws
package internal

object Exceptions {
  case class KinesisCheckpointException(message: String) extends Exception(message)

  sealed class KinesisSettingsException(message: String) extends Exception(message)
  case class BufferSizeException(message: String)        extends KinesisSettingsException(message)
  case class MaxBatchWaitException(message: String)      extends KinesisSettingsException(message)
  case class MaxBatchSizeException(message: String)      extends KinesisSettingsException(message)
}
