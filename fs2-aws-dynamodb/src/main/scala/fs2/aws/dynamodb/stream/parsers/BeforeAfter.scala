package fs2.aws.dynamodb.stream.parsers

sealed trait DynamoDBBeforeAfter[T]

case class Insert[T](after: T)            extends DynamoDBBeforeAfter[T]
case class Update[T](before: T, after: T) extends DynamoDBBeforeAfter[T]
case class Delete[T](before: T)           extends DynamoDBBeforeAfter[T]
case class Unsupported[T](reason: String) extends DynamoDBBeforeAfter[T]
