package io.laserdisc.pure.sqs.tagless

// Library imports
import cats.data.Kleisli
import cats.effect.{ Async, Blocker, ContextShift, Resource }
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder

import java.util.concurrent.CompletionException

// Types referenced
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import java.util.concurrent.CompletableFuture

object Interpreter {

  @deprecated("Use Interpreter[M]. blocker is not needed anymore", "3.2.0")
  def apply[M[_]](b: Blocker)(
    implicit am: Async[M],
    cs: ContextShift[M]
  ): Interpreter[M] =
    new Interpreter[M] {
      val asyncM        = am
      val contextShiftM = cs
    }

  def apply[M[_]](
    implicit am: Async[M],
    cs: ContextShift[M]
  ): Interpreter[M] =
    new Interpreter[M] {
      val asyncM        = am
      val contextShiftM = cs
    }

}

// Family of interpreters into Kleisli arrows for some monad M.
trait Interpreter[M[_]] { outer =>

  implicit val asyncM: Async[M]

  // to support shifting blocking operations to another pool.
  val contextShiftM: ContextShift[M]

  lazy val SqsAsyncClientInterpreter: SqsAsyncClientInterpreter = new SqsAsyncClientInterpreter {}
  // Some methods are common to all interpreters and can be overridden to change behavior globally.

  def primitive[J, A](f: J => A): Kleisli[M, J, A] = Kleisli(a => primitive1(f(a)))

  def primitive1[J, A](f: =>A): M[A] = asyncM.delay(f)

  def eff[J, A](fut: J => CompletableFuture[A]): Kleisli[M, J, A] = Kleisli(a => eff1(fut(a)))

  def eff1[J, A](fut: =>CompletableFuture[A]): M[A] =
    asyncM.guarantee(
      asyncM
        .async[A] { cb =>
          fut.handle[Unit] { (a, x) =>
            if (a == null)
              x match {
                case t: CompletionException => cb(Left(t.getCause))
                case t                      => cb(Left(t))
              }
            else
              cb(Right(a))
          }
          ()
        }
    )(contextShiftM.shift)

  // Interpreters
  trait SqsAsyncClientInterpreter extends SqsAsyncClientOp[Kleisli[M, SqsAsyncClient, *]] {

    // domain-specific operations are implemented in terms of `primitive`
    override def addPermission(a: AddPermissionRequest) = eff(_.addPermission(a))
    override def changeMessageVisibility(a: ChangeMessageVisibilityRequest) =
      eff(_.changeMessageVisibility(a))
    override def changeMessageVisibilityBatch(a: ChangeMessageVisibilityBatchRequest) =
      eff(_.changeMessageVisibilityBatch(a))
    override def close                                            = primitive(_.close)
    override def createQueue(a: CreateQueueRequest)               = eff(_.createQueue(a))
    override def deleteMessage(a: DeleteMessageRequest)           = eff(_.deleteMessage(a))
    override def deleteMessageBatch(a: DeleteMessageBatchRequest) = eff(_.deleteMessageBatch(a))
    override def deleteQueue(a: DeleteQueueRequest)               = eff(_.deleteQueue(a))
    override def getQueueAttributes(a: GetQueueAttributesRequest) = eff(_.getQueueAttributes(a))
    override def getQueueUrl(a: GetQueueUrlRequest)               = eff(_.getQueueUrl(a))
    override def listDeadLetterSourceQueues(a: ListDeadLetterSourceQueuesRequest) =
      eff(_.listDeadLetterSourceQueues(a))
    override def listDeadLetterSourceQueuesPaginator(a: ListDeadLetterSourceQueuesRequest) =
      primitive(_.listDeadLetterSourceQueuesPaginator(a))
    override def listQueueTags(a: ListQueueTagsRequest)           = eff(_.listQueueTags(a))
    override def listQueues                                       = eff(_.listQueues)
    override def listQueues(a: ListQueuesRequest)                 = eff(_.listQueues(a))
    override def listQueuesPaginator                              = primitive(_.listQueuesPaginator)
    override def listQueuesPaginator(a: ListQueuesRequest)        = primitive(_.listQueuesPaginator(a))
    override def purgeQueue(a: PurgeQueueRequest)                 = eff(_.purgeQueue(a))
    override def receiveMessage(a: ReceiveMessageRequest)         = eff(_.receiveMessage(a))
    override def removePermission(a: RemovePermissionRequest)     = eff(_.removePermission(a))
    override def sendMessage(a: SendMessageRequest)               = eff(_.sendMessage(a))
    override def sendMessageBatch(a: SendMessageBatchRequest)     = eff(_.sendMessageBatch(a))
    override def serviceName                                      = primitive(_.serviceName)
    override def setQueueAttributes(a: SetQueueAttributesRequest) = eff(_.setQueueAttributes(a))
    override def tagQueue(a: TagQueueRequest)                     = eff(_.tagQueue(a))
    override def untagQueue(a: UntagQueueRequest)                 = eff(_.untagQueue(a))
    def lens[E](f: E => SqsAsyncClient): SqsAsyncClientOp[Kleisli[M, E, *]] =
      new SqsAsyncClientOp[Kleisli[M, E, *]] {
        override def addPermission(a: AddPermissionRequest) =
          Kleisli(e => eff1(f(e).addPermission(a)))
        override def changeMessageVisibility(a: ChangeMessageVisibilityRequest) =
          Kleisli(e => eff1(f(e).changeMessageVisibility(a)))
        override def changeMessageVisibilityBatch(a: ChangeMessageVisibilityBatchRequest) =
          Kleisli(e => eff1(f(e).changeMessageVisibilityBatch(a)))
        override def close                              = Kleisli(e => primitive1(f(e).close))
        override def createQueue(a: CreateQueueRequest) = Kleisli(e => eff1(f(e).createQueue(a)))
        override def deleteMessage(a: DeleteMessageRequest) =
          Kleisli(e => eff1(f(e).deleteMessage(a)))
        override def deleteMessageBatch(a: DeleteMessageBatchRequest) =
          Kleisli(e => eff1(f(e).deleteMessageBatch(a)))
        override def deleteQueue(a: DeleteQueueRequest) = Kleisli(e => eff1(f(e).deleteQueue(a)))
        override def getQueueAttributes(a: GetQueueAttributesRequest) =
          Kleisli(e => eff1(f(e).getQueueAttributes(a)))
        override def getQueueUrl(a: GetQueueUrlRequest) = Kleisli(e => eff1(f(e).getQueueUrl(a)))
        override def listDeadLetterSourceQueues(a: ListDeadLetterSourceQueuesRequest) =
          Kleisli(e => eff1(f(e).listDeadLetterSourceQueues(a)))
        override def listDeadLetterSourceQueuesPaginator(a: ListDeadLetterSourceQueuesRequest) =
          Kleisli(e => primitive1(f(e).listDeadLetterSourceQueuesPaginator(a)))
        override def listQueueTags(a: ListQueueTagsRequest) =
          Kleisli(e => eff1(f(e).listQueueTags(a)))
        override def listQueues                       = Kleisli(e => eff1(f(e).listQueues))
        override def listQueues(a: ListQueuesRequest) = Kleisli(e => eff1(f(e).listQueues(a)))
        override def listQueuesPaginator              = Kleisli(e => primitive1(f(e).listQueuesPaginator))
        override def listQueuesPaginator(a: ListQueuesRequest) =
          Kleisli(e => primitive1(f(e).listQueuesPaginator(a)))
        override def purgeQueue(a: PurgeQueueRequest) = Kleisli(e => eff1(f(e).purgeQueue(a)))
        override def receiveMessage(a: ReceiveMessageRequest) =
          Kleisli(e => eff1(f(e).receiveMessage(a)))
        override def removePermission(a: RemovePermissionRequest) =
          Kleisli(e => eff1(f(e).removePermission(a)))
        override def sendMessage(a: SendMessageRequest) = Kleisli(e => eff1(f(e).sendMessage(a)))
        override def sendMessageBatch(a: SendMessageBatchRequest) =
          Kleisli(e => eff1(f(e).sendMessageBatch(a)))
        override def serviceName = Kleisli(e => primitive1(f(e).serviceName))
        override def setQueueAttributes(a: SetQueueAttributesRequest) =
          Kleisli(e => eff1(f(e).setQueueAttributes(a)))
        override def tagQueue(a: TagQueueRequest)     = Kleisli(e => eff1(f(e).tagQueue(a)))
        override def untagQueue(a: UntagQueueRequest) = Kleisli(e => eff1(f(e).untagQueue(a)))
      }
  }

  def SqsAsyncClientResource(builder: SqsAsyncClientBuilder): Resource[M, SqsAsyncClient] =
    Resource.fromAutoCloseable(asyncM.delay(builder.build()))
  def SqsAsyncClientOpResource(builder: SqsAsyncClientBuilder) =
    SqsAsyncClientResource(builder).map(create)
  def create(client: SqsAsyncClient): SqsAsyncClientOp[M] = new SqsAsyncClientOp[M] {

    // domain-specific operations are implemented in terms of `primitive`
    override def addPermission(a: AddPermissionRequest) = eff1(client.addPermission(a))
    override def changeMessageVisibility(a: ChangeMessageVisibilityRequest) =
      eff1(client.changeMessageVisibility(a))
    override def changeMessageVisibilityBatch(a: ChangeMessageVisibilityBatchRequest) =
      eff1(client.changeMessageVisibilityBatch(a))
    override def close                                  = primitive1(client.close)
    override def createQueue(a: CreateQueueRequest)     = eff1(client.createQueue(a))
    override def deleteMessage(a: DeleteMessageRequest) = eff1(client.deleteMessage(a))
    override def deleteMessageBatch(a: DeleteMessageBatchRequest) =
      eff1(client.deleteMessageBatch(a))
    override def deleteQueue(a: DeleteQueueRequest) = eff1(client.deleteQueue(a))
    override def getQueueAttributes(a: GetQueueAttributesRequest) =
      eff1(client.getQueueAttributes(a))
    override def getQueueUrl(a: GetQueueUrlRequest) = eff1(client.getQueueUrl(a))
    override def listDeadLetterSourceQueues(a: ListDeadLetterSourceQueuesRequest) =
      eff1(client.listDeadLetterSourceQueues(a))
    override def listDeadLetterSourceQueuesPaginator(a: ListDeadLetterSourceQueuesRequest) =
      primitive1(client.listDeadLetterSourceQueuesPaginator(a))
    override def listQueueTags(a: ListQueueTagsRequest) = eff1(client.listQueueTags(a))
    override def listQueues                             = eff1(client.listQueues)
    override def listQueues(a: ListQueuesRequest)       = eff1(client.listQueues(a))
    override def listQueuesPaginator                    = primitive1(client.listQueuesPaginator)
    override def listQueuesPaginator(a: ListQueuesRequest) =
      primitive1(client.listQueuesPaginator(a))
    override def purgeQueue(a: PurgeQueueRequest)             = eff1(client.purgeQueue(a))
    override def receiveMessage(a: ReceiveMessageRequest)     = eff1(client.receiveMessage(a))
    override def removePermission(a: RemovePermissionRequest) = eff1(client.removePermission(a))
    override def sendMessage(a: SendMessageRequest)           = eff1(client.sendMessage(a))
    override def sendMessageBatch(a: SendMessageBatchRequest) = eff1(client.sendMessageBatch(a))
    override def serviceName                                  = primitive1(client.serviceName)
    override def setQueueAttributes(a: SetQueueAttributesRequest) =
      eff1(client.setQueueAttributes(a))
    override def tagQueue(a: TagQueueRequest)     = eff1(client.tagQueue(a))
    override def untagQueue(a: UntagQueueRequest) = eff1(client.untagQueue(a))

  }

}
