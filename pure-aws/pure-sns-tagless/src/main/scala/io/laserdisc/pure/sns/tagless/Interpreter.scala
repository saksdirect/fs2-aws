package io.laserdisc.pure.sns.tagless

// Library imports
import cats.data.Kleisli
import cats.effect.{ Async, Blocker, ContextShift, Resource }
import software.amazon.awssdk.services.sns.SnsAsyncClientBuilder

import java.util.concurrent.CompletionException

// Types referenced
import software.amazon.awssdk.services.sns.SnsAsyncClient
import software.amazon.awssdk.services.sns.model.{
  AddPermissionRequest,
  CheckIfPhoneNumberIsOptedOutRequest,
  ConfirmSubscriptionRequest,
  CreatePlatformApplicationRequest,
  CreatePlatformEndpointRequest,
  CreateTopicRequest,
  DeleteEndpointRequest,
  DeletePlatformApplicationRequest,
  DeleteTopicRequest,
  GetEndpointAttributesRequest,
  GetPlatformApplicationAttributesRequest,
  GetSmsAttributesRequest,
  GetSubscriptionAttributesRequest,
  GetTopicAttributesRequest,
  ListEndpointsByPlatformApplicationRequest,
  ListPhoneNumbersOptedOutRequest,
  ListPlatformApplicationsRequest,
  ListSubscriptionsByTopicRequest,
  ListSubscriptionsRequest,
  ListTagsForResourceRequest,
  ListTopicsRequest,
  OptInPhoneNumberRequest,
  PublishRequest,
  RemovePermissionRequest,
  SetEndpointAttributesRequest,
  SetPlatformApplicationAttributesRequest,
  SetSmsAttributesRequest,
  SetSubscriptionAttributesRequest,
  SetTopicAttributesRequest,
  SubscribeRequest,
  TagResourceRequest,
  UnsubscribeRequest,
  UntagResourceRequest
}

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

  lazy val SnsAsyncClientInterpreter: SnsAsyncClientInterpreter = new SnsAsyncClientInterpreter {}
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
  trait SnsAsyncClientInterpreter extends SnsAsyncClientOp[Kleisli[M, SnsAsyncClient, *]] {

    // domain-specific operations are implemented in terms of `primitive`
    override def addPermission(a: AddPermissionRequest) = eff(_.addPermission(a))
    override def checkIfPhoneNumberIsOptedOut(a: CheckIfPhoneNumberIsOptedOutRequest) =
      eff(_.checkIfPhoneNumberIsOptedOut(a))
    override def close                                              = primitive(_.close)
    override def confirmSubscription(a: ConfirmSubscriptionRequest) = eff(_.confirmSubscription(a))
    override def createPlatformApplication(a: CreatePlatformApplicationRequest) =
      eff(_.createPlatformApplication(a))
    override def createPlatformEndpoint(a: CreatePlatformEndpointRequest) =
      eff(_.createPlatformEndpoint(a))
    override def createTopic(a: CreateTopicRequest)       = eff(_.createTopic(a))
    override def deleteEndpoint(a: DeleteEndpointRequest) = eff(_.deleteEndpoint(a))
    override def deletePlatformApplication(a: DeletePlatformApplicationRequest) =
      eff(_.deletePlatformApplication(a))
    override def deleteTopic(a: DeleteTopicRequest) = eff(_.deleteTopic(a))
    override def getEndpointAttributes(a: GetEndpointAttributesRequest) =
      eff(_.getEndpointAttributes(a))
    override def getPlatformApplicationAttributes(a: GetPlatformApplicationAttributesRequest) =
      eff(_.getPlatformApplicationAttributes(a))
    override def getSMSAttributes                             = eff(_.getSMSAttributes)
    override def getSMSAttributes(a: GetSmsAttributesRequest) = eff(_.getSMSAttributes(a))
    override def getSubscriptionAttributes(a: GetSubscriptionAttributesRequest) =
      eff(_.getSubscriptionAttributes(a))
    override def getTopicAttributes(a: GetTopicAttributesRequest) = eff(_.getTopicAttributes(a))
    override def listEndpointsByPlatformApplication(a: ListEndpointsByPlatformApplicationRequest) =
      eff(_.listEndpointsByPlatformApplication(a))
    override def listEndpointsByPlatformApplicationPaginator(
      a: ListEndpointsByPlatformApplicationRequest
    )                                     = primitive(_.listEndpointsByPlatformApplicationPaginator(a))
    override def listPhoneNumbersOptedOut = eff(_.listPhoneNumbersOptedOut)
    override def listPhoneNumbersOptedOut(a: ListPhoneNumbersOptedOutRequest) =
      eff(_.listPhoneNumbersOptedOut(a))
    override def listPlatformApplications = eff(_.listPlatformApplications)
    override def listPlatformApplications(a: ListPlatformApplicationsRequest) =
      eff(_.listPlatformApplications(a))
    override def listPlatformApplicationsPaginator = primitive(_.listPlatformApplicationsPaginator)
    override def listPlatformApplicationsPaginator(a: ListPlatformApplicationsRequest) =
      primitive(_.listPlatformApplicationsPaginator(a))
    override def listSubscriptions                              = eff(_.listSubscriptions)
    override def listSubscriptions(a: ListSubscriptionsRequest) = eff(_.listSubscriptions(a))
    override def listSubscriptionsByTopic(a: ListSubscriptionsByTopicRequest) =
      eff(_.listSubscriptionsByTopic(a))
    override def listSubscriptionsByTopicPaginator(a: ListSubscriptionsByTopicRequest) =
      primitive(_.listSubscriptionsByTopicPaginator(a))
    override def listSubscriptionsPaginator = primitive(_.listSubscriptionsPaginator)
    override def listSubscriptionsPaginator(a: ListSubscriptionsRequest) =
      primitive(_.listSubscriptionsPaginator(a))
    override def listTagsForResource(a: ListTagsForResourceRequest) = eff(_.listTagsForResource(a))
    override def listTopics                                         = eff(_.listTopics)
    override def listTopics(a: ListTopicsRequest)                   = eff(_.listTopics(a))
    override def listTopicsPaginator                                = primitive(_.listTopicsPaginator)
    override def listTopicsPaginator(a: ListTopicsRequest)          = primitive(_.listTopicsPaginator(a))
    override def optInPhoneNumber(a: OptInPhoneNumberRequest)       = eff(_.optInPhoneNumber(a))
    override def publish(a: PublishRequest)                         = eff(_.publish(a))
    override def removePermission(a: RemovePermissionRequest)       = eff(_.removePermission(a))
    override def serviceName                                        = primitive(_.serviceName)
    override def setEndpointAttributes(a: SetEndpointAttributesRequest) =
      eff(_.setEndpointAttributes(a))
    override def setPlatformApplicationAttributes(a: SetPlatformApplicationAttributesRequest) =
      eff(_.setPlatformApplicationAttributes(a))
    override def setSMSAttributes(a: SetSmsAttributesRequest) = eff(_.setSMSAttributes(a))
    override def setSubscriptionAttributes(a: SetSubscriptionAttributesRequest) =
      eff(_.setSubscriptionAttributes(a))
    override def setTopicAttributes(a: SetTopicAttributesRequest) = eff(_.setTopicAttributes(a))
    override def subscribe(a: SubscribeRequest)                   = eff(_.subscribe(a))
    override def tagResource(a: TagResourceRequest)               = eff(_.tagResource(a))
    override def unsubscribe(a: UnsubscribeRequest)               = eff(_.unsubscribe(a))
    override def untagResource(a: UntagResourceRequest)           = eff(_.untagResource(a))
    def lens[E](f: E => SnsAsyncClient): SnsAsyncClientOp[Kleisli[M, E, *]] =
      new SnsAsyncClientOp[Kleisli[M, E, *]] {
        override def addPermission(a: AddPermissionRequest) =
          Kleisli(e => eff1(f(e).addPermission(a)))
        override def checkIfPhoneNumberIsOptedOut(a: CheckIfPhoneNumberIsOptedOutRequest) =
          Kleisli(e => eff1(f(e).checkIfPhoneNumberIsOptedOut(a)))
        override def close = Kleisli(e => primitive1(f(e).close))
        override def confirmSubscription(a: ConfirmSubscriptionRequest) =
          Kleisli(e => eff1(f(e).confirmSubscription(a)))
        override def createPlatformApplication(a: CreatePlatformApplicationRequest) =
          Kleisli(e => eff1(f(e).createPlatformApplication(a)))
        override def createPlatformEndpoint(a: CreatePlatformEndpointRequest) =
          Kleisli(e => eff1(f(e).createPlatformEndpoint(a)))
        override def createTopic(a: CreateTopicRequest) = Kleisli(e => eff1(f(e).createTopic(a)))
        override def deleteEndpoint(a: DeleteEndpointRequest) =
          Kleisli(e => eff1(f(e).deleteEndpoint(a)))
        override def deletePlatformApplication(a: DeletePlatformApplicationRequest) =
          Kleisli(e => eff1(f(e).deletePlatformApplication(a)))
        override def deleteTopic(a: DeleteTopicRequest) = Kleisli(e => eff1(f(e).deleteTopic(a)))
        override def getEndpointAttributes(a: GetEndpointAttributesRequest) =
          Kleisli(e => eff1(f(e).getEndpointAttributes(a)))
        override def getPlatformApplicationAttributes(a: GetPlatformApplicationAttributesRequest) =
          Kleisli(e => eff1(f(e).getPlatformApplicationAttributes(a)))
        override def getSMSAttributes = Kleisli(e => eff1(f(e).getSMSAttributes))
        override def getSMSAttributes(a: GetSmsAttributesRequest) =
          Kleisli(e => eff1(f(e).getSMSAttributes(a)))
        override def getSubscriptionAttributes(a: GetSubscriptionAttributesRequest) =
          Kleisli(e => eff1(f(e).getSubscriptionAttributes(a)))
        override def getTopicAttributes(a: GetTopicAttributesRequest) =
          Kleisli(e => eff1(f(e).getTopicAttributes(a)))
        override def listEndpointsByPlatformApplication(
          a: ListEndpointsByPlatformApplicationRequest
        ) = Kleisli(e => eff1(f(e).listEndpointsByPlatformApplication(a)))
        override def listEndpointsByPlatformApplicationPaginator(
          a: ListEndpointsByPlatformApplicationRequest
        )                                     = Kleisli(e => primitive1(f(e).listEndpointsByPlatformApplicationPaginator(a)))
        override def listPhoneNumbersOptedOut = Kleisli(e => eff1(f(e).listPhoneNumbersOptedOut))
        override def listPhoneNumbersOptedOut(a: ListPhoneNumbersOptedOutRequest) =
          Kleisli(e => eff1(f(e).listPhoneNumbersOptedOut(a)))
        override def listPlatformApplications = Kleisli(e => eff1(f(e).listPlatformApplications))
        override def listPlatformApplications(a: ListPlatformApplicationsRequest) =
          Kleisli(e => eff1(f(e).listPlatformApplications(a)))
        override def listPlatformApplicationsPaginator =
          Kleisli(e => primitive1(f(e).listPlatformApplicationsPaginator))
        override def listPlatformApplicationsPaginator(a: ListPlatformApplicationsRequest) =
          Kleisli(e => primitive1(f(e).listPlatformApplicationsPaginator(a)))
        override def listSubscriptions = Kleisli(e => eff1(f(e).listSubscriptions))
        override def listSubscriptions(a: ListSubscriptionsRequest) =
          Kleisli(e => eff1(f(e).listSubscriptions(a)))
        override def listSubscriptionsByTopic(a: ListSubscriptionsByTopicRequest) =
          Kleisli(e => eff1(f(e).listSubscriptionsByTopic(a)))
        override def listSubscriptionsByTopicPaginator(a: ListSubscriptionsByTopicRequest) =
          Kleisli(e => primitive1(f(e).listSubscriptionsByTopicPaginator(a)))
        override def listSubscriptionsPaginator =
          Kleisli(e => primitive1(f(e).listSubscriptionsPaginator))
        override def listSubscriptionsPaginator(a: ListSubscriptionsRequest) =
          Kleisli(e => primitive1(f(e).listSubscriptionsPaginator(a)))
        override def listTagsForResource(a: ListTagsForResourceRequest) =
          Kleisli(e => eff1(f(e).listTagsForResource(a)))
        override def listTopics                       = Kleisli(e => eff1(f(e).listTopics))
        override def listTopics(a: ListTopicsRequest) = Kleisli(e => eff1(f(e).listTopics(a)))
        override def listTopicsPaginator              = Kleisli(e => primitive1(f(e).listTopicsPaginator))
        override def listTopicsPaginator(a: ListTopicsRequest) =
          Kleisli(e => primitive1(f(e).listTopicsPaginator(a)))
        override def optInPhoneNumber(a: OptInPhoneNumberRequest) =
          Kleisli(e => eff1(f(e).optInPhoneNumber(a)))
        override def publish(a: PublishRequest) = Kleisli(e => eff1(f(e).publish(a)))
        override def removePermission(a: RemovePermissionRequest) =
          Kleisli(e => eff1(f(e).removePermission(a)))
        override def serviceName = Kleisli(e => primitive1(f(e).serviceName))
        override def setEndpointAttributes(a: SetEndpointAttributesRequest) =
          Kleisli(e => eff1(f(e).setEndpointAttributes(a)))
        override def setPlatformApplicationAttributes(a: SetPlatformApplicationAttributesRequest) =
          Kleisli(e => eff1(f(e).setPlatformApplicationAttributes(a)))
        override def setSMSAttributes(a: SetSmsAttributesRequest) =
          Kleisli(e => eff1(f(e).setSMSAttributes(a)))
        override def setSubscriptionAttributes(a: SetSubscriptionAttributesRequest) =
          Kleisli(e => eff1(f(e).setSubscriptionAttributes(a)))
        override def setTopicAttributes(a: SetTopicAttributesRequest) =
          Kleisli(e => eff1(f(e).setTopicAttributes(a)))
        override def subscribe(a: SubscribeRequest)     = Kleisli(e => eff1(f(e).subscribe(a)))
        override def tagResource(a: TagResourceRequest) = Kleisli(e => eff1(f(e).tagResource(a)))
        override def unsubscribe(a: UnsubscribeRequest) = Kleisli(e => eff1(f(e).unsubscribe(a)))
        override def untagResource(a: UntagResourceRequest) =
          Kleisli(e => eff1(f(e).untagResource(a)))
      }
  }

  def SnsAsyncClientResource(builder: SnsAsyncClientBuilder): Resource[M, SnsAsyncClient] =
    Resource.fromAutoCloseable(asyncM.delay(builder.build()))
  def SnsAsyncClientOpResource(builder: SnsAsyncClientBuilder) =
    SnsAsyncClientResource(builder).map(create)
  def create(client: SnsAsyncClient): SnsAsyncClientOp[M] = new SnsAsyncClientOp[M] {

    // domain-specific operations are implemented in terms of `primitive`
    override def addPermission(a: AddPermissionRequest) = eff1(client.addPermission(a))
    override def checkIfPhoneNumberIsOptedOut(a: CheckIfPhoneNumberIsOptedOutRequest) =
      eff1(client.checkIfPhoneNumberIsOptedOut(a))
    override def close = primitive1(client.close)
    override def confirmSubscription(a: ConfirmSubscriptionRequest) =
      eff1(client.confirmSubscription(a))
    override def createPlatformApplication(a: CreatePlatformApplicationRequest) =
      eff1(client.createPlatformApplication(a))
    override def createPlatformEndpoint(a: CreatePlatformEndpointRequest) =
      eff1(client.createPlatformEndpoint(a))
    override def createTopic(a: CreateTopicRequest)       = eff1(client.createTopic(a))
    override def deleteEndpoint(a: DeleteEndpointRequest) = eff1(client.deleteEndpoint(a))
    override def deletePlatformApplication(a: DeletePlatformApplicationRequest) =
      eff1(client.deletePlatformApplication(a))
    override def deleteTopic(a: DeleteTopicRequest) = eff1(client.deleteTopic(a))
    override def getEndpointAttributes(a: GetEndpointAttributesRequest) =
      eff1(client.getEndpointAttributes(a))
    override def getPlatformApplicationAttributes(a: GetPlatformApplicationAttributesRequest) =
      eff1(client.getPlatformApplicationAttributes(a))
    override def getSMSAttributes                             = eff1(client.getSMSAttributes)
    override def getSMSAttributes(a: GetSmsAttributesRequest) = eff1(client.getSMSAttributes(a))
    override def getSubscriptionAttributes(a: GetSubscriptionAttributesRequest) =
      eff1(client.getSubscriptionAttributes(a))
    override def getTopicAttributes(a: GetTopicAttributesRequest) =
      eff1(client.getTopicAttributes(a))
    override def listEndpointsByPlatformApplication(a: ListEndpointsByPlatformApplicationRequest) =
      eff1(client.listEndpointsByPlatformApplication(a))
    override def listEndpointsByPlatformApplicationPaginator(
      a: ListEndpointsByPlatformApplicationRequest
    )                                     = primitive1(client.listEndpointsByPlatformApplicationPaginator(a))
    override def listPhoneNumbersOptedOut = eff1(client.listPhoneNumbersOptedOut)
    override def listPhoneNumbersOptedOut(a: ListPhoneNumbersOptedOutRequest) =
      eff1(client.listPhoneNumbersOptedOut(a))
    override def listPlatformApplications = eff1(client.listPlatformApplications)
    override def listPlatformApplications(a: ListPlatformApplicationsRequest) =
      eff1(client.listPlatformApplications(a))
    override def listPlatformApplicationsPaginator =
      primitive1(client.listPlatformApplicationsPaginator)
    override def listPlatformApplicationsPaginator(a: ListPlatformApplicationsRequest) =
      primitive1(client.listPlatformApplicationsPaginator(a))
    override def listSubscriptions                              = eff1(client.listSubscriptions)
    override def listSubscriptions(a: ListSubscriptionsRequest) = eff1(client.listSubscriptions(a))
    override def listSubscriptionsByTopic(a: ListSubscriptionsByTopicRequest) =
      eff1(client.listSubscriptionsByTopic(a))
    override def listSubscriptionsByTopicPaginator(a: ListSubscriptionsByTopicRequest) =
      primitive1(client.listSubscriptionsByTopicPaginator(a))
    override def listSubscriptionsPaginator = primitive1(client.listSubscriptionsPaginator)
    override def listSubscriptionsPaginator(a: ListSubscriptionsRequest) =
      primitive1(client.listSubscriptionsPaginator(a))
    override def listTagsForResource(a: ListTagsForResourceRequest) =
      eff1(client.listTagsForResource(a))
    override def listTopics                       = eff1(client.listTopics)
    override def listTopics(a: ListTopicsRequest) = eff1(client.listTopics(a))
    override def listTopicsPaginator              = primitive1(client.listTopicsPaginator)
    override def listTopicsPaginator(a: ListTopicsRequest) =
      primitive1(client.listTopicsPaginator(a))
    override def optInPhoneNumber(a: OptInPhoneNumberRequest) = eff1(client.optInPhoneNumber(a))
    override def publish(a: PublishRequest)                   = eff1(client.publish(a))
    override def removePermission(a: RemovePermissionRequest) = eff1(client.removePermission(a))
    override def serviceName                                  = primitive1(client.serviceName)
    override def setEndpointAttributes(a: SetEndpointAttributesRequest) =
      eff1(client.setEndpointAttributes(a))
    override def setPlatformApplicationAttributes(a: SetPlatformApplicationAttributesRequest) =
      eff1(client.setPlatformApplicationAttributes(a))
    override def setSMSAttributes(a: SetSmsAttributesRequest) = eff1(client.setSMSAttributes(a))
    override def setSubscriptionAttributes(a: SetSubscriptionAttributesRequest) =
      eff1(client.setSubscriptionAttributes(a))
    override def setTopicAttributes(a: SetTopicAttributesRequest) =
      eff1(client.setTopicAttributes(a))
    override def subscribe(a: SubscribeRequest)         = eff1(client.subscribe(a))
    override def tagResource(a: TagResourceRequest)     = eff1(client.tagResource(a))
    override def unsubscribe(a: UnsubscribeRequest)     = eff1(client.unsubscribe(a))
    override def untagResource(a: UntagResourceRequest) = eff1(client.untagResource(a))

  }

}
