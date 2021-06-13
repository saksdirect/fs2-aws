package io.laserdisc.pure.sns.tagless

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
  UntagResourceRequest,
  _
}
import software.amazon.awssdk.services.sns.paginators.{
  ListEndpointsByPlatformApplicationPublisher,
  ListPlatformApplicationsPublisher,
  ListSubscriptionsByTopicPublisher,
  ListSubscriptionsPublisher,
  ListTopicsPublisher
}

trait SnsAsyncClientOp[F[_]] {
  // SnsAsyncClient
  def addPermission(a: AddPermissionRequest): F[AddPermissionResponse]
  def checkIfPhoneNumberIsOptedOut(
    a: CheckIfPhoneNumberIsOptedOutRequest
  ): F[CheckIfPhoneNumberIsOptedOutResponse]
  def close: F[Unit]
  def confirmSubscription(a: ConfirmSubscriptionRequest): F[ConfirmSubscriptionResponse]
  def createPlatformApplication(
    a: CreatePlatformApplicationRequest
  ): F[CreatePlatformApplicationResponse]
  def createPlatformEndpoint(a: CreatePlatformEndpointRequest): F[CreatePlatformEndpointResponse]
  def createTopic(a: CreateTopicRequest): F[CreateTopicResponse]
  def deleteEndpoint(a: DeleteEndpointRequest): F[DeleteEndpointResponse]
  def deletePlatformApplication(
    a: DeletePlatformApplicationRequest
  ): F[DeletePlatformApplicationResponse]
  def deleteTopic(a: DeleteTopicRequest): F[DeleteTopicResponse]
  def getEndpointAttributes(a: GetEndpointAttributesRequest): F[GetEndpointAttributesResponse]
  def getPlatformApplicationAttributes(
    a: GetPlatformApplicationAttributesRequest
  ): F[GetPlatformApplicationAttributesResponse]
  def getSMSAttributes: F[GetSmsAttributesResponse]
  def getSMSAttributes(a: GetSmsAttributesRequest): F[GetSmsAttributesResponse]
  def getSubscriptionAttributes(
    a: GetSubscriptionAttributesRequest
  ): F[GetSubscriptionAttributesResponse]
  def getTopicAttributes(a: GetTopicAttributesRequest): F[GetTopicAttributesResponse]
  def listEndpointsByPlatformApplication(
    a: ListEndpointsByPlatformApplicationRequest
  ): F[ListEndpointsByPlatformApplicationResponse]
  def listEndpointsByPlatformApplicationPaginator(
    a: ListEndpointsByPlatformApplicationRequest
  ): F[ListEndpointsByPlatformApplicationPublisher]
  def listPhoneNumbersOptedOut: F[ListPhoneNumbersOptedOutResponse]
  def listPhoneNumbersOptedOut(
    a: ListPhoneNumbersOptedOutRequest
  ): F[ListPhoneNumbersOptedOutResponse]
  def listPlatformApplications: F[ListPlatformApplicationsResponse]
  def listPlatformApplications(
    a: ListPlatformApplicationsRequest
  ): F[ListPlatformApplicationsResponse]
  def listPlatformApplicationsPaginator: F[ListPlatformApplicationsPublisher]
  def listPlatformApplicationsPaginator(
    a: ListPlatformApplicationsRequest
  ): F[ListPlatformApplicationsPublisher]
  def listSubscriptions: F[ListSubscriptionsResponse]
  def listSubscriptions(a: ListSubscriptionsRequest): F[ListSubscriptionsResponse]
  def listSubscriptionsByTopic(
    a: ListSubscriptionsByTopicRequest
  ): F[ListSubscriptionsByTopicResponse]
  def listSubscriptionsByTopicPaginator(
    a: ListSubscriptionsByTopicRequest
  ): F[ListSubscriptionsByTopicPublisher]
  def listSubscriptionsPaginator: F[ListSubscriptionsPublisher]
  def listSubscriptionsPaginator(a: ListSubscriptionsRequest): F[ListSubscriptionsPublisher]
  def listTagsForResource(a: ListTagsForResourceRequest): F[ListTagsForResourceResponse]
  def listTopics: F[ListTopicsResponse]
  def listTopics(a: ListTopicsRequest): F[ListTopicsResponse]
  def listTopicsPaginator: F[ListTopicsPublisher]
  def listTopicsPaginator(a: ListTopicsRequest): F[ListTopicsPublisher]
  def optInPhoneNumber(a: OptInPhoneNumberRequest): F[OptInPhoneNumberResponse]
  def publish(a: PublishRequest): F[PublishResponse]
  def removePermission(a: RemovePermissionRequest): F[RemovePermissionResponse]
  def serviceName: F[String]
  def setEndpointAttributes(a: SetEndpointAttributesRequest): F[SetEndpointAttributesResponse]
  def setPlatformApplicationAttributes(
    a: SetPlatformApplicationAttributesRequest
  ): F[SetPlatformApplicationAttributesResponse]
  def setSMSAttributes(a: SetSmsAttributesRequest): F[SetSmsAttributesResponse]
  def setSubscriptionAttributes(
    a: SetSubscriptionAttributesRequest
  ): F[SetSubscriptionAttributesResponse]
  def setTopicAttributes(a: SetTopicAttributesRequest): F[SetTopicAttributesResponse]
  def subscribe(a: SubscribeRequest): F[SubscribeResponse]
  def tagResource(a: TagResourceRequest): F[TagResourceResponse]
  def unsubscribe(a: UnsubscribeRequest): F[UnsubscribeResponse]
  def untagResource(a: UntagResourceRequest): F[UntagResourceResponse]

}
