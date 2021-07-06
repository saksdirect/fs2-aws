package io.laserdisc.pure.s3.tagless

import software.amazon.awssdk.core.async.{ AsyncRequestBody, AsyncResponseTransformer }
import software.amazon.awssdk.services.s3.S3Utilities
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.paginators._
import software.amazon.awssdk.services.s3.waiters.S3AsyncWaiter

import java.nio.file.Path

trait S3AsyncClientOp[F[_]] {
  // S3AsyncClient
  def abortMultipartUpload(a: AbortMultipartUploadRequest): F[AbortMultipartUploadResponse]
  def close: F[Unit]
  def completeMultipartUpload(a: CompleteMultipartUploadRequest): F[CompleteMultipartUploadResponse]
  def copyObject(a: CopyObjectRequest): F[CopyObjectResponse]
  def createBucket(a: CreateBucketRequest): F[CreateBucketResponse]
  def createMultipartUpload(a: CreateMultipartUploadRequest): F[CreateMultipartUploadResponse]
  def deleteBucket(a: DeleteBucketRequest): F[DeleteBucketResponse]
  def deleteBucketAnalyticsConfiguration(
    a: DeleteBucketAnalyticsConfigurationRequest
  ): F[DeleteBucketAnalyticsConfigurationResponse]
  def deleteBucketCors(a: DeleteBucketCorsRequest): F[DeleteBucketCorsResponse]
  def deleteBucketEncryption(a: DeleteBucketEncryptionRequest): F[DeleteBucketEncryptionResponse]
  def deleteBucketIntelligentTieringConfiguration(
    a: DeleteBucketIntelligentTieringConfigurationRequest
  ): F[DeleteBucketIntelligentTieringConfigurationResponse]
  def deleteBucketInventoryConfiguration(
    a: DeleteBucketInventoryConfigurationRequest
  ): F[DeleteBucketInventoryConfigurationResponse]
  def deleteBucketLifecycle(a: DeleteBucketLifecycleRequest): F[DeleteBucketLifecycleResponse]
  def deleteBucketMetricsConfiguration(
    a: DeleteBucketMetricsConfigurationRequest
  ): F[DeleteBucketMetricsConfigurationResponse]
  def deleteBucketOwnershipControls(
    a: DeleteBucketOwnershipControlsRequest
  ): F[DeleteBucketOwnershipControlsResponse]
  def deleteBucketPolicy(a: DeleteBucketPolicyRequest): F[DeleteBucketPolicyResponse]
  def deleteBucketReplication(a: DeleteBucketReplicationRequest): F[DeleteBucketReplicationResponse]
  def deleteBucketTagging(a: DeleteBucketTaggingRequest): F[DeleteBucketTaggingResponse]
  def deleteBucketWebsite(a: DeleteBucketWebsiteRequest): F[DeleteBucketWebsiteResponse]
  def deleteObject(a: DeleteObjectRequest): F[DeleteObjectResponse]
  def deleteObjectTagging(a: DeleteObjectTaggingRequest): F[DeleteObjectTaggingResponse]
  def deleteObjects(a: DeleteObjectsRequest): F[DeleteObjectsResponse]
  def deletePublicAccessBlock(a: DeletePublicAccessBlockRequest): F[DeletePublicAccessBlockResponse]
  def getBucketAccelerateConfiguration(
    a: GetBucketAccelerateConfigurationRequest
  ): F[GetBucketAccelerateConfigurationResponse]
  def getBucketAcl(a: GetBucketAclRequest): F[GetBucketAclResponse]
  def getBucketAnalyticsConfiguration(
    a: GetBucketAnalyticsConfigurationRequest
  ): F[GetBucketAnalyticsConfigurationResponse]
  def getBucketCors(a: GetBucketCorsRequest): F[GetBucketCorsResponse]
  def getBucketEncryption(a: GetBucketEncryptionRequest): F[GetBucketEncryptionResponse]
  def getBucketIntelligentTieringConfiguration(
    a: GetBucketIntelligentTieringConfigurationRequest
  ): F[GetBucketIntelligentTieringConfigurationResponse]
  def getBucketInventoryConfiguration(
    a: GetBucketInventoryConfigurationRequest
  ): F[GetBucketInventoryConfigurationResponse]
  def getBucketLifecycleConfiguration(
    a: GetBucketLifecycleConfigurationRequest
  ): F[GetBucketLifecycleConfigurationResponse]
  def getBucketLocation(a: GetBucketLocationRequest): F[GetBucketLocationResponse]
  def getBucketLogging(a: GetBucketLoggingRequest): F[GetBucketLoggingResponse]
  def getBucketMetricsConfiguration(
    a: GetBucketMetricsConfigurationRequest
  ): F[GetBucketMetricsConfigurationResponse]
  def getBucketNotificationConfiguration(
    a: GetBucketNotificationConfigurationRequest
  ): F[GetBucketNotificationConfigurationResponse]
  def getBucketOwnershipControls(
    a: GetBucketOwnershipControlsRequest
  ): F[GetBucketOwnershipControlsResponse]
  def getBucketPolicy(a: GetBucketPolicyRequest): F[GetBucketPolicyResponse]
  def getBucketPolicyStatus(a: GetBucketPolicyStatusRequest): F[GetBucketPolicyStatusResponse]
  def getBucketReplication(a: GetBucketReplicationRequest): F[GetBucketReplicationResponse]
  def getBucketRequestPayment(a: GetBucketRequestPaymentRequest): F[GetBucketRequestPaymentResponse]
  def getBucketTagging(a: GetBucketTaggingRequest): F[GetBucketTaggingResponse]
  def getBucketVersioning(a: GetBucketVersioningRequest): F[GetBucketVersioningResponse]
  def getBucketWebsite(a: GetBucketWebsiteRequest): F[GetBucketWebsiteResponse]
  def getObject[ReturnT](
    a: GetObjectRequest,
    b: AsyncResponseTransformer[GetObjectResponse, ReturnT]
  ): F[ReturnT]
  def getObject(a: GetObjectRequest, b: Path): F[GetObjectResponse]
  def getObjectAcl(a: GetObjectAclRequest): F[GetObjectAclResponse]
  def getObjectLegalHold(a: GetObjectLegalHoldRequest): F[GetObjectLegalHoldResponse]
  def getObjectLockConfiguration(
    a: GetObjectLockConfigurationRequest
  ): F[GetObjectLockConfigurationResponse]
  def getObjectRetention(a: GetObjectRetentionRequest): F[GetObjectRetentionResponse]
  def getObjectTagging(a: GetObjectTaggingRequest): F[GetObjectTaggingResponse]
  def getObjectTorrent[ReturnT](
    a: GetObjectTorrentRequest,
    b: AsyncResponseTransformer[GetObjectTorrentResponse, ReturnT]
  ): F[ReturnT]
  def getObjectTorrent(a: GetObjectTorrentRequest, b: Path): F[GetObjectTorrentResponse]
  def getPublicAccessBlock(a: GetPublicAccessBlockRequest): F[GetPublicAccessBlockResponse]
  def headBucket(a: HeadBucketRequest): F[HeadBucketResponse]
  def headObject(a: HeadObjectRequest): F[HeadObjectResponse]
  def listBucketAnalyticsConfigurations(
    a: ListBucketAnalyticsConfigurationsRequest
  ): F[ListBucketAnalyticsConfigurationsResponse]
  def listBucketIntelligentTieringConfigurations(
    a: ListBucketIntelligentTieringConfigurationsRequest
  ): F[ListBucketIntelligentTieringConfigurationsResponse]
  def listBucketInventoryConfigurations(
    a: ListBucketInventoryConfigurationsRequest
  ): F[ListBucketInventoryConfigurationsResponse]
  def listBucketMetricsConfigurations(
    a: ListBucketMetricsConfigurationsRequest
  ): F[ListBucketMetricsConfigurationsResponse]
  def listBuckets: F[ListBucketsResponse]
  def listBuckets(a: ListBucketsRequest): F[ListBucketsResponse]
  def listMultipartUploads(a: ListMultipartUploadsRequest): F[ListMultipartUploadsResponse]
  def listMultipartUploadsPaginator(
    a: ListMultipartUploadsRequest
  ): F[ListMultipartUploadsPublisher]
  def listObjectVersions(a: ListObjectVersionsRequest): F[ListObjectVersionsResponse]
  def listObjectVersionsPaginator(a: ListObjectVersionsRequest): F[ListObjectVersionsPublisher]
  def listObjects(a: ListObjectsRequest): F[ListObjectsResponse]
  def listObjectsV2(a: ListObjectsV2Request): F[ListObjectsV2Response]
  def listObjectsV2Paginator(a: ListObjectsV2Request): F[ListObjectsV2Publisher]
  def listParts(a: ListPartsRequest): F[ListPartsResponse]
  def listPartsPaginator(a: ListPartsRequest): F[ListPartsPublisher]
  def putBucketAccelerateConfiguration(
    a: PutBucketAccelerateConfigurationRequest
  ): F[PutBucketAccelerateConfigurationResponse]
  def putBucketAcl(a: PutBucketAclRequest): F[PutBucketAclResponse]
  def putBucketAnalyticsConfiguration(
    a: PutBucketAnalyticsConfigurationRequest
  ): F[PutBucketAnalyticsConfigurationResponse]
  def putBucketCors(a: PutBucketCorsRequest): F[PutBucketCorsResponse]
  def putBucketEncryption(a: PutBucketEncryptionRequest): F[PutBucketEncryptionResponse]
  def putBucketIntelligentTieringConfiguration(
    a: PutBucketIntelligentTieringConfigurationRequest
  ): F[PutBucketIntelligentTieringConfigurationResponse]
  def putBucketInventoryConfiguration(
    a: PutBucketInventoryConfigurationRequest
  ): F[PutBucketInventoryConfigurationResponse]
  def putBucketLifecycleConfiguration(
    a: PutBucketLifecycleConfigurationRequest
  ): F[PutBucketLifecycleConfigurationResponse]
  def putBucketLogging(a: PutBucketLoggingRequest): F[PutBucketLoggingResponse]
  def putBucketMetricsConfiguration(
    a: PutBucketMetricsConfigurationRequest
  ): F[PutBucketMetricsConfigurationResponse]
  def putBucketNotificationConfiguration(
    a: PutBucketNotificationConfigurationRequest
  ): F[PutBucketNotificationConfigurationResponse]
  def putBucketOwnershipControls(
    a: PutBucketOwnershipControlsRequest
  ): F[PutBucketOwnershipControlsResponse]
  def putBucketPolicy(a: PutBucketPolicyRequest): F[PutBucketPolicyResponse]
  def putBucketReplication(a: PutBucketReplicationRequest): F[PutBucketReplicationResponse]
  def putBucketRequestPayment(a: PutBucketRequestPaymentRequest): F[PutBucketRequestPaymentResponse]
  def putBucketTagging(a: PutBucketTaggingRequest): F[PutBucketTaggingResponse]
  def putBucketVersioning(a: PutBucketVersioningRequest): F[PutBucketVersioningResponse]
  def putBucketWebsite(a: PutBucketWebsiteRequest): F[PutBucketWebsiteResponse]
  def putObject(a: PutObjectRequest, b: AsyncRequestBody): F[PutObjectResponse]
  def putObject(a: PutObjectRequest, b: Path): F[PutObjectResponse]
  def putObjectAcl(a: PutObjectAclRequest): F[PutObjectAclResponse]
  def putObjectLegalHold(a: PutObjectLegalHoldRequest): F[PutObjectLegalHoldResponse]
  def putObjectLockConfiguration(
    a: PutObjectLockConfigurationRequest
  ): F[PutObjectLockConfigurationResponse]
  def putObjectRetention(a: PutObjectRetentionRequest): F[PutObjectRetentionResponse]
  def putObjectTagging(a: PutObjectTaggingRequest): F[PutObjectTaggingResponse]
  def putPublicAccessBlock(a: PutPublicAccessBlockRequest): F[PutPublicAccessBlockResponse]
  def restoreObject(a: RestoreObjectRequest): F[RestoreObjectResponse]
  def serviceName: F[String]
  def uploadPart(a: UploadPartRequest, b: AsyncRequestBody): F[UploadPartResponse]
  def uploadPart(a: UploadPartRequest, b: Path): F[UploadPartResponse]
  def uploadPartCopy(a: UploadPartCopyRequest): F[UploadPartCopyResponse]
  def utilities: F[S3Utilities]
  def waiter: F[S3AsyncWaiter]
  def writeGetObjectResponse(
    a: WriteGetObjectResponseRequest,
    b: AsyncRequestBody
  ): F[WriteGetObjectResponseResponse]
  def writeGetObjectResponse(
    a: WriteGetObjectResponseRequest,
    b: Path
  ): F[WriteGetObjectResponseResponse]

}
