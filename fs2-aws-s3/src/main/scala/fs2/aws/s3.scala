package fs2.aws

import java.nio.ByteBuffer
import cats.effect._
import cats.implicits._
import eu.timepit.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.boolean.Or
import eu.timepit.refined.generic.Equal
import eu.timepit.refined.numeric.Greater
import eu.timepit.refined.types.string.NonEmptyString
import fs2.{ Chunk, Pipe, Pull }
import io.laserdisc.pure.s3.tagless.S3AsyncClientOp
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.model._

import scala.jdk.CollectionConverters._

object s3 {
  // Each part must be at least 5 MB in size
  // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html#uploadPart-software.amazon.awssdk.services.s3.model.UploadPartRequest-software.amazon.awssdk.core.sync.RequestBody-
  type PartSizeMB = Int Refined (Greater[W.`5`.T] Or Equal[W.`5`.T])
  type ETag       = String

  final case class BucketName(value: NonEmptyString)
  final case class FileKey(value: NonEmptyString)

  /* A purely functional abstraction over the S3 API based on fs2.Stream */
  trait S3[F[_]] {
    def delete(bucket: BucketName, key: FileKey): F[Unit]
    def uploadFile(bucket: BucketName, key: FileKey): Pipe[F, Byte, ETag]
    def uploadFileMultipart(
      bucket: BucketName,
      key: FileKey,
      partSize: PartSizeMB
    ): Pipe[F, Byte, ETag]
    def readFile(bucket: BucketName, key: FileKey): fs2.Stream[F, Byte]
    def readFileMultipart(
      bucket: BucketName,
      key: FileKey,
      partSize: PartSizeMB
    ): fs2.Stream[F, Byte]
  }

  object S3 {
    type PartETag = String
    type PartId   = Int
    type UploadId = String

    /**
      * It creates an instance of the purely functional S3 API.
      *
      * Example:
      *
      * {{{
      * S3.create[IO](client, blocker).flatMap { s3 =>
      *   streamOfBytes
      *     .through(s3.uploadFileMultipart(BucketName("foo"), FileKey("bar"), partSize = 5))
      *     .evalMap(t => IO(println(s"eTag: $t")))
      *     .compile
      *     .drain
      * }
      * }}}
      */
    def create[F[_]: ConcurrentEffect: ContextShift](
      s3: S3AsyncClientOp[F],
      blocker: Blocker
    ): F[S3[F]] =
      new S3[F] {

        /**
          * Deletes a file in a single request.
          */
        def delete(bucket: BucketName, key: FileKey): F[Unit] =
          s3.deleteObject(
              DeleteObjectRequest
                .builder()
                .bucket(bucket.value)
                .key(key.value)
                .build()
            )
            .void

        /**
          * Uploads a file in a single request. Suitable for small files.
          *
          * For big files, consider using [[uploadFileMultipart]] instead.
          */
        def uploadFile(bucket: BucketName, key: FileKey): Pipe[F, Byte, ETag] =
          in => {
            fs2.Stream.eval {
              in.compile.toVector.flatMap { vs =>
                val bs = ByteBuffer.wrap(vs.toArray)
                s3.putObject(
                    PutObjectRequest.builder().bucket(bucket.value).key(key.value).build(),
                    AsyncRequestBody.fromByteBuffer(bs)
                  )
                  .map(_.eTag())
              }
            }
          }

        /**
          * Uploads a file in multiple parts of the specified @partSize per request. Suitable for big files.
          *
          * It does so in constant memory. So at a given time, only the number of bytes indicated by @partSize
          * will be loaded in memory.
          *
          * For small files, consider using [[uploadFile]] instead.
          *
          * @partSize the part size indicated in MBs. It must be at least 5, as required by AWS.
          */
        def uploadFileMultipart(
          bucket: BucketName,
          key: FileKey,
          partSize: PartSizeMB
        ): Pipe[F, Byte, ETag] = {
          val chunkSizeBytes = partSize * 1048576

          def initiateMultipartUpload: F[UploadId] =
            s3.createMultipartUpload(
                CreateMultipartUploadRequest.builder().bucket(bucket.value).key(key.value).build()
              )
              .map(_.uploadId())

          def uploadPart(uploadId: UploadId): Pipe[F, (Chunk[Byte], Long), (PartETag, PartId)] =
            _.evalMap {
              case (c, i) =>
                s3.uploadPart(
                    UploadPartRequest
                      .builder()
                      .bucket(bucket.value)
                      .key(key.value)
                      .uploadId(uploadId)
                      .partNumber(i.toInt)
                      .contentLength(c.size)
                      .build(),
                    AsyncRequestBody.fromBytes(c.toArray)
                  )
                  .map(_.eTag() -> i.toInt)
            }

          def completeUpload(uploadId: UploadId): Pipe[F, List[(PartETag, PartId)], ETag] =
            _.evalMap { tags =>
              val parts = tags.map {
                case (t, i) => CompletedPart.builder().partNumber(i).eTag(t).build()
              }.asJava
              s3.completeMultipartUpload(
                  CompleteMultipartUploadRequest
                    .builder()
                    .bucket(bucket.value)
                    .key(key.value)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                    .build()
                )
                .map(_.eTag())
            }

          def cancelUpload(uploadId: UploadId): F[Unit] =
            s3.abortMultipartUpload(
                AbortMultipartUploadRequest
                  .builder()
                  .bucket(bucket.value)
                  .key(key.value)
                  .uploadId(uploadId)
                  .build()
              )
              .void

          in => {
            fs2.Stream
              .eval(initiateMultipartUpload)
              .flatMap { uploadId =>
                in.chunkN(chunkSizeBytes)
                  .zip(fs2.Stream.iterate(1L)(_ + 1))
                  .through(uploadPart(uploadId))
                  .fold[List[(PartETag, PartId)]](List.empty)(_ :+ _)
                  .through(completeUpload(uploadId))
                  .handleErrorWith(ex =>
                    fs2.Stream.eval(cancelUpload(uploadId) >> Sync[F].raiseError[ETag](ex))
                  )
              }
          }
        }

        /**
          * Reads a file in a single request. Suitable for small files.
          *
          * For big files, consider using [[readFileMultipart]] instead.
          */
        def readFile(
          bucket: BucketName,
          key: FileKey
        ): fs2.Stream[F, Byte] =
          fs2.Stream
            .eval(
              s3.getObject(
                  GetObjectRequest
                    .builder()
                    .bucket(bucket.value)
                    .key(key.value)
                    .build(),
                  Fs2StreamAsyncResponseTransformer[F, GetObjectResponse]
                )
                .map {
                  case (response, stream) => stream
                }
            )
            .flatten

        /**
          * Reads a file in multiple parts of the specifed @partSize per request. Suitable for big files.
          *
          * It does so in constant memory. So at a given time, only the number of bytes indicated by @partSize
          * will be loaded in memory.
          *
          * For small files, consider using [[readFile]] instead.
          */
        def readFileMultipart(
          bucket: BucketName,
          key: FileKey,
          partSize: PartSizeMB
        ): fs2.Stream[F, Byte] = {
          val chunkSizeBytes = partSize.value * 1000000
          val ContentRangeRE = """bytes (\d+)-(\d+)/(\d+)""".r

          // Range must be in the form "bytes=0-500" -> https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
          def go(offset: Long): Pull[F, Byte, Unit] =
            Pull.eval {
              s3.getObject(
                  GetObjectRequest
                    .builder()
                    .range(s"bytes=$offset-${offset + chunkSizeBytes - 1}")
                    .bucket(bucket.value)
                    .key(key.value)
                    .build(),
                  Fs2StreamAsyncResponseTransformer[F, GetObjectResponse]
                )
                .map {
                  case (response, stream) =>
                    stream.chunks.pull.last
                      .flatMap {
                        case Some(o) =>
                          response.contentRange() match {
                            case ContentRangeRE(_, rangeEndStr, sizeStr) =>
                              // ContentRange=bytes 65-95/96 means multipart download is completed
                              val rangeEnd = rangeEndStr.toLong
                              val size     = sizeStr.toLong
                              if (rangeEnd < size - 1) Pull.output(o) >> go(rangeEnd + 1)
                              else Pull.output(o)
                            case invalidContentRange =>
                              Pull.eval(
                                new RuntimeException(
                                  s"Invalid response 'ContentRange=$invalidContentRange'. Expected 'ContentRange=$ContentRangeRE'"
                                ).raiseError[F, Nothing]
                              )
                          }
                        case None => Pull.done
                      }
                }
            }.flatten

          go(0).stream
        }

      }.pure[F]
  }
}
