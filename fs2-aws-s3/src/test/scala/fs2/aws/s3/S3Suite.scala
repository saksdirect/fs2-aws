package fs2.aws

import java.net.URI
import java.nio.file.Paths
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{ S3AsyncClient, S3AsyncClientBuilder, S3Client }
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import cats.effect._
import cats.implicits._
import eu.timepit.refined.auto._
import fs2.aws.s3._
import io.laserdisc.pure.s3.tagless.{ Interpreter, S3AsyncClientOp }

class S3Suite extends IOSuite {

  val credentials =
    AwsBasicCredentials
      .create("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

  val s3R: Resource[IO, S3AsyncClientOp[IO]] =
    Interpreter[IO].S3AsyncClientOpResource(
      S3AsyncClient
        .builder()
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .endpointOverride(URI.create("http://localhost:9000"))
        .region(Region.US_EAST_1)
    )

  val testFile = getClass.getResource("/jsontest.json")

  val bucket  = BucketName("resources")
  val fileKey = FileKey("jsontest.json")

  test("Upload JSON test file & read it back") {
    s3R.use { s3Op =>
      S3.create[IO](s3Op, blocker).flatMap { s3 =>
        val upload =
          fs2.io.file
            .readAll[IO](Paths.get(testFile.getPath), blocker, 4096)
            .through(s3.uploadFile(bucket, fileKey))
            .compile
            .drain

        val read =
          s3.readFile(bucket, fileKey)
            .through(fs2.text.utf8Decode)
            .through(fs2.text.lines)
            .compile
            .toVector
            .map { res =>
              val expected =
                """{"test": 1}{"test": 2}{"test": 3}{"test": 4}{"test": 5}{"test": 6}{"test": 7}{"test": 8}"""
              assert(res.reduce(_ + _).concat("") === expected)
            }

        upload >> read
      }
    }
  }

  test(
    "Upload JSON test file in multipart fashion & read it back in a one go, then delete it and try to read it again"
  ) {
    s3R.use { s3 =>
      S3.create[IO](s3, blocker).flatMap { s3 =>
        val fileKeyMix = FileKey("jsontest-mix.json")

        val upload =
          fs2.io.file
            .readAll[IO](Paths.get(testFile.getPath), blocker, 4096)
            .through(s3.uploadFileMultipart(bucket, fileKeyMix, 5))
            .compile
            .drain

        val read =
          s3.readFile(bucket, fileKeyMix)
            .through(fs2.text.utf8Decode)
            .through(fs2.text.lines)
            .compile
            .toVector
            .map { res =>
              val expected =
                """{"test": 1}{"test": 2}{"test": 3}{"test": 4}{"test": 5}{"test": 6}{"test": 7}{"test": 8}"""
              assert(res.reduce(_ + _).concat("") === expected)
            }

        val delete =
          s3.delete(bucket, fileKeyMix)

        val readNonExisting =
          read.as(fail("expecting NoSuchKeyException")).handleError {
            case _: NoSuchKeyException => assert(true)
          }

        upload >> read >> delete >> readNonExisting
      }
    }
  }

  test("Upload JSON test file & read it back in multipart fashion all the way") {

    s3R.use { s3 =>
      S3.create[IO](s3, blocker).flatMap { s3 =>
        val fileKeyMultipart = FileKey("jsontest-multipart.json")

        val upload =
          fs2.io.file
            .readAll[IO](Paths.get(testFile.getPath), blocker, 4096)
            .through(s3.uploadFileMultipart(bucket, fileKeyMultipart, 5))
            .compile
            .drain

        val read =
          s3.readFileMultipart(bucket, fileKeyMultipart, 5)
            .through(fs2.text.utf8Decode)
            .through(fs2.text.lines)
            .compile
            .toVector
            .map { res =>
              val expected =
                """{"test": 1}{"test": 2}{"test": 3}{"test": 4}{"test": 5}{"test": 6}{"test": 7}{"test": 8}"""
              assert(res.reduce(_ + _).concat("") === expected)
            }

        upload >> read
      }
    }
  }

}
