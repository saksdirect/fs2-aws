import TaglessGen.{
  taglessAwsService,
  taglessGenClasses,
  taglessGenDir,
  taglessGenPackage,
  taglessGenSettings
}
import sbt.Keys.scalaSource
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.sns.SnsAsyncClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

organization := "io.laserdisc"
name         := "fs2-aws"

lazy val scala212 = "2.12.14"
lazy val scala213 = "2.13.6"

lazy val supportedScalaVersions = List(scala212, scala213)

ThisBuild / crossScalaVersions := supportedScalaVersions

ThisBuild / scalaVersion := scala213

lazy val root = (project in file("."))
  .aggregate(
    `fs2-aws`,
    `fs2-aws-s3`,
    `fs2-aws-sqs`,
    `fs2-aws-testkit`,
    `fs2-aws-dynamodb`,
    `fs2-aws-sns`,
    `fs2-aws-core`,
    `fs2-aws-examples`,
    `fs2-aws-ciris`,
    `fs2-aws-benchmarks`,
    `pure-sqs-tagless`,
    `pure-sns-tagless`,
    `pure-s3-tagless`,
    `pure-cloudwatch-tagless`,
    `pure-dynamodb-tagless`,
    `pure-kinesis-tagless`
  )
  .settings(
    publishArtifact    := false,
    crossScalaVersions := Nil
  )

lazy val `fs2-aws-core` = (project in file("fs2-aws-core"))
  .settings(
    name := "fs2-aws-core",
    libraryDependencies ++= Seq(
      "co.fs2"        %% "fs2-core"                % V.Fs2,
      "co.fs2"        %% "fs2-io"                  % V.Fs2,
      "org.mockito"   % "mockito-core"             % V.MockitoCore % Test,
      "org.mockito"   %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "org.scalatest" %% "scalatest"               % V.ScalaTest % Test
    ),
    coverageMinimumStmtTotal := 40,
    coverageFailOnMinimum    := true
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))

lazy val `fs2-aws-ciris` = (project in file("fs2-aws-ciris"))
  .dependsOn(`fs2-aws`)
  .settings(
    name := "fs2-aws-ciris",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"   % "mockito-core"             % V.MockitoCore % Test,
      "org.mockito"   %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "is.cir"        %% "ciris"                   % "1.2.1"
    ),
    coverageMinimumStmtTotal := 40,
    coverageFailOnMinimum    := true
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))

lazy val `fs2-aws-dynamodb` = (project in file("fs2-aws-dynamodb"))
  .dependsOn(`fs2-aws-core`, `pure-dynamodb-tagless`)
  .settings(
    name                     := "fs2-aws-dynamodb",
    coverageMinimumStmtTotal := 40,
    coverageFailOnMinimum    := true,
    libraryDependencies ++= Seq(
      "co.fs2"        %% "fs2-core"                        % V.Fs2,
      "co.fs2"        %% "fs2-io"                          % V.Fs2,
      "org.scalatest" %% "scalatest"                       % V.ScalaTest % Test,
      "org.mockito"   % "mockito-core"                     % V.MockitoCore % Test,
      "org.mockito"   %% "mockito-scala-scalatest"         % V.MockitoScalaTest % Test,
      "com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.5.2",
      "io.laserdisc"  %% "scanamo-circe"                   % "1.0.8"
    )
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))

lazy val `fs2-aws-examples` = (project in file("fs2-aws-examples"))
  .dependsOn(
    `fs2-aws-dynamodb`,
    `pure-s3-tagless`,
    `pure-sns-tagless`,
    `pure-sqs-tagless`,
    `pure-kinesis-tagless`,
    `pure-dynamodb-tagless`,
    `pure-cloudwatch-tagless`,
    `fs2-aws`,
    `fs2-aws-s3`
  )
  .settings(
    name                     := "fs2-aws-examples",
    coverageMinimumStmtTotal := 0,
    libraryDependencies ++= Seq(
      "org.mockito"    % "mockito-core"             % V.MockitoCore % Test,
      "org.mockito"    %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "com.amazonaws"  % "aws-java-sdk-sts"         % "1.12.5",
      "ch.qos.logback" % "logback-classic"          % "1.2.3",
      "ch.qos.logback" % "logback-core"             % "1.2.3",
      "org.slf4j"      % "jcl-over-slf4j"           % "1.7.31",
      "org.slf4j"      % "jul-to-slf4j"             % "1.7.31",
      "org.typelevel"  %% "log4cats-slf4j"          % "1.3.1",
      "io.laserdisc"   %% "scanamo-circe"           % "1.0.8"
    )
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))
  .settings(
    publish / skip := true
  )

lazy val `fs2-aws-s3` = (project in file("fs2-aws-s3"))
  .settings(
    name := "fs2-aws-s3",
    libraryDependencies ++= Seq(
      "co.fs2"                 %% "fs2-core" % V.Fs2,
      "co.fs2"                 %% "fs2-io"   % V.Fs2,
      "eu.timepit"             %% "refined"  % V.Refined,
      "software.amazon.awssdk" % "s3"        % V.AwsSdk,
      "org.scalameta"          %% "munit"    % V.Munit % Test
    ),
    testFrameworks           += new TestFramework("munit.Framework"),
    coverageMinimumStmtTotal := 0,
    coverageFailOnMinimum    := true
  )
  .settings(commonSettings)
  .settings(scalacOptions := commonOptions(scalaVersion.value))
  .dependsOn(`pure-s3-tagless`)

lazy val `fs2-aws` = (project in file("fs2-aws"))
  .dependsOn(
    `fs2-aws-core`,
    `pure-cloudwatch-tagless`,
    `pure-dynamodb-tagless`,
    `pure-kinesis-tagless`
  )
  .settings(
    name := "fs2-aws",
    libraryDependencies ++= Seq(
      "co.fs2"                  %% "fs2-core"                % V.Fs2,
      "co.fs2"                  %% "fs2-io"                  % V.Fs2,
      "com.amazonaws"           % "amazon-kinesis-producer"  % "0.14.6",
      "software.amazon.kinesis" % "amazon-kinesis-client"    % "2.3.4",
      "software.amazon.awssdk"  % "sts"                      % V.AwsSdk,
      "eu.timepit"              %% "refined"                 % V.Refined,
      "org.scalatest"           %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"             %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "org.mockito"             % "mockito-core"             % V.MockitoCore % Test,
      "ch.qos.logback"          % "logback-classic"          % "1.2.3" % Test,
      "ch.qos.logback"          % "logback-core"             % "1.2.3" % Test,
      "org.slf4j"               % "jcl-over-slf4j"           % "1.7.30" % Test,
      "org.slf4j"               % "jul-to-slf4j"             % "1.7.30" % Test
    ),
    coverageMinimumStmtTotal := 40,
    coverageFailOnMinimum    := true
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))

lazy val `fs2-aws-sqs` = (project in file("fs2-aws-sqs"))
  .settings(
    name := "fs2-aws-sqs",
    libraryDependencies ++= Seq(
      "co.fs2"                 %% "fs2-core"                % V.Fs2,
      "co.fs2"                 %% "fs2-io"                  % V.Fs2,
      "software.amazon.awssdk" % "sqs"                      % V.AwsSdk,
      "org.mockito"            % "mockito-core"             % V.MockitoCore % Test,
      "org.scalatest"          %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"            %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "eu.timepit"             %% "refined"                 % V.Refined
    ),
    coverageMinimumStmtTotal := 55.80,
    coverageFailOnMinimum    := true
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))
  .dependsOn(`pure-sqs-tagless`)

lazy val `fs2-aws-sns` = (project in file("fs2-aws-sns"))
  .settings(
    name := "fs2-aws-sns",
    libraryDependencies ++= Seq(
      "co.fs2"                 %% "fs2-core"                % V.Fs2,
      "co.fs2"                 %% "fs2-io"                  % V.Fs2,
      "software.amazon.awssdk" % "sns"                      % V.AwsSdk,
      "org.mockito"            % "mockito-core"             % V.MockitoCore % Test,
      "org.scalatest"          %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"            %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "software.amazon.awssdk" % "sqs"                      % V.AwsSdk % Test,
      "eu.timepit"             %% "refined"                 % V.Refined
    ),
    coverageMinimumStmtTotal := 55.80,
    coverageFailOnMinimum    := true
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))
  .dependsOn(`pure-sqs-tagless`, `pure-sns-tagless`)

lazy val `pure-sqs-tagless` = (project in file("pure-aws/pure-sqs-tagless"))
  .settings(taglessGenSettings)
  .settings(
    name := "pure-sqs-tagless",
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "sqs"                      % V.AwsSdk,
      "org.mockito"            % "mockito-core"             % V.MockitoCore % Test,
      "org.scalatest"          %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"            %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "eu.timepit"             %% "refined"                 % V.Refined,
      "org.typelevel"          %% "cats-effect"             % "2.5.0"
    ),
    taglessGenDir     := (Compile / scalaSource).value / "io" / "laserdisc" / "pure" / "sqs" / "tagless",
    taglessGenPackage := "io.laserdisc.pure.sqs.tagless",
    taglessAwsService := "sqs",
    taglessGenClasses := {
      List[Class[_]](
        classOf[SqsAsyncClient]
      )
    }
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))

lazy val `pure-s3-tagless` = (project in file("pure-aws/pure-s3-tagless"))
  .settings(taglessGenSettings)
  .settings(
    name := "pure-s3-tagless",
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "s3"                       % V.AwsSdk,
      "org.mockito"            % "mockito-core"             % V.MockitoCore % Test,
      "org.scalatest"          %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"            %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "eu.timepit"             %% "refined"                 % V.Refined,
      "org.typelevel"          %% "cats-effect"             % "2.5.0"
    ),
    taglessGenDir     := (Compile / scalaSource).value / "io" / "laserdisc" / "pure" / "s3" / "tagless",
    taglessGenPackage := "io.laserdisc.pure.s3.tagless",
    taglessAwsService := "s3",
    taglessGenClasses := {
      List[Class[_]](
        classOf[S3AsyncClient]
      )
    }
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))

lazy val `pure-sns-tagless` = (project in file("pure-aws/pure-sns-tagless"))
  .settings(taglessGenSettings)
  .settings(
    name := "pure-sns-tagless",
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "sns"                      % V.AwsSdk,
      "org.mockito"            % "mockito-core"             % V.MockitoCore % Test,
      "org.scalatest"          %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"            %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "eu.timepit"             %% "refined"                 % V.Refined,
      "org.typelevel"          %% "cats-effect"             % "2.5.0"
    ),
    taglessGenDir     := (Compile / scalaSource).value / "io" / "laserdisc" / "pure" / "sns" / "tagless",
    taglessGenPackage := "io.laserdisc.pure.sns.tagless",
    taglessAwsService := "sns",
    taglessGenClasses := {
      List[Class[_]](
        classOf[SnsAsyncClient]
      )
    }
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))

lazy val `pure-kinesis-tagless` = (project in file("pure-aws/pure-kinesis-tagless"))
  .settings(taglessGenSettings)
  .settings(
    name := "pure-kinesis-tagless",
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "kinesis"                  % V.AwsSdk,
      "org.mockito"            % "mockito-core"             % V.MockitoCore % Test,
      "org.scalatest"          %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"            %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "eu.timepit"             %% "refined"                 % V.Refined,
      "org.typelevel"          %% "cats-effect"             % "2.5.0"
    ),
    taglessGenDir     := (Compile / scalaSource).value / "io" / "laserdisc" / "pure" / "kinesis" / "tagless",
    taglessGenPackage := "io.laserdisc.pure.kinesis.tagless",
    taglessAwsService := "kinesis",
    taglessGenClasses := {
      List[Class[_]](
        classOf[KinesisAsyncClient]
      )
    }
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))

lazy val `pure-dynamodb-tagless` = (project in file("pure-aws/pure-dynamodb-tagless"))
  .settings(taglessGenSettings)
  .settings(
    name := "pure-dynamodb-tagless",
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "dynamodb"                 % V.AwsSdk,
      "org.mockito"            % "mockito-core"             % V.MockitoCore % Test,
      "org.scalatest"          %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"            %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "eu.timepit"             %% "refined"                 % V.Refined,
      "org.typelevel"          %% "cats-effect"             % V.CE
    ),
    taglessGenDir     := (Compile / scalaSource).value / "io" / "laserdisc" / "pure" / "dynamodb" / "tagless",
    taglessGenPackage := "io.laserdisc.pure.dynamodb.tagless",
    taglessAwsService := "dynamodb",
    taglessGenClasses := {
      List[Class[_]](
        classOf[DynamoDbAsyncClient]
      )
    }
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))

lazy val `pure-cloudwatch-tagless` = (project in file("pure-aws/pure-cloudwatch-tagless"))
  .settings(taglessGenSettings)
  .settings(
    name := "pure-cloudwatch-tagless",
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "cloudwatch"               % V.AwsSdk,
      "org.mockito"            % "mockito-core"             % V.MockitoCore % Test,
      "org.scalatest"          %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"            %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "eu.timepit"             %% "refined"                 % V.Refined,
      "org.typelevel"          %% "cats-effect"             % "2.5.0"
    ),
    taglessGenDir     := (Compile / scalaSource).value / "io" / "laserdisc" / "pure" / "cloudwatch" / "tagless",
    taglessGenPackage := "io.laserdisc.pure.cloudwatch.tagless",
    taglessAwsService := "cloudwatch",
    taglessGenClasses := {
      List[Class[_]](
        classOf[CloudWatchAsyncClient]
      )
    }
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))

lazy val `fs2-aws-testkit` = (project in file("fs2-aws-testkit"))
  .dependsOn(`fs2-aws`)
  .settings(
    name := "fs2-aws-testkit",
    libraryDependencies ++= Seq(
      "io.circe"      %% "circe-core"              % V.Circe,
      "io.circe"      %% "circe-generic"           % V.Circe,
      "io.circe"      %% "circe-generic-extras"    % V.Circe,
      "io.circe"      %% "circe-parser"            % V.Circe,
      "org.scalatest" %% "scalatest"               % V.ScalaTest,
      "org.mockito"   % "mockito-core"             % V.MockitoCore,
      "org.mockito"   %% "mockito-scala-scalatest" % V.MockitoScalaTest
    )
  )
  .settings(commonSettings)
  .settings(scalacOptions ++= commonOptions(scalaVersion.value))

lazy val `fs2-aws-benchmarks` = (project in file("fs2-aws-benchmarks"))
  .dependsOn(`fs2-aws`)
  .dependsOn(`fs2-aws-testkit`)
  .settings(
    name := "fs2-aws-benchmarks",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "ch.qos.logback" % "logback-core"    % "1.2.3",
      "org.slf4j"      % "jcl-over-slf4j"  % "1.7.31",
      "org.slf4j"      % "jul-to-slf4j"    % "1.7.31"
    ),
    publishArtifact := false
  )
  .enablePlugins(JmhPlugin)

addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")
addCommandAlias("format", ";scalafmt;test:scalafmt;scalafmtSbt")
addCommandAlias("checkFormat", ";scalafmtCheck;test:scalafmtCheck;scalafmtSbtCheck")
addCommandAlias("build", ";checkFormat;clean;+test;coverage")

def commonOptions(scalaVersion: String) =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, 12)) =>
      Seq("-Ypartial-unification")
    case _ => Seq.empty
  }

lazy val commonSettings = Seq(
  organization := "io.laserdisc",
  developers := List(
    Developer(
      "semenodm",
      "Dmytro Semenov",
      "sdo.semenov@gmail.com",
      url("https://github.com/semenodm")
    )
  ),
  licenses                ++= Seq(("MIT", url("http://opensource.org/licenses/MIT"))),
  homepage                := Some(url("https://github.com/laserdisc-io/fs2-aws")),
  crossScalaVersions      := supportedScalaVersions,
  scalaVersion            := scala213,
  Compile / doc / sources := Nil,
  Test / fork             := true,
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",                         // source files are in UTF-8
    "-deprecation",                  // warn about use of deprecated APIs
    "-unchecked",                    // warn about unchecked type parameters
    "-feature",                      // warn about misused language features
    "-language:higherKinds",         // allow higher kinded types without `import scala.language.higherKinds`
    "-language:implicitConversions", // allow use of implicit conversions
    "-language:postfixOps",
    "-Xlint",             // enable handy linter warnings
    "-Xfatal-warnings",   // turn compiler warnings into errors
    "-Ywarn-macros:after" // allows the compiler to resolve implicit imports being flagged as unused
  ),
  addCompilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1"),
  addCompilerPlugin("org.typelevel" %% "kind-projector"     % "0.10.3"),
  libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.3"
)

addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")
