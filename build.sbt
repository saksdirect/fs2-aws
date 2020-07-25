import scoverage.ScoverageKeys.coverageMinimum

organization := "io.laserdisc"
name         := "fs2-aws"

lazy val scala212               = "2.12.10"
lazy val scala213               = "2.13.2"
lazy val supportedScalaVersions = List(scala212, scala213)

crossScalaVersions in ThisBuild := supportedScalaVersions

scalaVersion in ThisBuild := scala213

lazy val root = (project in file("."))
  .aggregate(
    `fs2-aws`,
    `fs2-aws-s3`,
    `fs2-aws-testkit`,
    `fs2-aws-dynamodb`,
    `fs2-aws-core`,
    `fs2-aws-examples`,
    `fs2-aws-ciris`
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
    coverageMinimum       := 40,
    coverageFailOnMinimum := true
  )
  .settings(commonSettings)
  .settings(scalacOptions := commonOptions(scalaVersion.value))

lazy val `fs2-aws-ciris` = (project in file("fs2-aws-ciris"))
  .dependsOn(`fs2-aws`)
  .settings(
    name := "fs2-aws-ciris",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"   % "mockito-core"             % V.MockitoCore % Test,
      "org.mockito"   %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "is.cir"        %% "ciris"                   % "1.1.1"
    ),
    coverageMinimum       := 40,
    coverageFailOnMinimum := true
  )
  .settings(commonSettings)
  .settings(scalacOptions := commonOptions(scalaVersion.value))

lazy val `fs2-aws-dynamodb` = (project in file("fs2-aws-dynamodb"))
  .dependsOn(`fs2-aws-core`)
  .settings(
    name                  := "fs2-aws-dynamodb",
    coverageMinimum       := 40,
    coverageFailOnMinimum := true,
    libraryDependencies ++= Seq(
      "co.fs2"        %% "fs2-core"                        % V.Fs2,
      "co.fs2"        %% "fs2-io"                          % V.Fs2,
      "org.scalatest" %% "scalatest"                       % V.ScalaTest % Test,
      "org.mockito"   % "mockito-core"                     % V.MockitoCore % Test,
      "org.mockito"   %% "mockito-scala-scalatest"         % V.MockitoScalaTest % Test,
      "com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.5.2",
      "io.laserdisc"  %% "scanamo-circe"                   % "1.0.8",
      "org.scanamo"   %% "scanamo-cats-effect"             % "1.0.0-M12-1"
    )
  )
  .settings(commonSettings)
  .settings(scalacOptions := commonOptions(scalaVersion.value))

lazy val `fs2-aws-examples` = (project in file("fs2-aws-examples"))
  .dependsOn(`fs2-aws-dynamodb`)
  .settings(
    name            := "fs2-aws-examples",
    coverageMinimum := 0,
    libraryDependencies ++= Seq(
      "org.mockito"       % "mockito-core"             % V.MockitoCore % Test,
      "org.mockito"       %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "ch.qos.logback"    % "logback-classic"          % "1.2.3",
      "ch.qos.logback"    % "logback-core"             % "1.2.3",
      "org.slf4j"         % "jcl-over-slf4j"           % "1.7.30",
      "org.slf4j"         % "jul-to-slf4j"             % "1.7.30",
      "io.chrisdavenport" %% "log4cats-slf4j"          % "1.1.1",
      "io.laserdisc"      %% "scanamo-circe"           % "1.0.8"
    )
  )
  .settings(commonSettings)
  .settings(scalacOptions := commonOptions(scalaVersion.value))
  .settings(
    skip in publish := true
  )

lazy val `fs2-aws-s3` = (project in file("fs2-aws-s3"))
  .settings(
    name := "fs2-aws-s3",
    libraryDependencies ++= Seq(
      "co.fs2"                 %% "fs2-core" % V.Fs2,
      "co.fs2"                 %% "fs2-io"   % V.Fs2,
      "eu.timepit"             %% "refined"  % V.Refined,
      "software.amazon.awssdk" % "s3"        % V.AwsSdkS3,
      "org.scalameta"          %% "munit"    % V.Munit % Test
    ),
    testFrameworks        += new TestFramework("munit.Framework"),
    coverageMinimum       := 0,
    coverageFailOnMinimum := true
  )
  .settings(commonSettings)
  .settings(scalacOptions := commonOptions(scalaVersion.value))

lazy val `fs2-aws` = (project in file("fs2-aws"))
  .dependsOn(`fs2-aws-core`)
  .settings(
    name := "fs2-aws",
    libraryDependencies ++= Seq(
      "co.fs2"                  %% "fs2-core"                % V.Fs2,
      "co.fs2"                  %% "fs2-io"                  % V.Fs2,
      "com.amazonaws"           % "aws-java-sdk-kinesis"     % V.AwsSdk,
      "com.amazonaws"           % "aws-java-sdk-s3"          % V.AwsSdk,
      "com.amazonaws"           % "amazon-kinesis-producer"  % "0.14.0",
      "software.amazon.kinesis" % "amazon-kinesis-client"    % "2.2.11",
      "org.mockito"             % "mockito-core"             % V.MockitoCore % Test,
      "software.amazon.awssdk"  % "sts"                      % "2.13.50",
      "org.scalatest"           %% "scalatest"               % V.ScalaTest % Test,
      "org.mockito"             %% "mockito-scala-scalatest" % V.MockitoScalaTest % Test,
      "eu.timepit"              %% "refined"                 % V.Refined
    ),
    coverageMinimum       := 40,
    coverageFailOnMinimum := true
  )
  .settings(commonSettings)
  .settings(scalacOptions := commonOptions(scalaVersion.value))

lazy val `fs2-aws-sqs` = (project in file("fs2-aws-sqs"))
  .settings(
    name := "fs2-aws",
    libraryDependencies ++= Seq(
      "co.fs2"        %% "fs2-core"                     % V.Fs2,
      "co.fs2"        %% "fs2-io"                       % V.Fs2,
      "com.amazonaws" % "aws-java-sdk-sqs"              % V.AwsSdk excludeAll ("commons-logging", "commons-logging"),
      "com.amazonaws" % "amazon-sqs-java-messaging-lib" % "1.0.8" excludeAll ("commons-logging", "commons-logging"),
      "org.mockito"   % "mockito-core"                  % V.MockitoCore % Test,
      "org.scalatest" %% "scalatest"                    % V.ScalaTest % Test,
      "org.mockito"   %% "mockito-scala-scalatest"      % V.MockitoScalaTest % Test,
      "eu.timepit"    %% "refined"                      % V.Refined
    ),
    coverageMinimum       := 55.80,
    coverageFailOnMinimum := true
  )
  .settings(commonSettings)
  .settings(scalacOptions := commonOptions(scalaVersion.value))

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
  .settings(scalacOptions := commonOptions(scalaVersion.value))

lazy val `fs2-aws-sqs-testkit` = (project in file("fs2-aws-sqs-testkit"))
  .dependsOn(`fs2-aws-sqs`)
  .settings(
    name := "fs2-aws-testkit",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest"               % V.ScalaTest,
      "org.mockito"   % "mockito-core"             % V.MockitoCore,
      "org.mockito"   %% "mockito-scala-scalatest" % V.MockitoScalaTest
    )
  )
  .settings(commonSettings)
  .settings(scalacOptions := commonOptions(scalaVersion.value))

addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")
addCommandAlias("format", ";scalafmt;test:scalafmt;scalafmtSbt")
addCommandAlias("checkFormat", ";scalafmtCheck;test:scalafmtCheck;scalafmtSbtCheck")

def commonOptions(scalaVersion: String) =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, 12)) =>
      Seq("-Ypartial-unification")
    case _ => Seq.empty
  }

lazy val commonSettings = Seq(
  organization       := "io.laserdisc",
  crossScalaVersions := supportedScalaVersions,
  scalaVersion       := scala213,
  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-encoding",
    "UTF-8",                         // source files are in UTF-8
    "-deprecation",                  // warn about use of deprecated APIs
    "-unchecked",                    // warn about unchecked type parameters
    "-feature",                      // warn about misused language features
    "-language:higherKinds",         // allow higher kinded types without `import scala.language.higherKinds`
    "-language:implicitConversions", // allow use of implicit conversions
    "-Xlint",                        // enable handy linter warnings
    "-Xfatal-warnings"               // turn compiler warnings into errors
  ),
  addCompilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1"),
  addCompilerPlugin("org.typelevel" %% "kind-projector"     % "0.10.3")
)

addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")

lazy val publishSettings = Seq(
  )

inThisBuild(
  List(
    licenses := Seq(
      "MIT" -> url("https://raw.githubusercontent.com/laserdisc-io/fs2-aws/master/LICENSE")
    ),
    homepage := Some(url("https://github.com/laserdisc-io/fs2-aws/")),
    developers := List(
      Developer(
        "dmateusp",
        "Daniel Mateus Pires",
        "dmateusp@gmail.com",
        url("https://github.com/dmateusp")
      ),
      Developer("semenodm", "Dmytro Semenov", "", url("https://github.com/semenodm"))
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/laserdisc-io/fs2-aws/tree/master"),
        "scm:git:git@github.com:laserdisc-io/fs2-aws.git",
        "scm:git:git@github.com:laserdisc-io/fs2-aws.git"
      )
    ),
    publishMavenStyle      := true,
    Test / publishArtifact := true,
    pomIncludeRepository   := (_ => false),
    pgpPublicRing          := file(".travis/local.pubring.asc"),
    pgpSecretRing          := file(".travis/local.secring.asc"),
    releaseEarlyWith       := SonatypePublisher
  )
)
