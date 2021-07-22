// Required for the freegen definition for postgres in ../build.sbt
val AwsSdk = "2.16.104"
libraryDependencies += "software.amazon.awssdk" % "sqs"        % AwsSdk
libraryDependencies += "software.amazon.awssdk" % "s3"         % AwsSdk
libraryDependencies += "software.amazon.awssdk" % "sns"        % AwsSdk
libraryDependencies += "software.amazon.awssdk" % "kinesis"    % AwsSdk
libraryDependencies += "software.amazon.awssdk" % "dynamodb"   % AwsSdk
libraryDependencies += "software.amazon.awssdk" % "cloudwatch" % AwsSdk
