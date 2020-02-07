name := "fs2-aws"

// coverage
coverageMinimum := 40
coverageFailOnMinimum := true

scalaVersion := "2.13.0"

releasePublishArtifactsAction := PgpKeys.publishSigned.value

val fs2Version    = "2.2.1"
val AwsSdkVersion = "1.11.687"
val cirisVersion  = "1.0.4"
val scalaCollectionCompatVersion = "2.1.3"

libraryDependencies ++= Seq(
  "co.fs2"                  %% "fs2-core"                     % fs2Version,
  "co.fs2"                  %% "fs2-io"                       % fs2Version,
  "org.typelevel"           %% "alleycats-core"               % "2.0.0",
  "com.amazonaws"           % "aws-java-sdk-kinesis"          % AwsSdkVersion,
  "com.amazonaws"           % "aws-java-sdk-s3"               % AwsSdkVersion,
  "com.amazonaws"           % "aws-java-sdk-sqs"              % AwsSdkVersion,
  "com.amazonaws"           % "amazon-kinesis-producer"       % "0.14.0",
  "software.amazon.kinesis" % "amazon-kinesis-client"         % "2.2.7",
  "software.amazon.awssdk"  % "sts"                           % "2.10.29",
  "org.scalatest"           %% "scalatest"                    % "3.0.8" % Test,
  "org.mockito"             % "mockito-core"                  % "3.1.0" % Test,
  "com.amazonaws"           % "aws-java-sdk-sqs"              % AwsSdkVersion excludeAll ("commons-logging", "commons-logging"),
  "com.amazonaws"           % "amazon-sqs-java-messaging-lib" % "1.0.8" excludeAll ("commons-logging", "commons-logging"),
  "is.cir"                  %% "ciris"                        % cirisVersion,
  "is.cir"                  %% "ciris-enumeratum"             % cirisVersion,
  "is.cir"                  %% "ciris-refined"                % cirisVersion,
  "eu.timepit"              %% "refined"                      % "0.9.10",
  "org.scala-lang.modules"  %% "scala-collection-compat"      % scalaCollectionCompatVersion
)
