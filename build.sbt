name := "fs2-aws"
organization in ThisBuild := "io.github.dmateusp"

lazy val scala212 = "2.12.9"
lazy val scala213 = "2.13.0"
lazy val supportedScalaVersions = List(scala213, scala212)

scalaVersion := scala213

scalacOptions in ThisBuild ++= Seq(
  "-target:jvm-1.8",
  "-encoding",
  "UTF-8", // source files are in UTF-8
  "-deprecation", // warn about use of deprecated APIs
  "-unchecked", // warn about unchecked type parameters
  "-feature", // warn about misused language features
  "-language:higherKinds", // allow higher kinded types without `import scala.language.higherKinds`
  "-language:implicitConversions", // allow use of implicit conversions
  "-Xlint", // enable handy linter warnings
  "-Xfatal-warnings" // turn compiler warnings into errors
)

def enablePartialUnificationIn2_12(scalaVersion: String) =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, 12)) => Seq("-Ypartial-unification")
    case _ => Seq()
  }

lazy val root = (project in file("."))
  .aggregate(`fs2-aws`, `fs2-aws-testkit`)
  .settings(
    crossScalaVersions := Nil,
    skip in publish := true
  )

lazy val `fs2-aws`         = (project in file("fs2-aws"))
  .settings(
    crossScalaVersions := supportedScalaVersions,
    scalacOptions ++= enablePartialUnificationIn2_12(scalaVersion.value)
  )
lazy val `fs2-aws-testkit` = (project in file("fs2-aws-testkit"))
  .settings(
    crossScalaVersions := supportedScalaVersions)
  .dependsOn(`fs2-aws`)

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.10")

// publish
publishTo in ThisBuild := Some(
  "Sonatype Nexus" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")

licenses in ThisBuild := Seq(
  "MIT" -> url("https://github.com/dmateusp/fs2-aws/blob/master/LICENSE"))
developers in ThisBuild := List(
  Developer(id = "dmateusp",
            name = "Daniel Mateus Pires",
            email = "dmateusp@gmail.com",
            url = url("https://github.com/dmateusp"))
)
homepage in ThisBuild := Some(url("https://github.com/dmateusp/fs2-aws"))
scmInfo in ThisBuild := Some(
  ScmInfo(url("https://github.com/dmateusp/fs2-aws"),
          "scm:git:git@github.com:dmateusp/fs2-aws.git"))

// release
import ReleaseTransformations._

// signed releases
releasePublishArtifactsAction in ThisBuild := PgpKeys.publishSigned.value
credentials in ThisBuild += Credentials("Sonatype Nexus Repository Manager",
                                        "oss.sonatype.org",
                                        sys.env.getOrElse("SONATYPE_USERNAME", ""),
                                        sys.env.getOrElse("SONATYPE_PASSWORD", ""))

publishArtifact in ThisBuild in Test := true

// release steps
releaseProcess := Seq[ReleaseStep](
  inquireVersions,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  pushChanges
)

releaseTagComment := s"Releasing ${(version in ThisBuild).value}"
releaseCommitMessage := s"[skip travis] Setting version to ${(version in ThisBuild).value}"
