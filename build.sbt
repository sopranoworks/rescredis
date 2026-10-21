name := "scredis-pekko"
organization := "com.sopranoworks"
organizationName := "Sopranoworks"
organizationHomepage := Some(url("https://sopranoworks.com"))

// scredis-pekko X has the API of scredis X, ported to Pekko.
version := "2.4.3"

description := "Pekko port of scredis 2.4.3 (https://github.com/scredis/scredis, tag v2.4.3)"

scalaVersion := "3.3.8"
crossScalaVersions := Seq("2.12.21", "2.13.18", scalaVersion.value)

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-Xfatal-warnings",
)

scalacOptions ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, n)) =>
      List("-Ywarn-dead-code")
    case _ =>
      List("-explain")
  }
}

scalacOptions in (Compile,doc) := Seq("-no-link-warnings")
autoAPIMappings := true

enablePlugins(BuildInfoPlugin)
enablePlugins(GhpagesPlugin)
enablePlugins(SiteScaladocPlugin)

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := "scredis"
buildInfoOptions += BuildInfoOption.BuildTime

git.remoteRepo := "git@github.com:sopranoworks/scredis-pekko.git"

val pekkoV = "1.7.1"
val loggingV = "3.9.6"
val collectionCompatV = "2.14.0"
val typesafeConfigV = "1.4.9"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % loggingV,
  "com.typesafe" % "config" % typesafeConfigV,
  "org.apache.pekko" %% "pekko-actor" % pekkoV,

  "org.scala-lang.modules" %% "scala-collection-compat" % collectionCompatV,

  "org.scalatest" %% "scalatest" % "3.2.20" % Test,
  "org.scalatestplus" %% "scalacheck-1-20" % "3.2.20.0" % Test,
  "org.scalacheck" %% "scalacheck" % "1.20.0" % Test,
  "org.slf4j" % "slf4j-simple" % "2.0.20" % Test
)

// ScalaMeter is not published for Scala 3. 0.19 is the version src/bench is written for (its DSL
// does not compile against 0.21, which is not published for Scala 2.12 either).
libraryDependencies ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, _)) => List("com.storm-enroute" %% "scalameter" % "0.19" % Test)
    case _ => Nil
  }
}

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-F", sys.props.getOrElse("F", "1.0"))

// required so that test actor systems don't get messed up
Test / fork := true
Test / parallelExecution := false

publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

ThisBuild / publishTo := {
  if (isSnapshot.value) Some("central-snapshots" at "https://central.sonatype.com/repository/maven-snapshots/")
  else localStaging.value
}

// Apache-2.0: ship LICENSE and NOTICE inside the binary and sources jars
Compile / packageBin / mappings ++= Seq(
  (baseDirectory.value / "LICENSE") -> "META-INF/LICENSE",
  (baseDirectory.value / "NOTICE") -> "META-INF/NOTICE"
)
Compile / packageSrc / mappings ++= Seq(
  (baseDirectory.value / "LICENSE") -> "META-INF/LICENSE",
  (baseDirectory.value / "NOTICE") -> "META-INF/NOTICE"
)

licenses := Seq(License.Apache2)

scmInfo := Some(ScmInfo(
  url("https://github.com/sopranoworks/scredis-pekko"),
  "scm:git@github.com:sopranoworks/scredis-pekko.git"
))

homepage := Some(url("https://github.com/sopranoworks/scredis-pekko"))

developers := List(
  Developer(
    id="sopranoworks",
    name="Osamu Takahashi",
    email="osamu.takahashi@sopranoworks.com",
    url=url("https://sopranoworks.com")
  ))

val scalaMeterFramework = new TestFramework("org.scalameter.ScalaMeterFramework")
testFrameworks += scalaMeterFramework

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

// Documentation
enablePlugins(ParadoxPlugin)
paradoxTheme := Some(builtinParadoxTheme("generic"))


// Scalameter Benchmark tests
lazy val Benchmark = config("bench") extend Test
configs(Benchmark)
inConfig(Benchmark)(Defaults.testSettings)

// No benchmarks on Scala 3: ScalaMeter is not published for it, so src/bench is not compiled there
Benchmark / unmanagedSourceDirectories := {
  if (scalaBinaryVersion.value == "3") Nil else Seq(baseDirectory.value / "src" / "bench" / "scala")
}
val benchNotice = taskKey[Unit]("Says that benchmarks are not built for Scala 3")
benchNotice := {
  if (scalaBinaryVersion.value == "3") {
    streams.value.log.info("bench: benchmarks are not built for Scala 3 (ScalaMeter is not published for Scala 3)")
  }
}
Benchmark / compile := ((Benchmark / compile) dependsOn benchNotice).value
