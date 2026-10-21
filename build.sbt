// rescredis: scredis, maintained. One Akka source (src/), two artifacts:
//   com.sopranoworks %% rescredis        built from the Akka source
//   com.sopranoworks %% rescredis-pekko  built from the same source, rewritten at build time
//                                         (project/PekkoGeneration.scala, docs/pekko-generation.md)
// Both always have the same version.

ThisBuild / organization := "com.sopranoworks"
ThisBuild / organizationName := "Sopranoworks"
ThisBuild / organizationHomepage := Some(url("https://sopranoworks.com"))

// rescredis 1.0.0 is scredis 2.4.3 with the fixes listed in README.md. Fixes are patch releases,
// features are minor releases. The Akka and the Pekko artifact always have the same version.
ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "3.3.8"
ThisBuild / crossScalaVersions := Seq("2.12.21", "2.13.18", "3.3.8")

val akkaV = "2.6.21" // the last Akka release under Apache-2.0 (2.7 and later are BSL)
val pekkoV = "1.7.1"
val loggingV = "3.9.6"
val collectionCompatV = "2.14.0"
val typesafeConfigV = "1.4.9"
// circe: the lowest 0.14.x that builds and passes the suite on 2.12, 2.13 and 3; a newer 0.14.x is binary
// compatible (-Dcirce.version=... runs the build against another one)
val circeV = sys.props.getOrElse("circe.version", "0.14.0")

// The sources of both artifacts are in the repository root, not below the module directories
val repoRoot = file(".").getCanonicalFile
val akkaVariant = "akka"
val pekkoVariant = "pekko"

def isVariantDir(dir: File): Boolean =
  dir.getName.endsWith("-" + akkaVariant) || dir.getName.endsWith("-" + pekkoVariant)

val upstreamDescription = "scredis (https://github.com/scredis/scredis) 2.4.3, maintained"

lazy val common = Seq(
  autoAPIMappings := true,

  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-Xfatal-warnings",
  ),
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) => List("-Ywarn-dead-code")
      case _ => List("-explain")
    }
  },
  Compile / doc / scalacOptions := Seq("-no-link-warnings"),

  // the shared sources and the version directories (scala-2.13, scala-3, ...) are in src/
  Compile / sourceDirectory := repoRoot / "src" / "main",
  Test / sourceDirectory := repoRoot / "src" / "test",

  libraryDependencies ++= Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % loggingV,
    "com.typesafe" % "config" % typesafeConfigV,
    "org.scala-lang.modules" %% "scala-collection-compat" % collectionCompatV,
    "io.circe" %% "circe-core" % circeV,
    "io.circe" %% "circe-parser" % circeV,

    "org.scalatest" %% "scalatest" % "3.2.20" % Test,
    "org.scalatestplus" %% "scalacheck-1-20" % "3.2.20.0" % Test,
    "org.scalacheck" %% "scalacheck" % "1.20.0" % Test,
    "org.slf4j" % "slf4j-simple" % "2.0.20" % Test
  ),

  // ScalaMeter is not published for Scala 3. 0.19 is the version src/bench is written for (its DSL
  // does not compile against 0.21, which is not published for Scala 2.12 either).
  libraryDependencies ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) => List("com.storm-enroute" %% "scalameter" % "0.19" % Test)
      case _ => Nil
    }
  },

  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-F", sys.props.getOrElse("F", "1.0")),
  // required so that test actor systems don't get messed up
  Test / fork := true,
  Test / parallelExecution := false,
  testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),

  buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
  buildInfoPackage := "scredis",
  buildInfoOptions += BuildInfoOption.BuildTime,

  publishMavenStyle := true,
  Test / publishArtifact := false,
  pomIncludeRepository := { _ => false },
  licenses := Seq(License.Apache2),
  homepage := Some(url("https://github.com/sopranoworks/rescredis")),
  scmInfo := Some(ScmInfo(
    url("https://github.com/sopranoworks/rescredis"),
    "scm:git:git@github.com:sopranoworks/rescredis.git"
  )),
  developers := List(
    Developer(
      id = "sopranoworks",
      name = "Osamu Takahashi",
      email = "osamu.takahashi@sopranoworks.com",
      url = url("https://sopranoworks.com")
    )),

  // Apache-2.0: ship LICENSE and NOTICE inside the binary and sources jars
  Compile / packageBin / mappings ++= Seq(
    (repoRoot / "LICENSE") -> "META-INF/LICENSE",
    (repoRoot / "NOTICE") -> "META-INF/NOTICE"
  ),
  Compile / packageSrc / mappings ++= Seq(
    (repoRoot / "LICENSE") -> "META-INF/LICENSE",
    (repoRoot / "NOTICE") -> "META-INF/NOTICE"
  ),
)

// Scalameter benchmarks (src/bench)
lazy val Benchmark = config("bench") extend Test
val benchNotice = taskKey[Unit]("Says that benchmarks are not built for Scala 3")
val benchNoticeSettings = Seq(
  Benchmark / compile := ((Benchmark / compile) dependsOn benchNotice).value,
  benchNotice := {
    if (scalaBinaryVersion.value == "3") {
      streams.value.log.info("bench: benchmarks are not built for Scala 3 (ScalaMeter is not published for Scala 3)")
    }
  }
)
// No benchmarks on Scala 3: ScalaMeter is not published for it
def benchDirs(dirs: File*) = Def.setting {
  if (scalaBinaryVersion.value == "3") Seq.empty[File] else dirs.toSeq
}

// ---------------------------------------------------------------------------------------------
// rescredis: the Akka artifact, built from src/{main,test}/scala* and the -akka directories
// ---------------------------------------------------------------------------------------------
lazy val rescredis = (project in file("modules/akka"))
  .enablePlugins(BuildInfoPlugin)
  .configs(Benchmark)
  .settings(common)
  .settings(inConfig(Benchmark)(Defaults.testSettings))
  .settings(
    name := "rescredis",
    description := s"Akka client for Redis: $upstreamDescription. Same version and behaviour as rescredis-pekko.",
    libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaV,

    Compile / unmanagedSourceDirectories += repoRoot / "src" / "main" / s"scala-$akkaVariant",
    Test / unmanagedSourceDirectories += repoRoot / "src" / "test" / s"scala-$akkaVariant",
    Compile / unmanagedResourceDirectories := Seq(repoRoot / "src" / "main" / s"resources-$akkaVariant"),
    Test / unmanagedResourceDirectories :=
      Seq(repoRoot / "src" / "test" / "resources", repoRoot / "src" / "test" / s"resources-$akkaVariant"),
  )
  .settings(benchNoticeSettings)
  .settings(Benchmark / unmanagedSourceDirectories := benchDirs(repoRoot / "src" / "bench" / "scala").value)

// ---------------------------------------------------------------------------------------------
// rescredis-pekko: the Pekko artifact. Its sources are the Akka sources of rescredis, rewritten by
// PekkoGeneration into target/, plus the -pekko directories. Nothing generated is committed.
// ---------------------------------------------------------------------------------------------
def akkaSharedDirs(config: Configuration) =
  Def.setting((rescredis / config / unmanagedSourceDirectories).value.filterNot(isVariantDir))

// Generates the Pekko sources and fails the build if any of them still references akka.
def generateChecked(sourceDirs: Seq[File], out: File): Seq[File] = {
  val files = PekkoGeneration.generate(sourceDirs, out)
  PekkoGeneration.check(files)
  files
}

lazy val rescredisPekko = (project in file("modules/pekko"))
  .enablePlugins(BuildInfoPlugin)
  .configs(Benchmark)
  .settings(common)
  .settings(inConfig(Benchmark)(Defaults.testSettings))
  .settings(
    name := "rescredis-pekko",
    description := s"Apache Pekko client for Redis: $upstreamDescription. Same version and behaviour as rescredis.",
    libraryDependencies += "org.apache.pekko" %% "pekko-actor" % pekkoV,

    // the Pekko variant files; the shared sources come from the generators below
    Compile / unmanagedSourceDirectories := Seq(repoRoot / "src" / "main" / s"scala-$pekkoVariant"),
    Test / unmanagedSourceDirectories := Seq(repoRoot / "src" / "test" / s"scala-$pekkoVariant"),
    Compile / unmanagedResourceDirectories := Seq(repoRoot / "src" / "main" / s"resources-$pekkoVariant"),
    Test / unmanagedResourceDirectories :=
      Seq(repoRoot / "src" / "test" / "resources", repoRoot / "src" / "test" / s"resources-$pekkoVariant"),

    Compile / sourceGenerators += Def.task {
      generateChecked(akkaSharedDirs(Compile).value, (Compile / sourceManaged).value / "pekko-generated")
    }.taskValue,
    Test / sourceGenerators += Def.task {
      generateChecked(akkaSharedDirs(Test).value, (Test / sourceManaged).value / "pekko-generated")
    }.taskValue,
    Benchmark / unmanagedSourceDirectories := Nil,
    Benchmark / sourceGenerators += Def.task {
      val dirs = benchDirs(repoRoot / "src" / "bench" / "scala").value
      generateChecked(dirs, (Benchmark / sourceManaged).value / "pekko-generated")
    }.taskValue,
  )
  .settings(benchNoticeSettings)

// ---------------------------------------------------------------------------------------------
// Root: aggregates both artifacts; not published. Documentation (Paradox) is built from here.
// ---------------------------------------------------------------------------------------------
lazy val root = (project in file("."))
  .aggregate(rescredis, rescredisPekko)
  .enablePlugins(ParadoxPlugin, GhpagesPlugin)
  .settings(
    name := "rescredis-root",
    // the root has no sources of its own: they belong to the two modules
    Compile / unmanagedSourceDirectories := Nil,
    Test / unmanagedSourceDirectories := Nil,
    Compile / unmanagedResourceDirectories := Nil,
    Test / unmanagedResourceDirectories := Nil,
    git.remoteRepo := "git@github.com:sopranoworks/rescredis.git",
    publish / skip := true,
    publishArtifact := false,
    paradoxTheme := Some(builtinParadoxTheme("generic")),
  )

ThisBuild / publishTo := {
  if (isSnapshot.value) Some("central-snapshots" at "https://central.sonatype.com/repository/maven-snapshots/")
  else localStaging.value
}

Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)
