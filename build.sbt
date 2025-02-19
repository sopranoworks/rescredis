
name := "scredis-pekko"
organization := "com.github.scredis"

scalaVersion := "3.3.1"
crossScalaVersions := Seq(/*"2.11.12", */"2.12.12", "2.13.10", scalaVersion.value)

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-Xfatal-warnings",
//  "-Ywarn-dead-code"
)

scalacOptions ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, n)) =>
      List("-Ywarn-dead-code")
    case _ =>
      List("-source:3.0-migration", "-explain", "-rewrite")
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

git.remoteRepo := "git@github.com:scredis/scredis.git"

val pekkoV = "1.1.3"
val loggingV = "3.9.5"
val configV = "1.4.0"
val collectionCompatV = "2.2.0"
val typesafeConfigV = "1.4.3"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % loggingV,
  "com.typesafe" % "config" % typesafeConfigV,
  "org.apache.pekko" %% "pekko-actor" % pekkoV,

//  "org.scala-lang.modules" %% "scala-collection-compat" % collectionCompatV,

  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test,
  "org.scalacheck" %% "scalacheck" % "1.18.1" % Test,
//  "com.storm-enroute" %% "scalameter" % "0.21" % Test,
  "org.slf4j" % "slf4j-simple" % "2.0.16" % Test
)

libraryDependencies ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, n)) =>
      List(
        "org.scala-lang.modules" %% "scala-collection-compat" % collectionCompatV,
        "com.storm-enroute" %% "scalameter" % "0.21" % Test
      )
    case _ => List(
      "org.scala-lang.modules" % "scala-collection-compat_2.13" % collectionCompatV,
    )
  }
}

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-F", sys.props.getOrElse("F", "1.0"))

// required so that test actor systems don't get messed up
Test / fork := true
Test / parallelExecution := false

publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")))

scmInfo := Some(ScmInfo(
  url("https://github.com/scredis/scredis"),
  "scm:git@github.com:scredis/scredis.git"
))

homepage := Some(url("https://github.com/scredis/scredis"))

developers := List(
  Developer(
    id="kpbochenek",
    name="kpbochenek",
    email="kpbochenek@gmail.com",
    url=url("https://github.com/kpbochenek")
  ))

bintrayVcsUrl := Some("https://github.com/scredis/scredis.git")
bintrayOrganization := Some("scredis")
bintrayRepository := "maven"
bintrayPackageLabels := Seq("redis")

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
