// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
import java.nio.charset.StandardCharsets
import sbt._

import scala.util.matching.Regex

/**
 * Generates Apache Pekko sources from Akka sources.
 *
 * The Akka source is the source of truth. The Pekko sources are produced from it by a list of
 * rewrite rules and live under `target/` only. Nothing in this file is specific to one project:
 * a build passes the directories to read, the directory to write to, and (optionally) its own
 * rules. See docs/pekko-generation.md.
 *
 * The rules are syntactic (regular expressions over source text). No semantic information is
 * needed for an Akka to Pekko rename, so no compiler, semanticdb or external tool is run.
 */
object PekkoGeneration {

  /** A named replacement applied to the text of every source file. */
  final case class Rule(name: String, pattern: Regex, replacement: String)

  /**
   * The rules of the Akka to Pekko rename. Applied in order.
   *
   *  - `akka-package`: a package or class path that starts with `akka.` becomes
   *    `org.apache.pekko.`. The pattern does not match `akka.` that is part of a longer path
   *    (`scredis.io.akka.x`) or of an identifier (`myakka.x`).
   */
  val akkaToPekko: Seq[Rule] = Seq(
    Rule("akka-package", """(?<![\w.])akka\.""".r, Regex.quoteReplacement("org.apache.pekko."))
  )

  /** First line of every generated file. */
  val defaultHeader: String =
    "// Generated from the Akka sources (akka.* -> org.apache.pekko.*) by PekkoGeneration; do not edit. " +
      "Modified by Sopranoworks: Apache Pekko variant."

  /** Text left after the rules ran that must not remain in generated sources: `akka.` paths. */
  val leftover: Regex = """(?<![\w.])akka\.[A-Za-z]""".r

  /**
   * Rewrites every `.scala` file below `sourceDirs` into `out` (keeping the path relative to its
   * source directory) and returns the files written.
   */
  def generate(
    sourceDirs: Seq[File],
    out: File,
    rules: Seq[Rule] = akkaToPekko,
    header: Option[String] = Some(defaultHeader)
  ): Seq[File] = {
    IO.delete(out)
    val written = for {
      dir <- sourceDirs if dir.isDirectory
      file <- (dir ** "*.scala").get if file.isFile
      relative <- IO.relativize(dir, file).toSeq
    } yield {
      val rewritten = rules.foldLeft(IO.read(file, StandardCharsets.UTF_8)) { (text, rule) =>
        rule.pattern.replaceAllIn(text, rule.replacement)
      }
      val target = out / relative
      IO.write(target, header.fold(rewritten)(_ + "\n" + rewritten), StandardCharsets.UTF_8)
      target
    }
    written
  }

  /**
   * Fails if a generated file still contains `akka.` (outside identifiers and longer paths).
   * Files for which `allowed` is true (deliberate compatibility code) are not checked.
   */
  def check(generated: Seq[File], allowed: File => Boolean = _ => false): Unit = {
    val offending = for {
      file <- generated if !allowed(file)
      (line, number) <- IO.readLines(file, StandardCharsets.UTF_8).zipWithIndex
      if line != defaultHeader && leftover.findFirstIn(line).isDefined
    } yield s"${file.getPath}:${number + 1}: ${line.trim}"
    if (offending.nonEmpty) {
      sys.error("Generated Pekko sources still reference akka.:\n" + offending.mkString("\n"))
    }
  }
}
