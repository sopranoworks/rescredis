<!--
Copyright 2026 Sopranoworks
SPDX-License-Identifier: Apache-2.0
-->

# Generating the Apache Pekko artifact from the Akka source

rescredis has one source tree, written for Akka. The Pekko artifact (`rescredis-pekko`) is not
maintained separately: its sources are generated from the Akka source every time it is built. Fixes
and features go into the Akka source, and the Pekko artifact gets them, with the same behaviour and the
same version.

## Layout

| Directory | Used by | Content |
|---|---|---|
| `src/main/scala`, `src/main/scala-<scala version>` | both | shared code, written against `akka.*` |
| `src/test/scala`, `src/test/scala-<scala version>` | both | shared tests |
| `src/bench/scala` | both | ScalaMeter benchmarks |
| `src/{main,test}/scala-akka`, `resources-akka` | Akka only | what the rewrite cannot express |
| `src/{main,test}/scala-pekko`, `resources-pekko` | Pekko only | what the rewrite cannot express |
| `src/test/resources` | both | resources that do not depend on the variant |

`modules/akka` and `modules/pekko` are the sbt project directories (`target/` only; no sources).
Generated sources are written to `modules/pekko/target/scala-<v>/src_managed/{main,test,bench}/pekko-generated`.
They are never committed and never edited by hand: fix the Akka source or the rules.

## What is rewritten

The rules are in `project/PekkoGeneration.scala`, value `akkaToPekko`. Applied to every `.scala` file of
the shared directories, in order:

1. `akka-package`: `akka.` at the start of a path (`(?<![\w.])akka\.`) becomes `org.apache.pekko.`.
   Imports, fully qualified names, type references, class names in strings and in scaladoc. A longer
   path (`scredis.io.akka.x`) and an identifier (`myakka.x`) are not touched.

Then a header line is put on top of each generated file saying that it is generated and modified (the
sources jar of the Pekko artifact contains the generated files, and Apache-2.0 §4(b) asks that modified
files say so).

Names that merely contain "Akka" (`AkkaNonBlockingConnection`, `IO.Akka`, `akkaIODispatcherPath`) are not
rewritten. They are scredis's own names and stay the same in both artifacts.

## What the rewrite cannot express

Anything that differs in content, not in a package name, is a variant file. In this repository:

* `ConfigVariant` (`scala-akka`, `scala-pekko`): the key under `scredis.io` and how configuration and
  the `ActorSystem` are created. The Akka build uses the configuration as it is. The Pekko build accepts
  configuration written for Akka (`AkkaConfigCompat`, Pekko only).
* `AkkaConfigCompat` and its spec `AkkaConfigCompatSpec` (Pekko only).
* `reference.conf` (block name, dispatcher paths, mailbox class) and the test `application.conf`
  (`resources-akka`, `resources-pekko`).

Keep this surface small. Before adding a variant file, check whether a rule can express the difference.

## The check

Every generation is followed by a check (`PekkoGeneration.check`): the build fails if a generated source
still contains `akka.` (outside an identifier or a longer path). The variant directories are not generated
and not checked, so deliberate Akka references (the compatibility code) live there.

## Running it

Nothing to run separately. Generation is a source generator of the `rescredisPekko` project:

    sbt rescredisPekko/compile       # generates, checks and compiles
    sbt rescredisPekko/Test/compile
    sbt rescredisPekko/test          # needs Redis, see testing.md
    sbt +publish                     # both artifacts, every Scala version

To look at the output, build and open `modules/pekko/target/scala-<v>/src_managed/main/pekko-generated`.

## Adding a rule

Add a `Rule(name, regex, replacement)` to `akkaToPekko` in `project/PekkoGeneration.scala`. Rules are
syntactic: they see source text, not types, so write the pattern narrowly (a lookbehind that keeps it off
identifiers and longer paths) and run the build: the check reports every `akka.` that is left, and the
compiler reports what the rewrite got wrong. If the difference cannot be written as a narrow pattern, use
a variant file instead.

`PekkoGeneration.scala` does not know scredis: `generate(sourceDirs, out, rules, header)` and
`check(files, allowed)` take directories and rules as arguments, so another project can copy the file to its
`project/` directory, pass its own directories and extend `akkaToPekko`.

## Why a rule table and not Scalafix

The rename needs no type information: it is a package prefix. A rule table over text is a source generator
that runs inside sbt without another tool, plugin, network access or semanticdb (Scalafix would have to be
run per Scala version against compiled output, or in place on sources, which is not what a generated tree
under `target/` is). If a rewrite ever needs types (renamed members, changed signatures), Scalafix can take
over that rule without changing the layout above.
