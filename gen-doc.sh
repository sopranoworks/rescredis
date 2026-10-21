#!/bin/bash

sbt paradox
cp -r ./target/paradox/site/main/* docs/

sbt rescredis/doc
cp -r ./modules/akka/target/scala-2.13/api/ docs/
