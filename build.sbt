/*
 *
 * Copyright (c) 2016, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory
 *
 * Written by Joshua Asplund <asplund1@llnl.gov>
 * LLNL-CODE-699384
 *
 * All rights reserved.
 *
 * This file is part of spark-hdf5.
 * For details, see https://github.com/LLNL/spark-hdf5
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Project metadata
organization := "LLNL"
name := "spark-hdf5"
version := "0.0.4"

scalaVersion := "2.11.8"
scalacOptions := Seq( "deprecation", "-feature" )

// Spark specific information
sparkVersion := "2.0.0"
sparkComponents ++= Seq("core", "sql")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
spIncludeMaven := false

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

// Test dependencies
val deps = Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.novocode" % "junit-interface" % "0.9" % "test"
)

lazy val shaded = Project("shaded", file(".")).settings(
  libraryDependencies ++= (deps.map(_ % "provided")),
  unmanagedJars += file("lib/sis-jhdf5-batteries_included.jar"),
  target := target.value / "shaded"
)

lazy val distribute = Project("distribution", file(".")).settings(
  spName := "LLNL/spark-hdf5",
  spShortDescription := "A plugin to enable Apache Spark to read HDF5 files",
  spDescription := "Integrates HDF5 into Spark",

  target := target.value / "distribution",
  spShade := true,
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  libraryDependencies ++= deps
)

scalastyleConfig := baseDirectory.value / "project/scalastyle-config.xml"

lazy val testScalastyle = taskKey[Unit]("testScalastyle")

testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value

(test in Test) <<= (test in Test) dependsOn testScalastyle

