ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "nga-test-task"
  )

libraryDependencies ++= Seq(
  "co.fs2"        %% "fs2-core"                      % "3.10.2",
  "co.fs2"        %% "fs2-io"                        % "3.10.2",
  "org.gnieh"     %% "fs2-data-csv"                  % "1.11.0",
  "org.gnieh"     %% "fs2-data-csv-generic"          % "1.11.0",
  "org.scalatest" %% "scalatest"                     % "3.2.18" % Test,
  "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0"  % Test
)
