ThisBuild / scalaVersion := "2.12.10"
ThisBuild / organization := "com.example"
Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val reco = (project in file("."))
  .settings(
    name := "Recommendation System",
    libraryDependencies ++= Seq (
      "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.0",
      "org.apache.spark" %% "spark-core" % "3.0.0",
      "org.apache.spark" %% "spark-sql" % "3.0.0",
      "org.apache.spark" %% "spark-mllib" % "3.0.0"
    )
  )