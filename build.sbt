ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "com.ecommerce"

val sparkVersion = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "EcommerceAnalytics",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      "com.typesafe"      % "config"     % "1.4.3"
    ),

    // Point d'entrée par défaut pour `sbt run` / le JAR assembly (Question 6.1)
    Compile / mainClass := Some("com.ecommerce.analytics.MainApp")
  )

// --- Génération du JAR exécutable (Question 1.2) : sbt-assembly ---
// Les dépendances Spark/Hadoop embarquent des fichiers META-INF en double
// (licences, signatures de service) : sans cette stratégie de fusion,
// `sbt assembly` échoue avec des erreurs de "deduplicate".
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}
