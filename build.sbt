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

// Exécute `sbt run`/`runMain` dans une JVM séparée (évite le bruit au shutdown lié au
// classpath temporaire de sbt) tout en gardant les mêmes ouvertures de module que .jvmopts.
Compile / run / fork := true
Compile / run / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false"
)
