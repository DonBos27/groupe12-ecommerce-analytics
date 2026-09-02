# EcommerceAnalytics

Projet Spark & Scala — Système d'analyse de données e-commerce (Groupe 3).

## Prérequis

- **Java 17** (Temurin/OpenJDK) — Spark 3.5.x ne supporte officiellement que Java 8/11/17. Si ta machine a une autre version par défaut (ex. Java 21/23 installée pour autre chose), fixe le JDK utilisé pour ce projet à chaque session de travail :
  ```
  export JAVA_HOME=$(/usr/libexec/java_home -v 17)
  java -version   
  ```
- **Scala 2.12.18** — géré directement par sbt via `build.sbt`, pas d'installation globale requise.
- **sbt 1.10.x**
- (Optionnel, pour tester `spark-submit` en local — voir section Déploiement) Spark **3.5.x** téléchargé depuis https://spark.apache.org/downloads.html (Homebrew n'installe que la dernière version majeure de Spark, donc pas utilisable directement ici).

## Compilation

_À compléter (Membre 1, Question 1.2) une fois build.sbt écrit :_
```
sbt compile
sbt assembly   
```

## Exécution locale

_À compléter :_
```
sbt run
```

## Déploiement (spark-submit)

_À compléter :_
```
spark-submit --class com.ecommerce.analytics.MainApp target/scala-2.12/EcommerceAnalytics-assembly-0.1.jar
```
