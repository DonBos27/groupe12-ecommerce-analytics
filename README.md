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

Le projet utilise le JDK 17 (Spark 3.5.x ne supporte officiellement que Java 8/11/17). Si un autre JDK est actif par défaut sur le poste, utiliser le script `sbt17.sh` fourni à la racine plutôt que `sbt` directement : il résout et exporte automatiquement `JAVA_HOME` vers le JDK 17 avant de lancer sbt.

```
./sbt17.sh compile
```

Compile les 16 fichiers Scala du projet (modèles, ingestion, validation, transformation, analytics, optimisations, orchestration). En cas de succès, sbt affiche `[success]`.

Pour générer le jar exécutable autonome (voir décision technique 5 dans CONTRIBUTIONS.md — `sbt assembly`, pas `sbt package`, car ce dernier n'embarque pas les dépendances) :

```
./sbt17.sh assembly
```

Le jar est produit dans `target/scala-2.12/EcommerceAnalytics-assembly-0.1.0.jar`.

## Exécution locale

```
./sbt17.sh run
```

Lance `MainApp`, qui exécute le pipeline complet (ingestion des 4 sources, validation, rapport de qualité, transformation/enrichissement, analytics marchands, analyse de cohortes) en mode `local[*]` (paramétrable dans `application.conf`, clé `app.spark.master`). Les chemins des fichiers d'entrée (`src/main/resources/data/`) et de sortie (`output/`) sont également lus depuis `application.conf`, pas codés en dur dans `MainApp`.

À la fin de l'exécution, on doit retrouver dans `output/` : `data_quality_report/` (CSV), `cohort_retention/` (CSV), les résultats d'analytics marchands, et les données enrichies (`transformed/` en CSV et `transformed_parquet/` en Parquet).

Pour ne rejouer que l'ingestion et vérifier les volumes attendus (138 047 transactions, 12 000 utilisateurs, 6 000 produits, 600 marchands) sans lancer tout le pipeline :

```
./sbt17.sh "runMain com.ecommerce.analytics.IngestionSmokeTest"
```

## Déploiement (spark-submit)

Nécessite une installation locale de Spark 3.5.x (voir Prérequis) et le jar assemblé (`./sbt17.sh assembly`). Le jar embarque déjà Spark et ses dépendances (voir décision technique 5), donc `spark-submit` n'a besoin de rien d'autre sur son classpath :

```
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
$SPARK_HOME/bin/spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master local[*] \
  target/scala-2.12/EcommerceAnalytics-assembly-0.1.0.jar
```

`$SPARK_HOME` est le dossier où l'archive Spark 3.5.x a été décompressée. Les chemins d'entrée/sortie restent ceux d'`application.conf` : lancer la commande depuis la racine du projet (`EcommerceAnalytics/`), ou adapter les chemins dans `application.conf` si le jar est déplacé ailleurs.
