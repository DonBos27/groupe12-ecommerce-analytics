package com.ecommerce.analytics

import com.ecommerce.utils.{ConfigLoader, SparkSessionBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Vérification autonome de la Partie 3 (Membre 3), exécutable sans le reste du pipeline :
  *
  *   sbt "runMain com.ecommerce.analytics.TransformationCheck"
  *
  * 1. Tests unitaires de l'UDF `extractTimeFeatures` : fonctions pures, sans SparkSession.
  * 2. Invariants du DataFrame enrichi, calculés sur les données réelles du projet :
  *    conservation du nombre de lignes, cohérence des rangs, bornes des indicateurs, etc.
  * 3. Écriture CSV et Parquet puis relecture, pour s'assurer que le schéma est sérialisable.
  *
  * Le programme se termine avec un code de retour non nul dès qu'une vérification échoue,
  * ce qui permet de l'enchaîner dans un script. `build.sbt` ne déclarant aucune librairie
  * de test, ce format « programme principal » remplace une suite ScalaTest.
  */
object TransformationCheck {

  private val failures = ListBuffer.empty[String]

  /** Enregistre le résultat d'une vérification et l'affiche immédiatement. */
  private def check(label: String)(condition: => Boolean): Unit = {
    val passed = try condition catch { case e: Exception => println(s"      exception : ${e.getMessage}"); false }
    println(s"  [${if (passed) "OK" else "KO"}] $label")
    if (!passed) failures += label
  }

  def main(args: Array[String]): Unit = {
    println("=== Partie 3 — Vérifications de l'UDF (sans Spark) ===")
    checkTimeFeatures()

    println("\n=== Partie 3 — Vérifications sur les données réelles ===")
    val spark = SparkSessionBuilder.build(appName = "TransformationCheck")
    spark.sparkContext.setLogLevel("WARN")
    try {
      checkEnrichment(spark)
    } finally {
      spark.stop()
    }

    println()
    if (failures.isEmpty) {
      println("Toutes les vérifications de la Partie 3 sont passées.")
    } else {
      println(s"${failures.size} vérification(s) en échec :")
      failures.foreach(label => println(s"  - $label"))
      sys.exit(1)
    }
  }

  // ---------------------------------------------------------------------------
  // 1. UDF extractTimeFeatures (Question 3.1)
  // ---------------------------------------------------------------------------

  private def checkTimeFeatures(): Unit = {
    // Robustesse : aucune de ces entrées ne doit lever d'exception, toutes donnent None.
    val nullString: String = null
    val invalid = Seq(
      nullString      -> "chaîne nulle",
      ""              -> "chaîne vide",
      "NA"            -> "texte quelconque",
      "20250715"      -> "date seule (8 caractères)",
      "202507151908"  -> "sans les secondes (12 caractères)",
      "2025071519080900" -> "deux caractères de trop",
      "20250230120000" -> "30 février (jour inexistant)",
      "20250715250000" -> "heure 25"
    )
    invalid.foreach { case (input, description) =>
      check(s"UDF : $description -> None")(TimeFeatures.compute(input).isEmpty)
    }

    // Valeurs de référence calculées à la main sur le calendrier 2024-2025.
    check("UDF : mardi 15/07/2025 19:08:09 -> Evening, hors week-end, hors heures ouvrées") {
      TimeFeatures.compute("20250715190809").contains(TimeFeatures(19, "Tuesday", "July", 0, "Evening", 0))
    }
    check("UDF : samedi 19/07/2025 10:15:00 -> Morning, week-end, heures ouvrées") {
      TimeFeatures.compute("20250719101500").contains(TimeFeatures(10, "Saturday", "July", 1, "Morning", 1))
    }
    check("UDF : lundi 01/01/2024 00:00:00 -> Night (0h rattaché à Night)") {
      TimeFeatures.compute("20240101000000").contains(TimeFeatures(0, "Monday", "January", 0, "Night", 0))
    }
    check("UDF : dimanche 20/07/2025 13:30:00 -> Afternoon, week-end, heures ouvrées") {
      TimeFeatures.compute("20250720133000").contains(TimeFeatures(13, "Sunday", "July", 1, "Afternoon", 1))
    }

    // Bornes des intervalles demi-ouverts.
    check("day_period : bornes [6,12[ [12,18[ [18,22[ et Night ailleurs") {
      TimeFeatures.dayPeriod(5) == "Night" && TimeFeatures.dayPeriod(6) == "Morning" &&
      TimeFeatures.dayPeriod(11) == "Morning" && TimeFeatures.dayPeriod(12) == "Afternoon" &&
      TimeFeatures.dayPeriod(17) == "Afternoon" && TimeFeatures.dayPeriod(18) == "Evening" &&
      TimeFeatures.dayPeriod(21) == "Evening" && TimeFeatures.dayPeriod(22) == "Night" &&
      TimeFeatures.dayPeriod(23) == "Night" && TimeFeatures.dayPeriod(0) == "Night"
    }
    check("is_working_hours : 1 sur [9,17[, 0 ailleurs") {
      TimeFeatures.isWorkingHours(8) == 0 && TimeFeatures.isWorkingHours(9) == 1 &&
      TimeFeatures.isWorkingHours(16) == 1 && TimeFeatures.isWorkingHours(17) == 0
    }
  }

  // ---------------------------------------------------------------------------
  // 2. DataFrame enrichi (Questions 3.2, 3.3 et bonus 3.4)
  // ---------------------------------------------------------------------------

  private def checkEnrichment(spark: SparkSession): Unit = {
    val ingestion = new DataIngestion(spark)
    val (validTransactions, _) = DataValidation.validateTransactions(ingestion.readTransactions())
    val (validUsers, _)        = DataValidation.validateUsers(ingestion.readUsers())
    val (validProducts, _)     = DataValidation.validateProducts(ingestion.readProducts())
    val (validMerchants, _)    = DataValidation.validateMerchants(ingestion.readMerchants())

    val enriched = DataTransformation
      .enrichTransactionData(validTransactions, validUsers, validProducts, validMerchants)
      .cache()

    val inputCount = validTransactions.count()
    val enrichedCount = enriched.count()
    println(s"  transactions valides : $inputCount, lignes enrichies : $enrichedCount")

    println("\n  Aperçu du DataFrame enrichi :")
    enriched
      .select("transaction_id", "user_id", "amount", "timestamp", "merchant_name", "age_group",
        "day_of_week", "day_period", "user_transaction_rank", "user_transaction_count",
        "amount_last_7_days", "active_days_last_7_days", "is_active_user", "days_since_previous_transaction")
      .orderBy("user_id", "user_transaction_rank")
      .show(12, truncate = false)

    // --- Jointures (3.2) -----------------------------------------------------
    check("jointures LEFT : autant de lignes en sortie qu'en entrée")(enrichedCount == inputCount)
    check("jointures : aucune ligne dupliquée par les jointures") {
      enriched.select("transaction_id").distinct().count() == enrichedCount
    }
    val orphanUsers = enriched.filter(col("age").isNull).count()
    val orphanMerchants = enriched.filter(col("merchant_name").isNull).count()
    println(s"      transactions sans utilisateur connu : $orphanUsers, sans marchand connu : $orphanMerchants")
    check("jointures : les références orphelines sont conservées (colonnes nulles)")(orphanUsers > 0 && orphanMerchants > 0)

    // --- UDF appliquée (3.1) -------------------------------------------------
    check("temps : aucun horodatage validé ne reste sans caractéristiques") {
      enriched.filter(col("hour").isNull || col("transaction_ts").isNull).count() == 0
    }
    check("temps : l'UDF et to_timestamp s'accordent sur l'heure") {
      enriched.filter(col("hour") =!= hour(col("transaction_ts"))).count() == 0
    }
    check("temps : day_period ne prend que les quatre valeurs attendues") {
      distinctValues(enriched, "day_period").subsetOf(Set("Morning", "Afternoon", "Evening", "Night"))
    }
    check("temps : is_weekend et is_working_hours valent 0 ou 1") {
      distinctValues(enriched, "is_weekend").subsetOf(Set(0, 1)) &&
      distinctValues(enriched, "is_working_hours").subsetOf(Set(0, 1))
    }

    // --- Fenêtres et tranche d'âge (3.2) -------------------------------------
    val rankSummary = enriched
      .groupBy("user_id")
      .agg(
        count(lit(1)).as("nb"),
        max("user_transaction_rank").as("max_rank"),
        countDistinct("user_transaction_rank").as("nb_ranks"),
        min("user_transaction_count").as("min_total"),
        max("user_transaction_count").as("max_total")
      )
    check("fenêtres : rangs uniques de 1 à N pour chaque utilisateur") {
      rankSummary.filter(col("max_rank") =!= col("nb") || col("nb_ranks") =!= col("nb")).count() == 0
    }
    check("fenêtres : user_transaction_count égal au nombre réel de transactions") {
      rankSummary.filter(col("min_total") =!= col("nb") || col("max_total") =!= col("nb")).count() == 0
    }
    check("tranche d'âge : cinq valeurs attendues, « Inconnu » exactement lorsque l'âge est nul") {
      distinctValues(enriched, "age_group").subsetOf(Set("Jeune", "Adulte", "Âge Moyen", "Senior", "Inconnu")) &&
      enriched.filter((col("age").isNull) =!= (col("age_group") === "Inconnu")).count() == 0
    }
    check("tranche d'âge : bornes 25 / 44 / 64 respectées") {
      enriched.filter(
        (col("age") <= 25 && col("age_group") =!= "Jeune") ||
        (col("age").between(26, 44) && col("age_group") =!= "Adulte") ||
        (col("age").between(45, 64) && col("age_group") =!= "Âge Moyen") ||
        (col("age") >= 65 && col("age_group") =!= "Senior")
      ).count() == 0
    }

    // --- Comportements (3.3) -------------------------------------------------
    check("7 jours : le montant cumulé inclut au moins la transaction courante") {
      enriched.filter(col("amount_last_7_days") < col("amount") - 0.01).count() == 0
    }
    check("7 jours : le nombre de jours actifs est compris entre 1 et 7") {
      enriched.filter(!col("active_days_last_7_days").between(1, 7)).count() == 0
    }
    check("7 jours : is_active_user vaut 1 si et seulement si au moins 5 jours actifs") {
      enriched.filter((col("active_days_last_7_days") >= 5) =!= (col("is_active_user") === 1)).count() == 0
    }
    val activeUsers = enriched.filter(col("is_active_user") === 1).select("user_id").distinct().count()
    println(s"      utilisateurs actifs (5 jours distincts sur 7) : $activeUsers")
    check("7 jours : des utilisateurs actifs sont détectés")(activeUsers > 0)
    check("lag : délai nul exactement pour le premier achat, positif ou nul ensuite") {
      enriched.filter(
        (col("days_since_previous_transaction").isNull =!= (col("user_transaction_rank") === 1)) ||
        col("days_since_previous_transaction") < 0
      ).count() == 0
    }

    println("\n  Répartition par période de la journée :")
    enriched.groupBy("day_period").count().orderBy("day_period").show(false)
    println("  Répartition par tranche d'âge :")
    enriched.groupBy("age_group").count().orderBy("age_group").show(false)

    // --- Bonus 3.4 -----------------------------------------------------------
    println("  Bonus 3.4 — transactions suspectes :")
    val flagged = DataTransformation.addSuspiciousFlag(enriched)
    DataTransformation.showSuspiciousTransactions(flagged)
    check("suspectes : is_suspicious vaut 0 ou 1 et détecte des cas") {
      distinctValues(flagged, "is_suspicious") == Set(0, 1)
    }

    // --- Écriture / relecture ------------------------------------------------
    val basePath = ConfigLoader.getStringOrDefault("app.data.output.path", "output/") + "transformation_check/"
    DataTransformation.saveTransformedData(enriched, basePath + "csv", "csv")
    DataTransformation.saveTransformedData(enriched, basePath + "parquet", "parquet")
    check("écriture : le CSV relu contient autant de lignes que le DataFrame") {
      spark.read.option("header", "true").csv(basePath + "csv").count() == enrichedCount
    }
    check("écriture : le Parquet relu conserve le schéma et le nombre de lignes") {
      val reread = spark.read.parquet(basePath + "parquet")
      reread.count() == enrichedCount && reread.schema.fieldNames.toSet == enriched.schema.fieldNames.toSet
    }

    enriched.unpersist()
  }

  private def distinctValues(df: DataFrame, column: String): Set[Any] =
    df.select(column).distinct().collect().map(_.get(0)).toSet
}
