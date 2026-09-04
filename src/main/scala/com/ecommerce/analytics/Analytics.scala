package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Partie 4 — Analytique Business.
  * Responsable : Membre C (CISSE ABDOULAHI DIT DIORO)
  * 
  * Questions :
  * - 4.1 : Rapport détaillé par marchand
  * - 4.2 : Analyse de cohortes (déléguée à CohortAnalysis)
  * - 4.3 : Segmentation RFM (Bonus)
  * - 4.4 : Analyse produits et catégories (Bonus)
  */
object Analytics {

  /**
    * Question 4.1 — Rapport détaillé par marchand.
    * Calcule les KPI pour chaque marchand.
    */
  def merchantReport(enrichedDF: DataFrame): DataFrame = {
    
    // 1. Métriques de base par marchand
    val merchantMetrics = enrichedDF
      .groupBy("merchant_id", "name", "category", "region", "commission_rate")
      .agg(
        sum("amount").as("chiffre_affaires_total"),
        count("transaction_id").as("nb_transactions"),
        countDistinct("user_id").as("nb_clients_uniques"),
        avg("amount").as("montant_moyen_transaction"),
        sum(col("amount") * col("commission_rate")).as("commission_totale")
      )

    // 2. Répartition des ventes par tranche d'âge
    val ageDistribution = enrichedDF
      .groupBy("merchant_id", "age_group")
      .agg(sum("amount").as("ventes_par_tranche_age"))
      .groupBy("merchant_id")
      .pivot("age_group", Seq("Jeune", "Adulte", "Âge Moyen", "Senior", "Inconnu"))
      .agg(first("ventes_par_tranche_age"))
      .withColumnRenamed("Jeune", "ventes_jeune")
      .withColumnRenamed("Adulte", "ventes_adulte")
      .withColumnRenamed("Âge Moyen", "ventes_age_moyen")
      .withColumnRenamed("Senior", "ventes_senior")
      .withColumnRenamed("Inconnu", "ventes_inconnu")

    // 3. Classements par catégorie et région (Window functions)
    val categoryWindow = Window.partitionBy("category").orderBy(col("chiffre_affaires_total").desc)
    val regionWindow = Window.partitionBy("region").orderBy(col("chiffre_affaires_total").desc)

    val rankedMetrics = merchantMetrics
      .withColumn("rang_categorie", row_number().over(categoryWindow))
      .withColumn("rang_region", row_number().over(regionWindow))

    // 4. Jointure avec la répartition par âge
    val finalReport = rankedMetrics
      .join(ageDistribution, Seq("merchant_id"), "left")
      .select(
        "merchant_id",
        "name",
        "category",
        "region",
        "commission_rate",
        "chiffre_affaires_total",
        "nb_transactions",
        "nb_clients_uniques",
        "montant_moyen_transaction",
        "commission_totale",
        "rang_categorie",
        "rang_region",
        "ventes_jeune",
        "ventes_adulte",
        "ventes_age_moyen",
        "ventes_senior",
        "ventes_inconnu"
      )
      .orderBy(col("chiffre_affaires_total").desc)

    finalReport
  }

  /**
    * Question 4.3 (Bonus) — Segmentation RFM des clients.
    * Calcule Récence, Fréquence, Montant et attribue des segments.
    */
  def rfmSegmentation(enrichedDF: DataFrame, referenceDate: String = "20251231235959"): DataFrame = {
    
    import enrichedDF.sparkSession.implicits._

    // Date de référence (dernière transaction du dataset)
    val maxDate = enrichedDF.agg(max("timestamp")).first().getString(0)
    val refDate = if (maxDate != null) maxDate else referenceDate

    // 1. Calcul des métriques RFM par utilisateur
    val rfmMetrics = enrichedDF
      .groupBy("user_id")
      .agg(
        // Récence : jours depuis la dernière transaction
        datediff(to_date(lit(refDate), "yyyyMMddHHmmss"), max(to_date(col("timestamp"), "yyyyMMddHHmmss"))).as("recence"),
        // Fréquence : nombre de transactions
        count("transaction_id").as("frequence"),
        // Montant : chiffre d'affaires total
        sum("amount").as("montant")
      )

    // 2. Attribution des scores RFM (quintiles)
    val rfmScored = rfmMetrics
      .withColumn("score_recence", ntile(5).over(Window.orderBy(col("recence").asc)))
      .withColumn("score_frequence", ntile(5).over(Window.orderBy(col("frequence").desc)))
      .withColumn("score_montant", ntile(5).over(Window.orderBy(col("montant").desc)))

    // 3. Score RFM combiné
    val rfmCombined = rfmScored
      .withColumn("rfm_score", concat(col("score_recence"), col("score_frequence"), col("score_montant")))
      .withColumn("rfm_score_numeric", col("score_recence") + col("score_frequence") + col("score_montant"))

    // 4. Attribution des segments métier
    val rfmSegmented = rfmCombined
      .withColumn("segment_rfm",
        when(col("score_recence") >= 4 && col("score_frequence") >= 4 && col("score_montant") >= 4, "Champions")
          .when(col("score_recence") >= 3 && col("score_frequence") >= 3 && col("score_montant") >= 4, "Clients fidèles")
          .when(col("score_recence") <= 2 && col("score_frequence") >= 3 && col("score_montant") >= 3, "À risque")
          .when(col("score_recence") <= 2 && col("score_frequence") <= 2 && col("score_montant") <= 2, "Perdus")
          .when(col("score_recence") >= 4 && col("score_frequence") <= 2 && col("score_montant") <= 2, "Nouveaux")
          .otherwise("Autre")
      )

    // 5. Croisement avec customer_segment de users.json
    val userSegments = enrichedDF
      .select("user_id", "customer_segment")
      .distinct()

    val finalRFM = rfmSegmented
      .join(userSegments, Seq("user_id"), "left")
      .select(
        "user_id",
        "recence",
        "frequence",
        "montant",
        "score_recence",
        "score_frequence",
        "score_montant",
        "rfm_score",
        "segment_rfm",
        "customer_segment"
      )
      .orderBy(col("rfm_score_numeric").desc)

    finalRFM
  }

  /**
    * Tableau croisé segment RFM vs customer_segment.
    */
  def rfmVsCustomerSegment(rfmDF: DataFrame): DataFrame = {
    rfmDF
      .groupBy("segment_rfm", "customer_segment")
      .agg(count("user_id").as("nb_clients"))
      .orderBy("segment_rfm", "customer_segment")
  }

  /**
    * Question 4.4 (Bonus) — Analyse produits et catégories.
    */
  def productCategoryAnalysis(enrichedDF: DataFrame): (DataFrame, DataFrame, DataFrame) = {

    // 1. Top 10 produits par chiffre d'affaires
    val topProducts = enrichedDF
      .groupBy("product_id", "name", "category", "price", "rating", "stock")
      .agg(
        sum("amount").as("chiffre_affaires"),
        count("transaction_id").as("nb_ventes")
      )
      .orderBy(col("chiffre_affaires").desc)
      .limit(10)
      .select(
        "product_id",
        "name",
        "category",
        "price",
        "rating",
        "stock",
        "chiffre_affaires",
        "nb_ventes"
      )

    // 2. CA par catégorie et région
    val categoryRegion = enrichedDF
      .groupBy("category", "region")
      .agg(
        sum("amount").as("chiffre_affaires"),
        count("transaction_id").as("nb_transactions")
      )

    // Calcul du pourcentage par région
    val regionTotal = categoryRegion
      .groupBy("region")
      .agg(sum("chiffre_affaires").as("total_region"))

    val categoryRegionWithPct = categoryRegion
      .join(regionTotal, Seq("region"), "left")
      .withColumn("pct_region", round(col("chiffre_affaires") / col("total_region") * 100, 2))
      .select(
        "category",
        "region",
        "chiffre_affaires",
        "nb_transactions",
        "pct_region"
      )
      .orderBy(col("region"), col("chiffre_affaires").desc)  // <-- LIGNE CORRIGÉE

    // 3. Répartition du CA par méthode de paiement et période de la journée
    val paymentPeriod = enrichedDF
      .groupBy("payment_method", "day_period")
      .agg(
        sum("amount").as("chiffre_affaires"),
        count("transaction_id").as("nb_transactions")
      )
      .orderBy("payment_method", "day_period")

    (topProducts, categoryRegionWithPct, paymentPeriod)
  }

  /**
    * Sauvegarde des résultats analytiques.
    */
  def saveAnalyticsResults(
    merchantReport: DataFrame,
    rfmResults: Option[DataFrame] = None,
    rfmCrossTab: Option[DataFrame] = None,
    topProducts: Option[DataFrame] = None,
    categoryRegion: Option[DataFrame] = None,
    paymentPeriod: Option[DataFrame] = None
  ): Unit = {

    val basePath = ConfigLoader.getStringOrDefault("app.data.output.path", "output/")
    val analyticsPath = basePath + "analytics/"

    // Sauvegarde du rapport marchand
    merchantReport
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(analyticsPath + "merchant_report")
    println(s"[Analytics] Rapport marchand sauvegardé dans ${analyticsPath}merchant_report")

    // Sauvegarde RFM si présent
    rfmResults.foreach { rfm =>
      rfm.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(analyticsPath + "rfm_segmentation")
      println(s"[Analytics] Segmentation RFM sauvegardée dans ${analyticsPath}rfm_segmentation")
    }

    rfmCrossTab.foreach { cross =>
      cross.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(analyticsPath + "rfm_vs_customer_segment")
      println(s"[Analytics] Croisement RFM/Customer sauvegardé dans ${analyticsPath}rfm_vs_customer_segment")
    }

    // Sauvegarde analyse produits
    topProducts.foreach { tp =>
      tp.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(analyticsPath + "top_products")
      println(s"[Analytics] Top produits sauvegardé dans ${analyticsPath}top_products")
    }

    categoryRegion.foreach { cr =>
      cr.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(analyticsPath + "category_region")
      println(s"[Analytics] Analyse catégorie/région sauvegardée dans ${analyticsPath}category_region")
    }

    paymentPeriod.foreach { pp =>
      pp.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(analyticsPath + "payment_period")
      println(s"[Analytics] Analyse paiement/période sauvegardée dans ${analyticsPath}payment_period")
    }
  }

  /**
    * Affiche un résumé des résultats analytiques dans la console.
    */
  def showAnalyticsSummary(
    merchantReport: DataFrame,
    rfmResults: Option[DataFrame] = None,
    topProducts: Option[DataFrame] = None
  ): Unit = {

    println("\n" + "=" * 80)
    println("  ANALYTICS - RÉSUMÉ DES RÉSULTATS")
    println("=" * 80)

    // Rapport marchand
    println("\n--- Top 10 marchands par chiffre d'affaires ---")
    merchantReport
      .select("name", "category", "region", "chiffre_affaires_total", "nb_transactions", "nb_clients_uniques")
      .limit(10)
      .show(10, false)

    // RFM
    rfmResults.foreach { rfm =>
      println("\n--- Top 10 clients par score RFM ---")
      rfm
        .select("user_id", "segment_rfm", "recence", "frequence", "montant", "rfm_score")
        .limit(10)
        .show(10, false)

      val counts = rfm.groupBy("segment_rfm").agg(count("user_id").as("nb_clients"))
      println("\n--- Distribution des segments RFM ---")
      counts.show(false)
    }

    // Top produits
    topProducts.foreach { tp =>
      println("\n--- Top 10 produits par chiffre d'affaires ---")
      tp.select("product_id", "name", "category", "chiffre_affaires", "nb_ventes", "rating", "stock")
        .show(10, false)
    }

    println("\n" + "=" * 80)
  }
}