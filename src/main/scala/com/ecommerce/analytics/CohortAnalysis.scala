package com.ecommerce.analytics

import com.ecommerce.models.Transaction
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Question 4.2 — Analyse de cohortes utilisateurs.
  * Mesure la fidélité : parmi les clients arrivés un mois donné,
  * combien sont encore actifs les mois suivants.
  */
object CohortAnalysis {

  def prepare(transactions: Dataset[Transaction]): DataFrame = {
    val avecMois = transactions.select(
      col("user_id"),
      col("amount"),
      trunc(to_date(substring(col("timestamp"), 1, 8), "yyyyMMdd"), "month").as("transaction_month")
    )

    val parUtilisateur = Window.partitionBy("user_id")

    avecMois
      .withColumn("cohort_month", min(col("transaction_month")).over(parUtilisateur))
      .withColumn("period_index", months_between(col("transaction_month"), col("cohort_month")).cast("int"))
  }

  def cohortSizes(prepared: DataFrame): DataFrame =
    prepared
      .groupBy("cohort_month")
      .agg(countDistinct("user_id").as("nb_utilisateurs_initiaux"))

  def retentionMatrix(prepared: DataFrame): DataFrame = {
    val actifs = prepared
      .groupBy("cohort_month", "period_index")
      .agg(countDistinct("user_id").as("nb_utilisateurs_actifs"))

    actifs
      .join(cohortSizes(prepared), Seq("cohort_month"))
      .withColumn(
        "taux_retention",
        round(col("nb_utilisateurs_actifs") / col("nb_utilisateurs_initiaux") * 100, 2)
      )
      .select(
        date_format(col("cohort_month"), "yyyy-MM").as("cohort_month"),
        col("period_index"),
        col("nb_utilisateurs_initiaux"),
        col("nb_utilisateurs_actifs"),
        col("taux_retention")
      )
      .orderBy(col("cohort_month"), col("period_index"))
  }

  def bestCohortAt(matrice: DataFrame, periode: Int = 3): DataFrame =
    matrice
      .filter(col("period_index") === periode)
      .orderBy(col("taux_retention").desc)
      .limit(1)

  def save(matrice: DataFrame): Unit = {
    val base = ConfigLoader.getStringOrDefault("app.data.output.path", "output/")
    val chemin = base + "cohort_retention"

    matrice.coalesce(1).write.mode("overwrite").option("header", "true").csv(chemin)
    println(s"[Cohortes] matrice de rétention sauvegardée dans $chemin")
  }
}