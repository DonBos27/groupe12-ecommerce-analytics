package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class QualitySummary(
  dataset: String,
  nb_lignes_lues: Long,
  nb_lignes_valides: Long,
  nb_lignes_rejetees: Long,
  taux_rejet: Double,
  nb_valeurs_nulles: Long
)

/**
  * Question 2.4 — Rapport de qualité des données.
  * Une ligne par dataset, affichée en console et sauvegardée en CSV.
  */
object DataQualityReport {

  private def countNulls(df: DataFrame): Long = {
    if (df.columns.isEmpty) return 0L

    val nullsParColonne = df.columns.map { nomColonne =>
      coalesce(sum(when(col(nomColonne).isNull, lit(1L)).otherwise(lit(0L))), lit(0L)).as(nomColonne)
    }

    val ligne = df.select(nullsParColonne: _*).first()

    (0 until ligne.length).map(i => ligne.getLong(i)).sum
  }

  def summarize(nom: String, brut: Dataset[_], valides: Dataset[_], rejetees: DataFrame): QualitySummary = {
    val nbLues     = brut.count()
    val nbValides  = valides.count()
    val nbRejetees = rejetees.count()

    val taux = if (nbLues == 0) 0.0
               else math.round(nbRejetees.toDouble / nbLues * 10000) / 100.0

    QualitySummary(nom, nbLues, nbValides, nbRejetees, taux, countNulls(brut.toDF()))
  }

  def build(spark: SparkSession, resumes: Seq[QualitySummary]): DataFrame = {
    import spark.implicits._
    resumes.toDF()
  }

  def show(rapport: DataFrame): Unit = {
    println("=== Rapport de qualité des données (Question 2.4) ===")
    rapport.show(false)
  }

  def save(rapport: DataFrame): Unit = {
    val base = ConfigLoader.getStringOrDefault("app.data.output.path", "output/")
    val chemin = base + "data_quality_report"

    rapport
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(chemin)

    println(s"[Qualité] rapport sauvegardé dans $chemin")
  }
}