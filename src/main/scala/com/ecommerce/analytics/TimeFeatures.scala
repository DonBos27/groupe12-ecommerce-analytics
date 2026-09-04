package com.ecommerce.analytics

import java.time.format.{DateTimeFormatter, ResolverStyle, TextStyle}
import java.time.{DayOfWeek, LocalDateTime}
import java.util.Locale

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Try

/**
  * Caractéristiques temporelles extraites d'un horodatage (Question 3.1).
  *
  * Les noms de champs sont en snake_case, comme les case classes du package
  * `com.ecommerce.models` : ils deviennent directement les noms de colonnes
  * lorsque la structure renvoyée par l'UDF est aplatie dans le DataFrame.
  *
  * @param hour             heure de la transaction, de 0 à 23
  * @param day_of_week      jour de la semaine en toutes lettres (Monday … Sunday)
  * @param month            mois en toutes lettres (January … December)
  * @param is_weekend       1 si samedi ou dimanche, 0 sinon
  * @param day_period       Morning [6h, 12h[, Afternoon [12h, 18h[, Evening [18h, 22h[, Night sinon
  * @param is_working_hours 1 si l'heure est comprise dans [9h, 17h[, 0 sinon
  */
case class TimeFeatures(
  hour: Int,
  day_of_week: String,
  month: String,
  is_weekend: Int,
  day_period: String,
  is_working_hours: Int
)

/**
  * Question 3.1 — UDF `extractTimeFeatures`.
  *
  * La logique métier est isolée dans des fonctions pures (`compute`, `dayPeriod`, …)
  * indépendantes de Spark : elles se testent sans SparkSession et l'UDF n'est qu'un
  * adaptateur autour de `compute`.
  *
  * Robustesse : une chaîne nulle, vide ou mal formée produit `None`, donc une valeur
  * nulle côté Spark ; le job ne s'interrompt jamais à cause d'un horodatage invalide.
  */
object TimeFeatures {

  /** Format attendu par le sujet : yyyyMMddHHmmss (ex. 20240701021822). */
  val TimestampPattern: String = "uuuuMMddHHmmss"

  /**
    * Analyseur strict : un jour inexistant (20250230…) ou une heure à 25 est rejeté,
    * alors que la résolution par défaut (SMART) le corrigerait silencieusement.
    * En mode STRICT, la lettre `y` (année d'ère) exige un champ d'ère absent de nos
    * chaînes et ferait échouer toutes les analyses ; on utilise donc `u` (année proleptique).
    */
  private val Formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern(TimestampPattern).withResolverStyle(ResolverStyle.STRICT)

  /**
    * Locale figée pour les libellés textuels : sans cela, `day_of_week` et `month`
    * dépendraient de la locale de la machine qui exécute le job (« Monday » ici,
    * « lundi » ailleurs), ce qui rendrait les résultats non reproductibles.
    */
  private val LabelLocale: Locale = Locale.ENGLISH

  /** Bornes des périodes de la journée, telles que définies par le sujet. */
  private val MorningStart   = 6
  private val AfternoonStart = 12
  private val EveningStart   = 18
  private val NightStart     = 22

  /** Bornes des heures ouvrées : [9h, 17h[, demi-ouvert comme les périodes de la journée. */
  private val WorkingHoursStart = 9
  private val WorkingHoursEnd   = 17

  /**
    * Étiquette de période de la journée.
    * Le sujet ne mentionne pas explicitement les heures de 0h à 5h ; elles sont
    * rattachées à « Night », prolongement naturel de la tranche « 22h et au-delà ».
    */
  def dayPeriod(hour: Int): String = hour match {
    case h if h >= MorningStart   && h < AfternoonStart => "Morning"
    case h if h >= AfternoonStart && h < EveningStart   => "Afternoon"
    case h if h >= EveningStart   && h < NightStart     => "Evening"
    case _                                              => "Night"
  }

  /** 1 si l'heure appartient à [9h, 17h[, 0 sinon. */
  def isWorkingHours(hour: Int): Int =
    if (hour >= WorkingHoursStart && hour < WorkingHoursEnd) 1 else 0

  /** 1 si samedi ou dimanche, 0 sinon. */
  def isWeekend(day: DayOfWeek): Int =
    if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) 1 else 0

  /**
    * Analyse un horodatage au format yyyyMMddHHmmss.
    * Renvoie `None` pour une chaîne nulle, vide ou non conforme au format.
    */
  def parseTimestamp(raw: String): Option[LocalDateTime] =
    Option(raw)
      .filter(_.nonEmpty)
      .flatMap(value => Try(LocalDateTime.parse(value, Formatter)).toOption)

  /** Construit les caractéristiques temporelles d'une date-heure déjà analysée. */
  def fromDateTime(dateTime: LocalDateTime): TimeFeatures = {
    val hour = dateTime.getHour
    TimeFeatures(
      hour             = hour,
      day_of_week      = dateTime.getDayOfWeek.getDisplayName(TextStyle.FULL, LabelLocale),
      month            = dateTime.getMonth.getDisplayName(TextStyle.FULL, LabelLocale),
      is_weekend       = isWeekend(dateTime.getDayOfWeek),
      day_period       = dayPeriod(hour),
      is_working_hours = isWorkingHours(hour)
    )
  }

  /** Point d'entrée pur : chaîne brute → caractéristiques temporelles, ou `None` si invalide. */
  def compute(raw: String): Option[TimeFeatures] = parseTimestamp(raw).map(fromDateTime)

  /**
    * L'UDF demandée par le sujet. UDF typée : Spark déduit le schéma de sortie
    * (une structure `TimeFeatures` nullable) de la signature Scala.
    * Usage : `df.withColumn("time_features", TimeFeatures.extractTimeFeatures(col("timestamp")))`.
    */
  val extractTimeFeatures: UserDefinedFunction = udf((raw: String) => compute(raw))

  /** Enregistre l'UDF pour un usage en Spark SQL : `SELECT extractTimeFeatures(timestamp) …`. */
  def register(spark: SparkSession): UserDefinedFunction =
    spark.udf.register("extractTimeFeatures", extractTimeFeatures)
}
