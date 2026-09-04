package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
  * Partie 3 — Transformations avancées (Questions 3.2, 3.3 et bonus 3.4).
  *
  * Le point d'entrée `enrichTransactionData` enchaîne des étapes publiques et
  * indépendantes (jointures, UDF temporelle, fenêtres, tranche d'âge, comportements),
  * chacune prenant et renvoyant un DataFrame : elles se testent et se démontrent
  * séparément, et l'ordre d'exécution reste lisible dans une seule fonction.
  *
  * Schéma du DataFrame enrichi (en plus des neuf colonnes de `Transaction`) :
  *  - utilisateur : age, annual_income, city, customer_segment, preferred_categories, registration_date
  *  - produit     : product_name, product_category, price, rating, stock
  *  - marchand    : merchant_name, merchant_category, region, commission_rate, establishment_date
  *  - temps (3.1) : transaction_ts, transaction_date, hour, day_of_week, month,
  *                  is_weekend, day_period, is_working_hours
  *  - fenêtres (3.2) : user_transaction_rank, user_transaction_count, age_group
  *  - comportements (3.3) : amount_last_7_days, active_days_last_7_days, is_active_user,
  *                          days_since_previous_transaction
  *
  * Règle de nommage : les colonnes des tables de référence gardent leur nom d'origine,
  * sauf lorsqu'il entre en collision avec une autre table (`name`, `category`) ; elles
  * sont alors préfixées par le nom de leur table. Les colonnes de `Transaction` ne sont
  * jamais renommées : le DataFrame enrichi reste un sur-ensemble des transactions.
  */
object DataTransformation {

  /** Format des horodatages de `transactions.csv`, tel que compris par `to_timestamp`. */
  private val TimestampFormat = "yyyyMMddHHmmss"

  /** Largeur de la fenêtre glissante (Question 3.3) : le jour courant et les 6 jours précédents. */
  private val RollingWindowDays = 7

  /** Nombre minimal de jours distincts d'achat dans la fenêtre pour qualifier un utilisateur d'actif. */
  private val ActiveUserMinDays = 5

  /** Bornes des tranches d'âge (Question 3.2), incluses. */
  private val YoungMaxAge      = 25
  private val AdultMaxAge      = 44
  private val MiddleAgedMaxAge = 64

  // ---------------------------------------------------------------------------
  // Fenêtres partagées entre les étapes
  // ---------------------------------------------------------------------------

  /** Toutes les transactions d'un utilisateur, sans ordre : agrégats globaux par utilisateur. */
  private val perUser: WindowSpec = Window.partitionBy("user_id")

  /**
    * Transactions d'un utilisateur dans l'ordre chronologique.
    * `transaction_id` sert de second critère : deux achats à la même seconde reçoivent
    * ainsi des rangs distincts et reproductibles d'une exécution à l'autre.
    */
  private val perUserChronological: WindowSpec =
    Window.partitionBy("user_id").orderBy(col("transaction_ts"), col("transaction_id"))

  /**
    * Fenêtre glissante de 7 jours (Question 3.3), exprimée en jours calendaires :
    * pour une transaction du jour J, la fenêtre couvre [J - 6, J]. Le cadre est de
    * type RANGE sur un numéro de jour entier, et non ROWS : il englobe toutes les
    * transactions des jours concernés quel que soit leur nombre.
    */
  private val rollingSevenDays: WindowSpec =
    Window
      .partitionBy("user_id")
      .orderBy(col("transaction_day"))
      .rangeBetween(-(RollingWindowDays - 1), Window.currentRow)

  // ---------------------------------------------------------------------------
  // Question 3.2 — enrichTransactionData
  // ---------------------------------------------------------------------------

  /**
    * Question 3.2 — Enrichit les transactions validées avec les trois tables de
    * référence, les caractéristiques temporelles, les fonctions de fenêtrage, la
    * tranche d'âge et les indicateurs comportementaux de la Question 3.3.
    *
    * Le résultat contient exactement une ligne par transaction d'entrée : les
    * jointures sont des LEFT JOIN sur des clés uniques (voir `joinReferenceData`).
    */
  def enrichTransactionData(
    transactions: Dataset[Transaction],
    users: Dataset[User],
    products: Dataset[Product],
    merchants: Dataset[Merchant]
  ): DataFrame = {
    val joined = joinReferenceData(transactions.toDF(), users.toDF(), products.toDF(), merchants.toDF())
    val withTime = addTimeFeatures(joined)
    val withRanks = addUserWindowFeatures(withTime)
    val withAgeGroup = addAgeGroup(withRanks)
    addBehaviouralFeatures(withAgeGroup)
  }

  /**
    * Jointures d'enrichissement. Type de jointure et justification (repris dans CONTRIBUTIONS.md) :
    *
    *  - transactions ⟕ users     : LEFT — une transaction dont l'utilisateur est inconnu
    *    (référence orpheline) ou a été rejeté par la validation reste un achat réel ;
    *    ses attributs client sont nuls et la tranche d'âge vaut « Inconnu ».
    *  - transactions ⟕ products  : LEFT — même raisonnement ; le montant payé est porté
    *    par la transaction, le catalogue n'est qu'un complément descriptif.
    *  - transactions ⟕ merchants : LEFT — même raisonnement ; le chiffre d'affaires
    *    d'un marchand absent du référentiel n'est pas perdu, il apparaît sans nom.
    *
    * Une jointure INNER ferait disparaître silencieusement des transactions valides ;
    * la stratégie LEFT garantit l'invariant « autant de lignes en sortie qu'en entrée »,
    * puisque `user_id`, `product_id` et `merchant_id` sont uniques dans leur table.
    *
    * Les trois tables de référence sont petites (12 000, 6 000 et 600 lignes) : elles sont
    * diffusées (`broadcast`) à tous les exécuteurs, ce qui évite de redistribuer les
    * 138 000 transactions par le réseau à chaque jointure (Question 5.2). Le mécanisme se
    * désactive par `app.optimization.enable-broadcast = false` dans application.conf.
    */
  def joinReferenceData(
    transactions: DataFrame,
    users: DataFrame,
    products: DataFrame,
    merchants: DataFrame
  ): DataFrame = {
    val userAttributes = users.select(
      col("user_id"),
      col("age"),
      col("annual_income"),
      col("city"),
      col("customer_segment"),
      col("preferred_categories"),
      col("registration_date")
    )

    // `merchant_id` du produit est volontairement écarté : la transaction porte déjà le marchand.
    val productAttributes = products.select(
      col("product_id"),
      col("name").as("product_name"),
      col("category").as("product_category"),
      col("price"),
      col("rating"),
      col("stock")
    )

    val merchantAttributes = merchants.select(
      col("merchant_id"),
      col("name").as("merchant_name"),
      col("category").as("merchant_category"),
      col("region"),
      col("commission_rate"),
      col("establishment_date")
    )

    val joined = transactions
      .join(broadcastIfEnabled(userAttributes), Seq("user_id"), "left")
      .join(broadcastIfEnabled(productAttributes), Seq("product_id"), "left")
      .join(broadcastIfEnabled(merchantAttributes), Seq("merchant_id"), "left")

    // Une jointure sur `Seq(clé)` place la clé en tête : on rétablit l'ordre des colonnes
    // de la transaction, suivies des attributs de référence dans l'ordre des jointures.
    val transactionColumns = transactions.columns
    val referenceColumns = joined.columns.filterNot(transactionColumns.contains)
    joined.select((transactionColumns ++ referenceColumns).map(col): _*)
  }

  /** Applique `broadcast` selon la configuration externalisée (Question 5.2 / 7.1). */
  private def broadcastIfEnabled(reference: DataFrame): DataFrame =
    if (ConfigLoader.getBooleanOrDefault("app.optimization.enable-broadcast", default = true)) broadcast(reference)
    else reference

  /**
    * Question 3.1 appliquée au DataFrame : ajoute les colonnes temporelles.
    *
    * Deux représentations coexistent volontairement :
    *  - `transaction_ts` / `transaction_date`, obtenues par `to_timestamp` (fonction
    *    native Catalyst), servent au tri des fenêtres et aux calculs de délais ;
    *  - les six caractéristiques métier proviennent de l'UDF imposée par le sujet.
    *
    * La structure renvoyée par l'UDF est aplatie en colonnes de premier niveau : les
    * étapes suivantes et l'écriture CSV (qui ne supporte pas les structures) y accèdent
    * directement. Un horodatage invalide donne des colonnes nulles, jamais une erreur.
    */
  def addTimeFeatures(df: DataFrame): DataFrame =
    df
      .withColumn("transaction_ts", to_timestamp(col("timestamp"), TimestampFormat))
      .withColumn("transaction_date", to_date(col("transaction_ts")))
      .withColumn("time_features", TimeFeatures.extractTimeFeatures(col("timestamp")))
      .select(col("*"), col("time_features.*"))
      .drop("time_features")

  /**
    * Question 3.2 — Fonctions de fenêtrage par utilisateur :
    *  - `user_transaction_rank`  : rang chronologique de la transaction chez cet utilisateur
    *    (1 = premier achat). `row_number` plutôt que `rank` : chaque transaction reçoit un
    *    rang unique, y compris en cas d'égalité d'horodatage.
    *  - `user_transaction_count` : nombre total de transactions de l'utilisateur, répété
    *    sur chacune de ses lignes.
    */
  def addUserWindowFeatures(df: DataFrame): DataFrame =
    df
      .withColumn("user_transaction_rank", row_number().over(perUserChronological))
      .withColumn("user_transaction_count", count(lit(1)).over(perUser))

  /**
    * Question 3.2 — Tranche d'âge du client.
    *
    * Le sujet définit « Jeune » (moins de 25 ans) et « Adulte » (26 à 44 ans) : l'âge de
    * 25 ans n'appartient à aucune tranche. Il est rattaché à « Jeune », la borne inférieure
    * de « Adulte » (26) étant explicite. Un âge nul (utilisateur inconnu ou rejeté) donne
    * « Inconnu », valeur attendue par le rapport marchand de la Question 4.1.
    */
  def ageGroup(age: Column): Column =
    when(age.isNull, lit("Inconnu"))
      .when(age <= YoungMaxAge, lit("Jeune"))
      .when(age <= AdultMaxAge, lit("Adulte"))
      .when(age <= MiddleAgedMaxAge, lit("Âge Moyen"))
      .otherwise(lit("Senior"))

  def addAgeGroup(df: DataFrame): DataFrame =
    df.withColumn("age_group", ageGroup(col("age")))

  // ---------------------------------------------------------------------------
  // Question 3.3 — Analyse par partition Window
  // ---------------------------------------------------------------------------

  /**
    * Question 3.3 — Indicateurs comportementaux calculés par fenêtrage :
    *
    *  - `amount_last_7_days` : montant cumulé des transactions de l'utilisateur sur la
    *    fenêtre glissante [J - 6, J], transaction courante incluse.
    *  - `active_days_last_7_days` : nombre de jours distincts avec au moins un achat sur
    *    cette même fenêtre. `countDistinct` n'est pas autorisé sur une fenêtre ; on
    *    collecte l'ensemble des jours (`collect_set`) et on en prend la taille.
    *  - `is_active_user` : 1 si l'utilisateur a acheté au moins 5 jours distincts sur la
    *    fenêtre se terminant à la transaction courante, 0 sinon.
    *  - `days_since_previous_transaction` : jours écoulés depuis l'achat précédent du même
    *    utilisateur (`lag`) ; nul pour son premier achat, 0 si le précédent date du même jour.
    *
    * `transaction_day` (nombre de jours depuis le 1er janvier 1970) est une colonne de
    * travail : un cadre RANGE exige une colonne d'ordre numérique, et raisonner en jours
    * entiers évite les effets de bord d'une fenêtre en secondes qui chevaucherait deux journées.
    */
  def addBehaviouralFeatures(df: DataFrame): DataFrame = {
    val previousDate = lag(col("transaction_date"), 1).over(perUserChronological)

    df
      .withColumn("transaction_day", unix_date(col("transaction_date")))
      .withColumn("amount_last_7_days", round(sum(col("amount")).over(rollingSevenDays), 2))
      .withColumn("active_days_last_7_days", size(collect_set(col("transaction_day")).over(rollingSevenDays)))
      .withColumn("is_active_user", when(col("active_days_last_7_days") >= ActiveUserMinDays, 1).otherwise(0))
      .withColumn("days_since_previous_transaction", datediff(col("transaction_date"), previousDate))
      .drop("transaction_day")
  }

  // ---------------------------------------------------------------------------
  // Question 3.4 (BONUS) — Détection de transactions suspectes
  // ---------------------------------------------------------------------------

  /** Écart au panier moyen au-delà duquel le montant est jugé anormal (en pourcentage). */
  private val SuspiciousDeviationPct = 300

  /** Délai avec l'achat précédent en deçà duquel la transaction est jugée précipitée (en secondes). */
  private val SuspiciousDelaySeconds = 5 * 60

  /** Nombre minimal de critères réunis pour déclarer une transaction suspecte. */
  private val SuspiciousMinCriteria = 2

  /**
    * Question 3.4 (BONUS) — Ajoute trois colonnes au DataFrame enrichi :
    *  - `avg_basket_deviation_pct` : écart, en pourcentage, entre le montant et le panier
    *    moyen de l'utilisateur (moyenne de toutes ses transactions validées) ;
    *  - `seconds_since_previous_transaction` : délai en secondes avec son achat précédent ;
    *  - `is_suspicious` : 1 si au moins deux des quatre critères sont réunis
    *    (écart > 300 %, période « Night », délai < 5 minutes, paiement CRYPTO), 0 sinon.
    *
    * Les critères sont convertis en 0/1 avant d'être additionnés : une comparaison sur
    * une valeur nulle vaut `null`, qui compterait comme un critère non rempli via
    * `otherwise(0)`, sans propager le nul dans la somme.
    */
  def addSuspiciousFlag(enriched: DataFrame): DataFrame = {
    val userAverageAmount = avg(col("amount")).over(perUser)
    val epochSeconds = unix_timestamp(col("transaction_ts"))
    val previousEpochSeconds = lag(epochSeconds, 1).over(perUserChronological)

    val withIndicators = enriched
      .withColumn("avg_basket_deviation_pct",
        round((col("amount") - userAverageAmount) / userAverageAmount * 100, 2))
      .withColumn("seconds_since_previous_transaction", epochSeconds - previousEpochSeconds)

    val criteria: Seq[Column] = Seq(
      col("avg_basket_deviation_pct") > SuspiciousDeviationPct,
      col("day_period") === "Night",
      col("seconds_since_previous_transaction") < SuspiciousDelaySeconds,
      col("payment_method") === "CRYPTO"
    )
    val criteriaMet = criteria.map(criterion => when(criterion, 1).otherwise(0)).reduce(_ + _)

    withIndicators.withColumn("is_suspicious", when(criteriaMet >= SuspiciousMinCriteria, 1).otherwise(0))
  }

  /** Question 3.4 (BONUS) — Affiche le nombre de transactions suspectes et les 20 montants les plus élevés. */
  def showSuspiciousTransactions(flagged: DataFrame, top: Int = 20): Unit = {
    val suspicious = flagged.filter(col("is_suspicious") === 1)
    println(s"[Transformation] Transactions suspectes détectées : ${suspicious.count()}")
    suspicious
      .select("transaction_id", "user_id", "amount", "avg_basket_deviation_pct", "day_period",
        "seconds_since_previous_transaction", "payment_method")
      .orderBy(col("amount").desc)
      .show(top, truncate = false)
  }

  // ---------------------------------------------------------------------------
  // Écriture des résultats
  // ---------------------------------------------------------------------------

  /**
    * Sauvegarde le DataFrame enrichi.
    *
    * @param format     "csv" (avec en-tête) ou "parquet".
    * @param singleFile regroupe la sortie en un seul fichier (`coalesce(1)`). À réserver aux
    *                   petits volumes : toutes les données transitent alors par un seul exécuteur.
    */
  def saveTransformedData(
    df: DataFrame,
    path: String,
    format: String = "csv",
    singleFile: Boolean = false
  ): Unit = {
    val output = if (singleFile) df.coalesce(1) else df

    format.toLowerCase match {
      case "csv"     => toCsvCompatible(output).write.mode("overwrite").option("header", "true").csv(path)
      case "parquet" => output.write.mode("overwrite").parquet(path)
      case other     => throw new IllegalArgumentException(s"Format de sortie non supporté : $other (attendu : csv ou parquet)")
    }
    println(s"[Transformation] Données enrichies sauvegardées au format $format dans $path")
  }

  /**
    * Le format CSV n'accepte que des colonnes scalaires : les tableaux (`preferred_categories`)
    * et les éventuelles structures sont sérialisés en JSON, sans modifier le schéma Parquet.
    */
  private def toCsvCompatible(df: DataFrame): DataFrame = {
    val columns = df.schema.fields.map { field =>
      field.dataType match {
        case _: ArrayType | _: StructType => to_json(col(field.name)).as(field.name)
        case _                            => col(field.name)
      }
    }
    df.select(columns: _*)
  }
}
