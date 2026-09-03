package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
  * Question 2.2 — Validation des données.
  * Chaque fonction renvoie un couple (lignes valides, lignes rejetées),
  * les rejetées portant une colonne rejection_reason décrivant la ou les règles violées.
  */
object DataValidation {

  private type Rule = (Column, String)
  private def safe(condition: Column): Column = coalesce(condition, lit(false))
  private def split[T](ds: Dataset[T], rules: Seq[Rule]): (Dataset[T], DataFrame) = {

    // une ligne est valide si TOUTES les règles passent
    val isValid: Column = rules.map(r => safe(r._1)).reduce(_ && _)

    val valid: Dataset[T] = ds.filter(isValid)

    val reason: Column = concat_ws(" ; ", rules.map { case (condition, message) =>
      when(!safe(condition), lit(message))
    }: _*)

    val rejected: DataFrame = ds.filter(!isValid).withColumn("rejection_reason", reason)

    (valid, rejected)
  }

  def validateTransactions(ds: Dataset[Transaction]): (Dataset[Transaction], DataFrame) = {
    val minAmount = ConfigLoader.getDoubleOrDefault("app.validation.transaction.min-amount", 0.0)
    val tsLength  = ConfigLoader.getIntOrDefault("app.validation.transaction.timestamp-length", 14)

    split(ds, Seq(
      (col("amount") > minAmount,                s"amount <= $minAmount"),
      (length(col("timestamp")) === tsLength,    s"timestamp n'a pas $tsLength caracteres")
    ))
  }

  def validateUsers(ds: Dataset[User]): (Dataset[User], DataFrame) = {
    val minAge = ConfigLoader.getIntOrDefault("app.validation.user.min-age", 16)
    val maxAge = ConfigLoader.getIntOrDefault("app.validation.user.max-age", 100)

    split(ds, Seq(
      (col("age").between(minAge, maxAge), s"age hors de [$minAge, $maxAge]"),
      (col("annual_income") > 0,           "annual_income <= 0")
    ))
  }

  def validateProducts(ds: Dataset[Product]): (Dataset[Product], DataFrame) = {
    val minPrice  = ConfigLoader.getDoubleOrDefault("app.validation.product.min-price", 0.0)
    val minRating = ConfigLoader.getDoubleOrDefault("app.validation.product.min-rating", 1.0)
    val maxRating = ConfigLoader.getDoubleOrDefault("app.validation.product.max-rating", 5.0)

    split(ds, Seq(
      (col("price") > minPrice,                     s"price <= $minPrice"),
      (col("rating").between(minRating, maxRating), s"rating hors de [$minRating, $maxRating]")
    ))
  }

  def validateMerchants(ds: Dataset[Merchant]): (Dataset[Merchant], DataFrame) = {
    val minRate = ConfigLoader.getDoubleOrDefault("app.validation.merchant.min-commission-rate", 0.0)
    val maxRate = ConfigLoader.getDoubleOrDefault("app.validation.merchant.max-commission-rate", 1.0)

    split(ds, Seq(
      (col("commission_rate").between(minRate, maxRate), s"commission_rate hors de [$minRate, $maxRate]")
    ))
  }
}
