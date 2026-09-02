package com.ecommerce.models

/** Une ligne de merchants.csv (schéma inféré par Spark, cf. Question 2.1). */
case class Merchant(
  merchant_id: String,
  name: String,
  category: String,
  region: String,
  commission_rate: Double,
  establishment_date: String   // format yyyyMMdd
)
