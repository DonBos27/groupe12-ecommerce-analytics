package com.ecommerce.models

/** Une ligne de users.json. `city` est nullable dans les données fournies (cf. sondage des fichiers). */
case class User(
  user_id: String,
  age: Int,
  annual_income: Double,
  city: Option[String],
  customer_segment: String,
  preferred_categories: Seq[String],
  registration_date: String   // format yyyyMMdd
)
