package com.ecommerce.models

/** Une ligne de transactions.csv. Schéma imposé par le sujet (Partie "Description des données"). */
case class Transaction(
  transaction_id: String,
  user_id: String,
  product_id: String,
  merchant_id: String,
  amount: Double,
  timestamp: String,       // format yyyyMMddHHmmss (14 caractères attendus, cf. Question 2.2)
  location: String,
  payment_method: String,
  category: String
)
