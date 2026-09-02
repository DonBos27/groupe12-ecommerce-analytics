package com.ecommerce.models

/** Une ligne de products.parquet. */
case class Product(
  product_id: String,
  name: String,
  category: String,
  price: Double,
  merchant_id: String,
  rating: Double,
  stock: Int
)
