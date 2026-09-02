package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

/** Construit la SparkSession à partir des paramètres externalisés dans application.conf (Question 6.1 / 7.1). */
object SparkSessionBuilder {

  def build(
    appName: String = ConfigLoader.getStringOrDefault("app.name", "EcommerceAnalytics"),
    master: String = ConfigLoader.getStringOrDefault("app.spark.master", "local[*]"),
    shufflePartitions: Int = ConfigLoader.getIntOrDefault("app.spark.shuffle.partitions", 8)
  ): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.shuffle.partitions", shufflePartitions)
      .getOrCreate()
  }
}
