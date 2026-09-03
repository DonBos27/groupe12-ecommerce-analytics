package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}


class DataIngestion(spark: SparkSession) {

  import spark.implicits._


  def readTransactions(): Dataset[Transaction] = {
    val path = ConfigLoader.getStringOrDefault(
      "app.data.input.transactions", "src/main/resources/data/transactions.csv"
    )

    val schema = StructType(Seq(
      StructField("transaction_id", StringType, nullable = true),
      StructField("user_id", StringType, nullable = true),
      StructField("product_id", StringType, nullable = true),
      StructField("merchant_id", StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true),
      StructField("timestamp", StringType, nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("payment_method", StringType, nullable = true),
      StructField("category", StringType, nullable = true)
    ))

    readWithErrorHandling("transactions", path) {
      spark.read
        .option("header", "true")
        .schema(schema)
        .csv(path)
        .as[Transaction]
    }
  }


  def readUsers(): Dataset[User] = {
    val path = ConfigLoader.getStringOrDefault(
      "app.data.input.users", "src/main/resources/data/users.json"
    )

    readWithErrorHandling("users", path) {
      spark.read
        .json(path)
        // Spark infère tout entier JSON en BIGINT (Long) par défaut, quelle que soit sa valeur ;
        // User.age est déclaré Int (cf. sujet), d'où ce cast explicite avant .as[User].
        .withColumn("age", col("age").cast(IntegerType))
        .as[User]
    }
  }

  def readProducts(): Dataset[Product] = {
    val path = ConfigLoader.getStringOrDefault(
      "app.data.input.products", "src/main/resources/data/products.parquet"
    )

    readWithErrorHandling("products", path) {
      spark.read
        .parquet(path)
        .as[Product]
    }
  }

  def readMerchants(): Dataset[Merchant] = {
    val path = ConfigLoader.getStringOrDefault(
      "app.data.input.merchants", "src/main/resources/data/merchants.csv"
    )

    readWithErrorHandling("merchants", path) {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .withColumn("establishment_date", col("establishment_date").cast(StringType))
        .as[Merchant]
    }
  }

  private def readWithErrorHandling[T](datasetName: String, path: String)(read: => Dataset[T]): Dataset[T] = {
    try {
      val ds = read
      val count = ds.count()
      println(s"[Ingestion] $datasetName : $count lignes lues depuis $path")
      ds
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        println(s"[Ingestion][ERREUR] $datasetName : fichier introuvable ou structure incorrecte ($path) -> ${e.getMessage}")
        throw e
      case e: Exception =>
        println(s"[Ingestion][ERREUR] $datasetName : échec de lecture ($path) -> ${e.getMessage}")
        throw e
    }
  }
}

object DataIngestion {

  def printValidationSummary(datasetName: String, rawCount: Long, validCount: Long): Unit = {
    val rejected = rawCount - validCount
    val rejectionRate = if (rawCount == 0) 0.0 else math.round(rejected.toDouble / rawCount * 10000) / 100.0
    println(s"[Validation] $datasetName : $validCount / $rawCount lignes valides ($rejected rejetées, taux de rejet $rejectionRate%)")
  }
}
