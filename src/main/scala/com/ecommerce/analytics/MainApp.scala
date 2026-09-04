package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.{ConfigLoader, SparkSessionBuilder}
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object MainApp {

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null

    try {
      // 1. Initialisation de la SparkSession (Partie 7.1)
      spark = SparkSessionBuilder.build()
      
      // ===== LA SEULE LIGNE À AJOUTER =====
      spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
      // ====================================
      
      spark.sparkContext.setLogLevel("WARN")

      // Application des optimisations Spark
      val optimizedSpark = SparkOptimizations.configureSparkSession(spark)

      import optimizedSpark.implicits._

      println("=" * 60)
      println("  PIPELINE D'ANALYSE E-COMMERCE - DÉMARRAGE")
      println("=" * 60)

      // 2. Phase d'ingestion (Partie 2.1)
      println("\n[Étape 1] Ingestion des données...")
      val ingestion = new DataIngestion(optimizedSpark)

      val transactions: Dataset[Transaction] = ingestion.readTransactions()
      val users: Dataset[User] = ingestion.readUsers()
      val products: Dataset[Product] = ingestion.readProducts()
      val merchants: Dataset[Merchant] = ingestion.readMerchants()

      // 3. Phase de validation (Partie 2.2)
      println("\n[Étape 2] Validation des données...")

      val (validTransactions, rejectedTransactions) = DataValidation.validateTransactions(transactions)
      val (validUsers, rejectedUsers) = DataValidation.validateUsers(users)
      val (validProducts, rejectedProducts) = DataValidation.validateProducts(products)
      val (validMerchants, rejectedMerchants) = DataValidation.validateMerchants(merchants)

      DataIngestion.printValidationSummary("transactions", transactions.count(), validTransactions.count())
      DataIngestion.printValidationSummary("users", users.count(), validUsers.count())
      DataIngestion.printValidationSummary("products", products.count(), validProducts.count())
      DataIngestion.printValidationSummary("merchants", merchants.count(), validMerchants.count())

      // 4. Rapport de qualité (Partie 2.4)
      println("\n[Étape 3] Rapport de qualité...")
      val qualitySummaries = Seq(
        DataQualityReport.summarize("transactions", transactions, validTransactions, rejectedTransactions),
        DataQualityReport.summarize("users", users, validUsers, rejectedUsers),
        DataQualityReport.summarize("products", products, validProducts, rejectedProducts),
        DataQualityReport.summarize("merchants", merchants, validMerchants, rejectedMerchants)
      )
      val qualityReport = DataQualityReport.build(optimizedSpark, qualitySummaries)
      DataQualityReport.show(qualityReport)
      DataQualityReport.save(qualityReport)

      // 5. Phase de transformation (Partie 3)
      println("\n[Étape 4] Transformation des données...")

      val cachedValidTransactions = SparkOptimizations.optimizeDataFrame(
        validTransactions.toDF(),
        cacheIfEnabled = true,
        StorageLevel.MEMORY_AND_DISK_SER
      )

      val validTransactionsDS = cachedValidTransactions.as[Transaction]

      val enrichedData = DataTransformation.enrichTransactionData(
        validTransactionsDS,
        validUsers,
        validProducts,
        validMerchants
      )

      val cachedEnriched = SparkOptimizations.optimizeDataFrame(
        enrichedData,
        cacheIfEnabled = true,
        StorageLevel.MEMORY_AND_DISK_SER
      )

      // 6. Phase analytique (Partie 4)
      println("\n[Étape 5] Analyses métier...")

      println("\n  [4.1] Rapport détaillé par marchand...")
      val merchantReportDF = Analytics.merchantReport(cachedEnriched)

      println("\n  [4.2] Analyse de cohortes...")
      val preparedCohort = CohortAnalysis.prepare(validTransactionsDS)
      val retentionMatrix = CohortAnalysis.retentionMatrix(preparedCohort)
      CohortAnalysis.save(retentionMatrix)
      retentionMatrix.show(20, false)

      val bestCohort = CohortAnalysis.bestCohortAt(retentionMatrix, 3)
      println("\n  Meilleure cohorte à 3 mois:")
      bestCohort.show(false)

      Analytics.saveAnalyticsResults(merchantReportDF)
      Analytics.showAnalyticsSummary(merchantReportDF)

      // 7. Sauvegarde des résultats
      println("\n[Étape 6] Sauvegarde des résultats...")
      DataTransformation.saveTransformedData(cachedEnriched, "output/transformed")
      cachedEnriched.write.mode("overwrite").parquet("output/transformed_parquet")

      SparkOptimizations.unpersistDataFrame(cachedValidTransactions)
      SparkOptimizations.unpersistDataFrame(cachedEnriched)

      println("\n" + "=" * 60)
      println("  PIPELINE TERMINÉ AVEC SUCCÈS ✓")
      println("=" * 60)

    } catch {
      case e: Exception =>
        println(s"\n[ERREUR] Le pipeline a échoué: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      if (spark != null) {
        spark.stop()
        println("\nSparkSession arrêtée.")
      }
    }
  }
}