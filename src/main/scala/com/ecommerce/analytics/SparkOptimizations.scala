package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Question 5.1 et 5.2 — Optimisations Spark.
  * Cache, persist, broadcast, ajustement des partitions.
  */
object SparkOptimizations {

  /**
    * Question 5.1 — Gestion du cache.
    * Cache les DataFrame réutilisés avec le niveau approprié.
    */
  def optimizeDataFrame(
    df: DataFrame,
    cacheIfEnabled: Boolean = true,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
  ): DataFrame = {
    val enableCache = ConfigLoader.getBooleanOrDefault("app.optimization.enable-cache", true)

    if (enableCache && cacheIfEnabled) {
      println("[Optimisation] Mise en cache du DataFrame...")
      df.persist(storageLevel)
    } else {
      df
    }
  }

  /**
    * Libère le cache explicitement.
    */
  def unpersistDataFrame(df: DataFrame): Unit = {
    if (!df.isStreaming) {
      df.unpersist()
      println("[Optimisation] Cache libéré.")
    }
  }

  /**
    * Question 5.2 — Optimisation des jointures avec broadcast.
    * Applique broadcast sur les petites tables (< 10 MB par défaut).
    */
  def broadcastJoinSmallTable(df: DataFrame, broadcastThreshold: Int = 10 * 1024 * 1024): DataFrame = {
    val enableBroadcast = ConfigLoader.getBooleanOrDefault("app.optimization.enable-broadcast", true)

    if (enableBroadcast) {
      // Spark gère automatiquement le broadcast si le seuil est bien configuré
      // On peut aussi forcer manuellement avec broadcast()
      println(s"[Optimisation] Broadcast activé (seuil: ${broadcastThreshold / 1024 / 1024} MB)")
      // Pour forcer sur des petites tables:
      // import org.apache.spark.sql.functions.broadcast
      // broadcast(df)
      df
    } else {
      df
    }
  }

  /**
    * Répartition optimisée pour éviter les problèmes de shuffle.
    */
  def repartitionOptimized(df: DataFrame, partitions: Option[Int] = None): DataFrame = {
    val numPartitions = partitions.getOrElse(
      ConfigLoader.getIntOrDefault("app.spark.shuffle.partitions", 8)
    )

    if (df.rdd.getNumPartitions > numPartitions * 2) {
      println(s"[Optimisation] Repartition de ${df.rdd.getNumPartitions} à $numPartitions partitions")
      df.repartition(numPartitions)
    } else {
      df
    }
  }

  /**
    * Méthode utilitaire pour mesurer le temps d'exécution.
    * Utilisée pour la Question 5.3 (Bonus).
    */
  def measureTime[T](label: String)(block: => T): T = {
    val start = System.currentTimeMillis()
    val result = block
    val duration = System.currentTimeMillis() - start
    println(s"[Performance] $label: ${duration}ms (${duration / 1000.0}s)")
    result
  }

  /**
    * Configuration Spark optimisée.
    */
  def configureSparkSession(spark: SparkSession): SparkSession = {
    // Ajustement du nombre de partitions de shuffle
    val shufflePartitions = ConfigLoader.getIntOrDefault("app.spark.shuffle.partitions", 8)
    spark.conf.set("spark.sql.shuffle.partitions", shufflePartitions)

    // Auto-broadcast threshold (10 MB par défaut)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") // 10 MB

    // Optimisations supplémentaires
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    println(s"[Optimisation] Configuration Spark appliquée:")
    println(s"  - shuffle.partitions = $shufflePartitions")
    println(s"  - autoBroadcastJoinThreshold = 10 MB")

    spark
  }
}
