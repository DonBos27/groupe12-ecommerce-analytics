package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Centralise la lecture d'application.conf (Question 7.1).
  * Aucun chemin de fichier, seuil de validation ou paramètre Spark ne doit être
  * codé en dur ailleurs dans le code : tout passe par ce module.
  * Fournit une valeur par défaut lorsque la clé est absente du fichier.
  */
object ConfigLoader {

  private val config: Config = ConfigFactory.load()

  def getStringOrDefault(path: String, default: String): String =
    if (config.hasPath(path)) config.getString(path) else default

  def getIntOrDefault(path: String, default: Int): Int =
    if (config.hasPath(path)) config.getInt(path) else default

  def getDoubleOrDefault(path: String, default: Double): Double =
    if (config.hasPath(path)) config.getDouble(path) else default

  def getBooleanOrDefault(path: String, default: Boolean): Boolean =
    if (config.hasPath(path)) config.getBoolean(path) else default
}
