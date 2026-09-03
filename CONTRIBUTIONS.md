# Contributions

## Tableau récapitulatif

| Question | Responsable                          | Relecteur                                |
|---|--------------------------------------|------------------------------------------|
| 1.1, 1.2, 1.3 | BUKA BOSENGA DON-CHRIST              | CISSE ABDOULAHI DIT DIORO                |
| 2.1, 2.3, 7.1 | BUKA BOSENGA DON-CHRIST              | CISSE ABDOULAHI DIT DIORO                                  |
| 2.2, 2.4 | ADIGBONON Mahoutondji Thérèse Rodica | BUKA BOSENGA DON-CHRIST                               |
| 4.2 | ADIGBONON Mahoutondji Thérèse Rodica | BUKA BOSENGA DON-CHRIST                               |
| 3.1, 3.2, 3.3 | DALIBILE DOUTI                       | ADIGBONON Mahoutondji Thérèse Rodica                                 |
| 4.1 | CISSE ABDOULAHI DIT DIORO            | DALIBILE DOUTI                                   |
| 5.1, 5.2 | CISSE ABDOULAHI DIT DIORO            | DALIBILE DOUTI                                   |
| 6.1 | CISSE ABDOULAHI DIT DIORO            | DALIBILE DOUTI   (intégration validée par les 4) |

## Charge de travail estimée (heures) et difficultés

_À compléter par chacun en fin de semaine._

- BUKA BOSENGA DON-CHRIST  : 
- ADIGBONON Mahoutondji Thérèse Rodica : **~2 h** (mise en place de l'environnement, questions 2.2, 2.4 et 4.2, vérification des résultats sur les données fournies).
  Difficultés rencontrées :
  - `sbt` démarrait sur le JDK 26 installé par Homebrew, sur lequel Spark 3.5.3 refuse de se lancer. Résolu en fixant `JAVA_HOME` sur le JDK 17 à chaque session de travail.
  - `sbt console` était inutilisable : dès que Spark construit un encodeur pour une case class (`.as[Transaction]`), le REPL Scala 2.12 lève `SecurityException: Prohibited package name: java.sql`. Contourné en testant via un `object` avec un `main` lancé par `sbt runMain`.
  - Piège des valeurs nulles dans les règles de validation : une condition portant sur une colonne nulle vaut `null`, si bien que la ligne n'était retenue ni dans les valides ni dans les rejetées et disparaissait des comptages. Résolu en enveloppant chaque condition dans `coalesce(condition, lit(false))`.
  - Distinction entre `groupBy` et fonction de fenêtrage pour déterminer le mois de cohorte : le `groupBy` écrase le détail des transactions, la fenêtre `min(...) OVER (PARTITION BY user_id)` calcule le mois de première transaction tout en conservant chaque ligne.
- DALIBILE DOUTI :
- CISSE ABDOULAHI DIT DIORO :

## Décisions techniques du groupe (minimum 5, justifiées en 2-3 lignes)

1. Version de Spark : **3.5.x** — version stable, très largement documentée (tutoriels, StackOverflow, cours), ce qui limite le risque de blocage sous pression de temps face à une version trop récente et peu documentée.
2. Version de Scala : **2.12.18** — dernière version 2.12 stable, compatible Spark 3.5.x et Java 8/11/17.
3. Stratégie de jointure retenue : _à compléter_
4. Format de sortie (CSV/Parquet) : **CSV avec en-tête**, écrit via `coalesce(1)` pour obtenir un fichier unique. Les sorties concernées (rapport de qualité, matrice de rétention) sont des livrables destinés à être lus par une équipe métier, donc ouvrables directement dans un tableur ; leur volume est négligeable, ce qui rend sans objet les avantages du Parquet (compression, typage, lecture sélective). `coalesce(1)` reste acceptable ici pour la même raison — il serait à proscrire sur un gros volume, puisqu'il ramène toutes les données sur une seule machine.
5. sbt package vs sbt assembly pour le JAR : _à compléter_

## Relectures croisées (obligatoires, datées)

| Module relu | Relecteur | Date | Remarques |
|---|---|---|---|
| | | | |
