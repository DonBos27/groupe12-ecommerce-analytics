# Contributions

## Tableau récapitulatif

| Question | Responsable                          | Relecteur                                |
|---|--------------------------------------|------------------------------------------|
| 1.1, 1.2, 1.3 | BUKA BOSENGA DON-CHRIST              | CISSE ABDOULAHI DIT DIORO                |
| 2.1, 2.3, 7.1 | BUKA BOSENGA DON-CHRIST              | CISSE ABDOULAHI DIT DIORO                                  |
| 2.2, 2.4 | ADIGBONON Mahoutondji Thérèse Rodica | BUKA BOSENGA DON-CHRIST                               |
| 4.2 | ADIGBONON Mahoutondji Thérèse Rodica | BUKA BOSENGA DON-CHRIST                               |
| 3.1, 3.2, 3.3 | DOUTI Dabilibe                       | ADIGBONON Mahoutondji Thérèse Rodica                                 |
| 4.1 | CISSE ABDOULAHI DIT DIORO            | DOUTI Dabilibe                                   |
| 5.1, 5.2 | CISSE ABDOULAHI DIT DIORO            | DOUTI Dabilibe                                   |
| 6.1 | CISSE ABDOULAHI DIT DIORO            | DOUTI Dabilibe   (intégration validée par les 4) |

## Charge de travail estimée (heures) et difficultés

_À compléter par chacun en fin de semaine._

- BUKA BOSENGA DON-CHRIST  : 
- ADIGBONON Mahoutondji Thérèse Rodica : **~2 h** (mise en place de l'environnement, questions 2.2, 2.4 et 4.2, vérification des résultats sur les données fournies).
  Difficultés rencontrées :
  - `sbt` démarrait sur le JDK 26 installé par Homebrew, sur lequel Spark 3.5.3 refuse de se lancer. Résolu en fixant `JAVA_HOME` sur le JDK 17 à chaque session de travail.
  - `sbt console` était inutilisable : dès que Spark construit un encodeur pour une case class (`.as[Transaction]`), le REPL Scala 2.12 lève `SecurityException: Prohibited package name: java.sql`. Contourné en testant via un `object` avec un `main` lancé par `sbt runMain`.
  - Piège des valeurs nulles dans les règles de validation : une condition portant sur une colonne nulle vaut `null`, si bien que la ligne n'était retenue ni dans les valides ni dans les rejetées et disparaissait des comptages. Résolu en enveloppant chaque condition dans `coalesce(condition, lit(false))`.
  - Distinction entre `groupBy` et fonction de fenêtrage pour déterminer le mois de cohorte : le `groupBy` écrase le détail des transactions, la fenêtre `min(...) OVER (PARTITION BY user_id)` calcule le mois de première transaction tout en conservant chaque ligne.
- DOUTI Dabilibe : **~ h** _(estimation à renseigner)_ (questions 3.1, 3.2, 3.3 et bonus 3.4, programme de vérification `TransformationCheck`, test d'intégration du pipeline complet, relecture des modules du membre 4).
  Difficultés rencontrées :
  - Environnement : le lanceur `sbt` installé par Coursier pointait vers une archive `sbt-2.0.2.zip` absente du cache (« No such file or directory »). Résolu par `cs install sbt`, puis `./sbt17.sh` pour forcer le JDK 17.
  - Analyse stricte des horodatages avec `java.time` : en résolution `STRICT`, la lettre `y` (année d'ère) exige un champ d'ère que nos chaînes n'ont pas, et toute analyse échoue. Il faut écrire le motif avec `u` (année proleptique) : `uuuuMMddHHmmss`. En contrepartie, un « 30 février » est bien rejeté, alors que la résolution par défaut l'aurait corrigé silencieusement.
  - `countDistinct` n'est pas autorisé sur une fenêtre Spark. Le nombre de jours distincts d'achat sur 7 jours glissants est obtenu par `size(collect_set(jour))` sur un cadre `RANGE` ; ce cadre exige une colonne d'ordre numérique, d'où une colonne de travail « numéro de jour depuis 1970 » (`unix_date`), supprimée en fin d'étape.
  - Le format CSV ne sait écrire ni tableaux ni structures : `preferred_categories` (tableau) et la structure renvoyée par l'UDF auraient fait échouer l'écriture. La structure est aplatie en colonnes de premier niveau et les tableaux sont sérialisés en JSON au moment de l'écriture CSV uniquement, le Parquet conservant les types d'origine.
  - Collisions de noms entre les quatre tables (`name`, `category`, `merchant_id`) : la règle retenue (préfixer uniquement les colonnes ambiguës : `merchant_name`, `product_category`, …) a été fixée après que le membre 4 avait écrit `Analytics.scala` contre un schéma supposé. Le pipeline complet a été testé avec les cinq renommages à reporter dans `Analytics.scala` (voir la relecture ci-dessous).
  - Ambiguïtés du sujet tranchées et documentées dans le code : l'âge de 25 ans n'appartient à aucune tranche (« moins de 25 » / « 26 à 44 »), rattaché à « Jeune » ; les heures de 0h à 5h ne sont pas citées pour `day_period`, rattachées à « Night » ; les heures ouvrées sont prises comme l'intervalle demi-ouvert [9h, 17h[, par cohérence avec les périodes de la journée.
- CISSE ABDOULAHI DIT DIORO :

## Décisions techniques du groupe (minimum 5, justifiées en 2-3 lignes)

1. Version de Spark : **3.5.x** — version stable, très largement documentée (tutoriels, StackOverflow, cours), ce qui limite le risque de blocage sous pression de temps face à une version trop récente et peu documentée.
2. Version de Scala : **2.12.18** — dernière version 2.12 stable, compatible Spark 3.5.x et Java 8/11/17.
3. Stratégie de jointure retenue : **LEFT JOIN depuis les transactions, tables de référence diffusées (`broadcast`)**. Les transactions sont la table de faits : un LEFT JOIN vers `users`, `products` et `merchants` garantit qu'aucune transaction validée n'est perdue (une référence orpheline ou rejetée donne des attributs nuls, exploitables ensuite comme « Inconnu »), et le nombre de lignes reste identique avant et après enrichissement puisque les clés de référence sont uniques. Les trois tables de référence sont petites (12 000, 6 000 et 600 lignes) : elles sont diffusées à tous les exécuteurs, ce qui évite de redistribuer trois fois les 138 000 transactions par le réseau ; le seul shuffle du module est celui des fonctions de fenêtrage, partitionnées par `user_id`. Le mécanisme se désactive par `app.optimization.enable-broadcast`.
4. Format de sortie (CSV/Parquet) : **CSV avec en-tête**, écrit via `coalesce(1)` pour obtenir un fichier unique. Les sorties concernées (rapport de qualité, matrice de rétention) sont des livrables destinés à être lus par une équipe métier, donc ouvrables directement dans un tableur ; leur volume est négligeable, ce qui rend sans objet les avantages du Parquet (compression, typage, lecture sélective). `coalesce(1)` reste acceptable ici pour la même raison — il serait à proscrire sur un gros volume, puisqu'il ramène toutes les données sur une seule machine.
5. sbt package vs sbt assembly pour le JAR : _à compléter_
6. Caractéristiques temporelles : **UDF typée imposée par le sujet, complétée par `to_timestamp` natif**. L'UDF `extractTimeFeatures` renvoie une case class `TimeFeatures` : Spark en déduit le schéma de la structure, et la logique reste une fonction Scala pure, testable sans SparkSession. Une UDF est cependant opaque pour l'optimiseur Catalyst ; le tri des fenêtres et les délais entre achats s'appuient donc sur une colonne `transaction_ts` calculée par la fonction native `to_timestamp`, que Catalyst sait optimiser. Un horodatage invalide donne des colonnes nulles dans les deux cas, jamais une erreur.

## Partie 3 — choix d'implémentation (DOUTI Dabilibe)

### Type de jointure par table (Question 3.2)

| Jointure | Type | Justification |
|---|---|---|
| transactions ⟕ users | LEFT | Une transaction dont l'utilisateur est inconnu (400 références orphelines injectées) ou rejeté par la validation reste un achat réel ; ses attributs client sont nuls et sa tranche d'âge vaut « Inconnu ». |
| transactions ⟕ products | LEFT | Même raisonnement : le montant payé est porté par la transaction, le catalogue n'est qu'un complément descriptif (nom, prix, note, stock). |
| transactions ⟕ merchants | LEFT | Même raisonnement : le chiffre d'affaires d'un marchand absent du référentiel n'est pas perdu, il apparaît sans nom ni région. |

Un INNER JOIN ferait disparaître silencieusement des transactions valides. Avec la stratégie LEFT, le programme `TransformationCheck` vérifie l'invariant « autant de lignes en sortie qu'en entrée » (136 157 lignes) et l'absence de doublons.

### Schéma du DataFrame enrichi (contrat avec la Partie 4)

Les neuf colonnes de `Transaction` gardent leur nom et leur position. Les colonnes des tables de référence gardent leur nom, sauf en cas de collision, auquel cas elles sont préfixées : `product_name`, `product_category`, `merchant_name`, `merchant_category`. Colonnes ajoutées : `transaction_ts`, `transaction_date`, `hour`, `day_of_week`, `month`, `is_weekend`, `day_period`, `is_working_hours` (3.1) ; `user_transaction_rank`, `user_transaction_count`, `age_group` (3.2) ; `amount_last_7_days`, `active_days_last_7_days`, `is_active_user`, `days_since_previous_transaction` (3.3). Le bonus 3.4 (`addSuspiciousFlag`) ajoute `avg_basket_deviation_pct`, `seconds_since_previous_transaction` et `is_suspicious`.

### Hypothèses d'interprétation

- Fenêtre glissante de 7 jours : jours calendaires [J − 6, J], transaction courante incluse. Toutes les transactions d'un même jour partagent la même fenêtre, ce qui évite qu'une fenêtre en secondes ne chevauche deux journées.
- `is_active_user` est évalué sur la fenêtre se terminant à la transaction courante : une ligne vaut 1 si, sur ces 7 jours, l'utilisateur a acheté au moins 5 jours distincts.
- Rang chronologique par `row_number` avec `transaction_id` en second critère : rangs uniques et reproductibles même à horodatage égal.
- Tranches d'âge : Jeune ≤ 25, Adulte 26–44, Âge Moyen 45–64, Senior ≥ 65, Inconnu si âge nul. `day_period` : Night couvre [22h, 24h[ et [0h, 6h[. `is_working_hours` : [9h, 17h[.

### Vérification

`sbt "runMain com.ecommerce.analytics.TransformationCheck"` exécute 14 tests unitaires de l'UDF (sans Spark) puis 19 invariants sur les données réelles, et se termine par un code de retour non nul en cas d'échec. Les sorties CSV et Parquet sont relues pour valider le schéma.

## Relectures croisées (obligatoires, datées)

| Module relu | Relecteur | Date | Remarques |
|---|---|---|---|
| Analytics.scala (4.1) | DOUTI Dabilibe | 2026-09-04 | `merchantReport` référence les colonnes `name` et `category` ; dans le DataFrame enrichi elles s'appellent `merchant_name` et `merchant_category` (la colonne `category` restante est celle de la transaction : regrouper dessus scinderait les KPI d'un marchand par catégorie de produit vendu). Cinq occurrences à adapter (lignes 28, 51, 63, 64 et 315) ; le pipeline complet a été exécuté avec succès avec ce seul changement. Même adaptation pour le bonus 4.4 avec `product_name` / `product_category`. |
| Analytics.scala (4.1) | DOUTI Dabilibe | 2026-09-04 | `commission_totale` est nulle pour les marchands hors référentiel (LEFT JOIN, `commission_rate` nul) : prévoir un `coalesce` ou un filtre selon que le rapport doit ou non lister ces marchands. Le pivot par tranche d'âge inclut « Inconnu », cohérent avec `age_group`. |
| SparkOptimizations.scala (5.1, 5.2) | DOUTI Dabilibe | 2026-09-04 | `broadcastJoinSmallTable` renvoie le DataFrame inchangé (l'appel à `broadcast` est resté en commentaire) : la fonction ne diffuse rien. Le broadcast effectif est appliqué dans `DataTransformation.joinReferenceData`, piloté par `app.optimization.enable-broadcast` ; soit y appeler réellement `broadcast(df)`, soit retirer la fonction. Le seuil `autoBroadcastJoinThreshold` (10 Mo) est codé en dur : à externaliser dans `application.conf` (Question 7.1). Cache et `unpersist` : conformes. |
| MainApp.scala (6.1) | DOUTI Dabilibe | 2026-09-04 | `sys.exit(1)` dans le `catch` arrête la JVM immédiatement : le bloc `finally`, donc `spark.stop()`, ne s'exécute pas en cas d'échec, contrairement à l'exigence d'arrêt propre. Appeler `spark.stop()` avant de sortir, ou mémoriser le code de retour et appeler `sys.exit` après le `finally`. |
| MainApp.scala (6.1) | DOUTI Dabilibe | 2026-09-04 | Chemins `output/transformed` et `output/transformed_parquet` codés en dur : utiliser `app.data.output.path` (Question 7.1) ; `saveTransformedData` accepte `format = "parquet"`, l'écriture Parquet peut passer par elle. `spark.sql.legacy.allowUntypedScalaUDF = true` est inutile, l'UDF de la Partie 3 étant typée. Le bonus 3.4 est disponible (`addSuspiciousFlag`, `showSuspiciousTransactions`) et peut être branché à l'Étape 4 si le groupe le retient. |
| TimeFeatures.scala (3.1) | ADIGBONON Mahoutondji Thérèse Rodica | 2026-09-04 | Logique métier isolée dans des fonctions pures (`compute`, `dayPeriod`, `isWorkingHours`, `isWeekend`), testables sans SparkSession ; l'UDF n'est qu'un adaptateur autour de `compute`. Robustesse conforme au sujet : chaîne nulle, vide ou mal formée donne `None`, donc des colonnes nulles, jamais l'échec du job. Motif `uuuuMMddHHmmss` en résolution STRICT : le choix de `u` plutôt que `y` est justifié et documenté dans le code. Locale figée à ENGLISH pour `day_of_week` et `month` : évite que les libellés changent selon la machine d'exécution. Aucune correction demandée. |
| DataTransformation.scala (3.2) | ADIGBONON Mahoutondji Thérèse Rodica | 2026-09-04 | Jointures LEFT depuis les transactions avec diffusion des tables de référence : conforme à la décision technique 3. L'invariant « autant de lignes en sortie qu'en entrée » (136 157) est vérifié par `TransformationCheck` et retrouvé lors de l'exécution du pipeline complet. Point de vigilance à l'interface avec la Question 2.2 : l'enrichissement suppose que tout horodatage validé est analysable, alors que ma règle de validation ne contrôle que la longueur (14 caractères). Vérification faite sur le jeu de données fourni : aucune date impossible (type 20250230…) ne franchit la validation, `transaction_ts` n'est donc jamais nul en pratique. Si le cas se présentait, la ligne concernée trierait en tête de la fenêtre chronologique et fausserait `user_transaction_rank` : à documenter, ou à couvrir par une règle de validation supplémentaire si le groupe le souhaite. |
| DataTransformation.scala (3.3) | ADIGBONON Mahoutondji Thérèse Rodica | 2026-09-04 | `size(collect_set(...))` sur un cadre RANGE pour contourner l'interdiction de `countDistinct` sur une fenêtre : correct, et le coût mémoire reste négligeable (au plus 7 valeurs par fenêtre). `user_transaction_count` et `amount_last_7_days` portent sur les seules transactions validées, et non sur l'historique brut du client : cohérent avec le pipeline, mais à annoncer pendant la démonstration pour éviter un malentendu côté métier. `days_since_previous_transaction` vaut null au premier achat et 0 pour un achat le même jour : la distinction entre « pas de précédent » et « précédent le même jour » est bien conservée. Aucune correction demandée. |
