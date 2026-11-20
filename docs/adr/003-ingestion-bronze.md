# ADR-003 : format et idempotence de l'ingestion bronze

- **Statut** : proposé (à valider par l'équipe lors de la revue de la PR)
- **Date** : 20 novembre 2026
- **Auteur** : TAMO FOTSO Michel Darryl
- **Ticket Jira** : BG-24
- **Code** : `src/bronze/ingestion.py`

## Contexte

L'énoncé demande, pour la couche bronze : « conserver les données brutes, sans perte », en « Parquet partitionné par date d'ingestion », et « le rejeu d'un même jour ne doit pas créer de doublons ». Les livraisons contiennent des anomalies volontaires. Le profilage de J1 en compte par exemple 56 lignes de commandes mal formées : séparateur `;`, ou 2 à 12 colonnes au lieu de 10. Le schéma peut aussi évoluer entre livraisons.

Toutes les mesures ci-dessous ont été faites sur l'échantillon J1 du kit, avec PySpark 3.5.3 (même version que le cluster).

## Décision 1 : tout lire en texte, et garder les lignes mal formées

| Option | Résultat mesuré sur l'échantillon J1 |
|---|---|
| A. `inferSchema` ou schéma typé (entiers, décimaux) | Avec `quantity` en entier, les quantités nulles passent de 58 à 138 : **80 valeurs non numériques** (`abc`, `2 pcs`, `NaN`…) deviennent `null`. La valeur d'origine est **perdue** avant même le contrôle qualité. |
| B. Schéma déduit de l'en-tête, sans colonne de rejet | 101 200 lignes lues, mais une ligne de 12 colonnes (ex. `ORD0000057555`) perd ses 2 colonnes en trop, et 16 lignes au séparateur `;` sont rangées entières dans `order_id`. C'est une **perte silencieuse**, et rien ne signale ces lignes. |
| **C. Schéma explicite tout en texte + `_corrupt_record` en mode PERMISSIVE (retenue)** | 101 200 lignes lues = 101 200 écrites, dont **56 lignes mal formées gardées intactes** dans `_corrupt_record`. C'est exactement le chiffre du profilage de Suzanne |

Les noms de colonnes sont lus dans l'en-tête des fichiers eux-mêmes. Si J2 ajoute une colonne, elle est conservée automatiquement. Le tri entre lignes valides et rejets avec motif se fait en silver (BG-30), à partir des données brutes complètes.

Les logs web sont stockés **ligne par ligne** (colonne `ligne`). Le découpage par expression régulière (`regexp_extract`) relève du silver (BG-34).

## Décision 2 : Parquet compressé en snappy

| Source | CSV / texte | Parquet snappy | Gain |
|---|---|---|---|
| commandes | 8,75 Mo | 2,18 Mo | ÷ 4,0 |
| logs_web | 9,82 Mo | 2,16 Mo | ÷ 4,5 |
| clients | 0,76 Mo | 0,29 Mo | ÷ 2,6 |
| produits | 0,05 Mo | 0,02 Mo | ÷ 2,5 |
| **Total** | **19,38 Mo** | **4,65 Mo** | **− 76 %** |

Même en texte, le format colonne compresse bien, parce que les valeurs se répètent beaucoup dans une colonne (pays, statuts, entrepôts). Parquet permet aussi aux couches suivantes de ne lire que les colonnes utiles. La comparaison avec d'autres formats et codecs est faite au lot 7 (BG-49, BG-51).

## Décision 3 : idempotence par écrasement dynamique de la partition

La partition `ingest_date` est la **date logique** de la livraison (J1 = 2026-02-01, J2 = 2026-03-01, J3 = 2026-04-01), jamais l'heure système. L'écriture utilise `mode("overwrite")` avec `spark.sql.sources.partitionOverwriteMode=dynamic` : seule la partition de la livraison traitée est remplacée.

| Mode d'écriture | Mesure |
|---|---|
| `append`, 2 exécutions de J1 | 101 200 → **202 400** commandes : doublons |
| `overwrite` statique, J1 puis J2 | Il ne reste **que** les lignes de J2 : J1 effacé |
| **`overwrite` dynamique (retenu)**, 2 exécutions de J1 puis J2 | J1 reste à 101 200 lignes après le rejeu ; J2 s'ajoute à côté (produits : 592 + 592 = 1 184) |

Preuve sur le cluster : `scripts/preuve_rejeu_bronze.sh J1` (BG-25).

## Décision 4 : colonnes techniques

| Colonne | Contenu | Utilité |
|---|---|---|
| `_corrupt_record` | Ligne d'origine si elle est mal formée, sinon `null` (CSV uniquement) | Rejets en silver, avec la ligne exacte comme preuve |
| `_source_file` | Chemin HDFS du fichier d'origine (`_metadata.file_path`) | Remonter d'une anomalie jusqu'au fichier livré |
| `_ingest_ts` | Heure d'exécution du job | Audit (une valeur différente à chaque rejeu, c'est voulu) |
| `livraison` | J1, J2 ou J3 | Filtrer une livraison, rejets par livraison |
| `ingest_date` | Date logique (colonne de partition) | Idempotence et lecture incrémentale |

## Points d'attention mesurés (utiles pour les lots suivants)

1. **Column pruning et `_corrupt_record`** : si une requête sur le CSV ne lit que `_corrupt_record`, Spark n'analyse plus les autres colonnes et ne détecte plus les lignes mal formées (**0 au lieu de 56** mesuré). Le job compte donc les lignes mal formées sur le Parquet écrit.
2. **Changement de schéma** : quand deux partitions n'ont pas les mêmes colonnes, `spark.read.parquet(...)` ne voit pas la nouvelle colonne. Il faut lire avec `.option("mergeSchema", True)` (mesuré). C'est à prendre en compte pour BG-35.
3. **Type de `ingest_date` en lecture** : Spark la relit comme une date (`DateType`). Le filtre `ingest_date = '2026-02-01'` fonctionne.
4. **Pas de `cache()`** : avec des exécuteurs de 512 Mo, garder 4 millions de lignes en mémoire coûterait plus cher que relire les CSV une seconde fois (un seul passage de plus, environ 350 Mo).

## Conséquences

- La couche bronze est la **source de vérité** : silver peut toujours être recalculé à partir d'elle.
- Chaque exécution affiche une ligne `BILAN` (lues, mal formées, écrites, total). Le job s'arrête avec le code 1 si le nombre de lignes écrites diffère du nombre de lignes lues.
- La table d'audit (BG-56) pourra reprendre ces mêmes comptages.
