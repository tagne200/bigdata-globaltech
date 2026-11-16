# Profilage de l'échantillon J1

- **Ticket Jira** : BG-14
- **Auteur** : NGO NGWA Suzanne
- **Date** : 16 novembre 2026
- **Données** : `data/echantillon/J1` (100 000 commandes, 1 mois : janvier 2026)
- **Reproduire** : `python scripts/profilage_j1.py data/echantillon/J1` (bibliothèque standard Python uniquement)

Toutes les sources contiennent des anomalies volontaires. Ce document les mesure une par une et indique le traitement prévu en silver. Les règles de traitement sont celles du document de cadrage (section « Règles de gestion et arbitrages »).

## 1. Commandes (3 fichiers CSV)

**Bilan de comptage** : 101 200 lignes lues = 56 lignes mal formées + 1 000 doublons exacts + 100 144 lignes distinctes. Parmi ces dernières, 199 `order_id` apparaissent deux fois (398 lignes).

| Anomalie | Nombre | Traitement prévu en silver |
|---|---|---|
| Ligne mal formée : séparateur `;` (1 seule colonne lue) | 17 | Rejet `FORMAT_INVALIDE` |
| Ligne mal formée : 2, 3, 4, 6, 8 ou 9 colonnes au lieu de 10 | 26 | Rejet `FORMAT_INVALIDE` |
| Ligne avec 12 colonnes (2 colonnes en trop) | 13 | Rejet `FORMAT_INVALIDE` (à surveiller : peut annoncer le changement de schéma de J2) |
| Doublon exact (ligne identique) | 1 000 | Compté comme doublon, une seule ligne gardée |
| Même `order_id`, contenu différent | 199 identifiants | Seule la colonne `status` diffère : on garde le statut le plus avancé |
| `order_ts` en epoch millisecondes | 81 | Converti |
| `order_ts` au format `yyyy-MM-dd HH:mm:ss` (espace au lieu de `T`) | 76 | Converti |
| `order_ts` au format `dd/MM/yyyy HH:mm:ss` | 59 | Converti |
| `order_ts` = `N/A`, `hier` ou vide | 16 + 13 + 15 | Rejet `DATE_INVALIDE` |
| `order_ts` valide mais hors janvier 2026 (de décembre 2025 à 2030) | 517 | Rejet `DATE_HORS_LIVRAISON` |
| `customer_id` vide | 328 | Rejet `CLIENT_MANQUANT` |
| Client inconnu du référentiel | 197 | Rejet `CLIENT_INCONNU` |
| Produit inconnu du référentiel | 264 | Rejet `PRODUIT_INCONNU` |
| Quantité ≤ 0 | 300 | Rejet `QUANTITE_INVALIDE` |
| Quantité non numérique (`abc`, `2 pcs`, `NaN`) ou vide | 79 + 31 | Rejet `QUANTITE_INVALIDE` |
| Prix unitaire vide | 194 | Rejet `PRIX_INVALIDE` |
| Prix unitaire ≤ 0 | 60 | Rejet `PRIX_INVALIDE` |
| Pays vide | 107 | Rejet `PAYS_INVALIDE` |
| Pays non normalisé (` FR `, `fr`, `France`, `FRA`, `Deutschland`, `USA`…) | 397 | Normalisé par `trim`, `upper` et table de correspondance |
| Statut en minuscules (`completed`, `cancelled`…) | 111 | Normalisé par `upper` |

Une même ligne peut cumuler plusieurs anomalies : elle n'est rejetée qu'une fois, avec la liste de ses motifs. Les nombres de ce tableau ne s'additionnent donc pas.

**Les 199 doublons de statut** : ce sont des évolutions de la commande.

| Transition | Nombre |
|---|---|
| COMPLETED puis CANCELLED | 105 |
| COMPLETED puis RETURNED | 74 (dont 1 avec `completed` en minuscules) |
| SHIPPED puis RETURNED | 10 |
| SHIPPED puis CANCELLED | 6 |
| RETURNED puis CANCELLED | 3 |

Ordre retenu : SHIPPED (1) < COMPLETED (2) < RETURNED (3) < CANCELLED (4).

**Déséquilibre de clé (skew)** : le produit **P00042** représente **25 041 commandes**, contre une médiane de 78 par produit. Le ratio max / médiane vaut **321**. Ce produit pèse 25 % des commandes à lui seul : c'est la clé à traiter au lot 7 (salting, broadcast).

## 2. Produits (1 fichier CSV)

**Bilan** : 592 lignes pour 500 produits. 413 produits ont 1 version, 82 en ont 2, et 5 en ont 3. Les versions s'étalent du 1er octobre 2025 au 31 janvier 2026.

| Anomalie | Nombre | Traitement prévu |
|---|---|---|
| Doublon exact | 8 | Supprimé |
| Même produit et même `updated_at` (dont 1 cas au contenu différent) | 9 | Une seule version gardée par date, règle à justifier |
| Prix avec virgule décimale (`205,16`) | 2 | Virgule remplacée par un point |
| Famille écrite de plusieurs façons (casse, espaces) : 17 libellés pour 6 familles réelles | - | `trim` + `initcap` |
| Produit qui change de famille d'une version à l'autre (après normalisation) | 7 | Historisé normalement en SCD2 |

## 3. Clients (1 fichier CSV, données personnelles)

**Bilan** : 5 127 lignes pour 5 000 clients. 127 identifiants apparaissent plusieurs fois, dont 83 doublons exacts.

| Anomalie | Nombre | Traitement prévu |
|---|---|---|
| Doublon exact | 83 | Supprimé |
| Même `customer_id`, contenu différent (souvent la casse de l'email) | 44 | Garder l'`updated_at` le plus récent |
| Email vide | 142 | Gardé, clé pseudonymisée nulle |
| Email en majuscules | 43 | Mis en minuscules avant hachage |
| Email invalide (`_at_` à la place de `@`…) | 29 | Gardé, signalé dans un contrôle qualité |
| Téléphone vide | 57 | Sans effet : colonne supprimée en silver |
| Pays vide / non normalisé | 111 / 48 | Normalisé comme pour les commandes |
| Segment vide | 53 | Remplacé par `Inconnu` |
| `created_at` au format `dd/MM/yyyy` | 25 | Converti |

## 4. Logs web (format Apache combined)

**Bilan** : 50 000 lignes, dont 49 751 lisibles et 249 en anomalie.

| Anomalie | Nombre | Traitement prévu |
|---|---|---|
| Ligne tronquée ou non conforme | 171 | Rejet `LOG_MAL_FORME` |
| Ligne vide | 20 | Ignorée |
| Horodatage au format `yyyy-MM-dd HH:mm:ss`, sans fuseau | 58 | Converti, fuseau Europe/Paris appliqué |

**Statuts HTTP** : 200 = 45 970 · 404 = 1 732 · 304 = 765 · 500 = 498 · 302 = 271 · 301 = 247 · 403 = 142 · 401 = 121 · 503 = 5.

**Incident** : le taux d'erreurs 5xx est d'environ 1 % en moyenne. Le 7 janvier entre 14 h et 15 h, il monte à **22 %** (10 erreurs sur 46 requêtes). Au niveau de la journée, ce pic est dilué (1,7 %) et ne ressort pas : **la détection d'incident du lot 4 doit se faire à l'heure**, pas au jour.

## 5. Capteurs (120 fichiers JSON Lines, un toutes les 5 s)

**Bilan** : 10 783 lignes sur 10 minutes de flux, réparties à parts égales entre 3 métriques : `temperature` (°C), `vibration` (mm/s) et `pression` (bar).

| Anomalie | Nombre | Traitement prévu au lot 5 |
|---|---|---|
| Ligne JSON invalide | 27 | Rejet (colonne `_corrupt_record`) |
| Doublon d'`event_id` | 202 | `dropDuplicates` avec watermark |
| `value` nulle | 52 | Rejet |
| `value` envoyée comme texte (`"58.14"`) | 22 | Convertie en nombre via le schéma explicite |
| Événement arrivé plus de 60 s après sa mesure | 263 | Gardé si le retard reste dans le watermark |
| Retard maximal observé | 300 s (5 min) | Watermark à fixer **au-dessus de 5 min** pour ne rien perdre |
| Retard médian | 2,6 s | - |

**Alertes attendues sur l'échantillon** : 20 dépassements par métrique (température > 85 °C, vibration > 12 mm/s, pression > 8 bar). Ces chiffres serviront à vérifier le job streaming.

Point d'attention : la métrique s'appelle `pression` dans les données, et non `pressure`.

## 6. Conséquences pour la suite

1. **Lot 3 (silver)** : les trois formats de date et les dix motifs de rejet ci-dessus forment la liste des tests pytest à écrire.
2. **Lot 4 (gold)** : détecter les incidents web à l'heure.
3. **Lot 5 (streaming)** : watermark supérieur à 5 minutes ; valeurs texte et nulles traitées.
4. **Lot 7 (performance)** : skew mesuré, avec un ratio de 321 sur P00042.
5. **Livraisons J2 et J3** : relancer ce script sur chaque livraison pour mesurer la dérive, notamment l'apparition de colonnes en plus.
