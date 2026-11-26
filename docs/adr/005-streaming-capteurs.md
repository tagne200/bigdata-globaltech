# ADR-005 : flux des capteurs (lecture, watermark, déduplication, fenêtres)

- **Statut** : proposé (BG-42, BG-43). Complété par BG-44 (alertes) et BG-45 (écriture, reprise).
- **Date** : 25 novembre 2026
- **Auteur** : TAMO FOTSO Michel Darryl
- **Code** : `src/streaming/capteurs.py` · **Tests** : `tests/test_capteurs_streaming.py`

## Contexte

`tools/stream_replay.py` dépose un fichier JSON Lines toutes les 5 s dans `/data/landing/capteurs`.
Échantillon J1 : 120 fichiers, 10 783 lignes, 180 capteurs (60 machines × 3 métriques), environ
6 mesures par capteur et par minute. Le profilage (`docs/profilage-J1.md`) annonce du JSON invalide,
des valeurs nulles ou en texte, des doublons d'`event_id` et des retards allant jusqu'à 5 min.
L'énoncé demande : fenêtres, watermark, déduplication, reprise après arrêt, écriture idempotente.

Toutes les mesures ci-dessous : Spark 3.5.3, échantillon J1.

## Décision 1 : lecture (BG-42)

| Choix | Justification mesurée |
|---|---|
| Schéma explicite, `_corrupt_record` en mode PERMISSIVE | Obligatoire en streaming. Les **27** lignes JSON invalides sont gardées et comptées, comme dans le profilage. |
| `value` lue en **texte**, puis `cast("double")` | Avec `value` déclarée en `double`, les **22** valeurs envoyées en texte (`"58.14"`) font rejeter la ligne entière : **49** lignes corrompues au lieu de 27. En texte puis `cast`, ces 22 mesures sont gardées. Le profilage supposait l'inverse : c'est la mesure qui tranche. |
| `event_time` en texte, puis `to_timestamp` | Le fuseau de la source (`+01:00`) est respecté, et une date illisible devient un motif de rejet au lieu d'une erreur. |
| `pathGlobFilter="*.jsonl"` | `hdfs dfs -put` écrit d'abord `<nom>._COPYING_`. Le filtre empêche de lire un fichier à moitié copié (testé). |
| `maxFilesPerTrigger=12` | 12 fichiers = 1 min de flux par micro-lot, soit environ 1 080 lignes : une charge régulière sur 2 NodeManagers de 2 Go. |
| Motifs en liste (`motifs`) | Même convention que le silver : un événement invalide est compté une fois, avec tous ses motifs. |

Résultat : 10 783 lues = 10 704 valides + 79 invalides (27 `JSON_INVALIDE`, 52 `VALEUR_MANQUANTE`).

## Décision 2 : watermark de 6 minutes (BG-43)

Retard mesuré (heure du fichier − heure de l'événement) : **maximum 300 s**, 99ᵉ centile 175 s,
263 événements à plus de 60 s. Le watermark doit être **strictement supérieur** au retard maximal.

| Watermark | Événements écartés comme « trop tard » | Mesures dans la fenêtre 08:00 |
|---|---|---|
| 2 minutes | **73** (perdus) | 1 065 |
| **6 minutes (retenu)** | **0** | **1 082** = valeur exacte (calcul batch de référence) |

Le coût d'un watermark plus long : l'état gardé en mémoire (6 min de clés d'`event_id` et de fenêtres
ouvertes, soit environ 6 × 1 080 événements) et le délai avant qu'une fenêtre soit publiée (environ
7 min après sa fin). Sur ce volume, c'est négligeable face au risque de perdre des mesures.

## Décision 3 : déduplication avec `dropDuplicatesWithinWatermark`

Mesuré : **201** `event_id` en double parmi les événements valides, tous avec la même heure et la même
valeur (des renvois). 10 704 valides donnent **10 503** événements uniques.

`dropDuplicatesWithinWatermark(["event_id"])` (nouveauté de Spark 3.5) oublie une clé une fois le
watermark passé : l'état reste borné. Avec `dropDuplicates` classique, il faudrait ajouter l'heure à
la clé pour borner l'état, et un renvoi avec une autre heure ne serait plus reconnu.

## Décision 4 : fenêtres fixes d'une minute, mode append

Agrégats par capteur et par fenêtre fixe (tumbling) d'1 min : nombre de mesures, moyenne, min, max.
Avec environ 6 mesures par capteur et par minute, la moyenne repose sur assez de points, et la
fenêtre reste assez courte pour une supervision. Mode **append** : une fenêtre n'est écrite qu'**une
fois**, quand le watermark a dépassé sa fin. Son résultat est alors définitif, ce qui prépare
l'écriture idempotente de BG-45.

Contrôle : pour chaque fenêtre publiée, le flux donne exactement le même nombre de mesures que le
calcul batch de référence (08:00 = 1 082, 08:01 = 1 075, 08:02 = 1 041).

## Points d'attention mesurés

1. **Deux opérateurs à état enchaînés** (déduplication puis fenêtres) : Spark 3.5 filtre les
   événements tardifs avec le watermark du **lot précédent**. La tolérance réelle est donc un peu
   plus large que 6 min, jamais plus courte (scénario reproduit dans les tests).
2. **Fin d'un rejeu** : avec `--disponible` (trigger `availableNow`), les dernières fenêtres
   (environ les 7 dernières minutes de données) restent en attente, car aucun événement plus récent
   ne fait avancer le watermark. C'est le comportement normal en mode append. Elles sont publiées dès
   que le flux reprend, grâce au checkpoint.
3. **Ressources** : un flux continu occupe des conteneurs YARN tant qu'il tourne. Il faut l'arrêter
   après chaque démonstration (option `--duree` ou `--disponible`).

## Conséquences

- BG-44 branchera les alertes sur les événements dédupliqués (pas de double alerte pour un renvoi).
- BG-45 remplacera l'affichage par une écriture idempotente dans `/data/gold`, avec un checkpoint
  par requête dans `/checkpoints/capteurs/`.
