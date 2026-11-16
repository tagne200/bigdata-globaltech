# Squelette projet Big Data : GlobalTech Industries

Le code initial ne contient volontairement pas la solution. Objectif : ingestion, couches bronze / silver / gold, analyses batch, streaming capteurs (voir l'énoncé).

## Données

Les données sont livrées en trois fois, aux jalons **J1, J2 et J3**. Elles sont produites par un générateur à graine fixe : un même jour livré deux fois est strictement identique (utile pour tester le rejeu sans doublon).

`data/echantillon/J1/` contient un **petit échantillon** (100 000 commandes) de même structure que les vraies livraisons, pour développer en local en attendant J1. Les volumes réels sont d'environ 4 millions de commandes par livraison.

Les anomalies ne sont pas listées : à vous de les découvrir, de les mesurer et de les traiter.

### Organisation d'une livraison

```
J1/
  commandes/commandes_J1_part-0001.csv ...   en-tête répété dans chaque part
  produits/produits_J1.csv
  clients/clients_J1.csv
  logs_web/access_J1_part-0001.log ...
  capteurs/capteurs_20260131T080005.jsonl ... un fichier par tranche de 5 s
```

| Livraison | Commandes | Produits | Clients |
|---|---|---|---|
| J1 | janvier 2026 | historique complet des versions jusqu'au 31/01/2026 | extraction complète |
| J2 | février 2026 | nouvelles versions uniquement (delta) | nouveaux clients et mises à jour (delta) |
| J3 | mars 2026 | nouvelles versions uniquement (delta) | delta |

### Dictionnaire des fichiers bruts

**Commandes** (CSV, séparateur virgule, UTF-8)

| Colonne | Description |
|---|---|
| `order_id` | identifiant de commande |
| `order_ts` | date et heure de commande, ISO 8601 sans fuseau, heure de Paris |
| `customer_id` | identifiant client |
| `product_id` | identifiant produit |
| `quantity` | quantité commandée |
| `unit_price_eur` | prix unitaire facturé (EUR), après remise éventuelle |
| `country` | pays de livraison, code ISO 3166-1 alpha-2 |
| `warehouse_id` | entrepôt expéditeur |
| `channel` | `web`, `edi`, `commercial`, `partenaire` |
| `status` | `COMPLETED`, `SHIPPED`, `CANCELLED`, `RETURNED` |

Le schéma peut évoluer d'une livraison à l'autre.

**Produits** (CSV) : une ligne = une version d'un produit.

| Colonne | Description |
|---|---|
| `product_id`, `product_name` | identifiant et libellé |
| `family`, `category` | famille et catégorie (peuvent changer dans le temps) |
| `unit_price_eur` | prix catalogue (EUR) |
| `updated_at` | date à partir de laquelle la version s'applique |
| `extracted_at` | date et heure de l'extraction depuis le système source |

**Clients** (CSV, champs texte éventuellement entre guillemets) : `customer_id, company_name, contact_first_name, contact_last_name, email, phone, country, city, segment, created_at, updated_at`. Contient des **données personnelles**.

**Logs web** (texte) : format Apache *combined*, suivi du temps de réponse en millisecondes.

```
203.0.113.12 - - [07/Jan/2026:14:21:03 +0100] "GET /catalogue/produit/P00042 HTTP/1.1" 200 5123 "-" "Mozilla/5.0 ..." 84
```

**Capteurs** (JSON Lines) : `event_id, sensor_id, machine_id, site, country, metric, value, unit, event_time` (ISO 8601 avec fuseau). Le nom de fichier donne l'heure d'arrivée ; `event_time` donne l'heure de mesure.

### Règles métier

| Règle | Valeur |
|---|---|
| Fuseau horaire de référence | Europe/Paris |
| Prix applicable | version produit en vigueur à `order_ts` (SCD type 2) |
| Seuils d'alerte capteurs | température > 85 °C, vibration > 12 mm/s, pression > 8 bar |
| Chiffre d'affaires | `quantity × unit_price_eur`, commandes non annulées |

## Simuler le flux capteurs

`tools/stream_replay.py` dépose un fichier toutes les 5 s dans un dossier d'atterrissage (dépôt atomique : Spark ne lit jamais un fichier à moitié copié).

```bash
# en local
python tools/stream_replay.py --src data/echantillon/J1/capteurs --dest /tmp/landing/capteurs
# vers HDFS, depuis un conteneur qui dispose de la commande hdfs
python tools/stream_replay.py --src /opt/project/data/J1/capteurs --hdfs-dest /data/landing/capteurs
# accéléré x10 ; reprise à partir du 200e fichier (test de reprise après arrêt)
python tools/stream_replay.py --src ... --dest ... --speed 10 --start-at 200
```

## Remarques sur le squelette

- `docker-compose.yml` lance Spark en mode *standalone*. Le lot 1 demande un cluster HDFS + YARN (NameNode, au moins 2 DataNodes, ResourceManager, NodeManagers, Spark History Server) : ce fichier est un point de départ à remplacer.
- Les images `bitnami/*` ne sont plus publiées sur `docker.io/bitnami` depuis septembre 2025 : choisissez vos images et justifiez ce choix.
