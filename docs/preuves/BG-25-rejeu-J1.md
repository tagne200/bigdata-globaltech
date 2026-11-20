# Preuve du rejeu sans doublon : livraison J1 (BG-25)

- **Date** : 2026-11-20 17:50 (heure de Paris)
- **Commande** : `bash scripts/preuve_rejeu_bronze.sh J1` (2 exécutions de `src/bronze/ingestion.py` sur YARN)
- **Durées** : exécution 1 = 53 s, exécution 2 = 65 s
- **Verdict** : **REJEU SANS DOUBLON : comptages identiques après les 2 exécutions**

| Source | Lignes lues | Dont mal formées (gardées) | Total table après exécution 1 | Total table après exécution 2 |
|---|---|---|---|---|
| commandes | 101200 | 56 | 101200 | 101200 |
| produits | 592 | 0 | 592 | 592 |
| clients | 5127 | 0 | 5127 | 5127 |
| logs_web | 50000 | 0 | 50000 | 50000 |

Contre-exemple (mesuré sur l'échantillon J1, voir docs/adr/003-ingestion-bronze.md) : avec
`mode("append")`, la 2e exécution double la table des commandes (101 200 -> 202 400 lignes).

Détail HDFS après l'exécution 2 (`hdfs dfs -count` : dossiers, fichiers, octets, chemin) :

```
           2            1             288964 /data/bronze/clients
           2            2            2164306 /data/bronze/commandes
           2            2            2140897 /data/bronze/logs_web
           2            1              17573 /data/bronze/produits
```
