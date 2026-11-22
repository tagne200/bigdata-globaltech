# ADR 004 : pseudonymisation des clients en silver

- **Statut** : proposé (BG-33)
- **Contexte** : les clients arrivent avec des données personnelles (nom, prénom, téléphone,
  email). Les indicateurs de la gold n'ont besoin ni de l'identité ni des coordonnées, mais ils
  doivent pouvoir compter et relier des clients.

## Décision

En silver (`src/silver/clients.py`) :

1. `contact_first_name`, `contact_last_name`, `phone` et l'email en clair sont **supprimés**.
2. L'email, mis en minuscules et sans espaces, devient `email_hash = sha2(sel || email, 256)`.
3. Le **sel** vient de la variable d'environnement `PSEUDO_SEL` (16 caractères minimum). Il n'est
   jamais dans Git : le job refuse de démarrer s'il est absent, et il n'a pas de valeur par défaut.
4. Les rejets de clients ne contiennent aucune donnée personnelle (ni nom, ni téléphone, ni email) :
   la ligne complète reste consultable en bronze, qui a ses propres droits d'accès (BG-26).

## Justification (RGPD)

| Principe | Application |
|---|---|
| Minimisation (art. 5-1-c) | Ce qui ne sert à aucun indicateur est supprimé : nom, prénom, téléphone. |
| Pseudonymisation (art. 4-5) | Sans le sel, qui est conservé à part, le hachage ne permet pas de remonter à la personne. |
| Protection dès la conception (art. 25) | La pseudonymisation est faite à l'entrée de la silver, avant toute analyse. |
| Sécurité du traitement (art. 32) | Le sel est hors du code, hors de Git, et absent des journaux Spark (voir ci-dessous). |

**Limite à connaître** : des données pseudonymisées restent des données personnelles (considérant 26).
Ce n'est pas une anonymisation : celui qui possède le sel peut vérifier si un email donné figure dans
la table. Le sel doit donc être protégé comme un secret.

## Pourquoi un hachage salé et pas autre chose

| Alternative | Raison de l'écarter |
|---|---|
| `sha2(email)` sans sel | Un attaquant hache une liste d'emails connus et retrouve les correspondances. |
| Supprimer l'email | On ne peut plus compter ni relier un client d'une livraison à l'autre. |
| Chiffrement réversible | Quiconque a la clé relit tous les emails : plus risqué pour un usage analytique. |
| Sel écrit dans le code ou un fichier versionné | Interdit par les règles du projet : il finirait dans l'historique Git. |

## Conséquences

- **Stabilité** : le même email donne toujours le même hachage, donc les jointures et les comptages
  de clients distincts fonctionnent (mesuré sur J1 : 4 832 hachages distincts pour 4 832 clients).
- **Le sel ne doit pas changer** : s'il change, tous les hachages changent. Il faut alors rejouer
  `clients.py` sur tout le bronze. Il est partagé dans l'équipe par un canal privé, jamais par Git.
- **Journaux** : un sel passé comme littéral Spark (`F.lit`) apparaîtrait dans les plans d'exécution,
  donc dans les journaux et le History Server. Il est donc injecté par un DataFrame d'une ligne
  (`pseudonymisation.table_du_sel`). Mesuré : 0 occurrence dans 29 717 lignes de journal INFO.
- **Email absent ou mal formé** (139 et 29 clients sur J1) : le client est gardé, avec un
  `email_hash` vide. On ne pseudonymise pas une valeur absente ou fausse.
- **Bronze** : elle garde la copie fidèle, donc des données personnelles en clair. L'accès y est
  réservé au rôle `data_eng` (BG-26).
