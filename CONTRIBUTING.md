# Règles de contribution

Rien n'arrive sur `main` sans pull request revue par un autre membre. Une vérification automatique (CI) s'y ajoutera au lot 8.

## Qui relit qui

| Auteur de la PR | Relecteur |
|---|---|
| Michel | Nathan |
| Nathan | Suzanne |
| Suzanne | Michel |

## Branches

`<type>/BG-<n>-<description-courte>`, où `BG-<n>` est la clé du ticket Jira (visible en haut du ticket).

Exemples : `feat/BG-14-docker-compose-hdfs`, `fix/BG-21-rejeu-bronze`, `docs/BG-60-runbook`.

Types : `feat` (fonctionnalité), `fix` (correction), `test`, `docs`, `chore` (outillage), `perf` (optimisation).

## Commits (Conventional Commits)

```
feat(silver): historise les produits en SCD2 BG-24
fix(bronze): rejeu de J1 sans doublon BG-17
```

- Un commit = un changement compréhensible seul.
- Chacun commite avec **son propre compte** : `git config user.email` = l'email de son compte GitHub.

## Déroulé d'une tâche

1. Passer le ticket Jira en **En cours**.
2. `git switch main && git pull`, puis `git switch -c feat/BG-14-...`.
3. Coder et vérifier son travail en local.
4. `git push -u origin feat/BG-14-...`, ouvrir la PR (titre : `BG-14 ...`), passer le ticket en **En revue**.
5. Le relecteur commente ou approuve ; l'auteur corrige.
6. Fusion en **Squash and merge**, suppression de la branche, ticket en **Terminé**.

## Règles de code

- Schémas Spark explicites (`StructType`), jamais `inferSchema` dans le code final.
- Pas d'UDF Python quand une fonction native existe, sauf justification écrite dans la PR.
- Configuration dans `conf/`, secrets uniquement en variables d'environnement.
- Tout choix technique est justifié par une mesure, notée dans la PR puis dans `docs/`.
