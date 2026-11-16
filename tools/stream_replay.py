#!/usr/bin/env python3
"""
Rejeu du flux capteurs : dépose un fichier toutes les N secondes (5 s par défaut)
dans un dossier d'atterrissage, local ou HDFS, pour alimenter Spark Structured Streaming.

Le dépôt est atomique : le fichier est d'abord écrit sous un nom caché
(préfixe « . » en local, suffixe « ._COPYING_ » côté HDFS), puis renommé.
Spark ignore les fichiers cachés : il ne lit jamais un fichier à moitié copié.

Exemples :
  # local
  python stream_replay.py --src donnees/J1/capteurs --dest /tmp/landing/capteurs
  # HDFS (commande hdfs disponible dans le conteneur)
  python stream_replay.py --src donnees/J1/capteurs --hdfs-dest /data/landing/capteurs
  # accéléré x10, reprise à partir du 200e fichier (test de reprise après arrêt)
  python stream_replay.py --src donnees/J1/capteurs --dest /tmp/landing --speed 10 --start-at 200
"""
import argparse
import os
import shutil
import subprocess
import sys
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="dossier capteurs d'une livraison (ex. donnees/J1/capteurs)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dest", help="dossier d'atterrissage local")
    g.add_argument("--hdfs-dest", help="dossier d'atterrissage HDFS")
    ap.add_argument("--interval", type=float, default=5.0, help="secondes entre deux fichiers (défaut 5)")
    ap.add_argument("--speed", type=float, default=1.0, help="facteur d'accélération (ex. 10)")
    ap.add_argument("--start-at", type=int, default=0, help="index du premier fichier à déposer")
    ap.add_argument("--limit", type=int, default=0, help="nombre max de fichiers (0 = tous)")
    args = ap.parse_args()

    files = sorted(f for f in os.listdir(args.src) if f.endswith(".jsonl"))
    files = files[args.start_at:]
    if args.limit:
        files = files[: args.limit]
    if not files:
        sys.exit("Aucun fichier .jsonl à rejouer.")

    if args.dest:
        os.makedirs(args.dest, exist_ok=True)
    else:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", args.hdfs_dest], check=True)

    pause = args.interval / args.speed
    print(f"{len(files)} fichiers, un toutes les {pause:.2f} s -> {args.dest or 'hdfs://' + args.hdfs_dest}")
    for i, name in enumerate(files, start=args.start_at):
        t0 = time.time()
        src = os.path.join(args.src, name)
        if args.dest:
            tmp = os.path.join(args.dest, "." + name + ".tmp")
            shutil.copyfile(src, tmp)
            os.replace(tmp, os.path.join(args.dest, name))
        else:
            # « hdfs dfs -put » écrit sous <nom>._COPYING_ puis renomme : dépôt atomique
            subprocess.run(["hdfs", "dfs", "-put", "-f", src, f"{args.hdfs_dest}/{name}"], check=True)
        print(f"[{i:05d}] {name}", flush=True)
        time.sleep(max(0.0, pause - (time.time() - t0)))
    print("Rejeu terminé.")


if __name__ == "__main__":
    main()
