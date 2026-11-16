#!/usr/bin/env python3
"""Profilage de l'échantillon J1 : compte les anomalies de chaque jeu brut.

Bibliothèque standard uniquement. Usage :
    python scripts/profilage_j1.py data/echantillon/J1
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime

ISO_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")
FR_TS = re.compile(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$")
EPOCH_MS = re.compile(r"^\d{13}$")
ISO2 = re.compile(r"^[A-Z]{2}$")
STATUTS = {"COMPLETED", "SHIPPED", "CANCELLED", "RETURNED"}
RANG = {"SHIPPED": 1, "COMPLETED": 2, "RETURNED": 3, "CANCELLED": 4}
APACHE = re.compile(
    r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)" (\d+)$'
)
SEUILS = {"temperature": 85, "vibration": 12, "pression": 8}


def num(s):
    try:
        v = float(s)
    except (TypeError, ValueError):
        return None
    return None if v != v else v  # NaN -> None


def profil_commandes(base):
    res = Counter()
    lignes, mal_formees = [], Counter()
    header = None
    for f in sorted(glob.glob(f"{base}/commandes/*.csv")):
        with open(f, encoding="utf-8", newline="") as fh:
            r = csv.reader(fh)
            h = next(r)
            header = header or h
            for row in r:
                res["lues"] += 1
                if len(row) != len(header):
                    mal_formees["separateur ;" if len(row) == 1 else f"{len(row)} colonnes"] += 1
                else:
                    lignes.append(dict(zip(header, row, strict=True)))
    res["mal_formees"] = sum(mal_formees.values())
    vus, ids = set(), Counter()
    uniques = []
    for d in lignes:
        key = tuple(d.values())
        if key in vus:
            res["doublons_exacts"] += 1
            continue
        vus.add(key)
        uniques.append(d)
        ids[d["order_id"]] += 1
    par_id = defaultdict(list)
    for d in uniques:
        par_id[d["order_id"]].append(d)
    conflits = {k: v for k, v in par_id.items() if len(v) > 1}
    res["ids_conflictuels"] = len(conflits)
    res["lignes_conflictuelles"] = sum(len(v) for v in conflits.values())
    cols_diff = Counter()
    transitions = Counter()
    for v in conflits.values():
        diff = tuple(c for c in header if len({x[c] for x in v}) > 1)
        cols_diff[diff] += 1
        st = sorted((x["status"] for x in v), key=lambda s: RANG.get(s.strip().upper(), 0))
        transitions[" -> ".join(st)] += 1

    ts_formats, hors_mois = Counter(), 0
    qte, prix, pays, statut = Counter(), Counter(), Counter(), Counter()
    for d in uniques:
        t = d["order_ts"].strip()
        if ISO_TS.match(t):
            ts_formats["ISO 8601"] += 1
            if not t.startswith("2026-01"):
                hors_mois += 1
        elif FR_TS.match(t):
            ts_formats["dd/MM/yyyy HH:mm:ss"] += 1
        elif EPOCH_MS.match(t):
            ts_formats["epoch en ms"] += 1
        elif t == "":
            ts_formats["vide"] += 1
        else:
            ts_formats[f"autre ({t})"] += 1
        q = num(d["quantity"])
        if d["quantity"].strip() == "":
            qte["vide"] += 1
        elif q is None or q != int(q):
            qte["non numérique"] += 1
        elif q <= 0:
            qte["<= 0"] += 1
        p = num(d["unit_price_eur"])
        if d["unit_price_eur"].strip() == "":
            prix["vide"] += 1
        elif p is None:
            prix["non numérique"] += 1
        elif p <= 0:
            prix["<= 0"] += 1
        c = d["country"]
        if c == "":
            pays["vide"] += 1
        elif not ISO2.match(c):
            pays["non normalisé"] += 1
        s = d["status"]
        if s not in STATUTS:
            statut["casse ou valeur non standard"] += 1
    prod_count = Counter(d["product_id"] for d in uniques)
    top = prod_count.most_common(1)[0]
    med = statistics.median(prod_count.values())
    return {
        "res": res,
        "mal_formees": mal_formees,
        "cols_diff": cols_diff,
        "transitions": transitions,
        "ts_formats": ts_formats,
        "hors_mois": hors_mois,
        "qte": qte,
        "prix": prix,
        "pays": pays,
        "statut": statut,
        "uniques": uniques,
        "skew": (top[0], top[1], med, top[1] / med),
        "pays_variantes": Counter(
            d["country"] for d in uniques if d["country"] and not ISO2.match(d["country"])
        ),
        "vides_client": sum(1 for d in uniques if d["customer_id"].strip() == ""),
    }


def profil_produits(base):
    with open(glob.glob(f"{base}/produits/*.csv")[0], encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    ids = Counter(r["product_id"] for r in rows)
    versions = Counter(ids.values())
    exact = len(rows) - len({tuple(r.values()) for r in rows})
    meme_date = len(rows) - len({(r["product_id"], r["updated_at"]) for r in rows})
    prix_virgule = sum(1 for r in rows if "," in r["unit_price_eur"])
    prix = [num(r["unit_price_eur"].replace(",", ".")) for r in rows]
    prix_bad = sum(1 for v in prix if v is None or v <= 0)
    fam_norm = {r["family"].strip().lower() for r in rows}
    fam_brut = {r["family"] for r in rows}
    chg_fam = sum(
        1 for pid in ids if len({r["family"].strip().lower() for r in rows if r["product_id"] == pid}) > 1
    )
    dates = sorted(r["updated_at"] for r in rows)
    return {
        "lignes": len(rows),
        "produits": len(ids),
        "versions": versions,
        "doublons_exacts": exact,
        "meme_id_meme_date": meme_date,
        "prix_virgule": prix_virgule,
        "prix_invalides": prix_bad,
        "familles": (len(fam_norm), len(fam_brut)),
        "chg_famille": chg_fam,
        "periode": (dates[0], dates[-1]),
        "ids": set(ids),
    }


def profil_clients(base):
    with open(glob.glob(f"{base}/clients/*.csv")[0], encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    ids = Counter(r["customer_id"] for r in rows)
    exact = len(rows) - len({tuple(r.values()) for r in rows})
    vides = {c: sum(1 for r in rows if r[c].strip() == "") for c in rows[0]}
    email_maj = sum(1 for r in rows if r["email"] and r["email"] != r["email"].lower())
    email_inv = sum(
        1 for r in rows if r["email"] and not re.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z]+$", r["email"])
    )
    date_fr = sum(1 for r in rows if re.match(r"^\d{2}/\d{2}/\d{4}$", r["created_at"]))
    pays_nn = sum(1 for r in rows if r["country"] and not ISO2.match(r["country"]))
    return {
        "lignes": len(rows),
        "clients": len(ids),
        "doublons_exacts": exact,
        "ids_en_double": sum(1 for v in ids.values() if v > 1),
        "vides": {k: v for k, v in vides.items() if v},
        "email_maj": email_maj,
        "email_inv": email_inv,
        "date_fr": date_fr,
        "pays_nn": pays_nn,
        "ids": set(ids),
    }


def profil_logs(base):
    ok, bad, statuts = 0, Counter(), Counter()
    par_jour = defaultdict(Counter)
    for f in sorted(glob.glob(f"{base}/logs_web/*.log")):
        with open(f, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.rstrip("\n")
                m = APACHE.match(line)
                if not m:
                    bad["vide" if not line.strip() else "tronquée ou non conforme"] += 1
                    continue
                ts = m.group(2)
                try:
                    jour = datetime.strptime(ts.split(":")[0], "%d/%b/%Y").date().isoformat()
                except ValueError:
                    bad["horodatage non Apache"] += 1
                    continue
                ok += 1
                st = m.group(6)
                statuts[st] += 1
                par_jour[jour]["total"] += 1
                if st.startswith("5"):
                    par_jour[jour]["5xx"] += 1
                if st == "404":
                    par_jour[jour]["404"] += 1
    taux = {j: c["5xx"] / c["total"] for j, c in par_jour.items()}
    moy = statistics.mean(taux.values())
    ecart = statistics.pstdev(taux.values())
    pics = sorted(((t, j) for j, t in taux.items() if t > moy + 3 * ecart), reverse=True)
    return {
        "ok": ok,
        "bad": bad,
        "statuts": statuts,
        "jours": len(par_jour),
        "moy": moy,
        "pics": pics,
        "par_jour": par_jour,
    }


def profil_capteurs(base):
    files = sorted(glob.glob(f"{base}/capteurs/*.jsonl"))
    total, bad_json, nulls = 0, 0, 0
    ids = Counter()
    retards, metriques, alertes, cles = [], Counter(), Counter(), Counter()
    valeurs_texte = Counter()
    for f in files:
        arrivee = datetime.strptime(os.path.basename(f)[9:24], "%Y%m%dT%H%M%S")
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                total += 1
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    bad_json += 1
                    continue
                cles[tuple(sorted(e))] += 1
                ids[e.get("event_id")] += 1
                metriques[(e.get("metric"), e.get("unit"))] += 1
                v = e.get("value")
                if v is None:
                    nulls += 1
                elif not isinstance(v, (int, float)):
                    valeurs_texte[repr(v)] += 1
                elif e.get("metric") in SEUILS and v > SEUILS[e["metric"]]:
                    alertes[e["metric"]] += 1
                try:
                    t = datetime.fromisoformat(e["event_time"]).replace(tzinfo=None)
                    retards.append((arrivee - t).total_seconds())
                except (KeyError, ValueError):
                    pass
    retards.sort()
    return {
        "fichiers": len(files),
        "total": total,
        "bad_json": bad_json,
        "doublons": sum(v - 1 for v in ids.values() if v > 1),
        "nulls": nulls,
        "valeurs_texte": valeurs_texte,
        "metriques": metriques,
        "alertes": alertes,
        "retard_med": statistics.median(retards),
        "retard_max": retards[-1],
        "retard_60": sum(1 for r in retards if r > 60),
        "schemas": len(cles),
    }


def main(base):
    c = profil_commandes(base)
    p = profil_produits(base)
    k = profil_clients(base)
    lg = profil_logs(base)
    s = profil_capteurs(base)
    orph_p = sum(1 for d in c["uniques"] if d["product_id"] not in p["ids"])
    orph_c = sum(1 for d in c["uniques"] if d["customer_id"] and d["customer_id"] not in k["ids"])
    out = {
        "commandes": c,
        "produits": p,
        "clients": k,
        "logs": lg,
        "capteurs": s,
        "orph_p": orph_p,
        "orph_c": orph_c,
    }
    for name, d in out.items():
        print(f"== {name}")
        if isinstance(d, dict):
            for kk, vv in d.items():
                if kk not in ("uniques", "ids", "par_jour"):
                    print(f"  {kk}: {vv}")
        else:
            print(f"  {d}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data/echantillon/J1")
