# Projet AirNaoned — Ingénierie des données

Projet réalisé dans le cadre du cours **Ingénierie des données (MIAGE M2)**, avec BPCE Solutions Informatiques.

## 1) Contexte et objectifs

L’objectif est de construire un pipeline de données de bout en bout pour analyser la qualité de l’air autour de Nantes :

- **Phase 1 — Mesurer** : collecte et consolidation des mesures des polluants, suivi de l’évolution et de l’IQA.
- **Phase 2 — Alerter** : détection des dépassements de seuils critiques et recommandations associées.
- **Phase 3 — Comprendre pour agir** : enrichissement avec des données externes pour identifier les sources de pollution et les corrélations.

Livrables attendus :
- Rapport de cadrage et de restitution
- Code source du pipeline
- Présentation orale

## 2) Sources de données

API AirPL (base du pipeline) :
- Mesures horaires : https://data.airpl.org/dataset/mesures
- Endpoint stations : `https://data.airpl.org/api/v1/mesure/station-list/`
- Endpoint départements : `https://data.airpl.org/api/v1/mesure/departement/`
- Endpoint polluants : `https://data.airpl.org/api/v1/mesure/polluant/`
- Endpoint mesures : `https://data.airpl.org/api/v1/mesure/horaire/`

Liens utiles projet :
- Maquette dashboard : https://www.figma.com/make/rq7cPliQskqQAII4H1NBIX/Tableau-de-bordqualit%C3%A9-de-l-air?t=Kbkuprv89xG4ozfx-1
- Calcul IQA (référence) : https://ecmwf-projects.github.io/copernicus-training-cams/proc-aq-index.html
- Seuils réglementaires : https://www.ecologie.gouv.fr/sites/default/files/06_Seuils%20r%C3%A9glementaires.pdf

## 3) Architecture du projet

### Fichiers principaux

- `main.py` : orchestration complète du pipeline ETL.
- Extraction :
  - `extractiondepartement.py`
  - `extractionstation.py`
  - `extractpolluant.py`
  - `extraction.py` (mesures horaires)
- Transformation / chargement BigQuery :
  - `TransformationTableJaune.py` → `dim_communes`
  - `TransformationTableBleu.py` → `dim_polluants`
  - `TransformationTableOrange.py` → `dim_stations`
  - `TransformationTableVerte.py` → `fait_mesures`
- `bq_utils.py` : utilitaire de chargement DataFrame vers BigQuery.
- `Dockerfile` : exécution conteneurisée.

### Flux de traitement

1. Extraction des données AirPL et écriture des CSV dans `/tmp/data`
2. Transformation des colonnes utiles (renommage, dédoublonnage, filtrage des valeurs nulles)
3. Chargement dans BigQuery (dataset `pollution_data`)

## 4) Modèle de données BigQuery

Projet BigQuery utilisé dans le code : `tp-donnees-gp1`  
Dataset : `pollution_data`

Tables alimentées automatiquement par le pipeline :
- `dim_communes`
- `dim_polluants`
- `dim_stations`
- `fait_mesures`

Tables créées manuellement en complément (commandes fournies) :

```bash
bq mk --table \
  pollution_data.dim_temps \
  id_temps:TIMESTAMP,annee:INTEGER,mois:INTEGER,jour:INTEGER,heure:INTEGER

bq mk --table \
  pollution_data.dim_communes \
  insee_com:INTEGER,nom_com:STRING,code_dept:STRING

bq mk --table \
  pollution_data.dim_polluants \
  code_poll:INTEGER,nom_poll:STRING,unite:STRING,id_poll_ue:INTEGER
```

> Remarque : `dim_polluants` est aussi chargée par le pipeline, les schémas doivent rester cohérents.

## 5) Prérequis

- Python 3.10+
- Compte GCP + accès BigQuery
- Authentification GCP active (`gcloud auth application-default login`)
- Dépendances Python du projet

## 6) Installation locale

```bash
cd /tmp/workspace/Romain357/tp_ingenierie-donnees
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 7) Exécution du pipeline

### Lancer tout le pipeline

```bash
cd /tmp/workspace/Romain357/tp_ingenierie-donnees
python main.py
```

### Modes de chargement des mesures (`ETL_MODE`)

- `INCREMENTAL` (par défaut) : append dans `fait_mesures`
- `FULL` : écrase `fait_mesures`

Exemple :

```bash
ETL_MODE=FULL python main.py
```

## 8) Détails fonctionnels actuels

- Les mesures extraites sont celles de **J-1** (date courante - 1 jour).
- Les fichiers d’extraction sont générés dans `/tmp/data`.
- Le chargement BigQuery se fait via `google-cloud-bigquery` à partir de DataFrames pandas.

## 9) Cadrage pédagogique des phases

### Phase 1 — Mesurer
- Suivi des polluants (PM2.5, PM10, SO2, NO2, O3)
- Construction des indicateurs du tableau de bord
- Historique attendu : 8h glissantes, 7j glissants, 12 mois glissants

### Phase 2 — Alerter
- Détection de dépassements des seuils réglementaires
- Alerte si IQA >= 100
- Affichage de recommandations santé selon le niveau d’IQA

### Phase 3 — Comprendre pour agir
- Enrichissement avec données complémentaires (communes, population, entreprises, météo, trafic, etc.)
- Analyse des corrélations et causes probables de pollution
- Bonus possible : prédiction

## 10) Exécution avec Docker

```bash
cd /tmp/workspace/Romain357/tp_ingenierie-donnees
docker build -t airnaoned-etl .
docker run --rm airnaoned-etl
```

## 11) Limites et pistes d’amélioration

- Ajouter des tests automatisés (unitaires + intégration)
- Rendre les paramètres (projet GCP, dataset, date de traitement) configurables
- Industrialiser l’ordonnancement (Cloud Composer / cron / Cloud Run Jobs)
- Implémenter les calculs IQA et la logique d’alerte directement dans le pipeline

## 12) Contact projet (cours)

**Jérémy KLAUER** — Tech Lead Data  
BPCE Solutions Informatiques  
12-14 rue des Piliers de la Chauvinière, 44800 Saint-Herblain  
Email : klauer-j@univ-nantes.fr
