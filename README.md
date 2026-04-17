# OCR - Projet 10  
**Mise en place d’un pipeline d’orchestration de données avec Kestra**  
*Avril 2026*  

---

## Contexte du projet  
BottleNeck est un marchand de vin qui dispose de données provenant de deux systèmes :

- **ERP** : produits, stock, prix  
- **CMS** : ventes web  
Les données sont fournies sous forme de fichiers (Excel / Parquet) et doivent être :
- nettoyées  
- contrôlées  
- réconciliées  
- exploitées pour analyse  

---

## Objectifs

- calculer le chiffre d’affaires par produit  
- calculer le chiffre d’affaires total  
- identifier les vins premium (z-score / IQR)  
- produire des datasets exploitables pour analyse  

---

##  Problème
Aujourd’hui, les traitements sont manuels.
Objectif : automatiser toute la chaîne de traitement avec un pipeline data orchestré.

---

## Solution mise en place
Mise en place d’un pipeline de données automatisé avec **Kestra** permettant de :
- orchestrer les étapes ETL  
- exécuter des contrôles qualité  
- transformer les données  
- produire des datasets exploitables  
- suivre les exécutions (logs + monitoring)  

---

## Structure du projet

```text
Projet_10/
├── data/
│   ├── raw/              # données sources ERP / CMS
│   ├── silver/           # données nettoyées / transformées
│   ├── output/           # résultats finaux (CA, analyses)
│   ├── tmp/              # fichiers temporaires
│
├── flows/
│   └── etl_data_pipeline.yml   # pipeline Kestra principal
│
├── presentation/
├── venv/
├── .env
├── docker-compose.yml
├── README.md
├── requirements.txt
```

Étape 1 : Logigramme ETL
Objectif : structurer le pipeline avant implémentation dans Kestra.
Pipeline technique
Extraction
lecture ERP / CMS (Excel → Parquet via DuckDB)
Validation
contrôle des colonnes obligatoires
vérification des fichiers vides
Transformation
jointure ERP / CMS
calcul du chiffre d’affaires
agrégations
Analyse
identification des vins premium (z-score / IQR)
Load
export des résultats dans silver/ et output/

Justification des choix technologiques
Kestra : orchestration des workflows (logs, retries, scheduling)
DuckDB : SQL rapide sur fichiers (Parquet / Excel)
Python : logique métier et validations
Docker / Docker Compose : environnement reproductible local
Parquet : format optimisé pour performance et analyse

Outils utilisés
Python
DuckDB
Kestra
Docker / Docker Compose
SQL (DuckDB)



