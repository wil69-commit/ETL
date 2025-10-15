# 🚀 Projet ETL avec Apache Airflow & MongoDB

![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7+-blue?logo=apacheairflow)
![Python](https://img.shields.io/badge/Python-3.7%2B-yellow?logo=python)
![MongoDB](https://img.shields.io/badge/MongoDB-Database-green?logo=mongodb)
![License](https://img.shields.io/badge/license-MIT-lightgrey)
![Status](https://img.shields.io/badge/status-Completed-success)

---

## Description du projet

Ce projet met en place un **pipeline ETL automatisé** (Extraction, Transformation, Chargement) à l’aide de **Apache Airflow**, connecté à une base de données **MongoDB Compass**.

Le pipeline permet :
- d’**extraire** les données brutes depuis MongoDB,
- de les **nettoyer et transformer** avec **Pandas**,
- de **charger** les données nettoyées dans une nouvelle base MongoDB,
- d’**exporter** les résultats sous forme de fichier CSV,
- et d’**envoyer une notification par email** à la fin du processus.

---

## ⚙️ Technologies utilisées

| Outil / Librairie | Rôle |
|--------------------|------|
| **Apache Airflow** | Orchestration et automatisation du pipeline |
| **MongoDB Compass** | Base de données NoSQL source et cible |
| **Python (Pandas)** | Traitement et nettoyage des données |
| **SMTP / Gmail** | Notification de fin de traitement |
| **Docker** | Environnement isolé et reproductible |

---

## 🧩 Architecture du projet

```plaintext
📁 airflow/
 ├── dags/
 │   └── Amazone_DAG.py         # Code principal du pipeline Airflow
 ├── data/
 │   ├── Amazone.csv            # Données extraites
 │   └── Amazone_clean.csv      # Données nettoyées
 ├── docker-compose.yaml        # Configuration Docker d’Airflow
 └── requirements.txt           # Dépendances Python (pymongo, pandas, etc.)

