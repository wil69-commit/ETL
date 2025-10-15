from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import smtplib
import os
import pandas as pd
from airflow.models import Variable
from pymongo import MongoClient

# VARIABLES GLOBALES 
EMAIL_RECIPIENT = "monmail@gmail.com"
MONGO_URI = "mongodb://host.docker.internal:27017/"
SOURCE_DB = "local"
SOURCE_COLLECTION = "Dataset"
TARGET_DB = "BD_Nettoyer"
TARGET_COLLECTION = "Amazone_clean"
DATA_DIR = "/opt/airflow/data"


# FONCTIONS DU PIPELINE 

def tester_connexion_mongo():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.server_info()  # Test de la connexion
        print("Connexion MongoDB réussie !")
    except Exception as e:
        print("Erreur:", e)
        raise


def Extraction_donnees():
    client = MongoClient(MONGO_URI)
    db = client[SOURCE_DB]
    collection = db[SOURCE_COLLECTION]

    data = list(collection.find())
    if not data:
        print("Aucune donnée trouvée")
        return

    df = pd.DataFrame(data)
    os.makedirs(DATA_DIR, exist_ok=True)
    output_file = os.path.join(DATA_DIR, "Amazone.csv")
    df.to_csv(output_file, index=False)
    print(f"Extraction terminée et exportée vers {output_file}")


def nettoyage():
    input_path = os.path.join(DATA_DIR, "Amazone.csv")
    output_path = os.path.join(DATA_DIR, "Amazone_clean.csv")

    # Lire le fichier source
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Le fichier source {input_path} introuvable !")

    data = pd.read_csv(input_path)

    # Nettoyage des colonnes
    data.columns = data.columns.str.strip().str.replace(' ', '_')
    
    # Conversions de types
    if 'Date' in data.columns:
        data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    if 'Amount' in data.columns:
        data['Amount'] = pd.to_numeric(data['Amount'], errors='coerce')
    if 'Qty' in data.columns:
        data['Qty'] = pd.to_numeric(data['Qty'], errors='coerce')

    # Suppression de colonnes inutiles (ne plante pas si la colonne n’existe pas)
    drop_cols = ['index', 'Unnamed:_22', 'Style', 'SKU', 'Order_ID', 'ASIN', 
                 'ship-postal-code', 'currency', 'fulfilled-by']
    data.drop(columns=[col for col in drop_cols if col in data.columns], inplace=True, errors='ignore')

    # Suppression des doublons
    data.drop_duplicates(inplace=True)

    # Valeurs manquantes
    if 'Courier_Status' in data.columns and not data['Courier_Status'].mode().empty:
        data['Courier_Status'].fillna(data['Courier_Status'].mode()[0], inplace=True)
    if 'Amount' in data.columns:
        data['Amount'].fillna(data['Amount'].mean(), inplace=True)
    for col in ['ship-city', 'ship-state', 'ship-country']:
        if col in data.columns:
            data.dropna(subset=[col], inplace=True)
    if 'promotion-ids' in data.columns and not data['promotion-ids'].mode().empty:
        data['promotion-ids'].fillna(data['promotion-ids'].mode()[0], inplace=True)
    if 'B2B' in data.columns:
        data['B2B'] = data['B2B'].map({True: 'B2B', False: 'B2C'})

    # Sauvegarde du résultat
    data.to_csv(output_path, index=False)
    print(f"Données nettoyées exportées avec succès : {output_path}")


def controle_qualite():
    path = os.path.join(DATA_DIR, "Amazone_clean.csv")
    data = pd.read_csv(path)

    print(f"Nombre de lignes : {len(data)}")
    print(f"Colonnes : {list(data.columns)}")
    if data.isnull().sum().sum() == 0:
        print("Aucune valeur manquante détectée.")
    else:
        print("Valeurs manquantes détectées !")
        print(data.isnull().sum())


def Charger():
    """Chargement des données nettoyées dans une autre base MongoDB"""
    df = pd.read_csv(os.path.join(DATA_DIR, "Amazone_clean.csv"))
    client = MongoClient(MONGO_URI)
    db = client[TARGET_DB]
    collection = db[TARGET_COLLECTION]

    records = df.to_dict(orient='records')
    collection.delete_many({})
    collection.insert_many(records)
    print(f"Chargement terminé dans {TARGET_DB}.{TARGET_COLLECTION}")


def envoie_email():
    password = Variable.get("GMAIL_APP_PASSWORD")  # Stocké dans Airflow
    sender = "expediteur@gmail.com"
    receiver = "receveur@gmail.com"

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(sender, password)
    message =  "Subject:  Succès ETL Airflow: ETL_Amazone terminé\n\nLe pipeline ETL_Amazone a été complété avec succès."

    server.sendmail(sender, receiver, message.encode("utf-8"))
    server.quit()
    print(" Email envoyé avec succès !")


# CONFIGURATION DU DAG

default_args = {
    'owner': 'Wilfrid',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="DAG_Amazone",
    default_args=default_args,
    description="Pipeline ETL complet depuis MongoDB",
    start_date=datetime(2025, 10, 15),
    schedule_interval='*/5 * * * *',  
    catchup=False,
    tags=["ETL", "MongoDB", "Amazone"]
) as dag:

    start = EmptyOperator(task_id="debut_du_pipeline")

    test_connexion = PythonOperator(
        task_id="test_connexion_mongo",
        python_callable=tester_connexion_mongo,
    )

    extract_task = PythonOperator(
        task_id="extract_mongo_to_csv",
        python_callable=Extraction_donnees,
    )

    clean = PythonOperator(
        task_id="Nettoyage",
        python_callable=nettoyage,
    )

    control = PythonOperator(
        task_id="Controle_qualite",
        python_callable=controle_qualite,
    )

    load_task = PythonOperator(
        task_id="Charger_donnees_dans_mongo",
        python_callable=Charger,
    )

    email_succes = PythonOperator(
        task_id="Envoyer_email_succes",
        python_callable=envoie_email,
    )

    end = EmptyOperator(task_id="fin_du_pipeline")

    # Chaînage logique du pipeline
    start >> test_connexion >> extract_task >> clean >> control >> load_task >> [email_succes , end]


