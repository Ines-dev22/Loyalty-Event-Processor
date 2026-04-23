Loyalty Event Processor & Analytics
Système d'ingestion de données événementielles et moteur de Business Intelligence (BI) pour programmes de fidélité. Ce projet transforme des flux bruts de transactions en insights actionnables.

Table des Matières

Architecture
Fonctionnalités Clés
Installation
Usage
Aperçu du Dashboard

Architecture
Le projet suit une architecture découplée pour garantir la maintenabilité et l'évolution vers des systèmes de streaming (comme Kafka) :

Ingestion (ingestor.py) : Nettoyage et validation des flux JSON/CSV.

Persistance (db.py) : Couche d'accès aux données (DAO) utilisant SQLite.

Analytics (reporting.py) : Moteur de calcul RFM et détection d'anomalies.

Visualisation (visualizer.py) : Génération de dashboards HTML interactifs avec Plotly.

 Fonctionnalités Clés
1. Ingestion de Données Robuste
Multi-format : Traitement transparent des fichiers JSON et CSV.

Idempotence : Protection contre les doublons via une gestion stricte des event_id.

Qualité des Données : Filtrage automatique des montants invalides et logs d'erreurs détaillés.

2. Business Intelligence & Marketing
Segmentation RFM : Classification automatique des clients (Champions, À Risque, Nouveaux).

Analyse de Performance : Suivi du CA et du panier moyen par boutique (Paris, Lyon, Online).

Insights Horaires : Identification des pics d'activité pour optimiser les campagnes marketing.

3. Sécurité & Fraude
Velocity Check : Détection des accumulations de points anormalement rapides.

Analyse de Ratio : Alerte en cas de disproportion entre le montant dépensé et les points acquis.

 Installation
Cloner le projet

Bash
git clone https://github.com/ton-pseudo/Loyalty-Event-Processor.git
cd Loyalty-Event-Processor
Configurer l'environnement virtuel

Bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
 Usage
Le pipeline est entièrement automatisé via un script Shell :

Bash
chmod +x run_pipeline.sh
./run_pipeline.sh
Cela va successivement :

Initialiser la base de données.

Ingerer les données de test.

Générer les rapports d'analyse dans la console.

Produire le dashboard interactif report.html.

 Aperçu du Dashboard
Le système génère un dashboard interactif permettant de visualiser le flux des ventes en temps réel et la répartition du chiffre d'affaires par point de vente.

 Ce que j'ai appris sur ce projet
Gestion de la persistance avec SQLite.

Manipulation et nettoyage de données avec Pandas.

Création de visualisations interactives avec Plotly.

Mise en place d'un environnement de développement professionnel avec venv et requirements.txt.