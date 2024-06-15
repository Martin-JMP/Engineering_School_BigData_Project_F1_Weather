## Engineering School ISEP Project 

# Advanced Databases and Big-Data

Ce projet, soumis le 15 juin 2024 par Martin JONCOURT et Lucas SAYAG, se concentre sur l'intégration et l'analyse des données des courses de Formule 1 avec les données météorologiques correspondantes. Le projet utilise plusieurs API pour collecter les données, les traite avec Apache Spark, et les visualise sur un tableau de bord Kibana.

## Membres du Projet
- Martin JONCOURT
- Lucas SAYAG

## Informations sur les APIs
### API Résultats des Courses de Formule 1
- **Fournisseur :** Ergast API
- **Données :** Résultats des courses, classements des pilotes, détails des circuits, classements des constructeurs.
- **Fréquence de Mise à Jour :** Après chaque Grand Prix de F1
- **Format des Données :** JSON
- **Exemple de Requête :** `http://ergast.com/api/f1/2024/1/results`

### API Données Météorologiques
- **Fournisseur :** Meteostat
- **Données :** Températures moyennes, minimales, maximales, vitesse du vent, précipitations, neige.
- **Fréquence de Mise à Jour :** Toutes les minutes
- **Format des Données :** CSV
- **Exemple de Requête :** `https://meteostat.p.rapidapi.com/point/daily?lat=-37.8497&lon=144.968&start=2024-03-16&end=2024-03-24`

## Traitement des Données
### API Résultats des Courses de Formule 1
- **Formatage du Temps :** Affichage des temps de course en minutes, secondes, millisecondes.
- **Nouvelles Variables :**
  - Points gagnés
  - Points cumulatifs
- **Format des Données :** Parquet

### API Données Météorologiques
- **Nouvelles Variables :**
  - Ville
  - Pays
- **Format des Données :** Parquet

### Combinaison des Données
- **Méthode de Combinaison :** Jointure interne sur ville, pays et date en utilisant les DataFrames Spark.
- **Objectif :** Enrichir les données de la Formule 1 avec des informations météorologiques contextuelles.
- **Format de Sortie :** Parquet

## Analyse et Utilisation
### Tâches d'Analyse
- Victoires par pilote, par année, par ville
- Tours les plus rapides par course
- Analyse des données météorologiques par année, ville, pilote
- Évolution des points des pilotes
- Analyse des arrêts au stand
- **Format de Sortie :** Fichiers Parquet pour chaque analyse

### Utilisation
- Les données sont visualisées sur un tableau de bord Kibana.

## Structure du DAG
- Le DAG Airflow `project_dag` orchestre les tâches de récupération, de traitement et d'analyse des données.

## Tâches
1. Récupérer les Données Brutes
2. Traiter les Données
3. Récupérer les Données Météorologiques
4. Agréger les Données
5. Combiner les Données
6. Analyser les Données d'Utilisation
7. Tâches d'Indexation

## Graphique du DAG
- Illustration de l'organisation du DAG et de la séquence des tâches.

## Structure du Data Lake
- Une collection organisée de fichiers de données, structurée pour des requêtes et analyses efficaces.

## Indexes et Vues de Données
- Création de vues de données individuelles dans Kibana pour chaque index afin d'améliorer l'organisation et la facilité d'utilisation, permettant une exploration efficace et une analyse ciblée.

## Tableau de Bord
- Fournit une analyse complète des performances des pilotes de F1 et des conditions météorologiques associées lors des différents Grands Prix.

## Conclusion
Le projet intègre et analyse avec succès les données des courses de F1 avec les données météorologiques, offrant des insights précieux sur les performances en course et l'impact des conditions météorologiques. Les résultats sont visualisés sur un tableau de bord Kibana, facilitant la prise de décisions informées et la planification stratégique pour les équipes de F1 et les passionnés.
