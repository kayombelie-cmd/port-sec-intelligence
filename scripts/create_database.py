import os
os.makedirs('data/processed', exist_ok=True)
import sqlite3
import pandas as pd
from datetime import datetime
import json

print("🗄️ CRÉATION DE LA BASE DE DONNÉES SQL")
print("="*50)

# 1. Connexion à SQLite (gratuit, intégré à Python)
conn = sqlite3.connect('data/processed/portsec.db')
cursor = conn.cursor()

print("1. Création des tables...")

# Table clients
cursor.execute('''
CREATE TABLE IF NOT EXISTS clients (
    client_id TEXT PRIMARY KEY,
    nom TEXT,
    segment TEXT,
    contrat_type TEXT,
    date_premier_contrat DATE,
    ca_annuel_ke REAL
)
''')

# Table conteneurs
cursor.execute('''
CREATE TABLE IF NOT EXISTS conteneurs (
    conteneur_id TEXT PRIMARY KEY,
    taille TEXT,
    type TEXT,
    client_proprietaire TEXT,
    client_locataire TEXT,
    poids_brut_kg REAL,
    statut_douane TEXT,
    FOREIGN KEY (client_proprietaire) REFERENCES clients(client_id)
)
''')

# Table operations
cursor.execute('''
CREATE TABLE IF NOT EXISTS operations (
    operation_id TEXT PRIMARY KEY,
    timestamp DATETIME,
    conteneur_id TEXT,
    type_operation TEXT,
    zone TEXT,
    engin TEXT,
    duree_minutes REAL,
    operateur TEXT,
    urgence BOOLEAN,
    erreur BOOLEAN,
    completed BOOLEAN,
    FOREIGN KEY (conteneur_id) REFERENCES conteneurs(conteneur_id)
)
''')

# Table meteo
cursor.execute('''
CREATE TABLE IF NOT EXISTS meteo (
    date DATE PRIMARY KEY,
    temperature_matin REAL,
    temperature_aprem REAL,
    precipitation_mm REAL,
    vent_kmh REAL,
    visibilite TEXT,
    impact_operations TEXT
)
''')

print("✅ Tables créées")

# 2. Chargement des données
print("\n2. Chargement des données dans les tables...")

tables_data = {
    'clients': 'data/external/clients.csv',
    'conteneurs': 'data/external/conteneurs.csv', 
    'operations': 'data/raw/operations.csv',
    'meteo': 'data/external/meteo.csv'
}

for table, filepath in tables_data.items():
    df = pd.read_csv(filepath)
    
    # Conversion des dates pour SQLite
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date']).dt.date
    
    df.to_sql(table, conn, if_exists='replace', index=False)
    print(f"   • {table}: {len(df):,} lignes importées")

# 3. Création de vues pour l'analyse
print("\n3. Création de vues analytiques...")

# Vue 1 : Opérations journalières agrégées
cursor.execute('''
CREATE VIEW IF NOT EXISTS vue_operations_journalieres AS
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as nb_operations,
    AVG(duree_minutes) as duree_moyenne,
    SUM(CASE WHEN urgence = 1 THEN 1 ELSE 0 END) as urgences,
    SUM(CASE WHEN erreur = 1 THEN 1 ELSE 0 END) as erreurs,
    COUNT(DISTINCT conteneur_id) as conteneurs_uniques,
    COUNT(DISTINCT engin) as engins_utilises
FROM operations
GROUP BY DATE(timestamp)
ORDER BY date
''')

# Vue 2 : Performance par engin
cursor.execute('''
CREATE VIEW IF NOT EXISTS vue_performance_engins AS
SELECT 
    engin,
    COUNT(*) as total_operations,
    AVG(duree_minutes) as duree_moyenne,
    SUM(CASE WHEN urgence = 1 THEN 1 ELSE 0 END) as urgences,
    SUM(CASE WHEN erreur = 1 THEN 1 ELSE 0 END) as erreurs,
    COUNT(DISTINCT DATE(timestamp)) as jours_actifs,
    COUNT(*) * 1.0 / COUNT(DISTINCT DATE(timestamp)) as operations_par_jour
FROM operations
GROUP BY engin
ORDER BY total_operations DESC
''')

# Vue 3 : Clients avec leur activité
cursor.execute('''
CREATE VIEW IF NOT EXISTS vue_clients_activite AS
SELECT 
    c.client_id,
    c.nom,
    c.segment,
    c.contrat_type,
    COUNT(DISTINCT o.conteneur_id) as conteneurs_utilises,
    COUNT(o.operation_id) as total_operations,
    AVG(o.duree_minutes) as duree_moyenne_operation,
    SUM(CASE WHEN o.urgence = 1 THEN 1 ELSE 0 END) as operations_urgentes
FROM clients c
LEFT JOIN conteneurs ct ON c.client_id = ct.client_proprietaire
LEFT JOIN operations o ON ct.conteneur_id = o.conteneur_id
GROUP BY c.client_id, c.nom, c.segment, c.contrat_type
ORDER BY total_operations DESC
''')

# Vue 4 : Analyse temporelle (heures de pointe)
cursor.execute('''
CREATE VIEW IF NOT EXISTS vue_analyse_horaire AS
SELECT 
    strftime('%H', timestamp) as heure,
    COUNT(*) as nb_operations,
    AVG(duree_minutes) as duree_moyenne,
    SUM(CASE WHEN urgence = 1 THEN 1 ELSE 0 END) as urgences,
    COUNT(DISTINCT engin) as engins_actifs
FROM operations
GROUP BY strftime('%H', timestamp)
ORDER BY heure
''')

print("✅ 4 vues analytiques créées")

# 4. Exécution de requêtes de test
print("\n4. Tests de requêtes...")

test_queries = [
    ("Nombre total d'opérations", "SELECT COUNT(*) FROM operations"),
    ("Opérations des 7 derniers jours", """
     SELECT DATE(timestamp) as jour, COUNT(*) as nb_ops
     FROM operations 
     WHERE timestamp >= DATE('now', '-7 days')
     GROUP BY DATE(timestamp)
     ORDER BY jour DESC
     """),
    ("Top 5 zones les plus actives", """
     SELECT zone, COUNT(*) as nb_operations, AVG(duree_minutes) as duree_moyenne
     FROM operations
     GROUP BY zone
     ORDER BY nb_operations DESC
     LIMIT 5
     """),
    ("Taux d'erreur global", """
     SELECT 
         ROUND(100.0 * SUM(CASE WHEN erreur = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as taux_erreur_pourcent
     FROM operations
     """)
]

for nom, query in test_queries:
    result = pd.read_sql_query(query, conn)
    print(f"\n📊 {nom}:")
    print(result.to_string(index=False))

# 5. Sauvegarde des métadonnées
print("\n5. Sauvegarde des métadonnées...")
metadata = {
    "database_created": datetime.now().isoformat(),
    "tables": list(tables_data.keys()),
    "total_operations": int(pd.read_sql_query("SELECT COUNT(*) FROM operations", conn).iloc[0,0]),  # ← CONVERTIR en int
    "total_clients": int(pd.read_sql_query("SELECT COUNT(*) FROM clients", conn).iloc[0,0]),        # ← CONVERTIR
    "total_containers": int(pd.read_sql_query("SELECT COUNT(*) FROM conteneurs", conn).iloc[0,0]), # ← CONVERTIR
    "date_range": {
        "min": str(pd.read_sql_query("SELECT MIN(timestamp) FROM operations", conn).iloc[0,0]),    # ← CONVERTIR en str
        "max": str(pd.read_sql_query("SELECT MAX(timestamp) FROM operations", conn).iloc[0,0])     # ← CONVERTIR
    }
}
with open('data/processed/database_metadata.json', 'w') as f:
    json.dump(metadata, f, indent=2)

print(f"📁 Métadonnées sauvegardées")

# Fermeture de la connexion
conn.close()

print("\n" + "="*50)
print("✅ BASE DE DONNÉES CRÉÉE AVEC SUCCÈS")
print("="*50)
print("📍 Emplacement: data/processed/portsec.db")
print("📊 Taille: ", end="")
import os
print(f"{os.path.getsize('data/processed/portsec.db') / 1024 / 1024:.1f} MB")
print("🗃️  Tables: clients, conteneurs, operations, meteo")
print("👁️  Vues: vue_operations_journalieres, vue_performance_engins, etc.")
print("="*50)