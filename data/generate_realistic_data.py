import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Créer les dossiers
os.makedirs('data/external', exist_ok=True)
os.makedirs('data/raw', exist_ok=True)
os.makedirs('data/processed', exist_ok=True)

# Configuration réaliste d'un port sec moyen
np.random.seed(42)  # Pour la reproductibilité
print("🚢 Génération de données réalistes pour port sec (2026)...")

# 1. Clients réalistes (50 clients types)
clients_data = []
segments = ['LOGISTICIEN', 'CHARGEUR', 'DISTRIBUTEUR', 'INDUSTRIEL']
for i in range(1, 51):
    clients_data.append({
        'client_id': f'CLIENT{i:03d}',
        'nom': f'{"SARL" if i % 3 == 0 else "SA"} {"LOGISTIQUE" if i % 2 == 0 else "TRANSPORT"} {chr(65 + (i % 26))}',
        'segment': segments[i % len(segments)],
        'contrat_type': 'PREMIUM' if i < 15 else 'STANDARD',
        'date_premier_contrat': datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365)),
        'ca_annuel_ke': np.random.lognormal(8, 1.2)  # CA en k$
    })

clients_df = pd.DataFrame(clients_data)
clients_df.to_csv('data/external/clients.csv', index=False)
print(f"✅ Clients générés: {len(clients_df)}")

# 2. Conteneurs (5000 conteneurs réalistes)
conteneurs_data = []
tailles = ['20"', '40"', '45"']
types_conteneur = ['DRY', 'REEFER', 'OPENTOP', 'FLATRACK', 'TANK']

for i in range(1, 5001):
    conteneurs_data.append({
        'conteneur_id': f'CONT{i:06d}',
        'taille': np.random.choice(tailles, p=[0.4, 0.5, 0.1]),
        'type': np.random.choice(types_conteneur, p=[0.65, 0.1, 0.15, 0.05, 0.05]),
        'client_proprietaire': f'CLIENT{random.randint(1, 50):03d}',
        'client_locataire': f'CLIENT{random.randint(1, 50):03d}' if random.random() > 0.7 else None,
        'poids_brut_kg': np.random.normal(25000, 5000) if random.random() > 0.3 else np.random.normal(12000, 3000),
        'statut_douane': np.random.choice(['LIBERE', 'EN_ATTENTE', 'BLOQUE'], p=[0.8, 0.15, 0.05])
    })

conteneurs_df = pd.DataFrame(conteneurs_data)
conteneurs_df.to_csv('data/external/conteneurs.csv', index=False)
print(f"✅ Conteneurs générés: {len(conteneurs_df)}")

# 3. Opérations journalières (le cœur du système) - ANNÉE 2026
print("⏳ Génération des opérations 2026 (cela peut prendre 30 secondes)...")

operations_data = []
date_debut = datetime(2026, 1, 1)  # ⭐ CHANGÉ EN 2026
date_fin = datetime(2026, 3, 31)   # ⭐ CHANGÉ EN 2026
current_date = date_debut

# Zones réalistes d'un port sec
zones = ['ZONE_A_STOCKAGE', 'ZONE_B_STOCKAGE', 'ZONE_C_DANGEREUX', 
         'QUAI_1_FERROVIAIRE', 'QUAI_2_ROUTIER', 'QUAI_3_FLUVIAL',
         'AIRE_DECONSOLIDATION', 'AIRE_REGROUPEMENT']

# Engins
engins = [f'GERBEUR_{i:02d}' for i in range(1, 13)] + \
         [f'PORTIQUE_{i:02d}' for i in range(1, 5)] + \
         [f'TRACTEUR_{i:02d}' for i in range(1, 8)]

# Types d'opérations avec probabilités réalistes
operations_types = {
    'DEBARQUEMENT_TRAIN': 0.25,
    'CHARGEMENT_CAMION': 0.20,
    'DEBARQUEMENT_CAMION': 0.15,
    'TRANSFERT_ZONE': 0.18,
    'INSPECTION_DOUANE': 0.08,
    'DECONSOLIDATION': 0.07,
    'REGROUPEMENT': 0.05,
    'MAINTENANCE': 0.02
}

operation_id = 1
while current_date <= date_fin:
    # Nombre d'opérations varie selon jour de la semaine
    if current_date.weekday() in [0, 1, 2]:  # Lundi-mardi-mercredi
        nb_operations = np.random.poisson(180)
    elif current_date.weekday() == 4:  # Vendredi
        nb_operations = np.random.poisson(160)
    else:  # Jeudi, weekend réduit
        nb_operations = np.random.poisson(100)
    
    # Heures d'ouverture : 6h-22h
    for _ in range(nb_operations):
        heure = np.random.normal(14, 4)  # Pic à 14h
        heure = max(6, min(22, heure))  # Borné entre 6h et 22h
        
        operation_type = np.random.choice(
            list(operations_types.keys()),
            p=list(operations_types.values())
        )
        
        # Durée réaliste selon type d'opération
        duree_params = {
            'DEBARQUEMENT_TRAIN': (45, 10),
            'CHARGEMENT_CAMION': (25, 8),
            'DEBARQUEMENT_CAMION': (20, 7),
            'TRANSFERT_ZONE': (15, 5),
            'INSPECTION_DOUANE': (60, 20),
            'DECONSOLIDATION': (120, 30),
            'REGROUPEMENT': (90, 25),
            'MAINTENANCE': (180, 45)
        }
        
        duree_moyenne, duree_std = duree_params[operation_type]
        duree = max(5, np.random.normal(duree_moyenne, duree_std))
        
        # Ajout des microsecondes réalistes
        microsecondes = random.randint(0, 999999)
        
        operations_data.append({
            'operation_id': f'OP{operation_id:06d}',
            'timestamp': (current_date + timedelta(hours=heure)).replace(microsecond=microsecondes),
            'conteneur_id': f'CONT{random.randint(1, 5000):06d}',
            'type_operation': operation_type,
            'zone': random.choice(zones),
            'engin': random.choice(engins),
            'duree_minutes': round(duree, 1),
            'operateur': f'OPE{random.randint(1, 25):03d}',
            'urgence': random.random() < 0.03,  # 3% d'urgences
            'erreur': random.random() < 0.02,   # 2% d'erreurs
            'completed': random.random() < 0.98  # 98% complétées
        })
        operation_id += 1
    
    # Progression
    if (current_date - date_debut).days % 10 == 0:
        print(f"  → Jour {(current_date - date_debut).days + 1}/90 traité")
    
    current_date += timedelta(days=1)

# Création du DataFrame et sauvegarde
operations_df = pd.DataFrame(operations_data)
operations_df.to_csv('data/raw/operations.csv', index=False)
print(f"✅ Opérations 2026 générées: {len(operations_df):,}")

# 4. Données GPS des engins (simplifié)
print("📍 Génération des données GPS 2026...")
gps_data = []
for engin in engins:
    for jour in range(90):
        date = date_debut + timedelta(days=jour)
        # Position aléatoire dans le port (coordonnées fictives)
        lat = 43.5 + random.uniform(-0.01, 0.01)
        lon = 1.4 + random.uniform(-0.01, 0.01)
        
        gps_data.append({
            'timestamp': date + timedelta(hours=8),  # Position à 8h
            'engin': engin,
            'latitude': round(lat, 6),
            'longitude': round(lon, 6),
            'vitesse_kmh': random.uniform(0, 25),
            'statut': random.choice(['EN_SERVICE', 'EN_CHARGE', 'EN_ATTENTE', 'EN_PANNE'])
        })

gps_df = pd.DataFrame(gps_data)
gps_df.to_csv('data/raw/gps_engins.csv', index=False)
print(f"✅ Données GPS 2026 générées: {len(gps_df)}")

# 5. Données météo (impact réel sur les opérations)
print("☀️ Génération des données météo 2026...")
dates = pd.date_range(start='2026-01-01', end='2026-03-31', freq='D')
meteo_data = []
for date in dates:
    # Saisonnalité réaliste
    jour_annee = date.timetuple().tm_yday
    temperature_base = 10 + 10 * np.sin(2 * np.pi * (jour_annee - 80) / 365)
    
    meteo_data.append({
        'date': date.date(),
        'temperature_matin': round(temperature_base + random.uniform(-3, 3), 1),
        'temperature_aprem': round(temperature_base + random.uniform(0, 5), 1),
        'precipitation_mm': np.random.exponential(2) if random.random() < 0.3 else 0,
        'vent_kmh': np.random.weibull(1.5) * 20,
        'visibilite': random.choice(['EXCELLENTE', 'BONNE', 'MOYENNE', 'REDUITE']),
        'impact_operations': random.choice(['NORMAL', 'LEGER_RALENTI', 'RALENTI']) if random.random() < 0.15 else 'NORMAL'
    })

meteo_df = pd.DataFrame(meteo_data)
meteo_df.to_csv('data/external/meteo.csv', index=False)
print(f"✅ Données météo 2026 générées: {len(meteo_df)}")

print("\n" + "="*50)
print("🎉 GÉNÉRATION 2026 TERMINÉE !")
print("="*50)
print(f"📁 Données sauvegardées dans /data/")
print(f"📊 {len(operations_df):,} opérations générées")
print(f"📦 {len(conteneurs_df)} conteneurs")
print(f"👥 {len(clients_df)} clients")
print(f"📅 Période : {date_debut.date()} au {date_fin.date()}")
print("="*50)
