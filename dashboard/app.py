import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sqlite3
import json
import time
import numpy as np
import folium
from streamlit_folium import folium_static
import random
from pathlib import Path

# ========== 1. CONFIGURATION DE LA PAGE ==========
st.set_page_config(
    page_title="Port Sec Intelligent Platform",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== 2. STYLE CSS ==========
st.markdown("""
<style>
    /* Thème principal */
    .stApp {
        background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
    }
    
    /* Cartes métriques */
    .metric-card {
        background: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        border-left: 5px solid #1E3A8A;
        margin: 10px 0;
    }
    
    /* Alertes */
    .alert-card {
        background: #FEF2F2;
        border-left: 5px solid #DC2626;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    
    .success-card {
        background: #F0FDF4;
        border-left: 5px solid #10B981;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    
    /* Titres */
    .main-title {
        color: #1E3A8A;
        font-size: 2.5rem;
        font-weight: 800;
        margin-bottom: 0.5rem;
    }
    
    .section-title {
        color: #334155;
        font-size: 1.5rem;
        font-weight: 700;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e2e8f0;
    }
    
    /* Badges */
    .badge {
        padding: 3px 8px;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: 600;
        display: inline-block;
    }
    
    .badge-success { background: #10B981; color: white; }
    .badge-warning { background: #F59E0B; color: white; }
    .badge-danger { background: #EF4444; color: white; }
    .badge-info { background: #3B82F6; color: white; }
</style>
""", unsafe_allow_html=True)

# ========== 3. FONCTIONS DE DONNÉES ==========
def create_sample_data(start_date, end_date):
    """Crée des données simulées pour la démo"""
    
    # Dates pour la période
    dates = pd.date_range(start_date, end_date, freq='D')
    
    # Données journalières simulées
    base_ops = 120
    daily_data = pd.DataFrame({
        'date': dates,
        'nb_operations': [base_ops + random.randint(-20, 30) for _ in dates],
        'duree_moyenne': [45 + random.uniform(-5, 5) for _ in dates],
        'urgences': [random.randint(0, 5) for _ in dates],
        'erreurs': [random.randint(0, 3) for _ in dates]
    })
    
    # Performance des engins
    engins = ['TRACTEUR_01', 'TRACTEUR_02', 'TRACTEUR_03', 'CHARIOT_01', 'CHARIOT_02', 'GRUE_01']
    engins_data = pd.DataFrame({
        'engin': engins,
        'total_operations': [random.randint(80, 200) for _ in engins],
        'erreurs': [random.randint(0, 8) for _ in engins],
        'duree_moyenne': [random.uniform(40, 55) for _ in engins]
    })
    
    # Données horaires
    hours = list(range(6, 22))
    hourly_data = pd.DataFrame({
        'heure': hours,
        'nb_operations': [random.randint(5, 25) for _ in hours]
    })
    
    # Dernières opérations
    recent_ops = pd.DataFrame({
        'timestamp': [datetime.now() - timedelta(minutes=random.randint(1, 120)) for _ in range(20)],
        'type_operation': random.choices(['CHARGEMENT', 'DÉCHARGEMENT', 'VÉRIFICATION'], k=20),
        'zone': random.choices(['QUAI_1', 'QUAI_2_ROUTIER', 'ZONE_STOCKAGE', 'CONTROLE_DOUANE'], k=20),
        'engin': random.choices(engins, k=20),
        'duree_minutes': [random.uniform(10, 60) for _ in range(20)],
        'urgence': [random.choice([0, 1]) for _ in range(20)],
        'erreur': [random.choice([0, 1]) for _ in range(20)]
    })
    
    return daily_data, engins_data, hourly_data, recent_ops

def load_data(start_date, end_date):
    """Charge les données depuis SQLite ou crée des données simulées"""
    try:
        db_path = Path("data/processed/portsec.db")
        
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            
            # Chargement des vues
            daily_data = pd.read_sql_query(f"""
                SELECT * FROM vue_operations_journalieres 
                WHERE date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            """, conn)
            
            engins_data = pd.read_sql_query("SELECT * FROM vue_performance_engins", conn)
            hourly_data = pd.read_sql_query("SELECT * FROM vue_analyse_horaire", conn)
            
            recent_ops = pd.read_sql_query(f"""
                SELECT timestamp, type_operation, zone, engin, duree_minutes, urgence, erreur
                FROM operations 
                WHERE timestamp BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY timestamp DESC LIMIT 100
            """, conn)
            
            conn.close()
            
            # Conversion des dates
            if not daily_data.empty:
                daily_data['date'] = pd.to_datetime(daily_data['date'])
            
            return daily_data, engins_data, hourly_data, recent_ops
        else:
            # Fichier inexistant, on crée des données simulées
            return create_sample_data(start_date, end_date)
            
    except Exception as e:
        st.sidebar.warning(f"Base de données non disponible. Utilisation de données simulées.")
        return create_sample_data(start_date, end_date)

def create_realtime_map():
    """Crée une carte interactive simulée"""
    # Coordonnées de Kasumbalesa, RDC
    m = folium.Map(location=[-11.664, 27.482], zoom_start=15, control_scale=True)
    
    # Zones du port
    zones = {
        'QUAI_1': {'lat': -11.664, 'lon': 27.482, 'color': 'blue', 'icon': 'ship'},
        'QUAI_2_ROUTIER': {'lat': -11.663, 'lon': 27.483, 'color': 'green', 'icon': 'truck'},
        'ZONE_STOCKAGE': {'lat': -11.665, 'lon': 27.481, 'color': 'orange', 'icon': 'boxes'},
        'CONTROLE_DOUANE': {'lat': -11.662, 'lon': 27.484, 'color': 'red', 'icon': 'shield-alt'},
        'MAINTENANCE': {'lat': -11.666, 'lon': 27.485, 'color': 'gray', 'icon': 'tools'}
    }
    
    # Ajout des marqueurs
    for zone, info in zones.items():
        folium.Marker(
            location=[info['lat'], info['lon']],
            popup=f'<b>{zone}</b><br>Statut: Normal<br>Activité: Élevée',
            tooltip=zone,
            icon=folium.Icon(color=info['color'], icon=info['icon'], prefix='fa')
        ).add_to(m)
    
    # Ajout d'un périmètre du port
    port_perimeter = [
        [-11.666, 27.480],
        [-11.661, 27.480],
        [-11.661, 27.486],
        [-11.666, 27.486]
    ]
    folium.Polygon(
        locations=port_perimeter,
        color='#1E3A8A',
        fill=True,
        fill_color='#1E3A8A',
        fill_opacity=0.1,
        weight=2,
        popup='Périmètre du Port Sec'
    ).add_to(m)
    
    return m

# ========== 4. SIDEBAR ==========
with st.sidebar:
    st.markdown("### 🎯 **PORT SEC INTELLIGENT**")
    st.markdown("---")
    
    # Bouton démo
    if st.button("🚀 **Lancer la démonstration complète**", type="primary", use_container_width=True):
        st.session_state.demo_launched = True
        st.rerun()
    
    st.markdown("---")
    st.markdown("### 📅 **PÉRIODE D'ANALYSE**")
    
    # Période par défaut
    default_end = datetime.now()
    default_start = default_end - timedelta(days=30)
    
    selected_period = st.selectbox(
        "Sélectionnez la période",
        ["30 derniers jours", "7 derniers jours", "3 derniers mois", "Personnalisée"]
    )
    
    if selected_period == "Personnalisée":
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Date début", value=default_start)
        with col2:
            end_date = st.date_input("Date fin", value=default_end)
    else:
        if selected_period == "7 derniers jours":
            start_date = default_end - timedelta(days=7)
        elif selected_period == "30 derniers jours":
            start_date = default_end - timedelta(days=30)
        else:  # 3 derniers mois
            start_date = default_end - timedelta(days=90)
        end_date = default_end
    
    start_date = datetime.combine(start_date, datetime.min.time())
    end_date = datetime.combine(end_date, datetime.max.time())
    
    st.markdown("---")
    st.markdown("### 🔧 **FILTRES**")
    
    show_errors = st.checkbox("Afficher les erreurs", value=True)
    show_alerts = st.checkbox("Afficher les alertes", value=True)
    auto_refresh = st.checkbox("Actualisation automatique", value=False)
    
    if st.button("🔄 Actualiser les données", use_container_width=True):
        st.rerun()
    
    st.markdown("---")
    st.markdown("#### 📊 **INFORMATIONS**")
    st.markdown("**Version:** 1.0.0")
    st.markdown("**Statut:** Prototype")
    st.markdown("**Données:** Simulées 2026")
    st.markdown("**Développeur:** ELIE KAYOMB MBUMB")

# ========== 5. CHARGEMENT DES DONNÉES ==========
with st.spinner("Chargement des données..."):
    daily_data, engins_data, hourly_data, recent_ops = load_data(start_date, end_date)

# Initialisation de session pour la démo
if 'demo_launched' not in st.session_state:
    st.session_state.demo_launched = False

# ========== 6. EN-TÊTE ==========
col1, col2 = st.columns([1, 5])
with col1:
   try:
        st.image("assets/logo.png", width=80)
   except:
        # Créer un logo simple avec Pillow
        from PIL import Image, ImageDraw
        import io
        
        img = Image.new('RGB', (80, 80), color='blue')
        d = ImageDraw.Draw(img)
        d.text((20, 35), "PSI", fill=(255, 255, 255))
        
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        
        st.image(buf, width=80)
  
with col2:
    st.markdown('<h1 class="main-title">PORT SEC INTELLIGENT PLATFORM</h1>', unsafe_allow_html=True)
    st.markdown("**Dashboard Opérationnel | Données Simulées 2026 | Kasumbalesa, RDC**")
st.markdown("---")

# Effet de démo si lancé
if st.session_state.demo_launched:
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(101):
        progress_bar.progress(i)
        status_text.text(f"Analyse des données en cours... {i}%")
        time.sleep(0.02)
    
    progress_bar.empty()
    status_text.empty()
    st.balloons()
    st.success("✅ **Démonstration terminée** - Données analysées avec succès")
    st.session_state.demo_launched = False

# ========== 7. KPIs PRINCIPAUX ==========
st.markdown('<h2 class="section-title">📊 SYNTHÈSE OPÉRATIONNELLE</h2>', unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_ops = daily_data['nb_operations'].sum() if not daily_data.empty else 0
    st.metric(
        label="📦 Opérations Total",
        value=f"{total_ops:,}",
        delta=f"+{int(total_ops * 0.136):,}" if total_ops > 0 else None
    )

with col2:
    avg_duration = daily_data['duree_moyenne'].mean() if not daily_data.empty else 0
    prev_duration = avg_duration * 1.05
    delta_pct = ((prev_duration - avg_duration) / prev_duration * 100) if prev_duration > 0 else 0
    st.metric(
        label="⏱️ Durée Moyenne",
        value=f"{avg_duration:.1f} min",
        delta=f"-{delta_pct:.1f}%" if delta_pct > 0 else None
    )

with col3:
    if not daily_data.empty and daily_data['nb_operations'].sum() > 0:
        error_rate = (daily_data['erreurs'].sum() / daily_data['nb_operations'].sum() * 100)
    else:
        error_rate = 0
    st.metric(
        label="❌ Taux d'Erreur",
        value=f"{error_rate:.1f}%",
        delta="-0.8%" if error_rate < 2.5 else None,
        delta_color="normal" if error_rate < 2.5 else "inverse"
    )

with col4:
    # Calcul des économies potentielles
    potential_savings = total_ops * 25 * 0.044  # 4.4% d'erreurs évitées à 25$ par erreur
    st.metric(
        label="💰 Économies Potentielles",
        value=f"${potential_savings:,.0f}",
        delta=f"${potential_savings/12:,.0f}/mois"
    )

st.markdown("---")

# ========== 8. VISUALISATIONS ==========
st.markdown('<h2 class="section-title">📈 ANALYSE DES PERFORMANCES</h2>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 📊 Activité Journalière")
    if not daily_data.empty:
        fig1 = go.Figure()
        fig1.add_trace(go.Bar(
            x=daily_data['date'],
            y=daily_data['nb_operations'],
            name='Opérations',
            marker_color='#3B82F6'
        ))
        
        # LIGNE ROUGE - DURÉE MOYENNE
        if 'duree_moyenne' in daily_data.columns:
            fig1.add_trace(go.Scatter(
                x=daily_data['date'],
                y=daily_data['duree_moyenne'],
                name='Durée moyenne',
                yaxis='y2',
                line=dict(color='#EF4444', width=2),
                mode='lines'
            ))
            
            fig1.update_layout(
                yaxis2=dict(
                    title='Durée (min)',
                    overlaying='y',
                    side='right',
                    showgrid=False,
                    title_font=dict(color='#EF4444'),
                    tickfont=dict(color='#EF4444')
                )
            )
        
        fig1.update_layout(
            xaxis_title="Date",
            yaxis_title="Nombre d'opérations",
            height=400,
            hovermode='x unified',
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
        )
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("Aucune donnée disponible pour la période sélectionnée")

with col2:
    st.markdown("#### 🕒 Distribution Horaire")
    if not hourly_data.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=hourly_data['heure'],
            y=hourly_data['nb_operations'],
            marker_color='#10B981',
            name='Opérations'
        ))
        fig2.update_layout(
            xaxis_title="Heure de la journée",
            yaxis_title="Nombre d'opérations",
            height=400
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Aucune donnée horaire disponible")

# ========== 9. PERFORMANCE DES ÉQUIPEMENTS ==========
st.markdown('<h2 class="section-title">🏗️ PERFORMANCE DES ÉQUIPEMENTS</h2>', unsafe_allow_html=True)

col1, col2 = st.columns([2, 1])

with col1:
    if not engins_data.empty:
        # Top 10 engins par volume
        top_engins = engins_data.nlargest(10, 'total_operations')
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            y=top_engins['engin'],
            x=top_engins['total_operations'],
            orientation='h',
            marker_color='#8B5CF6',
            name='Opérations'
        ))
        fig3.update_layout(
            title="Top 10 Engins par Volume d'Opérations",
            xaxis_title="Nombre d'opérations",
            yaxis_title="Engin",
            height=400
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("Aucune donnée d'équipement disponible")

with col2:
    st.markdown("#### ⚠️ Engins à Surveiller")
    if not engins_data.empty:
        # Calcul du taux d'erreur par engin
        engins_data['taux_erreur'] = (engins_data['erreurs'] / engins_data['total_operations'] * 100)
        problem_engins = engins_data[engins_data['taux_erreur'] > 1.5]
        
        if not problem_engins.empty:
            for _, engin in problem_engins.iterrows():
                error_class = "badge-danger" if engin['taux_erreur'] > 3 else "badge-warning"
                st.markdown(f"""
                <div class="metric-card">
                    <strong>{engin['engin']}</strong><br>
                    <span class="{error_class}">{engin['erreurs']} erreurs ({engin['taux_erreur']:.1f}%)</span><br>
                    <small>{engin['total_operations']} opérations</small>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="success-card">
                ✅ Tous les engins fonctionnent normalement
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Aucun engin problématique détecté")

# ========== 10. CARTE INTERACTIVE ==========
st.markdown('<h2 class="section-title">🗺️ CARTE TEMPS-RÉEL DU PORT</h2>', unsafe_allow_html=True)

col1, col2 = st.columns([3, 1])

with col1:
    # Création et affichage de la carte
    port_map = create_realtime_map()
    folium_static(port_map, width=800, height=500)

with col2:
    st.markdown("#### 🔍 FILTRES")
    st.multiselect(
        "Types d'engins",
        ["Tracteur", "Chariot", "Grue", "Camion"],
        default=["Tracteur", "Chariot"]
    )
    
    refresh_rate = st.slider("Rafraîchissement (secondes)", 5, 60, 30)
    
    st.checkbox("Afficher les trajets", value=True)
    st.checkbox("Afficher les zones congestion", value=True)
    st.checkbox("Afficher les alertes sur carte", value=True)
    
    st.markdown("---")
    st.markdown("#### 🎯 LÉGENDE")
    st.markdown("🔵 **Quai Principal**")
    st.markdown("🟢 **Quai Routier**")
    st.markdown("🟠 **Zone Stockage**")
    st.markdown("🔴 **Contrôle Douane**")
    st.markdown("⚫ **Maintenance**")

# ========== 11. ALERTES ET ACTIVITÉ ==========
st.markdown('<h2 class="section-title">🚨 ALERTES ET ACTIVITÉ EN TEMPS RÉEL</h2>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### ⚠️ ALERTES ACTIVES")
    
    # Génération d'alertes simulées
    alerts = []
    
    if not daily_data.empty and len(daily_data) > 1:
        latest_day = daily_data.iloc[-1]
        avg_operations = daily_data['nb_operations'].mean()
        
        if latest_day['nb_operations'] > avg_operations * 1.3:
            alerts.append("📈 **Volume anormalement élevé** - Augmentation de +30%")
        
        if latest_day['erreurs'] > 0 and (latest_day['erreurs'] / latest_day['nb_operations']) > 0.03:
            alerts.append("❌ **Taux d'erreur critique** - Supérieur à 3%")
    
    # Alertes prédéfinies pour la démo
    alerts.append("⚠️ **Maintenance préventive requise** - TRACTEUR_06 (taux erreur: 3.7%)")
    alerts.append("🚀 **Opportunité d'optimisation** - QUAI_2_ROUTIER (-27min possible)")
    
    if alerts:
        for alert in alerts:
            if "❌" in alert or "⚠️" in alert:
                st.markdown(f'<div class="alert-card">{alert}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="success-card">{alert}</div>', unsafe_allow_html=True)
    else:
        st.info("✅ Aucune alerte active")

with col2:
    st.markdown("#### 📝 DERNIÈRES OPÉRATIONS")
    
    if not recent_ops.empty:
        # Affichage des 10 dernières opérations
        recent_ops_display = recent_ops.head(10).copy()
        
        for idx, row in recent_ops_display.iterrows():
            timestamp_str = row['timestamp'].strftime('%H:%M') if isinstance(row['timestamp'], datetime) else row['timestamp']
            
            icon = ""
            if row.get('urgence', 0):
                icon += "⚠️ "
            if row.get('erreur', 0):
                icon += "❌ "
            
            st.markdown(f"""
            **{timestamp_str}** - {icon}{row['type_operation']}  
            *{row['zone']}* | {row['engin']} | {row['duree_minutes']:.0f} min
            """)
    else:
        st.info("Aucune opération récente")

# ========== 12. RECOMMANDATIONS ==========
st.markdown('<h2 class="section-title">💡 RECOMMANDATIONS INTELLIGENTES</h2>', unsafe_allow_html=True)

recommendations = [
    "**Optimiser QUAI_2_ROUTIER** : Réorganisation peut réduire la durée moyenne de 27 minutes (-15%)",
    "**Maintenance TRACTEUR_06** : Planifier maintenance préventive (taux erreur: 3.7%)",
    "**Équilibrage charge** : Déplacer 20% des opérations de 10h-12h vers 14h-16h",
    "**Formation équipe** : Session sur procédures chargement (erreurs réduisibles de 40%)",
    "**Investissement capteurs** : Ajouter 5 capteurs RFID pour tracking temps-réel"
]

for i, rec in enumerate(recommendations, 1):
    st.markdown(f"{i}. {rec}")
    # ========== 14. FOOTER ==========
st.markdown("---")
st.markdown(f"""
<div style="text-align: center; color: #6B7280; padding: 20px; font-size: 0.9rem;">
    <strong>PORT SEC INTELLIGENT PLATFORM</strong> - Prototype de Démonstration v1.0<br>
    Données simulées pour Kasumbalesa, RDC | Période: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}<br>
    <small>Ce dashboard démontre la valeur d'une plateforme data intelligence pour ports secs </small><br>
    <small>Dernière mise à jour: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</small>
</div>
""", unsafe_allow_html=True)


# ========== 15. AUTO-REFRESH ==========
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()