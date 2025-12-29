import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sqlite3
import json

# Configuration de la page
st.set_page_config(
    page_title="Port Sec Intelligence",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        font-weight: bold;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #374151;
        font-weight: 600;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #F3F4F6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #3B82F6;
    }
    .alert-card {
        background-color: #FEF3C7;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #F59E0B;
    }
</style>
""", unsafe_allow_html=True)

# Titre principal
st.markdown('<h1 class="main-header">🚢 PORT SEC INTELLIGENT PLATFORM</h1>', unsafe_allow_html=True)
st.markdown("### Tableau de Bord Opérationnel Temps-Réel - **Prototype**")

# Sidebar avec contrôles
with st.sidebar:
    st.header("🔧 Contrôles et Filtres")
    
    # Chargement des métadonnées
    with open('data/processed/database_metadata.json', 'r') as f:
        metadata = json.load(f)
    
    date_min = datetime.fromisoformat(metadata['date_range']['min'])
    date_max = datetime.fromisoformat(metadata['date_range']['max'])
    
    # Sélecteur de période
    selected_period = st.selectbox(
        "Période d'analyse",
        ["7 derniers jours", "30 derniers jours", "3 derniers mois", "Personnalisée"]
    )
    
    if selected_period == "Personnalisée":
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Date début", value=date_min)
        with col2:
            end_date = st.date_input("Date fin", value=date_max)
    else:
        if selected_period == "7 derniers jours":
            start_date = date_max - timedelta(days=7)
        elif selected_period == "30 derniers jours":
            start_date = date_max - timedelta(days=30)
        else:  # 3 derniers mois
            start_date = date_max - timedelta(days=90)
        end_date = date_max
    
    # Filtres supplémentaires
    st.subheader("Filtres Avancés")
    show_urgent = st.checkbox("Afficher seulement les urgences", value=False)
    show_errors = st.checkbox("Afficher les erreurs", value=True)
    
    # Bouton de rafraîchissement
    if st.button("🔄 Rafraîchir les données"):
        st.rerun()

# Connexion à la base de données
@st.cache_data(ttl=300)  # Cache pour 5 minutes
def load_data(start_date, end_date):
    conn = sqlite3.connect('data/processed/portsec.db')
    
    # Données agrégées
    query = f"""
    SELECT * FROM vue_operations_journalieres 
    WHERE date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
    """
    daily_data = pd.read_sql_query(query, conn)
    daily_data['date'] = pd.to_datetime(daily_data['date'], format='ISO8601')  # CORRIGÉ ICI
    
    # Performance engins
    engins_data = pd.read_sql_query("SELECT * FROM vue_performance_engins", conn)
    
    # Analyse horaire
    hourly_data = pd.read_sql_query("SELECT * FROM vue_analyse_horaire", conn)
    
    # Dernières opérations
    recent_ops = pd.read_sql_query(f"""
    SELECT 
        timestamp, 
        type_operation, 
        zone, 
        engin,
        duree_minutes,
        urgence,
        erreur
    FROM operations 
    WHERE timestamp BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY timestamp DESC 
    LIMIT 100
    """, conn)
    
    conn.close()
    
    return daily_data, engins_data, hourly_data, recent_ops

# Chargement des données
daily_data, engins_data, hourly_data, recent_ops = load_data(start_date, end_date)

# ===== SECTION 1 : KPIs PRINCIPAUX =====
st.markdown('<h2 class="sub-header">📊 Tableau de Bord Exécutif</h2>', unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_ops = daily_data['nb_operations'].sum()
    prev_period_ops = total_ops * 0.88  # Simulé
    delta = ((total_ops - prev_period_ops) / prev_period_ops * 100) if prev_period_ops > 0 else 0
    st.metric(
        label="📦 Opérations Total",
        value=f"{total_ops:,}",
        delta=f"{delta:+.1f}%"
    )

with col2:
    avg_duration = daily_data['duree_moyenne'].mean()
    prev_duration = avg_duration * 1.05  # Simulé
    delta_duration = ((prev_duration - avg_duration) / prev_duration * 100)
    st.metric(
        label="⏱️ Durée Moyenne",
        value=f"{avg_duration:.1f} min",
        delta=f"-{delta_duration:.1f}%" if delta_duration > 0 else f"+{abs(delta_duration):.1f}%"
    )

with col3:
    urgent_rate = (daily_data['urgences'].sum() / daily_data['nb_operations'].sum() * 100) if daily_data['nb_operations'].sum() > 0 else 0
    st.metric(
        label="⚠️ Taux d'Urgence",
        value=f"{urgent_rate:.1f}%",
        delta="+0.5%" if urgent_rate > 3 else "-0.3%"
    )

with col4:
    error_rate = (daily_data['erreurs'].sum() / daily_data['nb_operations'].sum() * 100) if daily_data['nb_operations'].sum() > 0 else 0
    st.metric(
        label="❌ Taux d'Erreur",
        value=f"{error_rate:.1f}%",
        delta="-0.8%" if error_rate < 2 else "+0.4%"
    )

# ===== SECTION 2 : GRAPHIQUES PRINCIPAUX =====
col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 📈 Activité Journalière")
    
    # Préparation des données
    fig_daily = go.Figure()
    
    # Barres pour le nombre d'opérations
    fig_daily.add_trace(go.Bar(
        x=daily_data['date'],
        y=daily_data['nb_operations'],
        name='Opérations',
        marker_color='#3B82F6',
        hovertemplate='%{x|%d/%m}<br>%{y:,} ops<extra></extra>'
    ))
    
    # Ligne pour la durée moyenne
    fig_daily.add_trace(go.Scatter(
        x=daily_data['date'],
        y=daily_data['duree_moyenne'],
        name='Durée moyenne (min)',
        yaxis='y2',
        line=dict(color='#EF4444', width=2),
        hovertemplate='%{x|%d/%m}<br>%{y:.1f} min<extra></extra>'
    ))
    
    # Configuration du graphique
    fig_daily.update_layout(
        hovermode='x unified',
        yaxis=dict(title='Nombre d\'opérations'),
        yaxis2=dict(
            title='Durée (min)',
            overlaying='y',
            side='right'
        ),
        height=400,
        showlegend=True,
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )
    
    st.plotly_chart(fig_daily, use_container_width=True)

with col2:
    st.markdown("#### 🕒 Distribution Horaire")
    
    fig_hourly = go.Figure()
    
    fig_hourly.add_trace(go.Bar(
        x=hourly_data['heure'],
        y=hourly_data['nb_operations'],
        name='Opérations',
        marker_color='#10B981',
        hovertemplate='%{x}h<br>%{y:,} ops<extra></extra>'
    ))
    
    fig_hourly.update_layout(
        title="Heures de pointe",
        xaxis=dict(title='Heure de la journée'),
        yaxis=dict(title='Nombre d\'opérations'),
        height=400,
        showlegend=False
    )
    
    # Ajout d'une ligne pour la moyenne
    avg_ops = hourly_data['nb_operations'].mean()
    fig_hourly.add_hline(
        y=avg_ops,
        line_dash="dash",
        line_color="gray",
        annotation_text=f"Moyenne: {avg_ops:.0f} ops/h",
        annotation_position="top right"
    )
    
    st.plotly_chart(fig_hourly, use_container_width=True)

# ===== SECTION 3 : PERFORMANCE DES ENGINS =====
st.markdown('<h2 class="sub-header">🏗️ Performance des Équipements</h2>', unsafe_allow_html=True)

# Top 10 engins par productivité
top_engins = engins_data.nlargest(10, 'total_operations')

col1, col2 = st.columns([2, 1])

with col1:
    fig_engins = go.Figure()
    
    fig_engins.add_trace(go.Bar(
        y=top_engins['engin'],
        x=top_engins['total_operations'],
        orientation='h',
        name='Opérations',
        marker_color='#8B5CF6',
        hovertemplate='%{y}<br>%{x:,} ops<extra></extra>'
    ))
    
    fig_engins.update_layout(
        title="Top 10 Engins par Volume d'Opérations",
        xaxis=dict(title='Nombre d\'opérations'),
        yaxis=dict(title='Engin', autorange='reversed'),
        height=400
    )
    
    st.plotly_chart(fig_engins, use_container_width=True)

with col2:
    st.markdown("#### 📋 Engins à Surveiller")
    
    # Engins avec erreurs
    engins_with_errors = engins_data[engins_data['erreurs'] > 0].sort_values('erreurs', ascending=False)
    
    if not engins_with_errors.empty:
        for _, row in engins_with_errors.head(3).iterrows():
            error_rate = (row['erreurs'] / row['total_operations'] * 100)
            st.markdown(f"""
            <div class="alert-card">
                <strong>{row['engin']}</strong><br>
                {row['erreurs']} erreurs ({error_rate:.1f}%)<br>
                <small>{row['total_operations']} ops totales</small>
            </div>
            """, unsafe_allow_html=True)
            st.write("")
    else:
        st.info("✅ Aucun engin avec erreurs détecté")

# ===== SECTION 4 : ALERTES ET DERNIÈRES OPÉRATIONS =====
st.markdown('<h2 class="sub-header">🚨 Alertes et Activité Récente</h2>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("##### ⚠️ Alertes Actives")
    
    # Détection d'anomalies simples
    today_avg = daily_data['nb_operations'].mean()
    today_std = daily_data['nb_operations'].std()
    
    latest_day = daily_data.iloc[-1] if len(daily_data) > 0 else None
    
    if latest_day is not None:
        alerts = []
        
        # Vérification du volume
        if latest_day['nb_operations'] > today_avg + 2*today_std:
            alerts.append(f"📈 **Volume anormalement haut** ({latest_day['nb_operations']} ops)")
        
        # Vérification du taux d'erreur
        error_rate_day = (latest_day['erreurs'] / latest_day['nb_operations'] * 100) if latest_day['nb_operations'] > 0 else 0
        if error_rate_day > 5:
            alerts.append(f"❌ **Taux d'erreur élevé** ({error_rate_day:.1f}%)")
        
        # Vérification des urgences
        urgent_rate_day = (latest_day['urgences'] / latest_day['nb_operations'] * 100) if latest_day['nb_operations'] > 0 else 0
        if urgent_rate_day > 10:
            alerts.append(f"⚠️ **Nombre d'urgences élevé** ({urgent_rate_day:.1f}%)")
        
        if alerts:
            for alert in alerts:
                st.warning(alert)
        else:
            st.success("✅ Aucune alerte active")
    else:
        st.info("📊 Données insuffisantes pour générer des alertes")

with col2:
    st.markdown("##### 📝 Dernières Opérations")
    
    # Affichage des dernières opérations
    if not recent_ops.empty:
        recent_ops_display = recent_ops.head(10).copy()
        recent_ops_display['timestamp'] = pd.to_datetime(recent_ops_display['timestamp'], format='mixed')
        recent_ops_display['timestamp'] = recent_ops_display['timestamp'].dt.strftime('%H:%M')
        
        # Ajout d'icônes pour les urgences/erreurs
        def format_row(row):
            icons = ""
            if row['urgence']:
                icons += "⚠️ "
            if row['erreur']:
                icons += "❌ "
            return f"{icons}{row['type_operation']} | {row['zone']} | {row['duree_minutes']:.0f}min"
        
        recent_ops_display['affichage'] = recent_ops_display.apply(format_row, axis=1)
        
        for _, row in recent_ops_display.iterrows():
            st.text(f"{row['timestamp']} - {row['affichage']}")
    else:
        st.info("Aucune opération récente")

# ===== SECTION 5 : RECOMMANDATIONS INTELLIGENTES =====
st.markdown('<h2 class="sub-header">💡 Recommandations Intelligentes</h2>', unsafe_allow_html=True)

# Analyse pour générer des recommandations
recommendations = []

# 1. Analyse des heures de pointe
peak_hour = hourly_data.loc[hourly_data['nb_operations'].idxmax()]
if peak_hour['nb_operations'] > hourly_data['nb_operations'].mean() * 1.5:
    recommendations.append(
        f"**Optimisation des ressources à {int(peak_hour['heure'])}h** : "
        f"Prévoir +20% d'engins pendant cette heure de pointe "
        f"({peak_hour['nb_operations']} ops/h vs moyenne de {hourly_data['nb_operations'].mean():.0f} ops/h)"
    )

# 2. Analyse des engins sous-performants
if not engins_with_errors.empty:
    worst_engin = engins_with_errors.iloc[0]
    recommendations.append(
        f"**Maintenance préventive pour {worst_engin['engin']}** : "
        f"{worst_engin['erreurs']} erreurs détectées "
        f"({worst_engin['erreurs']/worst_engin['total_operations']*100:.1f}% des opérations)"
    )

# 3. Analyse des zones
zone_stats = recent_ops.groupby('zone').agg({
    'duree_minutes': 'mean',
    'urgence': 'sum'
}).reset_index()

if not zone_stats.empty:
    slowest_zone = zone_stats.loc[zone_stats['duree_minutes'].idxmax()]
    if slowest_zone['duree_minutes'] > 40:  # Seuil de 40 minutes
        recommendations.append(
            f"**Optimisation de la zone {slowest_zone['zone']}** : "
            f"Durée moyenne de {slowest_zone['duree_minutes']:.1f} minutes "
            f"(contre moyenne de {zone_stats['duree_minutes'].mean():.1f} minutes)"
        )

# Affichage des recommandations
if recommendations:
    for i, rec in enumerate(recommendations, 1):
        st.markdown(f"{i}. {rec}")
else:
    st.info("Aucune recommandation spécifique pour le moment")

# ===== FOOTER =====
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #6B7280; font-size: 0.9rem;">
    <strong>Port Sec Intelligence Platform</strong> - Prototype de démonstration<br>
    Données simulées • Dernière mise à jour : {date}<br>
    <small>Ce dashboard montre le potentiel d'une plateforme data complète pour un port sec</small>
</div>
""".format(date=datetime.now().strftime("%d/%m/%Y %H:%M")), unsafe_allow_html=True)

# ===== BOUTON DE DÉMO AVANCÉE =====
if st.sidebar.button("🎬 Mode Démonstration Avancée", type="primary"):
    st.sidebar.success("Mode démo activé !")
    
    # Simulation d'une alerte en direct
    with st.spinner("Simulation d'une alerte en temps réel..."):
        import time
        placeholder = st.empty()
        
        for i in range(3):
            with placeholder.container():
                st.error(f"🚨 **ALERTE TEMPS-RÉEL** : Opération urgente détectée à {datetime.now().strftime('%H:%M:%S')}")
                st.info("Conteneur: CONT999999 | Type: DÉCONSOLIDATION URGENTE | Zone: QUAI_1")
                time.sleep(2)
        
        st.success("✅ Simulation terminée")