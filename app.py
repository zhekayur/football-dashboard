import streamlit as st
import awswrangler as wr
import pandas as pd
import boto3
import plotly.express as px
import os
from botocore.exceptions import ClientError, NoCredentialsError

# ----------------- Configuration -----------------
AWS_REGION = "eu-north-1"
DATABASE = "football_db"
TABLE = "live_portfolio_projected"
S3_OUTPUT = "s3://portfolio-lake-yevhen-3991/athena-results/"

st.set_page_config(
    page_title="Premier League Live Control Room",
    page_icon="⚽",
    layout="wide"
)

# -----------------------------------------------------------------------------
# AWS CREDENTIALS BRIDGE (Works on Local PC AND Streamlit Cloud)
# -----------------------------------------------------------------------------
try:
    # Attempt to load credentials from Streamlit Secrets (Cloud Environment)
    # We manually inject these into os.environ so boto3/awswrangler find them automatically.
    # Note: Streamlit secrets are dict-like, so we access them by key.
    os.environ["AWS_ACCESS_KEY_ID"] = st.secrets["aws"]["access_key_id"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = st.secrets["aws"]["secret_access_key"]
    os.environ["AWS_DEFAULT_REGION"] = st.secrets["aws"]["region"]
except (FileNotFoundError, KeyError):
    # If secrets are missing (Local Environment), we pass.
    # Boto3 will automatically fall back to ~/.aws/credentials or IAM Roles.
    pass
# -----------------------------------------------------------------------------

# ----------------- Data Fetching -----------------
@st.cache_data(ttl=600)
def get_data():
    try:
        session = boto3.Session(region_name=AWS_REGION)
        
        # SQL Query Logic with Deduplication and Data Validation
        query = f"""
        WITH RankedPlayers AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at DESC) as rank
            FROM "{DATABASE}"."{TABLE}" 
        )
        SELECT * FROM RankedPlayers WHERE rank = 1
        """
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=DATABASE,
            s3_output=S3_OUTPUT,
            boto3_session=session,
            ctas_approach=False
        )
        
        # Renaissance Refactor: Ensure compatibility if column names changed in projection
        if 'name' in df.columns and 'web_name' not in df.columns:
            df.rename(columns={'name': 'web_name'}, inplace=True)
            
        return df

    except NoCredentialsError:
        st.error("AWS Credentials not found. Please configure your AWS environment.")
        return None
    except Exception as e:
        st.error(f"Error connecting to AWS Athena: {e}")
        return None

# Load Data
df = get_data()

# ----------------- Dashboard Layout -----------------

# Sidebar Filters
with st.sidebar:
    st.header("Control Panel")
    
    if df is not None and not df.empty:
        st.subheader("Filters")
        
        # Team Filter
        teams = sorted(df['team'].unique().tolist())
        selected_teams = st.multiselect("Select Team(s)", options=teams, default=teams)
        
        # Player Filter (dependent on team selection)
        if selected_teams:
            available_players = sorted(df[df['team'].isin(selected_teams)]['web_name'].unique().tolist())
        else:
            available_players = sorted(df['web_name'].unique().tolist())
            
        selected_players = st.multiselect("Select Player(s)", options=available_players)
        
    st.markdown("---")
    st.markdown("**Data Source**:\nAWS Athena (Real-time)")

# Main Content
st.title("⚽ Premier League Live Control Room")

if df is not None and not df.empty:
    # ----------------- Apply Filters -----------------
    filtered_df = df.copy()
    
    # Filter by Team
    if selected_teams:
        filtered_df = filtered_df[filtered_df['team'].isin(selected_teams)]
    
    # Filter by Player
    if selected_players:
        filtered_df = filtered_df[filtered_df['web_name'].isin(selected_players)]
        
    if filtered_df.empty:
        st.warning("No data matches your filters.")
    else:
        # Use filtered_df for all downstream visualizations
        df = filtered_df 

        # Timestamp from the data
        last_updated = pd.to_datetime(df['ingested_at']).max()
        st.markdown(f"**Last Updated:** {last_updated.strftime('%Y-%m-%d %H:%M:%S')}")
        st.markdown("---")

    # Data Type Conversions
    numeric_cols = ['goals', 'assists', 'minutes', 'saves', 'clean_sheets',
                    'yellow_cards', 'red_cards', 'chance_of_playing', 'position_id']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Float conversions for advanced stats
    float_cols = ['form', 'influence', 'creativity', 'threat', 'ict_index']
    for col in float_cols:
         if col in df.columns:
             df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # Global KPI Row
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        st.metric("Total Players", len(df))
    with kpi2:
        st.metric("Total Goals", int(df['goals'].sum()))
    with kpi3:
        st.metric("Total Assists", int(df['assists'].sum()))
    with kpi4:
        active_injuries = df[df['injury_news'] != ''].shape[0]
        st.metric("Active Injuries", active_injuries)

    st.markdown("---")

    # TABS
    tab_attack, tab_defense, tab_creative, tab_injury = st.tabs([
        "⚽ Attack", "🛡️ Defense & Discipline", "🎨 Creativity & Impact", "🚑 Injuries"
    ])

    # --- TAB 1: ATTACK ---
    with tab_attack:
        st.subheader("Attacking Prowess")
        col_att1, col_att2 = st.columns(2)
        
        with col_att1:
            st.markdown("#### 🥅 Top 10 Goal Scorers")
            top_goals = df.sort_values('goals', ascending=False).head(10)
            fig_goals = px.bar(
                top_goals, x='goals', y='web_name', orientation='h', color='team',
                title="Most Goals", labels={'goals': 'Goals', 'web_name': ''}, height=400
            )
            fig_goals.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_goals, use_container_width=True)

        with col_att2:
            st.markdown("#### 🎯 Top 10 Assistants")
            top_assists = df.sort_values('assists', ascending=False).head(10)
            fig_assists = px.bar(
                top_assists, x='assists', y='web_name', orientation='h', color='team',
                title="Most Assists", labels={'assists': 'Assists', 'web_name': ''}, height=400
            )
            fig_assists.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_assists, use_container_width=True)
            
        st.markdown("#### 🔥 Threat Leaders")
        top_threat = df.sort_values('threat', ascending=False).head(15)
        fig_threat = px.bar(
            top_threat, x='web_name', y='threat', color='team',
            title="Highest Threat Index", labels={'threat': 'Threat', 'web_name': ''}
        )
        st.plotly_chart(fig_threat, use_container_width=True)

    # --- TAB 2: DEFENSE & DISCIPLINE ---
    with tab_defense:
        st.subheader("Defensive & Discipline Metrics")
        
        col_def1, col_def2 = st.columns(2)
        
        with col_def1:
            st.markdown("#### 🧤 Top Goalkeeper Saves")
            # Filter for players with saves > 0 (Likely GKs)
            saves_df = df[df['saves'] > 0].sort_values('saves', ascending=False).head(10)
            if not saves_df.empty:
                fig_saves = px.bar(
                    saves_df, x='saves', y='web_name', orientation='h', color='team',
                    title="Most Saves", labels={'saves': 'Saves', 'web_name': ''}, height=400
                )
                fig_saves.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_saves, use_container_width=True)
            else:
                st.info("No save data available.")

        with col_def2:
            st.markdown("#### 🛡️ Clean Sheets")
            cs_df = df[df['clean_sheets'] > 0].sort_values('clean_sheets', ascending=False).head(10)
            if not cs_df.empty:
                fig_cs = px.bar(
                    cs_df, x='clean_sheets', y='web_name', orientation='h', color='team',
                    title="Most Clean Sheets", labels={'clean_sheets': 'Clean Sheets', 'web_name': ''}, height=400
                )
                fig_cs.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_cs, use_container_width=True)
            else:
                st.info("No clean sheet data available.")

        st.markdown("---")
        
        col_misc1, col_misc2 = st.columns(2)
        with col_misc1:
            st.markdown("#### ⏱️ Most Played Minutes")
            top_min = df.sort_values('minutes', ascending=False).head(10)
            st.dataframe(top_min[['web_name', 'team', 'minutes']], hide_index=True, use_container_width=True)
            
        with col_misc2:
            st.markdown("#### 🟨🟥 Discipline")
            # Combined card view
            cards_df = df[(df['yellow_cards'] > 0) | (df['red_cards'] > 0)].sort_values('yellow_cards', ascending=False).head(10)
            st.dataframe(cards_df[['web_name', 'team', 'yellow_cards', 'red_cards']], hide_index=True, use_container_width=True)

    # --- TAB 3: CREATIVITY & IMPACT ---
    with tab_creative:
        st.subheader("Creativity, Influence & ICT Index")
        
        col_creat1, col_creat2 = st.columns(2)
        
        with col_creat1:
            st.markdown("#### 🎨 Top Creators")
            top_creative = df.sort_values('creativity', ascending=False).head(10)
            fig_creative = px.bar(
                top_creative, x='creativity', y='web_name', orientation='h', color='team',
                title="Highest Creativity Score", labels={'creativity': 'Creativity', 'web_name': ''}, height=400
            )
            fig_creative.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_creative, use_container_width=True)

        with col_creat2:
            st.markdown("#### 📊 ICT Index Leaders")
            top_ict = df.sort_values('ict_index', ascending=False).head(10)
            fig_ict = px.bar(
                top_ict, x='ict_index', y='web_name', orientation='h', color='team',
                title="Highest ICT Index", labels={'ict_index': 'ICT Index', 'web_name': ''}, height=400
            )
            fig_ict.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_ict, use_container_width=True)
            
        st.markdown("#### 🧠 Creativity vs Influence")
        fig_scatter = px.scatter(
            df, x='influence', y='creativity', size='ict_index', color='team',
            hover_data=['web_name'], title="Creativity vs Influence (Size = ICT Index)",
            height=500
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    # --- TAB 4: INJURIES ---
    with tab_injury:
        st.subheader("🚑 Injury Ward & Availability")
        
        injury_df = df[df['injury_news'] != ''].copy()
        
        if not injury_df.empty:
            view_cols = ['web_name', 'team', 'injury_news', 'chance_of_playing']
            injury_view = injury_df[view_cols].sort_values('chance_of_playing')
            
            def highlight_injury(row):
                val = row['chance_of_playing']
                if val == 0:
                    return ['background-color: #8B0000; color: white'] * len(row) # Dark Red
                elif val < 50:
                    return ['background-color: #B22222; color: white'] * len(row) # Firebrick
                elif val < 75:
                    return ['background-color: #DAA520; color: black'] * len(row) # Goldenrod
                return [''] * len(row)

            st.dataframe(
                injury_view.style.apply(highlight_injury, axis=1),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.success("No active injuries reported!")

elif df is not None:
    st.warning("Data fetched successfully but table is empty.")
