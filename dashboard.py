"""ELT Dashboard — мониторинг пайплайна и аналитика загрузки данных."""
import asyncio
import os
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Настройка страницы
st.set_page_config(
    page_title="ELT Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Стили для Ultra-Compact SaaS стиля
st.markdown("""
<style>
    /* Global Background & Font */
    .stApp {
        background-color: #f8fafc;
        color: #334155;
        font-family: 'Inter', sans-serif;
        font-size: 11px !important;
    }
    
    /* Main container padding */
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 2rem !important;
        max-width: 100% !important;
    }

    /* Metric Cards Compact */
    .stMetric {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 6px;
        padding: 8px 12px !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    }
    
    .stMetric label {
        color: #64748b !important;
        font-size: 10px !important;
        font-weight: 500 !important;
        margin-bottom: 0px !important;
    }
    
    .stMetric [data-testid="stMetricValue"] {
        color: #0f172a !important;
        font-size: 18px !important;
        font-weight: 600 !important;
        padding: 2px 0 !important;
    }
    
    /* Custom containers (mockup for cards) */
    div[data-testid="stHorizontalBlock"] > div {
        background: transparent; 
        padding: 0px;
        gap: 0.5rem !important;
    }

    /* Headings Compact */
    h1 {
        color: #0f172a;
        font-weight: 700;
        font-size: 16px !important;
        letter-spacing: -0.01em;
        margin-bottom: 0.5rem !important;
        padding: 0 !important;
    }
    
    h2 {
        font-size: 14px !important;
        margin-bottom: 0.5rem !important;
    }

    h3 {
        color: #334155;
        font-weight: 600;
        font-size: 13px !important;
        letter-spacing: -0.01em;
        margin-top: 1rem !important;
        margin-bottom: 0.5rem !important;
        padding-bottom: 4px;
        border-bottom: 1px solid #e2e8f0;
    }
    
    /* Sidebar Compact */
    [data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #e2e8f0;
        padding-top: 1rem !important;
    }
    
    [data-testid="stSidebar"] h2 {
        font-size: 12px !important;
        text-transform: uppercase;
        color: #94a3b8;
        letter-spacing: 0.05em;
    }
    
    /* Tables Compact */
    div[data-testid="stDataFrame"] {
        font-size: 11px !important;
    }
    
    table {
        font-size: 11px !important;
    }

    /* Alerts / Info */
    .stAlert {
        padding: 4px 12px !important;
        font-size: 11px !important;
    }

    hr {
        margin: 12px 0 !important;
        border-color: #e2e8f0;
    }
    
    /* Columns Gap */
    div[data-testid="column"] {
        padding: 0 !important;
    }
    
    /* Radio Button Compact */
    .stRadio > label {
        display: none;
    }
    .stRadio div[role="radiogroup"] > label {
        padding: 4px 8px !important;
        font-size: 12px !important;
    }
</style>
""", unsafe_allow_html=True)


async def get_db_conn():
    """Устанавливает прямое соединение с БД для Streamlit."""
    import asyncpg
    from src.config.settings import settings
    return await asyncpg.connect(settings.database_dsn)


@st.cache_data(ttl=30)
def fetch_runs_data(_days: int = 30) -> pd.DataFrame:
    """Получает историю запусков за последние N дней."""
    async def _fetch():
        conn = await get_db_conn()
        try:
            rows = await conn.fetch(f"""
                SELECT run_id, started_at, finished_at, status, mode,
                       tables_processed, total_rows_synced, validation_errors,
                       duration_seconds, error_message
                FROM elt_runs
                WHERE started_at > NOW() - INTERVAL '{_days} days'
                ORDER BY started_at DESC
            """)
            return [dict(r) for r in rows]
        finally:
            await conn.close()
    
    try:
        data = asyncio.run(_fetch())
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        # Не выводим ворнинг если таблиц еще нет (первый запуск)
        if "relation \"elt_runs\" does not exist" not in str(e):
            st.warning(f"Ошибка получения данных: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def fetch_table_stats(run_id: str = None) -> pd.DataFrame:
    """Получает статистику по таблицам."""
    async def _fetch():
        conn = await get_db_conn()
        try:
            if run_id:
                rows = await conn.fetch("""
                    SELECT * FROM elt_table_stats
                    WHERE run_id = $1
                    ORDER BY table_name
                """, run_id)
            else:
                rows = await conn.fetch("""
                    SELECT ts.*, r.started_at
                    FROM elt_table_stats ts
                    JOIN elt_runs r ON r.run_id = ts.run_id
                    ORDER BY r.started_at DESC, ts.table_name
                    LIMIT 100
                """)
            return [dict(r) for r in rows]
        finally:
            await conn.close()
    
    try:
        data = asyncio.run(_fetch())
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def fetch_validation_errors(run_id: str = None, limit: int = 50) -> pd.DataFrame:
    """Получает ошибки валидации."""
    async def _fetch():
        conn = await get_db_conn()
        try:
            if run_id:
                rows = await conn.fetch("""
                    SELECT * FROM validation_logs
                    WHERE run_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                """, run_id, limit)
            else:
                rows = await conn.fetch("""
                    SELECT * FROM validation_logs
                    ORDER BY created_at DESC
                    LIMIT $1
                """, limit)
            return [dict(r) for r in rows]
        finally:
            await conn.close()
    
    try:
        data = asyncio.run(_fetch())
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def render_status_badge(status: str) -> str:
    """Рендерит HTML badge для статуса."""
    icons = {'success': '✓', 'failed': '✗', 'running': '⟳'}
    return f'<span class="status-{status}">{icons.get(status, "?")} {status}</span>'



def main():
    # Sidebar Navigation
    with st.sidebar:
        st.header("Planeta ELT")
        page = st.radio(
            "Navigation", 
            ["Overview", "Table Stats", "Validation Errors"],
            label_visibility="collapsed"
        )
        
        st.divider()
        
        st.header("Filters")
        days_range = st.slider("Period (days)", 1, 90, 30)
        auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
        
        if auto_refresh:
            st.rerun()
        
        st.divider()
        st.caption(f"Last update: {datetime.now().strftime('%H:%M:%S')}")

    # Load shared data
    runs_df = fetch_runs_data(days_range)

    # PAGE: OVERVIEW
    if page == "Overview":
        st.markdown("### 📊 Pipeline Overview")
        
        if runs_df.empty:
            st.info("No data. Run pipeline: `python -m src.main`")
            return

        # KPI Compact
        col1, col2, col3, col4, col5 = st.columns(5)
        
        total_runs = len(runs_df)
        successful = len(runs_df[runs_df['status'] == 'success'])
        failed = len(runs_df[runs_df['status'] == 'failed'])
        success_rate = (successful / total_runs * 100) if total_runs > 0 else 0
        
        total_rows = runs_df['total_rows_synced'].sum()
        avg_duration = runs_df['duration_seconds'].mean()

        col1.metric("Total Runs", total_runs)
        col2.metric("Success", successful, f"{success_rate:.0f}%")
        col3.metric("Errors", failed, delta_color="inverse")
        col4.metric("Rows", f"{total_rows:,}")
        col5.metric("Avg Time", f"{avg_duration:.1f}s" if pd.notna(avg_duration) else "—")
        
        st.markdown("---")
        
        # Charts Compact
        c1, c2 = st.columns([1, 1])
        
        with c1:
            st.markdown("#### Run History")
            if 'started_at' in runs_df.columns:
                runs_df['date'] = pd.to_datetime(runs_df['started_at']).dt.date
                daily = runs_df.groupby(['date', 'status']).size().reset_index(name='count')
                
                fig = px.bar(
                    daily, x='date', y='count', color='status',
                    color_discrete_map={'success': '#10b981', 'failed': '#ef4444', 'running': '#f59e0b'},
                    barmode='stack', template='plotly_white', height=250
                )
                fig.update_layout(
                    margin=dict(l=0, r=0, t=0, b=0),
                    font=dict(size=10, color="#64748b"),
                    xaxis_title=None, yaxis_title=None,
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown("#### Duration (sec)")
            recent = runs_df.head(30)
            if not recent.empty and 'duration_seconds' in recent.columns:
                recent['label'] = pd.to_datetime(recent['started_at']).dt.strftime('%m/%d %H:%M')
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=recent['label'], y=recent['duration_seconds'],
                    mode='lines', line=dict(color='#3b82f6', width=1),
                    fill='tozeroy', fillcolor='rgba(59, 130, 246, 0.05)'
                ))
                fig.update_layout(
                    template='plotly_white', height=250,
                    margin=dict(l=0, r=0, t=0, b=0),
                    font=dict(size=10, color="#64748b"),
                    xaxis_title=None, yaxis_title=None,
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### Recent Runs")
        display_df = runs_df.head(15).copy()
        if not display_df.empty:
            display_df['started_at'] = pd.to_datetime(display_df['started_at']).dt.strftime('%Y-%m-%d %H:%M')
            display_df['duration'] = display_df['duration_seconds'].apply(lambda x: f"{x:.1f}s" if pd.notna(x) else "—")
            
            st.dataframe(
                display_df[['started_at', 'status', 'mode', 'tables_processed', 'total_rows_synced', 'validation_errors', 'duration']],
                height=300,
                hide_index=True,
                use_container_width=True
            )

    # PAGE: TABLE STATS
    elif page == "Table Stats":
        st.markdown("### 📋 Table Statistics")
        stats_df = fetch_table_stats()
        
        if not stats_df.empty:
            # Aggregate stats
            agg = stats_df.groupby('table_name').agg({
                'rows_inserted': 'sum',
                'rows_updated': 'sum',
                'rows_deleted': 'sum',
                'validation_errors': 'sum',
                'duration_ms': 'mean',
                'run_id': 'count' # count runs
            }).reset_index()
            agg['avg_ms'] = agg['duration_ms'].round(0).astype(int)
            agg.rename(columns={'run_id': 'runs_count'}, inplace=True)
            
            st.dataframe(
                agg[['table_name', 'runs_count', 'rows_inserted', 'rows_updated', 'rows_deleted', 'validation_errors', 'avg_ms']],
                use_container_width=True,
                height=600,
                hide_index=True
            )
        else:
            st.info("No table stats available.")

    # PAGE: VALIDATION ERRORS
    elif page == "Validation Errors":
        st.markdown("### ⚠️ Validation Errors Log")
        limit = st.selectbox("Rows limit", [50, 100, 500], index=1)
        errors_df = fetch_validation_errors(limit=limit)
        
        if not errors_df.empty:
            errors_df['created_at'] = pd.to_datetime(errors_df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
            st.dataframe(
                errors_df[['created_at', 'table_name', 'column_name', 'invalid_value', 'error_type', 'message']],
                use_container_width=True,
                height=700,
                hide_index=True
            )
        else:
            st.success("No validation errors found.")


if __name__ == "__main__":
    main()
