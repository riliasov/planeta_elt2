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

# Стили для светлого SaaS Minimal стиля
st.markdown("""
<style>
    /* Global Background */
    .stApp {
        background-color: #f8fafc;
        color: #0f172a;
        font-family: 'Inter', sans-serif;
    }

    /* Metric Cards */
    .stMetric {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    
    .stMetric label {
        color: #64748b !important;
        font-size: 0.875rem !important;
        font-weight: 500 !important;
    }
    
    .stMetric [data-testid="stMetricValue"] {
        color: #0f172a !important;
        font-size: 1.75rem !important;
        font-weight: 600 !important;
    }
    
    /* Custom containers (mockup for cards) */
    div[data-testid="stHorizontalBlock"] > div {
        background: transparent; 
        padding: 0px;
    }

    /* Headings */
    h1 {
        color: #0f172a;
        font-weight: 700;
        font-size: 2.25rem;
        letter-spacing: -0.025em;
    }
    
    h2, h3 {
        color: #334155;
        font-weight: 600;
        letter-spacing: -0.025em;
    }
    
    .stCaption {
        color: #64748b;
    }

    /* Status Badges */
    .status-success {
        background-color: #dcfce7;
        color: #166534;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
        border: 1px solid #bbf7d0;
    }
    
    .status-failed {
        background-color: #fee2e2;
        color: #991b1b;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
        border: 1px solid #fecaca;
    }
    
    .status-running {
        background-color: #fef3c7;
        color: #92400e;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
        border: 1px solid #fde68a;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #e2e8f0;
    }
    
    hr {
        margin: 24px 0;
        border-color: #e2e8f0;
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
    # Header
    st.title("📊 ELT Dashboard")
    st.caption("Мониторинг пайплайна • История запусков • Метрики загрузки")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Фильтры")
        days_range = st.slider("Период (дней)", 1, 90, 30)
        auto_refresh = st.checkbox("Авто-обновление (30с)", value=False)
        
        if auto_refresh:
            st.rerun()
        
        st.divider()
        st.caption("Последнее обновление:")
        st.caption(datetime.now().strftime("%H:%M:%S"))
    
    # Загрузка данных
    runs_df = fetch_runs_data(days_range)
    
    if runs_df.empty:
        st.info("📭 Нет данных о запусках ELT. Запустите пайплайн командой:\n\n```bash\npython -m src.main\n```")
        return
    
    # === KPI Cards ===
    st.subheader("📈 Обзор")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    total_runs = len(runs_df)
    successful = len(runs_df[runs_df['status'] == 'success'])
    failed = len(runs_df[runs_df['status'] == 'failed'])
    success_rate = (successful / total_runs * 100) if total_runs > 0 else 0
    
    with col1:
        st.metric("Всего запусков", total_runs)
    
    with col2:
        st.metric("Успешных", successful, delta=f"{success_rate:.0f}%")
    
    with col3:
        st.metric("Ошибок", failed, delta_color="inverse")
    
    with col4:
        total_rows = runs_df['total_rows_synced'].sum()
        st.metric("Строк загружено", f"{total_rows:,}")
    
    with col5:
        avg_duration = runs_df['duration_seconds'].mean()
        st.metric("Ср. время (сек)", f"{avg_duration:.1f}" if pd.notna(avg_duration) else "—")
    
    st.divider()
    
    # === Charts ===
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("📅 История запусков")
        
        if 'started_at' in runs_df.columns:
            runs_df['date'] = pd.to_datetime(runs_df['started_at']).dt.date
            daily = runs_df.groupby(['date', 'status']).size().reset_index(name='count')
            
            fig = px.bar(
                daily, 
                x='date', 
                y='count', 
                color='status',
                color_discrete_map={'success': '#10b981', 'failed': '#ef4444', 'running': '#f59e0b'},
                barmode='stack',
                template='plotly_white'
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                xaxis_title="",
                yaxis_title="Запусков",
                legend_title="Статус",
                margin=dict(l=0, r=0, t=10, b=0),
                font=dict(family="Inter, sans-serif", color="#64748b")
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col_chart2:
        st.subheader("⏱️ Время выполнения")
        
        recent_runs = runs_df.head(20).copy()
        if not recent_runs.empty and 'duration_seconds' in recent_runs.columns:
            recent_runs['run_label'] = pd.to_datetime(recent_runs['started_at']).dt.strftime('%m/%d %H:%M')
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=recent_runs['run_label'],
                y=recent_runs['duration_seconds'],
                mode='lines+markers',
                line=dict(color='#3b82f6', width=2),
                marker=dict(size=8, color='#3b82f6', line=dict(width=2, color='white')),
                fill='tozeroy',
                fillcolor='rgba(59, 130, 246, 0.1)'
            ))
            fig.update_layout(
                template='plotly_white',
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                xaxis_title="",
                yaxis_title="Секунды",
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False,
                font=dict(family="Inter, sans-serif", color="#64748b")
            )
            st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # === Recent Runs Table ===
    st.subheader("🕒 Последние запуски")
    
    display_df = runs_df.head(10).copy()
    if not display_df.empty:
        display_df['started_at'] = pd.to_datetime(display_df['started_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        display_df['duration'] = display_df['duration_seconds'].apply(lambda x: f"{x:.1f}s" if pd.notna(x) else "—")
        
        st.dataframe(
            display_df[['started_at', 'status', 'mode', 'tables_processed', 'total_rows_synced', 'validation_errors', 'duration']],
            column_config={
                'started_at': st.column_config.TextColumn('Время старта'),
                'status': st.column_config.TextColumn('Статус'),
                'mode': st.column_config.TextColumn('Режим'),
                'tables_processed': st.column_config.NumberColumn('Таблиц'),
                'total_rows_synced': st.column_config.NumberColumn('Строк'),
                'validation_errors': st.column_config.NumberColumn('Ошибок валидации'),
                'duration': st.column_config.TextColumn('Время')
            },
            hide_index=True,
            use_container_width=True
        )
    
    # === Table Stats (expandable) ===
    with st.expander("📊 Статистика по таблицам"):
        table_stats_df = fetch_table_stats()
        if not table_stats_df.empty:
            # Агрегация по таблицам
            agg = table_stats_df.groupby('table_name').agg({
                'rows_inserted': 'sum',
                'rows_updated': 'sum',
                'rows_deleted': 'sum',
                'validation_errors': 'sum',
                'duration_ms': 'mean'
            }).reset_index()
            agg['avg_duration_ms'] = agg['duration_ms'].round(0).astype(int)
            
            st.dataframe(
                agg[['table_name', 'rows_inserted', 'rows_updated', 'rows_deleted', 'validation_errors', 'avg_duration_ms']],
                column_config={
                    'table_name': st.column_config.TextColumn('Таблица'),
                    'rows_inserted': st.column_config.NumberColumn('Вставлено'),
                    'rows_updated': st.column_config.NumberColumn('Обновлено'),
                    'rows_deleted': st.column_config.NumberColumn('Удалено'),
                    'validation_errors': st.column_config.NumberColumn('Ошибок'),
                    'avg_duration_ms': st.column_config.NumberColumn('Ср. время (мс)')
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("Нет данных по таблицам")
    
    # === Validation Errors (expandable) ===
    with st.expander("⚠️ Ошибки валидации"):
        errors_df = fetch_validation_errors()
        if not errors_df.empty:
            errors_df['created_at'] = pd.to_datetime(errors_df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
            st.dataframe(
                errors_df[['created_at', 'table_name', 'row_index', 'column_name', 'error_type', 'message']].head(50),
                column_config={
                    'created_at': 'Время',
                    'table_name': 'Таблица',
                    'row_index': 'Строка',
                    'column_name': 'Колонка',
                    'error_type': 'Тип ошибки',
                    'message': 'Сообщение'
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.success("✓ Ошибок валидации не обнаружено")


if __name__ == "__main__":
    main()
