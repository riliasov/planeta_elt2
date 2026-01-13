
import asyncio
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from src.config.settings import settings


async def get_db_conn():
    import asyncpg
    # Supabase/PgBouncer compatibility
    return await asyncpg.connect(settings.database_dsn, statement_cache_size=0)

@st.cache_data(ttl=30)
def fetch_run_metrics(days: int = 30) -> pd.DataFrame:
    async def _fetch():
        conn = await get_db_conn()
        try:
            rows = await conn.fetch(f"""
                SELECT 
                    started_at, 
                    duration_seconds, 
                    total_rows_synced,
                    tables_processed,
                    status
                FROM {settings.schema_ops}.elt_runs
                WHERE started_at > NOW() - INTERVAL '{days} days'
                ORDER BY started_at ASC
            """)
            return [dict(r) for r in rows]
        finally:
            await conn.close()
    try:
        data = asyncio.run(_fetch())
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

st.set_page_config(page_title="Performance", page_icon="⏱", layout="wide")

st.markdown("### ⏱ Pipeline Performance")

df = fetch_run_metrics()


if not df.empty:
    # Timezone Correction: UTC -> UTC+5
    df['started_at'] = pd.to_datetime(df['started_at']) + pd.Timedelta(hours=5)
    df['timestamp'] = df['started_at']
    

    # Throughput Calculation
    # Avoid div by zero and None type (running jobs)
    df['throughput'] = df.apply(lambda x: x['total_rows_synced'] / x['duration_seconds'] if (x['duration_seconds'] and x['duration_seconds'] > 0) else 0, axis=1)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Avg Duration", f"{df['duration_seconds'].mean():.2f}s")
    with c2:
        st.metric("Max Throughput", f"{df['throughput'].max():.0f} rows/s")

    # Dual Axis Chart: Duration vs Rows
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['timestamp'],
        y=df['total_rows_synced'],
        name='Rows Synced',
        marker_color='#cbd5e1'
    ))

    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['duration_seconds'],
        name='Duration (sec)',
        yaxis='y2',
        line=dict(color='#2563eb', width=2),
        mode='lines+markers'
    ))

    fig.update_layout(
        title="Volume vs Latency",
        xaxis_title="Run Time",
        yaxis=dict(title="Rows Processed"),
        yaxis2=dict(
            title="Duration (s)",
            overlaying='y',
            side='right'
        ),
        template='plotly_white',
        legend=dict(x=0, y=1.1, orientation='h'),
        height=500
    )
    
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### 🕒 Scatter: Time vs Volume")
    fig2 = go.Figure(data=go.Scatter(
        x=df['total_rows_synced'],
        y=df['duration_seconds'],
        mode='markers',
        marker=dict(
            size=10,
            color=df['throughput'], # Color by throughput
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Rows/Sec")
        ),
        text=df['status']
    ))
    fig2.update_layout(
        xaxis_title="Total Rows Synced",
        yaxis_title="Duration (Seconds)",
        template="plotly_white",
        height=400
    )
    st.plotly_chart(fig2, use_container_width=True)

else:
    st.info("No run history found.")
