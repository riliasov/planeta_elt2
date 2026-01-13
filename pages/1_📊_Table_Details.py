
from datetime import datetime, timedelta
import asyncio
import pandas as pd
import streamlit as st
import plotly.express as px
from src.config.settings import settings

# Reuse connection function (ideally this should be in a utils file, but distinct for now)

async def get_db_conn():
    import asyncpg
    # Supabase/PgBouncer compatibility
    return await asyncpg.connect(settings.database_dsn, statement_cache_size=0)

@st.cache_data(ttl=30)
def fetch_table_stats() -> pd.DataFrame:
    """Fetch aggregated table stats with time context component."""
    async def _fetch():
        conn = await get_db_conn()
        try:
            # Join with runs to get dates
            rows = await conn.fetch(f"""

                SELECT 
                    ts.table_name,
                    ts.rows_extracted,
                    ts.rows_inserted,
                    ts.rows_updated,
                    ts.rows_deleted,
                    ts.validation_errors,
                    ts.duration_ms,
                    r.started_at,
                    r.run_id
                FROM {settings.schema_ops}.elt_table_stats ts

                JOIN {settings.schema_ops}.elt_runs r ON r.run_id = ts.run_id
                ORDER BY r.started_at DESC
                LIMIT 2000
            """)
            return [dict(r) for r in rows]
        finally:
            await conn.close()
    
    try:
        data = asyncio.run(_fetch())
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

st.set_page_config(page_title="Table Details", page_icon="📊", layout="wide")

st.markdown("### 📊 Table Operations Analysis")

df = fetch_table_stats()





if not df.empty:
    # Timezone Correction: UTC -> UTC+5 (Yekaterinburg)
    df['started_at'] = pd.to_datetime(df['started_at']) + timedelta(hours=5)
    df['date'] = df['started_at'].dt.date
    
    # Define Categories and Entities
    def get_category(table_name):
        if table_name.endswith('_cur'): return "Current (_cur)"
        if table_name.endswith('_hst'): return "History (_hst)"
        return "Legacy/Other"

    def get_entity(table_name):
        name = table_name.split('.')[-1]
        for entity in ['sales', 'trainings', 'clients', 'expenses']:
            if entity in name:
                return entity.capitalize()
        return "Other"

    df['category'] = df['table_name'].apply(get_category)
    df['entity'] = df['table_name'].apply(get_entity)

    # Filter out Others/Legacy completely
    df = df[df['category'] != "Legacy/Other"]


    # --- Global Filters ---
    st.markdown("#### 🔍 Filter View")
    
    # Entity Filter (Radio)
    all_entities = ["Show All"] + sorted(df['entity'].unique())
    selected_entity = st.radio("Select Entity", all_entities, horizontal=True)
    
    # Apply filter
    if selected_entity != "Show All":
        filtered_df = df[df['entity'] == selected_entity]
    else:
        filtered_df = df

    # Time Range calculation
    now = datetime.now()
    x_min = now - timedelta(days=30)
    x_max = now + timedelta(days=3)

    # Helper to plot Ops breakdown
    def plot_ops_breakdown(data_df, title):
        if data_df.empty:
            st.info(f"No data for {title}")
            return

        # Group by Date and aggregate ops
        daily = data_df.groupby(['date'])[['rows_inserted', 'rows_updated', 'rows_deleted', 'validation_errors']].sum().reset_index()
        
        # Melt for stacking: Date | Type | Count
        melted = daily.melt(
            id_vars=['date'], 
            value_vars=['rows_inserted', 'rows_updated', 'rows_deleted', 'validation_errors'],
            var_name='op_type', 
            value_name='count'
        )
        
        # Rename for nice legend
        op_map = {
            'rows_inserted': 'Inserts',
            'rows_updated': 'Updates', 
            'rows_deleted': 'Deletes',
            'validation_errors': 'Errors'
        }
        melted['Operation'] = melted['op_type'].map(op_map)
        
        # Custom colors
        color_map = {
            'Inserts': '#22c55e',  # Green
            'Updates': '#3b82f6',  # Blue
            'Deletes': '#64748b',  # Slate/Gray
            'Errors': '#ef4444'    # Red
        }

        fig = px.bar(
            melted, 
            x='date', 
            y='count', 
            color='Operation',
            title=title,
            color_discrete_map=color_map,
            height=350
        )
        fig.update_layout(
            template='plotly_white', 
            barmode='stack', 
            showlegend=True, 
            legend=dict(orientation="h", y=-0.2),
            xaxis=dict(range=[x_min, x_max], type="date", title=None),
            yaxis=dict(title="Count")
        )
        st.plotly_chart(fig, use_container_width=True)


    # Helper for Metrics
    def display_category_metrics(cat_df):
        if cat_df.empty: return
        total_read = cat_df['rows_extracted'].sum()
        total_ops = cat_df['rows_inserted'].sum() + cat_df['rows_updated'].sum() + cat_df['rows_deleted'].sum()
        total_err = cat_df['validation_errors'].sum()
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Rows Read (Source)", f"{total_read:,}")
        c2.metric("Rows Changed (DB)", f"{total_ops:,}")
        c3.metric("Validation Errors", f"{total_err}", delta_color="inverse" if total_err > 0 else "off")

    st.markdown("#### 📊 Summary Metrics")
    display_category_metrics(filtered_df)
    st.divider()

    # 1. Vertical Charts: Current followed by History
    st.markdown("#### 📈 Volume Trends (Inserts / Updates / Errors)")
    
    # Current Chart
    st.markdown("##### 🟢 Current Data (_cur)")
    plot_ops_breakdown(filtered_df[filtered_df['category'] == "Current (_cur)"], "Daily Operations (Current)")

    st.divider()

    # History Chart
    st.markdown("##### 📜 History Logs (_hst)")
    plot_ops_breakdown(filtered_df[filtered_df['category'] == "History (_hst)"], "Daily Operations (History)")

    # 2. Detailed Grid with Filters
    st.markdown("### 📋 Detailed Logs")
    

    # Grid always follows entity filtering
    st.dataframe(
        filtered_df[['started_at', 'category', 'table_name', 'rows_extracted', 'rows_inserted', 'rows_updated', 'rows_deleted', 'validation_errors', 'duration_ms']].sort_values('started_at', ascending=False),
        use_container_width=True,
        hide_index=True,
        column_config={
            "started_at": st.column_config.DatetimeColumn("Timestamp", format="D MMM HH:mm"),
            "duration_ms": st.column_config.NumberColumn("Duration (ms)")
        }
    )

else:
    st.info("No table statistics available yet. Run the pipeline to generate data.")
