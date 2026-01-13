
import asyncio
import pandas as pd
import streamlit as st
import plotly.express as px
from src.config.settings import settings


async def get_db_conn():
    import asyncpg
    # Supabase/PgBouncer compatibility
    return await asyncpg.connect(settings.database_dsn, statement_cache_size=0)

@st.cache_data(ttl=30)
def fetch_errors(limit: int = 500) -> pd.DataFrame:
    async def _fetch():
        conn = await get_db_conn()
        try:
            rows = await conn.fetch(f"""
                SELECT created_at, table_name, column_name, invalid_value, error_type, message
                FROM {settings.schema_ops}.validation_logs
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

st.set_page_config(page_title="Data Quality", page_icon="🚨", layout="wide")

st.markdown("### 🚨 Data Quality & Validation")

limit = st.slider("Limit rows", 100, 2000, 500)
errors_df = fetch_errors(limit)

if not errors_df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Errors by Table")
        err_by_table = errors_df['table_name'].value_counts().reset_index()
        err_by_table.columns = ['Table', 'Count']
        fig1 = px.pie(err_by_table, names='Table', values='Count', hole=0.4, color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig1, use_container_width=True)
        
    with col2:
        st.markdown("#### Types of Errors")
        err_by_type = errors_df['error_type'].value_counts().reset_index()
        err_by_type.columns = ['Type', 'Count']
        fig2 = px.bar(err_by_type, x='Count', y='Type', orientation='h', text='Count', color='Count', color_continuous_scale='Reds')
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("#### 🕵️ Error Inspector")
    
    # Filter by table
    selected_table = st.selectbox("Filter by Table", ["All"] + list(errors_df['table_name'].unique()))
    
    view_df = errors_df if selected_table == "All" else errors_df[errors_df['table_name'] == selected_table]
    
    st.dataframe(
        view_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "created_at": st.column_config.DatetimeColumn("Timestamp", format="D MMM HH:mm:ss"),
            "invalid_value": "Invalid Val",
            "message": "Error Message"
        }
    )
else:
    st.success("✅ No validation errors found in the logs! Clean data.")
