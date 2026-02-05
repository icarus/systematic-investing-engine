import streamlit as st
import pandas as pd
import plotly.express as px
from factor_engine.db.session import session_scope
from factor_engine.db.models import Run, SignalRecord, PortfolioPositionRecord, Symbol, FactorValue, LiquidityMetric
from factor_engine.universe import get_active_symbols
from sqlalchemy import desc

st.set_page_config(page_title="Factor Engine Dashboard", layout="wide", page_icon="📈")

st.title("Factor Engine Dashboard")

# --- Sidebar: Run Selection ---
with session_scope() as session:
    runs = session.query(Run).order_by(desc(Run.created_at)).all()

    if not runs:
        st.warning("No runs found in the database.")
        st.stop()

    run_options = {f"{r.as_of_date} ({r.run_id[:8]}...) - {r.stage}": r.run_id for r in runs}
    selected_run_label = st.sidebar.selectbox("Select Run", options=list(run_options.keys()))
    selected_run_id = run_options[selected_run_label]

    current_run = session.query(Run).filter(Run.run_id == selected_run_id).one()
    # Extract data to local dictionary to avoid DetachedInstanceError
    run_info = {
        "run_id": current_run.run_id,
        "as_of_date": current_run.as_of_date,
        "stage": current_run.stage,
        "survivorship_flag": current_run.survivorship_flag
    }

# --- Run Info ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Run ID", run_info["run_id"][:8])
col2.metric("As Of Date", str(run_info["as_of_date"]))
col3.metric("Stage", run_info["stage"])
col4.metric("Survivorship Flag", str(run_info["survivorship_flag"]))

st.divider()

# --- Signals ---
st.header("Signals")
with session_scope() as session:
    signals_query = (
        session.query(
            Symbol.ticker,
            SignalRecord.score,
            SignalRecord.liquidity
        )
        .join(Symbol, Symbol.id == SignalRecord.symbol_id)
        .filter(SignalRecord.run_id == selected_run_id)
        .order_by(desc(SignalRecord.score))
    )
    signals_df = pd.read_sql(signals_query.statement, session.bind)

if not signals_df.empty:
    col_left, col_right = st.columns([2, 1])
    with col_left:
        st.dataframe(signals_df.style.format({"score": "{:.4f}", "liquidity": "{:,.0f}"}), use_container_width=True)
    with col_right:
        fig = px.bar(signals_df.head(10), x='score', y='ticker', orientation='h', title="Top 10 Signal Scores")
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No signals generated for this run.")

st.divider()

# --- Portfolio ---
st.header("Portfolio Composition")
with session_scope() as session:
    portfolio_query = (
        session.query(
            Symbol.ticker,
            PortfolioPositionRecord.weight,
            PortfolioPositionRecord.liquidity_cap
        )
        .join(Symbol, Symbol.id == PortfolioPositionRecord.symbol_id)
        .filter(PortfolioPositionRecord.run_id == selected_run_id)
        .order_by(desc(PortfolioPositionRecord.weight))
    )
    portfolio_df = pd.read_sql(portfolio_query.statement, session.bind)

if not portfolio_df.empty:
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(portfolio_df.style.format({"weight": "{:.2%}", "liquidity_cap": "{:.2%}"}), use_container_width=True)
    with col2:
        # Filter out negative weights for pie chart visualization or handle them appropriately
        long_pos = portfolio_df[portfolio_df['weight'] > 0]
        if not long_pos.empty:
            fig = px.pie(long_pos, values='weight', names='ticker', title="Long Allocation")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("No long positions to visualize.")
else:
    st.info("No portfolio positions found for this run.")

st.divider()

# --- Factors Detail ---
st.header("Factor Details (Top 5 Assets)")
if not signals_df.empty:
    top_tickers = signals_df.head(5)['ticker'].tolist()

    with session_scope() as session:
        factors_query = (
            session.query(
                Symbol.ticker,
                FactorValue.factor_name,
                FactorValue.value
            )
            .join(Symbol, Symbol.id == FactorValue.symbol_id)
            .filter(FactorValue.run_id == selected_run_id)
            .filter(Symbol.ticker.in_(top_tickers))
        )
        factors_df = pd.read_sql(factors_query.statement, session.bind)

    if not factors_df.empty:
        pivot_df = factors_df.pivot(index='ticker', columns='factor_name', values='value')
        st.dataframe(pivot_df, use_container_width=True)
    else:
        st.info("No factor values found.")
