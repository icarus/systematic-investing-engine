"""ORM models for Factor Engine."""

from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Run(Base):
    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    as_of_date: Mapped[date] = mapped_column(Date, nullable=False)
    rebalance_date: Mapped[date | None] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    stage: Mapped[str] = mapped_column(String, default="pending")
    params_json: Mapped[dict | None] = mapped_column(JSON)
    survivorship_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    config_hash: Mapped[str | None] = mapped_column(String)

    provider_logs: Mapped[list["ProviderLog"]] = relationship(back_populates="run")


class Symbol(Base):
    __tablename__ = "symbols"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    name: Mapped[str | None]
    currency: Mapped[str] = mapped_column(String, default="CLP")
    sector: Mapped[str | None]

    memberships: Mapped[list["UniverseMembership"]] = relationship(back_populates="symbol")


class UniverseMembership(Base):
    __tablename__ = "universe_membership"
    __table_args__ = (
        UniqueConstraint("symbol_id", "start_date", name="uq_membership_symbol_start"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey("symbols.id"), nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date | None] = mapped_column(Date)
    source: Mapped[str] = mapped_column(String, default="manual")

    symbol: Mapped[Symbol] = relationship(back_populates="memberships")


class PriceAdjusted(Base):
    __tablename__ = "prices_adjusted"
    __table_args__ = (
        UniqueConstraint("symbol_id", "price_date", name="uq_price_symbol_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey("symbols.id"), nullable=False)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), nullable=False)
    price_date: Mapped[date] = mapped_column(Date, nullable=False)
    adj_close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float | None]
    currency: Mapped[str] = mapped_column(String, default="CLP")


class FactorValue(Base):
    __tablename__ = "factor_values"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey("symbols.id"), nullable=False)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), nullable=False)
    factor_name: Mapped[str] = mapped_column(String, nullable=False)
    value_date: Mapped[date] = mapped_column(Date, nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint("symbol_id", "run_id", "factor_name", "value_date", name="uq_factor_symbol_run_date"),
    )


class ProviderLog(Base):
    __tablename__ = "provider_logs"

    provider_run_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), nullable=False)
    provider: Mapped[str] = mapped_column(String, nullable=False)
    endpoint: Mapped[str] = mapped_column(String, nullable=False)
    params_hash: Mapped[str] = mapped_column(String, nullable=False)
    response_hash: Mapped[str] = mapped_column(String, nullable=False)
    checksum: Mapped[str | None] = mapped_column(String)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    run: Mapped[Run] = relationship(back_populates="provider_logs")


class OverrideAudit(Base):
    __tablename__ = "override_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str | None] = mapped_column(ForeignKey("runs.run_id"), nullable=True)
    source: Mapped[str] = mapped_column(String, nullable=False)
    field: Mapped[str] = mapped_column(String, nullable=False)
    old_value: Mapped[str | None]
    new_value: Mapped[str | None]
    author: Mapped[str | None]
    enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    applied_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class LiquidityMetric(Base):
    __tablename__ = "liquidity_metrics"
    __table_args__ = (
        CheckConstraint("median_traded_value_clp >= 0"),
        UniqueConstraint("symbol_id", "run_id", "metric_date", name="uq_liquidity_symbol_run_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey("symbols.id"), nullable=False)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), nullable=False)
    lookback_days: Mapped[int] = mapped_column(Integer, nullable=False)
    median_traded_value_clp: Mapped[float] = mapped_column(Float, nullable=False)
    metric_date: Mapped[date] = mapped_column(Date, nullable=False)


class SignalRecord(Base):
    __tablename__ = "signals"
    __table_args__ = (
        UniqueConstraint("run_id", "symbol_id", "as_of_date", name="uq_signal_run_symbol_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), nullable=False)
    symbol_id: Mapped[int] = mapped_column(ForeignKey("symbols.id"), nullable=False)
    as_of_date: Mapped[date] = mapped_column(Date, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    liquidity: Mapped[float] = mapped_column(Float, nullable=False)


class PortfolioPositionRecord(Base):
    __tablename__ = "portfolio_positions"
    __table_args__ = (
        UniqueConstraint("run_id", "symbol_id", "rebalance_date", name="uq_portfolio_run_symbol_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), nullable=False)
    symbol_id: Mapped[int] = mapped_column(ForeignKey("symbols.id"), nullable=False)
    rebalance_date: Mapped[date] = mapped_column(Date, nullable=False)
    weight: Mapped[float] = mapped_column(Float, nullable=False)
    liquidity_cap: Mapped[float] = mapped_column(Float, nullable=False)


class BacktestResult(Base):
    __tablename__ = "backtest_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), unique=True, nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    final_capital: Mapped[float] = mapped_column(Float, nullable=False)
    cagr: Mapped[float] = mapped_column(Float, nullable=True)
    volatility: Mapped[float] = mapped_column(Float, nullable=True)
    max_drawdown: Mapped[float] = mapped_column(Float, nullable=True)
    periods: Mapped[int] = mapped_column(Integer, nullable=False)
    notes: Mapped[str | None] = mapped_column(String)


class BacktestEquityPoint(Base):
    __tablename__ = "backtest_equity_curve"
    __table_args__ = (
        UniqueConstraint("run_id", "point_date", name="uq_equity_run_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), nullable=False)
    point_date: Mapped[date] = mapped_column(Date, nullable=False)
    capital: Mapped[float] = mapped_column(Float, nullable=False)
