---
slug: "github-finance-index-dashboard"
title: "finance-index-dashboard"
repo: "justin-napolitano/finance-index-dashboard"
githubUrl: "https://github.com/justin-napolitano/finance-index-dashboard"
generatedAt: "2025-11-23T08:56:03.370262Z"
source: "github-auto"
---


# Finance Index Dashboard: Technical Overview and Implementation Notes

This document serves as a technical reference for the Finance Index Dashboard project, outlining its motivation, architecture, and key implementation details.

## Motivation and Problem Statement

Financial indices are essential tools for tracking market segments and investment strategies. However, building and maintaining dynamic indices with live data feeds, signal computations, and automated rebalancing is complex. This project addresses the need for an integrated system that ingests financial data, computes relevant signals, manages index definitions, and provides an accessible API and frontend for exploration.

## System Architecture

The system is composed of three main layers:

1. **Data Layer:** PostgreSQL database stores core entities such as tickers, prices, signals, index definitions, constituents, and historical index levels. Alembic manages schema migrations.

2. **Backend Layer:** Python FastAPI application exposes REST endpoints for indices and tickers. It includes ETL modules that fetch data from external sources (e.g., yfinance, Wikipedia), compute signals like momentum scores and RSI, and rebalance indices according to defined rules.

3. **Frontend Layer:** A React/Next.js application consumes the backend API to render interactive visualizations of index performance and holdings.

Docker Compose orchestrates the multi-container setup including the database, backend, ETL jobs, and frontend.

## Data Ingestion and ETL

- Tickers are sourced from S&P 500 and Nasdaq-100 via Wikipedia scraping and normalized for compatibility with yfinance.
- Price data is fetched in batches using yfinance with robust retry logic and rate limiting.
- Signals such as returns over multiple horizons, RSI, ATR, moving averages, and a composite momentum score are computed per ticker.
- The ETL pipeline supports partial runs (skip prices, signals, or rebalancing) and can load ticker lists from files or the database.

## Index Definition and Rebalancing

- The default index "momentum-10" selects the top 10 tickers by momentum score within a US universe.
- Index rules include caps on sector and ticker weights and rebalance frequency.
- Constituents are reset and weighted equally upon rebalancing.

## API Design

- Endpoints provide listing and detail views for indices and tickers.
- Index details include metadata, historical levels, and current holdings with weights.
- Ticker endpoints return metadata and recent price data.

## Database Schema and Migrations

- Core tables: tickers, prices, signals, index_definitions, index_constituents, index_history.
- Prices table includes OHLCV data with a composite primary key on (ticker, date).
- Alembic scripts manage schema evolution.

## Maintenance and Utilities

- Audit module verifies database health, data recency, duplicates, orphans, and index integrity.
- Fixer module can auto-correct issues including schema upgrades, duplicate removal, orphan handling, and weight normalization.

## Frontend Implementation

- Built with Next.js and React, using Recharts for visualization.
- Pages include index list, index detail with line charts, and ticker detail views.
- API URL configurable via environment variables.

## Development and Deployment

- Docker Compose file orchestrates services with health checks and dependencies.
- Makefile provides common targets for stack management, ETL runs, and database operations.

## Assumptions and Notes

- The default index and rules are hardcoded but can be extended.
- ETL relies on environment variables for API keys and tuning parameters.
- Frontend is minimal but structured for extension.
- Some scripts and files (e.g., context.txt, roadmap.txt) are assumed to contain notes and plans.

## Summary

This project integrates financial data ingestion, signal computation, index management, and visualization into a cohesive system. It balances practical engineering concerns such as containerization, schema migration, and data integrity with domain-specific logic for financial indices. The modular design facilitates ongoing extension and maintenance.

This document should serve as a reference for understanding the system's components, data flows, and operational procedures when revisiting the project.