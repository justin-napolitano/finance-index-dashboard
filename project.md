---
slug: github-finance-index-dashboard
id: github-finance-index-dashboard
title: Finance Index Dashboard
repo: justin-napolitano/finance-index-dashboard
githubUrl: https://github.com/justin-napolitano/finance-index-dashboard
generatedAt: '2025-11-24T21:34:45.216Z'
source: github-auto
summary: >-
  Finance Index Dashboard is a Python-based application designed to provide
  dynamic financial indices and live performance tracking. It integrates data
  ingestion, signal computation, and index rebalancing with a modern web
  frontend for visualization.
tags: []
seoPrimaryKeyword: ''
seoSecondaryKeywords: []
seoOptimized: false
topicFamily: null
topicFamilyConfidence: null
kind: project
entryLayout: project
showInProjects: true
showInNotes: false
showInWriting: false
showInLogs: false
---

Finance Index Dashboard is a Python-based application designed to provide dynamic financial indices and live performance tracking. It integrates data ingestion, signal computation, and index rebalancing with a modern web frontend for visualization.

---

## Features

- ETL pipeline for fetching and processing financial data from multiple sources
- Computation of financial signals including momentum scores and technical indicators
- Dynamic index definitions and automated rebalancing
- REST API built with FastAPI exposing indices and ticker data
- React/Next.js frontend for interactive visualization of indices and holdings
- Dockerized setup for easy deployment and development
- Database schema migrations managed with Alembic
- Audit and auto-fix tools for database health and consistency

---

## Tech Stack

- **Backend:** Python, FastAPI, SQLAlchemy, Alembic
- **Database:** PostgreSQL
- **Data Sources:** yfinance, Wikipedia scraping for tickers
- **Frontend:** React, Next.js, Recharts
- **Containerization:** Docker, Docker Compose

---

## Getting Started

### Prerequisites

- Docker and Docker Compose installed
- Optional: Set environment variable `FINNHUB_API_KEY` for ETL data enrichment

### Setup and Run

```bash
# Clone the repo
git clone https://github.com/justin-napolitano/finance-index-dashboard.git
cd finance-index-dashboard

# Start the full stack (DB, backend, ETL job)
docker compose up -d

# To stop the stack
docker compose down

# Run ETL job manually
make tickers-refresh

# Run backend API locally (hot reload)
make bash-backend
# inside container: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Access frontend
cd frontend
npm install
npm run dev
# Open http://localhost:3000
```

---

## Project Structure

```
finance-index-dashboard/
├── backend/               # Python backend source code
│   ├── app/               # FastAPI app, ETL modules, DB models
│   ├── ops/               # DB audit and fix utilities
│   ├── migrations/        # Alembic migration scripts
│   ├── docker-compose.yml # Docker compose for backend and DB
│   └── makefile           # Common commands for dev and maintenance
├── db/                    # Database persistent data volume
├── frontend/              # React/Next.js frontend
│   ├── pages/             # React pages for indices and tickers
│   ├── package.json       # Frontend dependencies and scripts
│   └── next.config.js     # Next.js configuration
├── context.txt            # Assorted notes (assumed)
├── roadmap.txt            # Project roadmap and future tasks
└── docker-compose.yml     # Root compose file (if present)
```

---

## Future Work / Roadmap

- Expand index definitions beyond default "momentum-10" strategy
- Add more comprehensive signal computations and technical indicators
- Improve frontend UI/UX with richer charts and filtering
- Integrate authentication and user-specific portfolio tracking
- Enhance ETL robustness and add alternative data sources
- Add automated testing and CI/CD pipelines
- Optimize database queries and indexing strategies

---

*Note: Some assumptions were made regarding missing documentation based on code and file structure.*
