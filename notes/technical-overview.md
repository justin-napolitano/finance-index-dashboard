---
slug: github-finance-index-dashboard-note-technical-overview
id: github-finance-index-dashboard-note-technical-overview
title: Finance Index Dashboard
repo: justin-napolitano/finance-index-dashboard
githubUrl: https://github.com/justin-napolitano/finance-index-dashboard
generatedAt: '2025-11-24T18:36:03.344Z'
source: github-auto
summary: >-
  The Finance Index Dashboard is a Python app for tracking financial indices
  dynamically. It pulls data through an ETL pipeline, computes signals, and
  handles automated index rebalancing. The backend uses FastAPI, with PostgreSQL
  for data storage, while the frontend is built with React and Next.js.
tags: []
seoPrimaryKeyword: ''
seoSecondaryKeywords: []
seoOptimized: false
topicFamily: null
topicFamilyConfidence: null
kind: note
entryLayout: note
showInProjects: false
showInNotes: true
showInWriting: false
showInLogs: false
---

The Finance Index Dashboard is a Python app for tracking financial indices dynamically. It pulls data through an ETL pipeline, computes signals, and handles automated index rebalancing. The backend uses FastAPI, with PostgreSQL for data storage, while the frontend is built with React and Next.js.

## Key Components
- **Backend:** FastAPI, SQLAlchemy, and Alembic for DB migrations
- **Database:** PostgreSQL
- **Frontend:** React, Next.js
- **Containerization:** Docker

## Quick Start
1. Clone the repo:
    ```bash
    git clone https://github.com/justin-napolitano/finance-index-dashboard.git
    cd finance-index-dashboard
    ```

2. Run the stack:
    ```bash
    docker compose up -d
    ```

3. For manual ETL run:
    ```bash
    make tickers-refresh
    ```

4. Access frontend:
    ```bash
    cd frontend
    npm run dev
    # Open http://localhost:3000
    ```

## Gotchas
- Set `FINNHUB_API_KEY` for enriched data.
- Use `make bash-backend` for hot reloading the API during development.
