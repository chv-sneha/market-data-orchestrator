# ============================================================
# Market Data Orchestrator — Makefile
# ============================================================
# Usage: Run `make <command>` from the project root directory.
# Example: make start
# ============================================================

.PHONY: start stop restart logs logs-webserver logs-scheduler status db db-query reset help

# ── Start ────────────────────────────────────────────────────
start:
	@echo "🐳 Building and starting all containers in the background..."
	@docker compose up --build -d
	@echo ""
	@echo "====================================================="
	@echo "  ✅ Pipeline is up and running!"
	@echo ""
	@echo "  🌐 Airflow UI  →  http://localhost:8080"
	@echo "     Username : admin"
	@echo "     Password : admin"
	@echo ""
	@echo "  💡 Tip: Cmd+Click (Mac) / Ctrl+Click (Windows)"
	@echo "     the link above to open it in your browser."
	@echo "====================================================="
	@echo ""

# ── Stop (keeps data) ────────────────────────────────────────
stop:
	@echo "🛑 Stopping all containers (data is preserved)..."
	@docker compose down
	@echo "✅ All containers stopped."

# ── Restart ──────────────────────────────────────────────────
restart:
	@echo "🔄 Restarting all containers..."
	@docker compose down
	@docker compose up --build -d
	@echo "✅ Restarted. Airflow UI → http://localhost:8080"

# ── Status ───────────────────────────────────────────────────
status:
	@echo "📦 Running containers:"
	@docker ps --filter "name=stock_pipeline" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# ── Logs ─────────────────────────────────────────────────────
logs:
	@docker compose logs -f

logs-webserver:
	@docker compose logs -f airflow-webserver

logs-scheduler:
	@docker compose logs -f airflow-scheduler

# ── Database ─────────────────────────────────────────────────
db:
	@echo "🗄️  Connecting to PostgreSQL..."
	@docker exec -it stock_pipeline_postgres psql -U stockuser -d stockdb

db-query:
	@echo "📊 Fetching the 10 most recent stock price rows..."
	@docker exec -it stock_pipeline_postgres psql -U stockuser -d stockdb -c "SELECT * FROM stock_prices ORDER BY timestamp DESC LIMIT 10;"

# ── Reset (wipes all data) ───────────────────────────────────
reset:
	@echo "⚠️  WARNING: This will delete ALL data (database volumes)."
	@echo "   Press Ctrl+C within 5 seconds to cancel..."
	@sleep 5
	@docker compose down -v
	@echo "✅ Hard reset complete. All containers and volumes removed."

# ── Help ─────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  Market Data Orchestrator — Available Commands"
	@echo "  ─────────────────────────────────────────────"
	@echo "  make start           Build & start all containers, print the UI link"
	@echo "  make stop            Stop all containers (data is kept)"
	@echo "  make restart         Stop and re-build everything"
	@echo "  make status          Show which containers are running"
	@echo "  make logs            Stream logs from all containers"
	@echo "  make logs-webserver  Stream Airflow webserver logs only"
	@echo "  make logs-scheduler  Stream Airflow scheduler logs only"
	@echo "  make db              Open an interactive PostgreSQL shell"
	@echo "  make db-query        Print the 10 most recent stock price rows"
	@echo "  make reset           ⚠️  Stop and wipe ALL data (hard reset)"
	@echo "  make help            Show this help message"
	@echo ""
