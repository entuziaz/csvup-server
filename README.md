# CSV Upload API

A FastAPI backend for uploading, validating, and storing transaction data from CSV files. Designed for easy onboarding, robust testing, and rapid development.

---

## Project Structure

- **main.py**  
  Entry point. Initializes FastAPI, sets up CORS, creates DB tables, and includes API routers.

- **app/core/**  
  - `database.py`: Handles SQLAlchemy DB engine, session, and base class. Loads DB config from `.env`.

- **app/uploads/**  
  - `models.py`: SQLAlchemy models (currently, the `Transaction` table).
  - `routers.py`: FastAPI endpoints for CSV upload and validation.
  - `schemas.py`: Pydantic response schemas.
  - `services.py`: Core logic for processing and merging CSV data into the DB.
  - `validators.py`: CSV column validation logic.


- **app/tests/**  
  - `conftest.py`: Pytest fixtures for DB and FastAPI client.
  - `test_*.py`: Unit and integration tests for uploads, performance, and duplicate handling.

- **Dockerfile**  
  Containerizes the app for production or local development.

- **docker-compose.yml**  
  Orchestrates the app (and optionally a Postgres DB) for local development.

- **.env**  
  Stores DB connection strings and secrets. **You must create or update this before running the app!**

---

## Quickstart (Local)

1. **Clone the repo**
   ```sh
   git clone <your-repo-url>
   cd csvup-server
   ```

2. **Create `.env` file**  
   Copy `.env.example` (if present) or create `.env` with at least:
   ```
   DATABASE_URL=postgresql://<user>:<password>@<host>:<port>/<dbname>
   POSTGRES_DB=<dbname>
   POSTGRES_USER=<user>
   POSTGRES_PASSWORD=<password>
   ```

3. **Install dependencies**
   ```sh
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

4. **Run the app**
   ```sh
   uvicorn main:app --reload
   ```
   - Docs available at [http://localhost:8000/](http://localhost:8000/)

---

## Quickstart (Docker Compose)

1. **Ensure your `.env` is set up** (see above).

2. **Start services**
   ```sh
   docker-compose up --build
   ```
   The API will be available at [http://localhost:8000/](http://localhost:8000/)

> **Note:**  
>
>   The default `docker-compose.yml` expects an external Postgres DB (yours sincerely uses one hosted by Render.com free tier).  
   To use a local Postgres container, uncomment the `db:` service and related lines in `docker-compose.yml`.

---

## Running Tests

- **With local Python:**
  ```sh
  pytest
  ```

- **With Docker Compose:**
  ```sh
  docker-compose exec app pytest
  ```

Tests cover:
- Upload endpoint (valid, invalid, empty, malformed CSVs)
- Duplicate transaction handling
- Performance and edge cases

---

## Contribution Tips

- **Folder meanings:**  
  - `core/`: Shared infra (DB, config)  
  - `uploads/`: All upload logic, models, and validation  
  - `tests/`: All test code and fixtures

- **Major files:**  
  - `main.py`: App entrypoint  
  - `database.py`: DB setup  
  - `models.py`: DB schema  
  - `routers.py`: API endpoints  
  - `services.py`: Business logic  
  - `validators.py`: Data validation

- **Before pushing:**  
  - Run all tests (`pytest`)
  - Check `.env` is not committed (see `.gitignore`)

---

## FAQ

- **Where do I add new endpoints?**  
  In [`app/uploads/routers.py`](app/uploads/routers.py).

- **How do I add new DB fields?**  
  Update [`app/uploads/models.py`](app/uploads/models.py) and run migrations (if using Alembic).

- **How do I test with a clean DB?**  
  The test suite uses an in-memory SQLite DB for isolation.

---

**Welcome aboard!**  
If you get stuck, check the code comments or ask a teammate.