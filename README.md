# 🏀 NBA Stats ETL Pipeline

> A production-grade Extract-Transform-Load pipeline for NBA statistics combining high-performance data processing with intelligent workflow orchestration.

[![Python 3.13+](https://img.shields.io/badge/Python-3.13%2B-blue.svg)](https://www.python.org)
[![Polars](https://img.shields.io/badge/Polars-1.35%2B-FF6B35.svg)](https://www.pola-rs.com)
[![Prefect](https://img.shields.io/badge/Prefect-3.6%2B-00A9FF.svg)](https://www.prefect.io)
[![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900.svg)](https://aws.amazon.com/s3)

## ✨ Features

This project delivers a robust, scalable ETL solution for NBA data:

- **⚡ High-Performance Processing** - Leverages Polars for vectorized, parallel data transformations
- **🎯 Intelligent Orchestration** - Prefect manages workflow dependencies, monitoring, and resilience
- **☁️ Cloud-Native** - Seamless integration with AWS S3 for scalable data storage
- **🧪 Fully Tested** - Comprehensive test suite with pytest covering all components
- **🛠️ Developer-Friendly** - Clean architecture with modular, reusable components
- **📊 Player & Team Analytics** - Automated aggregation of season-level statistics

## 🚀 Quick Start

### Prerequisites

- **Python 3.13** or higher
- **pip** or virtual environment manager

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd nba-prefect-polars
   ```

2. **Set up virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install project with dependencies**
   ```bash
   pip install -e ".[dev]"
   ```

### Running the Pipeline

Execute the complete ETL workflow:
```bash
python src/main.py
```

This triggers the orchestrated Prefect flow, processing player and team season statistics in parallel.

## Quick Start

### Prerequisites

- Python 3.13 or higher
- Virtual environment management (venv, conda, or similar)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd nba-prefect-polars
   ```

2. **Create and activate a virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -e ".[dev]"
   ```

### Running the ETL

Execute the main orchestrated workflow:
```bash
python src/main.py
```

This will run the season statistics ETL pipeline for both players and teams.

## 🛠️ Development

### Quick Commands

Run tasks easily with Make:

```bash
make tests          # Run test suite with pytest
make format         # Format code with Ruff
make check          # Lint and style analysis
make fix-format     # Auto-fix formatting issues
make clean-target   # Remove output artifacts
```

### Testing

Execute the comprehensive test suite:
```bash
make tests
```

## 📁 Project Architecture

### Directory Tree

```
nba-prefect-polars/
├── 📄 src/                        ← Core Application Logic
│   ├── main.py                    # Prefect flow orchestration entry point
│   ├── config/                    # Configuration & credentials
│   │   ├── bucket.py              # AWS S3 bucket definitions
│   │   └── parameters.yml         # Runtime parameters
│   ├── players/                   # Player statistics pipeline
│   │   └── seasons/               # Season-level aggregation
│   │       ├── processor.py       # Polars transformation logic
│   │       └── task.py            # Prefect task definitions
│   ├── teams/                     # Team statistics pipeline
│   │   └── seasons/               # Season-level aggregation
│   │       ├── processor.py       # Polars transformation logic
│   │       └── task.py            # Prefect task definitions
│   └── games/                     # Game-level data processing
│
├── 🧪 tests/                      ← Comprehensive Test Suite
│   ├── players/seasons/           # Player pipeline tests
│   ├── teams/                     # Team pipeline tests
│   └── conftest.py                # Pytest shared fixtures
│
├── 📦 target/                     ← Processing Output
│   ├── test_get_player_season_stats/
│   └── test_get_team_season_stats/
│
├── ⚙️ conf/                       ← Configuration Files
├── 📋 pyproject.toml              # Dependencies & project metadata
├── 🔨 Makefile                    # Development commands
└── 📖 README.md                   # This file
```

### Module Responsibilities

| Module | Role |
|--------|------|
| **src/players/seasons/** | Extract and transform player season statistics with Polars |
| **src/teams/seasons/** | Process and aggregate team season performance metrics |
| **src/config/** | Manage AWS credentials and application settings |
| **src/games/** | Handle game-level scope and filtering logic |
| **tests/** | Mirror source structure with comprehensive unit tests |
| **target/** | Store processed results and test artifacts locally |

## 🔄 Data Pipeline

### End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    NBA DATA SOURCES                             │
└────────────────┬────────────────┬────────────────┬──────────────┘
                 │                │                │
        ╔════════▼═════╗  ╔═══════▼═════╗  ╔═════▼════════╗
        ║   Players    ║  ║    Teams    ║  ║    Games     ║
        ║   Extract    ║  ║   Extract   ║  ║   Extract    ║
        ╚════════╤═════╝  ╚═══════╤═════╝  ╚═════╤════════╝
                 │                │              │
        ╔════════▼══════════════╗  │             │
        ║  POLARS Transform     ║  │             │
        ║  (Vectorized, Parallel) ║  │             │
        ╚════════╤══════════════╝  │             │
                 │        ╔════════▼═════════╗   │
                 └────────║  POLARS Transform║   │
                          ║  (Team Stats)    ║   │
                          ╚════════╤═════════╝   │
                                   │             │
        ┌──────────────────────────┼─────────────┘
        │                          │
        ▼                          ▼
   ╔═════════════════════════════════════╗
   ║  PREFECT ORCHESTRATION               ║
   ║  season_stats() Flow                 ║
   ║  ├─ get_player_season_stats()        ║
   ║  └─ get_team_season_stats()          ║
   ╚═════════════════┬═══════════════════╝
                     │
        ╔════════════▼════════════╗
        ║   AWS S3 Destination    ║
        ║  (Production Storage)   ║
        ╚═════════════════════════╝
```

### Processing Stages

| Stage | Technology | Purpose |
|-------|-----------|---------|
| **Extract** | boto3, fsspec | Ingest NBA data from external APIs/sources |
| **Transform** | Polars | Vectorized aggregation, validation, and enrichment |
| **Orchestrate** | Prefect | Manage dependencies, error handling, monitoring |
| **Load** | S3, PyArrow | Persist processed data to cloud storage |

### Architecture Highlights

- **Parallel Processing**: Player and team stats are processed concurrently for efficiency
- **Type-Safe**: Polars schemas ensure data integrity throughout the pipeline
- **Observable**: Prefect provides full traceability, logging, and monitoring
- **Resilient**: Built-in retry logic and error handling via Prefect
- **Modular**: Clean separation between extraction, transformation, and loading concerns

## ⚙️ Configuration

- **Runtime Parameters**: `conf/parameters.yml` defines pipeline behavior
- **AWS Integration**: S3 bucket endpoints configured in `src/config/bucket.py`
- **Environment**: Load secrets from environment variables for security

## 📚 Tech Stack

### Core Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| **Polars** | 1.35.2+ | High-performance DataFrame operations |
| **Prefect** | 3.6.2+ | Workflow orchestration & monitoring |
| **boto3** | 1.40.74+ | AWS S3 integration |
| **PyArrow** | 22.0+ | Data serialization & interop |
| **fsspec** | 2025.10+ | Filesystem abstraction layer |
| **loguru** | 0.7.3+ | Structured logging |

### Development Tools

| Tool | Purpose |
|------|---------|
| **pytest** | Unit & integration testing |
| **Ruff** | Fast Python linting & formatting |

## 💡 Use Cases

- **Sports Analytics**: Aggregate and analyze NBA player/team performance
- **Data Warehousing**: Load processed stats into data lakes
- **Reporting**: Generate season summaries and comparisons
- **ML Pipelines**: Prepare clean data for predictive models
- **Real-time Dashboards**: Feed metrics to BI tools

## 📊 Example Output

After running the pipeline, processed files are available:
- `target/test_get_player_season_stats/` - Player statistics
- `target/test_get_team_season_stats/` - Team statistics

All data is available in cloud storage (S3) for production use.

---

**Built with attention to performance, reliability, and developer experience.** 🚀
