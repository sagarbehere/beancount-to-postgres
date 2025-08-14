# beancount-to-sql

Import [Beancount](https://github.com/beancount/beancount) accounting data into PostgreSQL for advanced analysis and reporting.

## Features

- **Idempotent imports** - Safely re-run imports without duplicating data
- **Chunked processing** - Handle large files with progress tracking
- **Resume capability** - Continue interrupted imports from where they left off
- **Deterministic IDs** - Generate consistent transaction IDs across imports
- **Full Beancount support** - Handles transactions, postings, accounts, commodities, tags, links, and metadata

## Prerequisites

- Python 3.7+
- PostgreSQL 12+
- A PostgreSQL database with appropriate user permissions

## Installation

1. Clone the repository:
```bash
git clone https://github.com/sagarbehere/beancount-to-sql.git
cd beancount-to-sql
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up the database:
```bash
# First, create the database (if it doesn't exist)
psql -U postgres -c "CREATE DATABASE beancount;"

# Then run the complete setup script (creates schema, users, and permissions)
psql -U postgres -d beancount -f utils/full_db_setup.sql
```

4. Configure the importer:
```bash
cp config.yaml.example config.yaml
# Edit config.yaml with your database credentials
```

## Usage

### Basic Import
```bash
python import_beancount.py -c config.yaml -i transactions.beancount
```

### Multiple Files
```bash
python import_beancount.py -c config.yaml -i file1.beancount -i file2.beancount
```

### Check Database Sync

This command checks if the data in the database matches the contents of `transactions.beancount`. This is useful to determine if there have been changes to the local beancount data that are not reflected in the database.

```bash
python import_beancount.py -c config.yaml -i transactions.beancount --check
```

### Resume Interrupted Import

Use this to resume a previously interrupted import to the database

```bash
python import_beancount.py -c config.yaml -i transactions.beancount --resume
```

### Rebuild From Scratch

If the `--check` option above indicates that the data in the database is out of sync with the local beancount transactions, use this command to completely discard all data in the Postgres db and re-upload from `transactions.beancount`.

```bash
python import_beancount.py -c config.yaml -i transactions.beancount --rebuild
```

### Dry Run (Preview Changes)
```bash
python import_beancount.py -c config.yaml -i transactions.beancount --dry-run
```

## Command-Line Options

| Option | Description |
|--------|-------------|
| `-c, --config-file` | Path to YAML configuration file (required) |
| `-i, --input-file` | Beancount file(s) to import (required, can specify multiple) |
| `--check` | Verify if data in db is in sync with source files |
| `--rebuild` | Delete all data in db and reimport from scratch |
| `--dry-run` | Preview changes without modifying database |
| `--resume` | Skip already-imported transactions. Used to continue an interrupted previous run |
| `--rollback-partial` | Remove partial import data before starting |
| `-v, --verbose` | Enable debug logging |

## Configuration

Edit `config.yaml` to customize:

- **Database connection** - Host, port, database name, credentials
- **Import behavior** - Duplicate handling, chunk size
- **Logging** - Log level, file output, append/overwrite mode

See `config.yaml.example` for all available options.

## Database Schema

- See `dev-docs/db-schema.md` and `utils/fulldb_setup.sql`.

## License

This project is licensed under the GNU General Public License v2.0 - see the LICENSE file for details.