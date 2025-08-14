# Python Import Script Specification

## 1. Overview

This document specifies the design and behavior of a Python script (`import_beancount.py`) responsible for parsing `beancount` text files and loading their data into a PostgreSQL database. The main script is located in the project root directory and calls the implementation in `src/import_beancount.py`. The script is designed to be robust, configurable, and idempotent.

The script operates on a "read-only" basis with respect to the source `beancount` files and will not modify them. It connects to the database using a dedicated, restricted user (`beancount_loader`) and wraps all database modifications for a given run within a single transaction to ensure data integrity.

## 2. Configuration (`config.yaml`)

The script is configured via a YAML file (e.g., `config.yaml`). This file centralizes all environment-specific and behavior-tuning settings.

```yaml
# ---------------------------------
# Database Connection Configuration
# ---------------------------------
# Details for the 'beancount_loader' user, which will run the import.
database:
  host: "127.0.0.1"
  port: 5432
  dbname: "beancount"
  user: "beancount_loader"
  password: "your-strong-password-here"

# ---------------------------------
# Import Behavior Configuration
# ---------------------------------
import_settings:
  # What to do when a transaction with a duplicate 'external_id' is found.
  # Options: 'skip' (default) or 'update'.
  on_duplicate: skip
  
  # Number of transactions to process in each chunk for progress tracking
  # and partial commit capability. Larger chunks = faster processing,
  # smaller chunks = more granular progress and better resume capability.
  chunk_size: 500

# ---------------------------------
# Logging Configuration
# ---------------------------------
logging:
  # Logging level for console and file output.
  # Options: 'DEBUG', 'INFO', 'WARNING', 'ERROR'
  level: INFO

  # Optional: Specify a file to write logs to.
  # If omitted, logs will only be sent to the console.
  file: beancount_import.log

  # How to handle the log file on each new run.
  # Options: 'append' (default) or 'overwrite'.
  mode: append
```

## 3. Command-Line Interface (CLI)

The script shall be executed from the command line. It will use Python's `argparse` library to handle arguments.

**Usage:**
```bash
python import_beancount.py -c config.yaml -i file1.beancount -i file2.beancount [OPTIONS]
```

**Arguments:**

| Short Form | Long Form         | Required? | Description                                                                                                     |
|------------|-------------------|-----------|-----------------------------------------------------------------------------------------------------------------|
| `-c`       | `--config-file`   | Yes       | Path to the YAML configuration file.                                                                            |
| `-i`       | `--input-file`    | Yes       | Path to a source `beancount` file to import. Use multiple `-i` flags to specify multiple files.                |
|            | `--check`         | No        | If present, runs a check to see if the database is in sync with the source files. See section 4.4.              |
|            | `--rebuild`       | No        | If present, triggers the "Rebuild from Source" mode. See section 4.3 for details.                             |
|            | `--dry-run`       | No        | If present, the script will not commit any changes to the database. It will only report what it would do.         |
|            | `--resume`        | No        | If present, automatically skip transactions that have already been imported (based on external_id).              |
|            | `--rollback-partial` | No     | If present, removes all data from a failed partial import before starting fresh.                                |
| `-v`       | `--verbose`       | No        | If present, sets the logging level to `DEBUG`, overriding the config file setting.                              |

**Examples:**
```bash
# Single file import
./import_beancount.py -c config.yaml -i transactions.beancount

# Multiple file import
./import_beancount.py -c config.yaml -i file1.beancount -i file2.beancount -i file3.beancount

# Dry run with multiple files
./import_beancount.py -c config.yaml -i file1.beancount -i file2.beancount --dry-run

# Rebuild from scratch
./import_beancount.py -c config.yaml -i transactions.beancount --rebuild

# Check if database is in sync
./import_beancount.py -c config.yaml -i transactions.beancount --check

# Resume a failed import (skip already-imported transactions)
./import_beancount.py -c config.yaml -i transactions.beancount --resume

# Clean up a partial import and start fresh
./import_beancount.py -c config.yaml -i transactions.beancount --rollback-partial

# Alternative: Call with python explicitly
python import_beancount.py -c config.yaml -i transactions.beancount
```

## 4. Core Logic and Behavior

### 4.1. Initialization
1.  Parse all command-line arguments.
2.  Load and validate the specified `config.yaml` file.
3.  Set up logging based on the `logging` configuration and the `--verbose` flag.

### 4.2. Pre-flight Checks
1.  Attempt to connect to the PostgreSQL database using the credentials from the config file.
2.  On successful connection, perform a check to verify that the required tables (e.g., `accounts`, `transactions`, `postings`) exist in the database. This can be done by querying `information_schema.tables`.
3.  If the tables do not exist, the script must exit with a clear error message instructing the user to run the `create_schema.sql` script first.

### 4.3. Rebuild Mode (`--rebuild`)

If the `--rebuild` flag is provided, the script will perform a full data refresh. This mode has special requirements:

1.  **Permissions Requirement:** This is a destructive operation that requires `TRUNCATE` privileges. The script should be run with database credentials that have these privileges (e.g., the database owner `sagar`), not the restricted `beancount_loader` user.
2.  **Safety Confirmation:** Before proceeding, the script **must** print a clear warning to the console and require the user to interactively confirm the action by typing `yes`.
    > **Example Prompt:** `WARNING: The --rebuild flag will permanently delete all data from the target tables. Are you sure you want to continue? (yes/no)`
3.  **Truncation Logic:** If confirmed, the script will execute a `TRUNCATE` command on all relevant tables. This command **must** include the `RESTART IDENTITY` and `CASCADE` clauses to ensure all data is wiped and all auto-incrementing ID counters are reset.
    ```sql
    TRUNCATE table1, table2, ... RESTART IDENTITY CASCADE;
    ```
4.  After the truncation is complete, the script proceeds to the regular import process for all specified input files.

### 4.4. Hybrid Transaction Management (Chunked Processing)

To provide progress tracking and handle large imports efficiently, the script uses a **hybrid transaction approach**:

#### Phase 1: Schema Operations (Single Transaction)
- Process all `Commodity` directives  
- Process all `Open` and `Close` directives
- These are typically small in number and processed quickly

#### Phase 2: Bulk Data Operations (Chunked Transactions)
- Process `Transaction` directives in chunks of size `import_settings.chunk_size`
- Each chunk is committed as a separate database transaction
- Progress is logged after each chunk completion
- If a chunk fails, only that chunk is rolled back (previous chunks remain committed)

#### Phase 3: Cleanup Operations (Single Transaction)
- Process other directives (`Balance`, `Price`, `Event`, `Document`)
- Update `import_state` table with final hash
- These are typically small in number

#### Resume and Recovery Logic
- **Resume (`--resume`)**: Automatically skip transactions with existing `external_id` values
- **Rollback Partial (`--rollback-partial`)**: Remove all data from the current file's import before starting
- **Duplicate Detection**: Use existing `external_id` uniqueness to prevent duplicate imports

### 4.5. Processing Logic

1.  The script will use `beancount.loader.load_file` to parse each input file specified.
2.  It will iterate through the list of all directives from all files, populating internal data structures before writing to the database.
3.  The order of operations for database writes should be:
    a.  Process all `Commodity` directives.
    b.  Process all `Open` and `Close` directives.
    c.  Process all `Transaction` directives (see section 4.9 for a detailed breakdown).
    d.  Process all other directives (`Balance`, `Price`, etc.).

### 4.6. `external_id` Generation and Handling

For each `Transaction` directive, the script must determine its `external_id`:

1.  It will first inspect the transaction's metadata for a key named **`transaction_id`**.
2.  If `meta['transaction_id']` exists, its value is used as the `external_id`.
3.  If it does not exist, the script will generate a new ID by creating a SHA256 hash of the transaction's key fields (date, flag, narration, and a sorted list of all its postings' accounts and amounts). This ensures the generated ID is deterministic.

### 4.7. Idempotency and Duplicate Handling

Before inserting a transaction, the script will use its `external_id` to check if it already exists in the database.

```sql
SELECT id FROM transactions WHERE external_id = %s;
```

- The behavior upon finding a duplicate is determined by the `on_duplicate` setting in `config.yaml`:
  - **`skip` (default):** The script will take no action for that transaction and log that it is skipping a duplicate.
  - **`update`:** (Future enhancement) The script would update the existing transaction's details. For V1, `skip` is sufficient.

### 4.8. Dry Run Mode

If the `--dry-run` flag is passed, the script will perform all steps *except* for database modifications.
- It will connect to the DB and perform reads (like checking for duplicates).
- It will log everything it *would* have done (e.g., `DRY-RUN: Would insert new transaction with external_id '...'`).
- If used with `--rebuild`, it will report that it would truncate tables but will not actually do so.
- It will not issue any `INSERT`, `UPDATE`, or `TRUNCATE` commands and will always issue a `ROLLBACK` at the end.

### 4.9. Transaction Processing Deep Dive

This section provides an explicit mapping from a parsed `beancount.core.data.Transaction` object to the database schema.

**Input:** A single `Transaction` object, hereafter referred to as `txn`.

1.  **Create the `transactions` record:**
    -   `external_id`: Determined using the logic from section 4.6.
    -   `date`: Mapped from `txn.date`.
    -   `flag`: Mapped from `txn.flag`.
    -   `payee`: Mapped from `txn.payee`.
    -   `narration`: Mapped from `txn.narration`.
    -   `source_file` / `source_line`: Mapped from `txn.meta['filename']` and `txn.meta['lineno']`.
    -   After inserting this record, retrieve its auto-generated internal `id` for use in child records.

2.  **Process Tags, Links, and Metadata:**
    -   **Tags:** For each `tag` in `txn.tags`, find-or-create the record in the `tags` table, then create a linking record in `transaction_tags` using the transaction's internal `id`.
    -   **Links:** For each `link` in `txn.links`, find-or-create the record in the `links` table, then create a linking record in `transaction_links`.
    -   **Transaction Metadata:** For each `key`, `value` pair in `txn.meta` (excluding reserved keys like `filename`, `lineno`, `transaction_id`), create a record in the `transaction_metadata` table.

3.  **Process Postings:**
    -   For each `posting` object in `txn.postings`:
        a.  **Create the `postings` record:**
            -   `transaction_id`: The internal `id` of the parent transaction.
            -   `flag`: Mapped from `posting.flag` (if present, otherwise NULL).
            -   `account_id`: The internal `id` corresponding to the `posting.account` string.
            -   `amount`: Mapped from `posting.units.number`.
            -   `currency_id`: The internal `id` corresponding to the `posting.units.currency` string.
            -   `cost_amount` / `cost_currency_id`: Mapped from `posting.cost.number` and `posting.cost.currency` if `posting.cost` is not `None`.
            -   `price_amount` / `price_currency_id`: Mapped from `posting.price.number` and `posting.price.currency` if `posting.price` is not `None`.
        b.  **Create `posting_metadata` records:** After inserting the posting and retrieving its internal `id`, iterate through the `posting.meta` dictionary. For each key-value pair, create a record in the `posting_metadata` table.

## 5. Logging

- If `logging.file` is specified in the config, a log file will be created.
- The `logging.mode` (`append` or `overwrite`) determines if the file is cleared on each run.
- **`INFO` level** will provide a high-level summary of the run (files processed, counts of inserts/skips, duration).
- **`DEBUG` level** (`--verbose`) will provide detailed, line-by-line information, including the `external_id` used for every transaction and whether it was found in metadata or generated.

## 6. Error Handling

- **Fatal Errors:** For issues like a missing input file, invalid config, or inability to connect to the database, the script will log the error and exit with a non-zero status code.
- **Data Errors:** For errors encountered while processing a specific directive in the file, the script will log the specific error (including file and line number), issue a `ROLLBACK` to cancel the entire transaction, and exit with a non-zero status code.

## 7. Dependencies

The implementation will require the following third-party Python libraries. They should be listed in a `requirements.txt` file.

```
beancount
psycopg2-binary
PyYAML
```

### 4.4. State Hash Check Mode (`--check`)

If the `--check` flag is provided, the script will determine if the database is in sync with the source files.

1.  **Calculate File State Hash:** The script will parse all specified input files. For every transaction, it will calculate a "content hash" using the logic defined in section 4.7 (always calculating, ignoring any `transaction_id` metadata). It will then create a single, aggregate SHA256 hash of all these content hashes. This is the file state hash.
2.  **Retrieve DB State Hash:** The script will query the `import_state` table for the `last_successful_hash`.
3.  **Compare:**
    -   If the hashes match, the script will print a success message (e.g., "Database is in sync with source files.") and exit.
    -   If the hashes do not match, the script will print a warning (e.g., "Database is out of sync. A --rebuild is recommended.") and exit.
