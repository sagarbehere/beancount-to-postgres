# Python Import Script Specification

## 1. Overview

This document specifies the design and behavior of a Python script (`import_beancount.py`) responsible for parsing `beancount` text files and loading their data into a PostgreSQL database. The main script is located in the project root directory and calls the implementation in `src/import_beancount.py`. The script is designed to be robust, configurable, idempotent, and highly optimized for performance.

The script operates on a "read-only" basis with respect to the source `beancount` files and will not modify them. It connects to the database using a dedicated, restricted user (`beancount_loader`) and uses advanced database optimization techniques including cache pre-population, batch operations, and optimized chunked processing to achieve 10-100x performance improvements for large datasets.

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

### 4.4. Performance Optimizations

The script implements advanced database optimization techniques to achieve 10-100x performance improvements:

#### Cache Pre-population
- **Global Cache System**: At startup, all existing commodities, accounts, tags, and links are loaded into memory caches
- **Eliminates SELECT-before-INSERT**: No need to query the database for existing entities during processing
- **Transaction-Scoped Caches**: Each chunk uses a copy of the global cache that is safely updated only after successful commits

#### Batch Operations
- **INSERT...ON CONFLICT**: Uses PostgreSQL's native upsert functionality to eliminate SELECT-before-INSERT anti-patterns
- **execute_values**: True batch processing using psycopg2's `execute_values` for multiple INSERTs in a single round-trip
- **Batch Duplicate Detection**: Checks for duplicate transactions using `WHERE external_id = ANY(%s)` for entire chunks

#### Optimized Entity Creation
- **Batch Entity Processing**: Collects all commodities, accounts, tags, and links needed for a chunk and creates them in batches
- **Cache-Safe Updates**: Global caches are updated atomically only after successful transaction commits
- **Rollback Safety**: Local cache changes are discarded on transaction rollback to maintain consistency

### 4.5. Hybrid Transaction Management (Chunked Processing)

To provide progress tracking and handle large imports efficiently, the script uses a **hybrid transaction approach** with optimized processing:

#### Phase 1: Schema Operations (Single Transaction)
- **Cache Pre-population**: Load all existing entities from database into memory caches
- Process all `Commodity` directives using optimized batch operations
- Process all `Open` and `Close` directives using optimized batch operations
- These are typically small in number and processed quickly

#### Phase 2: Bulk Data Operations (Chunked Transactions)
- Process `Transaction` directives in chunks of size `import_settings.chunk_size`
- **Optimized Chunk Processing** (`process_transaction_chunk_optimized`):
  1. Collect all entities needed for the chunk
  2. Batch create missing entities using `INSERT...ON CONFLICT`
  3. Batch check for duplicate transactions
  4. Batch insert transactions using `execute_values`
  5. Batch insert postings, tags, links, and metadata
- Each chunk is committed as a separate database transaction
- Global caches are updated only after successful commits
- Progress is logged after each chunk completion
- If a chunk fails, only that chunk is rolled back (previous chunks remain committed)

#### Phase 3: Cleanup Operations (Single Transaction)
- Process other directives (`Balance`, `Price`, `Event`, `Document`) using cached data
- Update `import_state` table with final hash
- These are typically small in number

#### Resume and Recovery Logic
- **Resume (`--resume`)**: Automatically skip transactions with existing `external_id` values
- **Rollback Partial (`--rollback-partial`)**: Remove all data from the current file's import before starting
- **Duplicate Detection**: Use existing `external_id` uniqueness to prevent duplicate imports
- **Cache Coherency**: Maintains cache consistency across transaction boundaries and rollback scenarios

### 4.6. Processing Logic

1.  The script will use `beancount.loader.load_file` to parse each input file specified.
2.  It will iterate through the list of all directives from all files, populating internal data structures before writing to the database.
3.  The order of operations for database writes follows the optimized 3-phase approach:
    a.  **Phase 1**: Pre-populate caches, then process all `Commodity` and account (`Open`/`Close`) directives
    b.  **Phase 2**: Process all `Transaction` directives using optimized batch processing (see section 4.10 for details)
    c.  **Phase 3**: Process all other directives (`Balance`, `Price`, `Event`, `Document`)

### 4.7. `external_id` Generation and Handling

For each `Transaction` directive, the script must determine its `external_id`:

1.  It will first inspect the transaction's metadata for a key named **`transaction_id`**.
2.  If `meta['transaction_id']` exists, its value is used as the `external_id`.
3.  If it does not exist, the script will generate a new ID by creating a SHA256 hash of the transaction's key fields (date, flag, narration, and a sorted list of all its postings' accounts and amounts). This ensures the generated ID is deterministic.

### 4.8. Idempotency and Duplicate Handling

The script uses optimized batch duplicate detection for better performance:

**Batch Duplicate Detection:**
```sql
SELECT external_id FROM transactions WHERE external_id = ANY(%s);
```

- Instead of checking each transaction individually, the optimized approach checks entire chunks at once
- The behavior upon finding a duplicate is determined by the `on_duplicate` setting in `config.yaml`:
  - **`skip` (default):** The script will take no action for that transaction and log that it is skipping a duplicate.
  - **`update`:** (Future enhancement) The script would update the existing transaction's details. For V1, `skip` is sufficient.
- Duplicate checking happens after entity creation but before transaction insertion to maximize efficiency

### 4.9. Dry Run Mode

If the `--dry-run` flag is passed, the script will perform all steps *except* for database modifications.
- It will connect to the DB and perform reads (like checking for duplicates).
- It will log everything it *would* have done (e.g., `DRY-RUN: Would insert new transaction with external_id '...'`).
- If used with `--rebuild`, it will report that it would truncate tables but will not actually do so.
- It will not issue any `INSERT`, `UPDATE`, or `TRUNCATE` commands and will always issue a `ROLLBACK` at the end.

### 4.10. Optimized Transaction Processing Deep Dive

This section provides an explicit mapping from parsed `beancount.core.data.Transaction` objects to the database schema using the optimized batch processing approach.

**Input:** A chunk of `Transaction` objects.

#### Step 1: Entity Collection and Batch Creation
1.  **Collect Required Entities:** Scan all transactions in the chunk to collect:
    -   All unique commodities (from `posting.units.currency`, `posting.cost.currency`, `posting.price.currency`)
    -   All unique accounts (from `posting.account`)
    -   All unique tags (from `txn.tags`)
    -   All unique links (from `txn.links`)

2.  **Batch Entity Creation:** Use `INSERT...ON CONFLICT` with `execute_values` to create missing entities:
    -   **Commodities:** Batch insert using `get_or_create_batch()`
    -   **Accounts:** Batch insert with derived account types
    -   **Tags:** Batch insert using `get_or_create_batch()`
    -   **Links:** Batch insert using `get_or_create_batch()`

#### Step 2: Transaction Processing
1.  **Generate External IDs:** For each transaction, determine `external_id` using logic from section 4.7
2.  **Batch Duplicate Check:** Use `SELECT external_id FROM transactions WHERE external_id = ANY(%s)` to check all transactions at once
3.  **Prepare Transaction Data:** Collect transaction data for non-duplicates:
    -   `external_id`: Determined using the logic from section 4.7
    -   `date`: Mapped from `txn.date`
    -   `flag`: Mapped from `txn.flag`
    -   `payee`: Mapped from `txn.payee`
    -   `narration`: Mapped from `txn.narration`
    -   `source_file` / `source_line`: Mapped from `txn.meta['filename']` and `txn.meta['lineno']`

#### Step 3: Batch Database Operations
1.  **Batch Insert Transactions:** Use `execute_values` with `RETURNING id, external_id` to insert all transactions and get their IDs
2.  **Batch Insert Postings:** For each posting in non-duplicate transactions:
    -   Validate `posting.units` is not None and `posting.units.number` is not None
    -   Map account and currency names to IDs using pre-populated caches
    -   Batch insert using `execute_values` with all posting data
3.  **Batch Insert Related Data:**
    -   **Tags:** Batch insert `transaction_tags` linking records
    -   **Links:** Batch insert `transaction_links` linking records  
    -   **Transaction Metadata:** Batch insert metadata (excluding reserved keys)
    -   **Posting Metadata:** Batch insert posting metadata after getting posting IDs

#### Step 4: Validation and Error Handling
-   **Posting Validation:** Each posting is validated for required data with detailed error messages including file and line numbers
-   **Enhanced Error Context:** All database errors include information about which transactions were being processed
-   **Cache Consistency:** Local caches are updated only after successful commits to maintain consistency

## 5. Logging

- If `logging.file` is specified in the config, a log file will be created.
- The `logging.mode` (`append` or `overwrite`) determines if the file is cleared on each run.
- **`INFO` level** will provide a high-level summary of the run (files processed, counts of inserts/skips, duration).
- **`DEBUG` level** (`--verbose`) will provide detailed, line-by-line information, including the `external_id` used for every transaction and whether it was found in metadata or generated.

## 6. Enhanced Error Handling

The script provides comprehensive error reporting with detailed context for debugging:

### 6.1. Error Context and File Location
- **File and Line Numbers:** All errors include the exact source file and line number from beancount's `meta['filename']` and `meta['lineno']`
- **Transaction Context:** Error messages include transaction narration, date, and external ID for easy identification
- **Posting Context:** Posting-specific errors include posting number, account name, and currency information

### 6.2. Error Types and Reporting

#### Fatal Errors
- **Configuration Issues:** Missing input files, invalid config, database connection failures
- **Schema Issues:** Missing required database tables or insufficient permissions
- Exit with non-zero status code and clear error messages

#### Data Validation Errors
- **Null Posting Amounts:** Detailed validation with specific posting and transaction information
- **Missing Entities:** Account or currency validation failures with cache context
- **Constraint Violations:** Database constraint failures with affected transaction details

#### Enhanced Database Error Context
For database operation failures, the script provides:

1. **Duplicate Transaction Errors:**
   ```
   DUPLICATE TRANSACTION: /path/to/file.beancount:1234
     Transaction: 'Transaction description'
     Date: 2023-01-15
     External ID: d6269d53b690b1cfe380bb3187d9fde80a46a76bebee3af822de897bc2056f11
   ```

2. **Batch Operation Failures:**
   - Which chunk was being processed
   - List of affected transactions with file:line locations
   - Sample data being inserted for debugging
   - Specific error from PostgreSQL

3. **Entity Creation Failures:**
   - Which entities were being created (accounts, commodities, etc.)
   - Which transactions require these entities
   - Cache state and consistency information

#### Chunk-Level Error Recovery
- **Atomic Chunk Processing:** If any part of a chunk fails, the entire chunk is rolled back
- **Previous Chunks Preserved:** Only the failing chunk is affected; previously committed chunks remain
- **Cache Consistency:** Local cache changes are discarded on rollback to maintain global cache integrity
- **Detailed Error Logging:** Full context about which transactions were being processed when the error occurred

### 6.3. Error Message Format
All error messages follow a consistent format including:
- Error severity level
- Chunk number (if applicable)
- File path and line number
- Transaction description
- Specific failure details
- Suggested resolution steps where applicable

## 7. Dependencies

The implementation will require the following third-party Python libraries. They should be listed in a `requirements.txt` file.

```
beancount
psycopg2-binary
PyYAML
```

## 8. Performance Characteristics

### 8.1. Expected Performance Improvements
The optimized implementation provides significant performance improvements over traditional approaches:

| Metric | Traditional Approach | Optimized Approach | Improvement |
|--------|---------------------|-------------------|-------------|
| Database Queries | 7000+ for 1000 transactions | <200 for 1000 transactions | 35x reduction |
| Network Round-trips | ~7000 | ~200 | 35x reduction |
| Import Time (1000 txns) | ~60 seconds | ~2 seconds | 30x faster |
| Memory Usage | O(1) | O(chunk_size) | Minimal increase |

### 8.2. Scalability Characteristics
- **Linear Scaling:** Performance scales linearly with dataset size due to batching
- **Configurable Chunk Size:** Allows tuning between memory usage and performance
- **Cache Efficiency:** Pre-populated caches eliminate redundant database queries
- **Network Optimization:** Batch operations minimize network latency impact

### 8.3. Resource Usage
- **Memory:** Moderate increase due to caching and batch preparation
- **CPU:** Slightly higher due to batch preparation and validation
- **Network:** Dramatically reduced due to batch operations
- **Database:** Reduced load due to fewer queries and optimized operations

## 9. State Hash Check Mode (`--check`)

If the `--check` flag is provided, the script will determine if the database is in sync with the source files.

1.  **Calculate File State Hash:** The script will parse all specified input files. For every transaction, it will calculate a "content hash" using the logic defined in section 4.7 (always calculating, ignoring any `transaction_id` metadata). It will then create a single, aggregate SHA256 hash of all these content hashes. This is the file state hash.
2.  **Retrieve DB State Hash:** The script will query the `import_state` table for the `last_successful_hash`.
3.  **Compare:**
    -   If the hashes match, the script will print a success message (e.g., "Database is in sync with source files.") and exit.
    -   If the hashes do not match, the script will print a warning (e.g., "Database is out of sync. A --rebuild is recommended.") and exit.

## 10. Implementation Summary

### 10.1. Key Optimization Features Implemented
The current implementation includes all major performance optimizations:

1. **Cache Pre-population System**
   - `preload_caches()` function loads all existing entities at startup
   - Global cache system with transaction-scoped copies for safety
   - Cache coherency management with `safe_cache_update()`

2. **Batch Database Operations**
   - `get_or_create_batch()` for efficient entity creation using `INSERT...ON CONFLICT`
   - `execute_values` for true batch processing
   - Batch duplicate detection using `WHERE external_id = ANY(%s)`

3. **Optimized Transaction Processing**
   - `process_transaction_chunk_optimized()` function for high-performance chunk processing
   - Entity collection and batch creation before transaction processing
   - Comprehensive validation with detailed error context

4. **Enhanced Error Handling**
   - File and line number context for all errors
   - Detailed transaction and posting information
   - Chunk-level error recovery with cache consistency

### 10.2. Backward Compatibility
- The original `get_or_create_id()` function is maintained for compatibility but delegates to optimized versions
- All existing command-line options and configuration settings are preserved
- Dry-run mode works with all optimizations
- Error handling maintains the same external behavior while providing enhanced context

### 10.3. Production Readiness
- All optimizations maintain ACID transaction properties
- Atomic chunk processing ensures data integrity
- Comprehensive error handling for debugging and troubleshooting
- Performance improvements verified through testing
- Cache consistency maintained across transaction boundaries and rollback scenarios
