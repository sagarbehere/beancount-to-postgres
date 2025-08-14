#!/usr/bin/env python3

"""
Beancount to PostgreSQL Import Script

This script parses Beancount files and loads their data into a PostgreSQL database.
It is designed to be robust, configurable, and idempotent.

Usage:
    python import_beancount.py -c config.yaml -i file1.beancount file2.beancount [OPTIONS]
"""

import argparse
import logging
import sys
import os
import yaml
import psycopg2
import psycopg2.extras
import hashlib
from typing import Dict, Set, List, Any, Optional, Tuple
from collections import defaultdict
from beancount.loader import load_file
from beancount.core import data

from shared.transaction_id_generator import (
    TransactionIdGenerator,
    TransactionIdValidationError,
    add_transaction_id_to_beancount_transaction
)

# Set of all tables required by this application
REQUIRED_TABLES = {
    'commodities', 'accounts', 'transactions', 'postings', 'posting_metadata',
    'tags', 'transaction_tags', 'links', 'transaction_links',
    'transaction_metadata', 'balance_assertions', 'prices', 'events', 'documents',
    'import_state'
}

# Reserved metadata keys that should not be stored in metadata tables
RESERVED_METADATA_KEYS = {'filename', 'lineno', 'transaction_id'}


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Import Beancount data into a PostgreSQL database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -c config.yaml -i transactions.beancount
  %(prog)s -c config.yaml -i file1.beancount -i file2.beancount --dry-run
  %(prog)s -c config.yaml -i file1.beancount -i file2.beancount -i file3.beancount --rebuild
  %(prog)s -c config.yaml -i transactions.beancount --check
  %(prog)s -c config.yaml -i transactions.beancount --resume
  %(prog)s -c config.yaml -i transactions.beancount --rollback-partial
        """
    )
    
    parser.add_argument(
        "-c", "--config-file", 
        required=True, 
        help="Path to the YAML configuration file"
    )
    parser.add_argument(
        "-i", "--input-file", 
        required=True, 
        action='append',
        help="Path to a source beancount file to import (use multiple -i flags for multiple files)"
    )
    parser.add_argument(
        "--check", 
        action="store_true",
        help="Run a check to see if the database is in sync with the source files"
    )
    parser.add_argument(
        "--rebuild", 
        action="store_true",
        help="Trigger 'Rebuild from Source' mode. All existing data will be deleted"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Do not commit any changes to the database. Only report what would be done"
    )
    parser.add_argument(
        "--resume", 
        action="store_true",
        help="Skip transactions already imported (based on external_id)"
    )
    parser.add_argument(
        "--rollback-partial", 
        action="store_true",
        help="Remove partial import data before starting fresh"
    )
    parser.add_argument(
        "-v", "--verbose", 
        action="store_true",
        help="Set logging level to DEBUG, overriding the config file setting"
    )
    
    return parser.parse_args()


def validate_input_files(file_paths: List[str]) -> None:
    """Validate that all input files exist and are readable."""
    logging.info("Validating input files...")
    
    missing_files = []
    for file_path in file_paths:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
        elif not os.path.isfile(file_path):
            logging.error(f"Path exists but is not a file: {file_path}")
            sys.exit(1)
        elif not os.access(file_path, os.R_OK):
            logging.error(f"File exists but is not readable: {file_path}")
            sys.exit(1)
    
    if missing_files:
        logging.error("The following input files do not exist:")
        for file_path in missing_files:
            logging.error(f"  - {file_path}")
        sys.exit(1)
    
    logging.info(f"Successfully validated {len(file_paths)} input file(s)")


def load_config(config_path: str) -> Dict[str, Any]:
    """Load the YAML configuration file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logging.info(f"Successfully loaded configuration from {config_path}")
            return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found at: {config_path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML configuration file: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error loading configuration: {e}")
        sys.exit(1)


def setup_logging(level: str, log_file: Optional[str], mode: str) -> None:
    """Configure logging for the application."""
    log_level = logging.DEBUG if level.upper() == 'DEBUG' else logging.INFO
    
    # Create root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Clear any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s', 
        '%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        file_mode = 'w' if mode == 'overwrite' else 'a'
        file_handler = logging.FileHandler(log_file, mode=file_mode)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logging.info(f"Logging to file: {log_file} (mode: {file_mode})")
    
    logging.info("Logging configured successfully")


def connect_to_db(db_config: Dict[str, Any]) -> psycopg2.extensions.connection:
    """Connect to the PostgreSQL database and return the connection object."""
    try:
        conn = psycopg2.connect(**db_config)
        logging.info(f"Successfully connected to database '{db_config.get('dbname')}' on host '{db_config.get('host')}'")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error connecting to database: {e}")
        sys.exit(1)


def check_schema(conn: psycopg2.extensions.connection) -> bool:
    """Verify that the database schema contains all required tables."""
    logging.info("Performing database schema pre-flight check...")
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            existing_tables = {row[0] for row in cur.fetchall()}
        
        missing_tables = REQUIRED_TABLES - existing_tables
        
        if not missing_tables:
            logging.info("Schema check passed. All required tables exist.")
            return True
        else:
            logging.error("Schema check failed. The following required tables are missing:")
            for table in sorted(missing_tables):
                logging.error(f"  - {table}")
            logging.error("Please run the create_schema.sql script to set up the database correctly.")
            return False
            
    except psycopg2.Error as e:
        logging.error(f"An error occurred during schema check: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error during schema check: {e}")
        return False


def handle_rebuild(conn: psycopg2.extensions.connection, dry_run: bool = False) -> None:
    """Handle the --rebuild logic, including safety prompt and truncation."""
    if dry_run:
        logging.info("[DRY RUN] Would truncate all tables if not in dry run mode")
        return
    
    # Safety confirmation
    print("\n*** WARNING ***", file=sys.stderr)
    print("The --rebuild flag will permanently delete all data from the target tables.", file=sys.stderr)
    print("This action cannot be undone.", file=sys.stderr)
    print()
    
    try:
        confirm = input("Are you sure you want to continue? (yes/no): ")
    except (EOFError, KeyboardInterrupt):
        logging.info("Rebuild aborted by user (interrupted)")
        sys.exit(0)
    
    if confirm.lower() != 'yes':
        logging.info("Rebuild aborted by user")
        sys.exit(0)
    
    logging.info("Rebuild confirmed. Truncating all tables...")
    
    try:
        with conn.cursor() as cur:
            # Build truncate statement with all required tables
            tables_to_truncate = ", ".join(sorted(REQUIRED_TABLES))
            truncate_sql = f"TRUNCATE TABLE {tables_to_truncate} RESTART IDENTITY CASCADE"
            
            logging.debug(f"Executing: {truncate_sql}")
            cur.execute(truncate_sql)
        
        # Note: We don't commit here - this will be part of the main transaction
        logging.info("All tables truncated successfully")
        
    except psycopg2.Error as e:
        logging.error(f"An error occurred during table truncation: {e}")
        logging.error("Please check the database user's permissions. TRUNCATE privilege is required for --rebuild.")
        conn.rollback()
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error during table truncation: {e}")
        conn.rollback()
        sys.exit(1)


def calculate_transaction_content_hash(txn: data.Transaction, id_generator: TransactionIdGenerator = None) -> str:
    """Calculate a hash based on the transaction's content for state comparison."""
    # Use a fresh generator to ensure consistent recalculation regardless of collision state
    # This ensures the content hash is purely based on transaction content, not import order
    fresh_generator = TransactionIdGenerator()
    txn_with_id = add_transaction_id_to_beancount_transaction(
        transaction=txn,
        force_recalculate=True,
        strict_validation=False,  # Don't fail on validation issues during check
        id_generator=fresh_generator
    )
    return txn_with_id.meta['transaction_id']


def calculate_file_state_hash(entries: List[data.Directive], id_generator: TransactionIdGenerator, config: Dict[str, Any]) -> str:
    """Calculate the overall state hash for a list of Beancount entries."""
    content_hashes = []
    on_duplicate = config.get('import_settings', {}).get('on_duplicate', 'skip')
    seen_external_ids = set()
    
    for entry in entries:
        if isinstance(entry, data.Transaction):
            # Use new API to get external_id (respects existing metadata)
            txn_with_id = add_transaction_id_to_beancount_transaction(
                transaction=entry,
                force_recalculate=False,  # Use existing transaction_id if present
                strict_validation=False,
                id_generator=id_generator
            )
            external_id = txn_with_id.meta['transaction_id']
            
            # Only include if it wouldn't be skipped during import
            if on_duplicate == 'skip' and external_id in seen_external_ids:
                continue
            
            seen_external_ids.add(external_id)
            # For state hash, always recalculate to verify content
            content_hashes.append(calculate_transaction_content_hash(entry, id_generator))
    
    # Create aggregate hash
    sorted_hashes = "".join(sorted(content_hashes))
    state_hash = hashlib.sha256(sorted_hashes.encode('utf-8')).hexdigest()
    return state_hash


def handle_check(conn: psycopg2.extensions.connection, file_paths: List[str], id_generator: TransactionIdGenerator) -> None:
    """Handle the --check logic by comparing state hashes."""
    logging.info("Performing state hash check...")
    
    try:
        # Load and parse all files
        if len(file_paths) == 1:
            entries, errors, _ = load_file(file_paths[0])
        else:
            entries, errors, _ = load_file(file_paths)
        
        if errors:
            # Categorize errors by type (same logic as main import)
            critical_errors = []
            warnings = []
            
            for error in errors:
                error_type = type(error).__name__
                if error_type in ['PadError', 'DeprecatedError']:
                    warnings.append(error)
                else:
                    critical_errors.append(error)
            
            # Log warnings
            if warnings:
                logging.warning(f"Beancount parsing warnings during check (non-critical):")
                for warning in warnings:
                    source = warning.source
                    warning_type = type(warning).__name__
                    if source:
                        logging.warning(f"  - [{warning_type}] {source.get('filename', 'unknown')}:{source.get('lineno', 'unknown')} - {warning.message}")
                    else:
                        logging.warning(f"  - [{warning_type}] {warning.message}")
            
            # Only fail if there are critical errors
            if critical_errors:
                logging.error("Critical errors encountered during Beancount file parsing:")
                for error in critical_errors:
                    source = error.source
                    error_type = type(error).__name__
                    if source:
                        logging.error(f"  - [{error_type}] {source.get('filename', 'unknown')}:{source.get('lineno', 'unknown')} - {error.message}")
                    else:
                        logging.error(f"  - [{error_type}] {error.message}")
                raise Exception("Beancount parsing failed during check due to critical errors")
        
        # Calculate file state hash
        # Note: We need to pass config, but in check mode we just use default settings
        check_config = {'import_settings': {'on_duplicate': 'skip'}}
        file_hash = calculate_file_state_hash(entries, id_generator, check_config)
        logging.debug(f"Calculated file state hash: {file_hash}")
        
        # Retrieve database state hash
        with conn.cursor() as cur:
            cur.execute("SELECT last_successful_hash FROM import_state WHERE id = 1")
            result = cur.fetchone()
            db_hash = result[0] if result else None
            logging.debug(f"Retrieved DB state hash: {db_hash}")
        
        # Compare hashes
        if file_hash == db_hash:
            logging.info("✅ Database is in sync with source files")
        else:
            logging.warning("⚠️ Database is out of sync. A --rebuild is recommended")
        
    except Exception as e:
        logging.error(f"An error occurred during check: {e}")
        sys.exit(1)


def derive_account_type(account_name: str) -> str:
    """Derive account type from account name (e.g., 'Expenses:Food:Groceries' -> 'Expenses')."""
    if ':' in account_name:
        return account_name.split(':', 1)[0]
    return account_name


def get_or_create_id(cur: psycopg2.extensions.cursor, cache: Dict[str, int], table: str, column: str, value: str) -> int:
    """Get the ID for a value from a cache or the DB, creating it if it doesn't exist."""
    if value in cache:
        return cache[value]
    
    # Try to find existing record
    cur.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value,))
    result = cur.fetchone()
    if result:
        cache[value] = result[0]
        return result[0]
    
    # Create new record
    cur.execute(f"INSERT INTO {table} ({column}) VALUES (%s) RETURNING id", (value,))
    new_id = cur.fetchone()[0]
    cache[value] = new_id
    logging.debug(f"Created new entry in '{table}' for '{value}' with ID {new_id}")
    return new_id


def process_commodities(cur: psycopg2.extensions.cursor, entries: List[data.Directive], commodity_cache: Dict[str, int], dry_run: bool) -> None:
    """Process Commodity directives."""
    commodity_count = 0
    
    for entry in entries:
        if isinstance(entry, data.Commodity):
            commodity_name = entry.currency
            
            if commodity_name not in commodity_cache:
                if dry_run:
                    logging.info(f"[DRY RUN] Would create commodity: {commodity_name}")
                    # For dry run, use a fake ID
                    commodity_cache[commodity_name] = -1
                else:
                    # Check if already exists
                    cur.execute("SELECT id FROM commodities WHERE name = %s", (commodity_name,))
                    result = cur.fetchone()
                    if result:
                        commodity_cache[commodity_name] = result[0]
                    else:
                        # Create new commodity
                        metadata_json = dict(entry.meta) if entry.meta else None
                        # Remove reserved keys
                        if metadata_json:
                            for key in RESERVED_METADATA_KEYS:
                                metadata_json.pop(key, None)
                        
                        cur.execute("""
                            INSERT INTO commodities (name, metadata) 
                            VALUES (%s, %s) RETURNING id
                        """, (commodity_name, psycopg2.extras.Json(metadata_json) if metadata_json else None))
                        
                        new_id = cur.fetchone()[0]
                        commodity_cache[commodity_name] = new_id
                        logging.debug(f"Created commodity: {commodity_name} with ID {new_id}")
                
                commodity_count += 1
    
    if commodity_count > 0:
        logging.info(f"Processed {commodity_count} commodity directive(s)")


def process_accounts(cur: psycopg2.extensions.cursor, entries: List[data.Directive], account_cache: Dict[str, int], dry_run: bool) -> None:
    """Process Open and Close directives for accounts."""
    account_count = 0
    
    for entry in entries:
        if isinstance(entry, (data.Open, data.Close)):
            account_name = entry.account
            account_type = derive_account_type(account_name)
            
            if isinstance(entry, data.Open):
                if account_name not in account_cache:
                    if dry_run:
                        logging.info(f"[DRY RUN] Would create account: {account_name}")
                        account_cache[account_name] = -1
                    else:
                        # Check if already exists
                        cur.execute("SELECT id FROM accounts WHERE name = %s", (account_name,))
                        result = cur.fetchone()
                        if result:
                            account_cache[account_name] = result[0]
                        else:
                            # Create new account
                            metadata_json = dict(entry.meta) if entry.meta else None
                            if metadata_json:
                                for key in RESERVED_METADATA_KEYS:
                                    metadata_json.pop(key, None)
                            
                            cur.execute("""
                                INSERT INTO accounts (name, type, status, open_date, metadata) 
                                VALUES (%s, %s, %s, %s, %s) RETURNING id
                            """, (account_name, account_type, 'open', entry.date, 
                                  psycopg2.extras.Json(metadata_json) if metadata_json else None))
                            
                            new_id = cur.fetchone()[0]
                            account_cache[account_name] = new_id
                            logging.debug(f"Created account: {account_name} with ID {new_id}")
                    
                    account_count += 1
                    
            elif isinstance(entry, data.Close):
                # Update existing account to closed status
                if dry_run:
                    logging.info(f"[DRY RUN] Would close account: {account_name}")
                else:
                    cur.execute("""
                        UPDATE accounts 
                        SET status = 'closed', close_date = %s 
                        WHERE name = %s
                    """, (entry.date, account_name))
                    
                    if cur.rowcount > 0:
                        logging.debug(f"Closed account: {account_name} on {entry.date}")
                    else:
                        logging.warning(f"Attempted to close non-existent account: {account_name}")
    
    if account_count > 0:
        logging.info(f"Processed {account_count} account directive(s)")



def process_other_directives(cur: psycopg2.extensions.cursor, entries: List[data.Directive], 
                           caches: Dict[str, Dict[str, int]], dry_run: bool) -> None:
    """Process Balance, Price, Event, and Document directives."""
    counts = {'balance': 0, 'price': 0, 'event': 0, 'document': 0}
    
    for entry in entries:
        # Get source location for better error reporting
        source_file = entry.meta.get('filename', 'unknown')
        source_line = entry.meta.get('lineno', 'unknown')
        directive_context = f"{source_file}:{source_line}"
        
        try:
            if isinstance(entry, data.Balance):
                # Ensure account exists
                account_id = caches['accounts'].get(entry.account)
                if account_id is None:
                    if not dry_run:
                        account_type = derive_account_type(entry.account)
                        cur.execute("INSERT INTO accounts (name, type, status) VALUES (%s, %s, %s) RETURNING id", 
                                  (entry.account, account_type, 'open'))
                        account_id = cur.fetchone()[0]
                        caches['accounts'][entry.account] = account_id
                    else:
                        account_id = -1
                
                # Ensure currency exists
                currency_id = caches['commodities'].get(entry.amount.currency)
                if currency_id is None:
                    if not dry_run:
                        # Check if commodity already exists in database
                        cur.execute("SELECT id FROM commodities WHERE name = %s", (entry.amount.currency,))
                        result = cur.fetchone()
                        if result:
                            currency_id = result[0]
                            caches['commodities'][entry.amount.currency] = currency_id
                        else:
                            cur.execute("INSERT INTO commodities (name) VALUES (%s) RETURNING id", (entry.amount.currency,))
                            currency_id = cur.fetchone()[0]
                            caches['commodities'][entry.amount.currency] = currency_id
                    else:
                        currency_id = -1
                
                if dry_run:
                    logging.debug(f"[DRY RUN] Would create balance assertion: {entry.account} {entry.amount}")
                else:
                    cur.execute("""
                        INSERT INTO balance_assertions (date, account_id, amount, currency_id, source_file, source_line)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (entry.date, account_id, entry.amount.number, currency_id,
                          entry.meta.get('filename'), entry.meta.get('lineno')))
                counts['balance'] += 1
                
            elif isinstance(entry, data.Price):
                # Ensure commodities exist
                commodity_id = caches['commodities'].get(entry.currency)
                if commodity_id is None:
                    if not dry_run:
                        # Check if commodity already exists in database
                        cur.execute("SELECT id FROM commodities WHERE name = %s", (entry.currency,))
                        result = cur.fetchone()
                        if result:
                            commodity_id = result[0]
                            caches['commodities'][entry.currency] = commodity_id
                        else:
                            cur.execute("INSERT INTO commodities (name) VALUES (%s) RETURNING id", (entry.currency,))
                            commodity_id = cur.fetchone()[0]
                            caches['commodities'][entry.currency] = commodity_id
                    else:
                        commodity_id = -1
                
                price_currency_id = caches['commodities'].get(entry.amount.currency)
                if price_currency_id is None:
                    if not dry_run:
                        # Check if commodity already exists in database
                        cur.execute("SELECT id FROM commodities WHERE name = %s", (entry.amount.currency,))
                        result = cur.fetchone()
                        if result:
                            price_currency_id = result[0]
                            caches['commodities'][entry.amount.currency] = price_currency_id
                        else:
                            cur.execute("INSERT INTO commodities (name) VALUES (%s) RETURNING id", (entry.amount.currency,))
                            price_currency_id = cur.fetchone()[0]
                            caches['commodities'][entry.amount.currency] = price_currency_id
                    else:
                        price_currency_id = -1
                
                if dry_run:
                    logging.debug(f"[DRY RUN] Would create price: {entry.currency} = {entry.amount}")
                else:
                    cur.execute("""
                        INSERT INTO prices (date, commodity_id, price_amount, price_currency_id, source_file, source_line)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (entry.date, commodity_id, entry.amount.number, price_currency_id,
                          entry.meta.get('filename'), entry.meta.get('lineno')))
                counts['price'] += 1
                
            elif isinstance(entry, data.Event):
                if dry_run:
                    logging.debug(f"[DRY RUN] Would create event: {entry.type} = {entry.description}")
                else:
                    cur.execute("""
                        INSERT INTO events (date, name, value, source_file, source_line)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (entry.date, entry.type, entry.description,
                          entry.meta.get('filename'), entry.meta.get('lineno')))
                counts['event'] += 1
                
            elif isinstance(entry, data.Document):
                # Ensure account exists
                account_id = caches['accounts'].get(entry.account)
                if account_id is None:
                    if not dry_run:
                        account_type = derive_account_type(entry.account)
                        cur.execute("INSERT INTO accounts (name, type, status) VALUES (%s, %s, %s) RETURNING id", 
                                  (entry.account, account_type, 'open'))
                        account_id = cur.fetchone()[0]
                        caches['accounts'][entry.account] = account_id
                    else:
                        account_id = -1
                
                if dry_run:
                    logging.debug(f"[DRY RUN] Would create document: {entry.account} -> {entry.filename}")
                else:
                    cur.execute("""
                        INSERT INTO documents (date, account_id, file_path, source_file, source_line)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (entry.date, account_id, entry.filename,
                          entry.meta.get('filename'), entry.meta.get('lineno')))
                    counts['document'] += 1
                
        except Exception as e:
            logging.error(f"Error processing directive at {directive_context}")
            logging.error(f"Error details: {e}")
            raise
    
    # Log counts
    for directive_type, count in counts.items():
        if count > 0:
            logging.info(f"Processed {count} {directive_type} directive(s)")


def update_import_state(cur: psycopg2.extensions.cursor, state_hash: str, dry_run: bool) -> None:
    """Update the import_state table with the new hash."""
    if dry_run:
        logging.info(f"[DRY RUN] Would update import state hash to: {state_hash}")
    else:
        cur.execute("""
            INSERT INTO import_state (id, last_successful_hash) 
            VALUES (1, %s) 
            ON CONFLICT (id) DO UPDATE SET 
                last_successful_hash = EXCLUDED.last_successful_hash,
                updated_at = CURRENT_TIMESTAMP
        """, (state_hash,))
        logging.debug(f"Updated import state hash to: {state_hash}")


def chunk_transactions(transactions: List[data.Transaction], chunk_size: int) -> List[List[data.Transaction]]:
    """Split transactions into chunks of specified size."""
    chunks = []
    for i in range(0, len(transactions), chunk_size):
        chunks.append(transactions[i:i + chunk_size])
    return chunks


def handle_rollback_partial(conn: psycopg2.extensions.connection, file_paths: List[str], dry_run: bool) -> None:
    """Remove all data from previous partial imports of these files."""
    if dry_run:
        logging.info("[DRY RUN] Would remove partial import data")
        return
    
    # Get source file names (just the basename, not full path)
    source_files = [os.path.basename(f) for f in file_paths]
    
    # Count existing data to show user what will be deleted
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM transactions WHERE source_file = ANY(%s)
        """, (source_files,))
        transaction_count = cur.fetchone()[0]
    
    if transaction_count == 0:
        logging.info("No partial import data found to remove")
        return
    
    # Confirmation prompt
    print(f"\n*** WARNING ***", file=sys.stderr)
    print(f"This will permanently delete {transaction_count} transactions and related data from files:", file=sys.stderr)
    for f in source_files:
        print(f"  - {f}", file=sys.stderr)
    print("This action cannot be undone.", file=sys.stderr)
    
    try:
        confirm = input("Are you sure you want to continue? (yes/no): ")
    except (EOFError, KeyboardInterrupt):
        logging.info("Rollback partial aborted by user (interrupted)")
        sys.exit(0)
    
    if confirm.lower() != 'yes':
        logging.info("Rollback partial aborted by user")
        sys.exit(0)
    
    logging.info(f"Removing partial import data for {transaction_count} transactions...")
    
    try:
        with conn.cursor() as cur:
            # Delete in dependency order to avoid foreign key constraint violations
            
            # 1. Delete posting metadata
            cur.execute("""
                DELETE FROM posting_metadata 
                WHERE posting_id IN (
                    SELECT p.id FROM postings p
                    JOIN transactions t ON p.transaction_id = t.id
                    WHERE t.source_file = ANY(%s)
                )
            """, (source_files,))
            posting_metadata_count = cur.rowcount
            
            # 2. Delete postings
            cur.execute("""
                DELETE FROM postings 
                WHERE transaction_id IN (
                    SELECT id FROM transactions WHERE source_file = ANY(%s)
                )
            """, (source_files,))
            postings_count = cur.rowcount
            
            # 3. Delete transaction metadata
            cur.execute("""
                DELETE FROM transaction_metadata
                WHERE transaction_id IN (
                    SELECT id FROM transactions WHERE source_file = ANY(%s)
                )
            """, (source_files,))
            transaction_metadata_count = cur.rowcount
            
            # 4. Delete transaction tags
            cur.execute("""
                DELETE FROM transaction_tags
                WHERE transaction_id IN (
                    SELECT id FROM transactions WHERE source_file = ANY(%s)
                )
            """, (source_files,))
            transaction_tags_count = cur.rowcount
            
            # 5. Delete transaction links
            cur.execute("""
                DELETE FROM transaction_links
                WHERE transaction_id IN (
                    SELECT id FROM transactions WHERE source_file = ANY(%s)
                )
            """, (source_files,))
            transaction_links_count = cur.rowcount
            
            # 6. Delete transactions
            cur.execute("DELETE FROM transactions WHERE source_file = ANY(%s)", (source_files,))
            final_transaction_count = cur.rowcount
            
            # 7. Delete other directives
            other_counts = {}
            for table in ['balance_assertions', 'prices', 'events', 'documents']:
                cur.execute(f"DELETE FROM {table} WHERE source_file = ANY(%s)", (source_files,))
                other_counts[table] = cur.rowcount
        
        conn.commit()
        
        # Log summary
        logging.info("Partial import data removed successfully:")
        logging.info(f"  - {final_transaction_count} transactions")
        logging.info(f"  - {postings_count} postings")
        logging.info(f"  - {posting_metadata_count} posting metadata entries")
        logging.info(f"  - {transaction_metadata_count} transaction metadata entries")
        logging.info(f"  - {transaction_tags_count} transaction tag links")
        logging.info(f"  - {transaction_links_count} transaction links")
        for table, count in other_counts.items():
            if count > 0:
                logging.info(f"  - {count} {table} entries")
        
    except psycopg2.Error as e:
        logging.error(f"An error occurred during partial rollback: {e}")
        conn.rollback()
        raise


def process_transaction_chunk(cur: psycopg2.extensions.cursor, chunk: List[data.Transaction], 
                            caches: Dict[str, Dict[str, int]], id_generator: TransactionIdGenerator, 
                            config: Dict[str, Any], dry_run: bool, resume_mode: bool,
                            chunk_info: Tuple[int, int, int, int]) -> Tuple[int, int]:
    """Process a single chunk of transactions with detailed progress tracking."""
    chunk_num, total_chunks, start_idx, end_idx = chunk_info
    processed_count = 0
    skipped_count = 0
    
    # Override duplicate behavior if in resume mode
    on_duplicate = 'skip' if resume_mode else config.get('import_settings', {}).get('on_duplicate', 'skip')
    
    logging.info(f"Processing chunk {chunk_num}/{total_chunks} (transactions {start_idx}-{end_idx})")
    
    for i, entry in enumerate(chunk):
        # Get source location for better error reporting
        source_file = entry.meta.get('filename', 'unknown')
        source_line = entry.meta.get('lineno', 'unknown')
        transaction_context = f"{source_file}:{source_line}"
        
        try:
            # Generate external_id using the new API
            # This respects existing transaction_id metadata if present
            txn_with_id = add_transaction_id_to_beancount_transaction(
                transaction=entry,
                force_recalculate=False,  # Use existing transaction_id if present
                strict_validation=False,  # Don't fail on validation issues
                id_generator=id_generator
            )
            external_id = txn_with_id.meta['transaction_id']
            
            # Check for duplicates
            duplicate_found = False
            if not dry_run:
                cur.execute("SELECT id FROM transactions WHERE external_id = %s", (external_id,))
                existing_txn = cur.fetchone()
                if existing_txn and on_duplicate == 'skip':
                    if resume_mode:
                        logging.debug(f"Resume: skipping already-imported transaction with external_id: {external_id} from {transaction_context}")
                    else:
                        logging.debug(f"Skipping duplicate transaction with external_id: {external_id} from {transaction_context}")
                    skipped_count += 1
                    duplicate_found = True
            
            if duplicate_found:
                continue
                
            # Continue processing the transaction
            if dry_run:
                logging.debug(f"[DRY RUN] Would insert transaction: {entry.narration} (external_id: {external_id})")
                txn_id = -1  # Fake ID for dry run
            else:
                # Insert transaction
                cur.execute("""
                    INSERT INTO transactions (external_id, date, flag, payee, narration, source_file, source_line)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (external_id, entry.date, entry.flag, entry.payee, entry.narration, 
                      entry.meta.get('filename'), entry.meta.get('lineno')))
                txn_id = cur.fetchone()[0]
                logging.debug(f"Created transaction with ID {txn_id}, external_id: {external_id}")
            
            # Process tags, links, metadata, and postings (same as before)
            # Process tags
            if entry.tags:
                for tag in entry.tags:
                    if dry_run:
                        logging.debug(f"[DRY RUN] Would link transaction to tag: {tag}")
                    else:
                        tag_id = get_or_create_id(cur, caches['tags'], 'tags', 'name', tag)
                        cur.execute("""
                            INSERT INTO transaction_tags (transaction_id, tag_id) 
                            VALUES (%s, %s) ON CONFLICT DO NOTHING
                        """, (txn_id, tag_id))
            
            # Process links
            if entry.links:
                for link in entry.links:
                    if dry_run:
                        logging.debug(f"[DRY RUN] Would link transaction to link: {link}")
                    else:
                        link_id = get_or_create_id(cur, caches['links'], 'links', 'name', link)
                        cur.execute("""
                            INSERT INTO transaction_links (transaction_id, link_id) 
                            VALUES (%s, %s) ON CONFLICT DO NOTHING
                        """, (txn_id, link_id))
            
            # Process transaction metadata (excluding reserved keys)
            if entry.meta:
                for key, value in entry.meta.items():
                    if key not in RESERVED_METADATA_KEYS:
                        if dry_run:
                            logging.debug(f"[DRY RUN] Would add transaction metadata: {key} = {value}")
                        else:
                            cur.execute("""
                                INSERT INTO transaction_metadata (transaction_id, key, value) 
                                VALUES (%s, %s, %s) ON CONFLICT (transaction_id, key) DO UPDATE SET value = EXCLUDED.value
                            """, (txn_id, key, str(value)))
            
            # Process postings (same logic as before but for this transaction only)
            for posting in entry.postings:
                # Ensure account exists
                account_id = caches['accounts'].get(posting.account)
                if account_id is None:
                    if dry_run:
                        logging.debug(f"[DRY RUN] Would create missing account: {posting.account}")
                        account_id = -1
                        caches['accounts'][posting.account] = account_id
                    else:
                        # Create account if it doesn't exist
                        account_type = derive_account_type(posting.account)
                        cur.execute("""
                            INSERT INTO accounts (name, type, status) 
                            VALUES (%s, %s, %s) RETURNING id
                        """, (posting.account, account_type, 'open'))
                        account_id = cur.fetchone()[0]
                        caches['accounts'][posting.account] = account_id
                        logging.debug(f"Auto-created account: {posting.account} with ID {account_id}")
                
                # Ensure currency exists
                currency_id = caches['commodities'].get(posting.units.currency)
                if currency_id is None:
                    if dry_run:
                        logging.debug(f"[DRY RUN] Would create missing commodity: {posting.units.currency}")
                        currency_id = -1
                        caches['commodities'][posting.units.currency] = currency_id
                    else:
                        # Check if commodity already exists in database
                        cur.execute("SELECT id FROM commodities WHERE name = %s", (posting.units.currency,))
                        result = cur.fetchone()
                        if result:
                            currency_id = result[0]
                            caches['commodities'][posting.units.currency] = currency_id
                            logging.debug(f"Found existing commodity: {posting.units.currency} with ID {currency_id}")
                        else:
                            cur.execute("""
                                INSERT INTO commodities (name) 
                                VALUES (%s) RETURNING id
                            """, (posting.units.currency,))
                            currency_id = cur.fetchone()[0]
                            caches['commodities'][posting.units.currency] = currency_id
                            logging.debug(f"Auto-created commodity: {posting.units.currency} with ID {currency_id}")
                
                # Handle cost and price currencies
                cost_currency_id = None
                if posting.cost and posting.cost.currency:
                    cost_currency_id = caches['commodities'].get(posting.cost.currency)
                    if cost_currency_id is None:
                        if dry_run:
                            cost_currency_id = -1
                            caches['commodities'][posting.cost.currency] = cost_currency_id
                        else:
                            # Check if commodity already exists in database
                            cur.execute("SELECT id FROM commodities WHERE name = %s", (posting.cost.currency,))
                            result = cur.fetchone()
                            if result:
                                cost_currency_id = result[0]
                                caches['commodities'][posting.cost.currency] = cost_currency_id
                            else:
                                cur.execute("INSERT INTO commodities (name) VALUES (%s) RETURNING id", (posting.cost.currency,))
                                cost_currency_id = cur.fetchone()[0]
                                caches['commodities'][posting.cost.currency] = cost_currency_id
                
                price_currency_id = None
                if posting.price and posting.price.currency:
                    price_currency_id = caches['commodities'].get(posting.price.currency)
                    if price_currency_id is None:
                        if dry_run:
                            price_currency_id = -1
                            caches['commodities'][posting.price.currency] = price_currency_id
                        else:
                            # Check if commodity already exists in database
                            cur.execute("SELECT id FROM commodities WHERE name = %s", (posting.price.currency,))
                            result = cur.fetchone()
                            if result:
                                price_currency_id = result[0]
                                caches['commodities'][posting.price.currency] = price_currency_id
                            else:
                                cur.execute("INSERT INTO commodities (name) VALUES (%s) RETURNING id", (posting.price.currency,))
                                price_currency_id = cur.fetchone()[0]
                                caches['commodities'][posting.price.currency] = price_currency_id
                
                if dry_run:
                    logging.debug(f"[DRY RUN] Would create posting: {posting.account} {posting.units}")
                    posting_id = -1
                else:
                    # Insert posting
                    cur.execute("""
                        INSERT INTO postings 
                        (transaction_id, flag, account_id, amount, currency_id, 
                         cost_amount, cost_currency_id, price_amount, price_currency_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                    """, (txn_id, posting.flag, account_id, posting.units.number, currency_id,
                          posting.cost.number if posting.cost else None, cost_currency_id,
                          posting.price.number if posting.price else None, price_currency_id))
                    posting_id = cur.fetchone()[0]
            
            # Process posting metadata
            if posting.meta:
                for key, value in posting.meta.items():
                    if key not in RESERVED_METADATA_KEYS:
                        if dry_run:
                            logging.debug(f"[DRY RUN] Would add posting metadata: {key} = {value}")
                        else:
                            cur.execute("""
                                INSERT INTO posting_metadata (posting_id, key, value) 
                                VALUES (%s, %s, %s) ON CONFLICT (posting_id, key) DO UPDATE SET value = EXCLUDED.value
                            """, (posting_id, key, str(value)))
            
            processed_count += 1
            
        except Exception as e:
            logging.error(f"Error processing transaction at {transaction_context}: {entry.narration}")
            logging.error(f"Error details: {e}")
            raise
    
    # Log chunk completion
    logging.info(f"Chunk {chunk_num} completed: {processed_count} processed, {skipped_count} skipped")
    if skipped_count > 0 and resume_mode:
        logging.info(f"Resume mode: skipped {skipped_count} already-imported transactions")
    
    return processed_count, skipped_count


def process_and_import(conn: psycopg2.extensions.connection, file_paths: List[str], 
                      id_generator: TransactionIdGenerator, config: Dict[str, Any], 
                      dry_run: bool, resume_mode: bool) -> None:
    """The main processing loop for parsing files and importing data using hybrid chunked approach."""
    logging.info(f"Starting import process for {len(file_paths)} file(s)...")
    
    try:
        # Load and parse all files
        if len(file_paths) == 1:
            entries, errors, _ = load_file(file_paths[0])
        else:
            entries, errors, _ = load_file(file_paths)
        
        if errors:
            # Categorize errors by type
            # PadError (unused pad) and DeprecatedError are non-critical warnings
            critical_errors = []
            warnings = []
            
            for error in errors:
                error_type = type(error).__name__
                # Non-critical error types that shouldn't stop import
                if error_type in ['PadError', 'DeprecatedError']:
                    warnings.append(error)
                else:
                    critical_errors.append(error)
            
            # Log warnings
            if warnings:
                logging.warning(f"Beancount parsing warnings (non-critical):")
                for warning in warnings:
                    source = warning.source
                    warning_type = type(warning).__name__
                    if source:
                        logging.warning(f"  - [{warning_type}] {source.get('filename', 'unknown')}:{source.get('lineno', 'unknown')} - {warning.message}")
                    else:
                        logging.warning(f"  - [{warning_type}] {warning.message}")
            
            # Only fail if there are critical errors
            if critical_errors:
                logging.error("Critical errors encountered during Beancount file parsing:")
                for error in critical_errors:
                    source = error.source
                    error_type = type(error).__name__
                    if source:
                        logging.error(f"  - [{error_type}] {source.get('filename', 'unknown')}:{source.get('lineno', 'unknown')} - {error.message}")
                    else:
                        logging.error(f"  - [{error_type}] {error.message}")
                raise Exception("Beancount parsing failed due to critical errors")
        
        logging.info(f"Successfully parsed {len(entries)} directives from source files")
        
        # Separate different types of directives for processing
        commodity_entries = [e for e in entries if isinstance(e, data.Commodity)]
        account_entries = [e for e in entries if isinstance(e, (data.Open, data.Close))]
        transaction_entries = [e for e in entries if isinstance(e, data.Transaction)]
        other_entries = [e for e in entries if isinstance(e, (data.Balance, data.Price, data.Event, data.Document))]
        
        logging.info(f"Found {len(commodity_entries)} commodities, {len(account_entries)} accounts, "
                    f"{len(transaction_entries)} transactions, {len(other_entries)} other directives")
        
        # Initialize caches for foreign key IDs
        caches = {
            'accounts': {},
            'commodities': {},
            'tags': {},
            'links': {}
        }
        
        # PHASE 1: Schema Operations (Single Transaction)
        logging.info("Phase 1: Processing schema operations (commodities and accounts)...")
        with conn.cursor() as cur:
            logging.info("Processing commodities...")
            process_commodities(cur, commodity_entries, caches['commodities'], dry_run)
            
            logging.info("Processing accounts...")
            process_accounts(cur, account_entries, caches['accounts'], dry_run)
        
        if not dry_run:
            conn.commit()
            logging.info("Phase 1 completed and committed")
        else:
            logging.info("[DRY RUN] Phase 1 completed (would be committed)")
        
        # PHASE 2: Bulk Data Operations (Chunked Transactions)
        if transaction_entries:
            chunk_size = config.get('import_settings', {}).get('chunk_size', 500)
            chunks = chunk_transactions(transaction_entries, chunk_size)
            total_chunks = len(chunks)
            
            logging.info(f"Phase 2: Processing {len(transaction_entries)} transactions in {total_chunks} chunks of {chunk_size}")
            
            total_processed = 0
            total_skipped = 0
            
            for chunk_num, chunk in enumerate(chunks, 1):
                start_idx = (chunk_num - 1) * chunk_size + 1
                end_idx = min(start_idx + len(chunk) - 1, len(transaction_entries))
                chunk_info = (chunk_num, total_chunks, start_idx, end_idx)
                
                with conn.cursor() as cur:
                    processed, skipped = process_transaction_chunk(
                        cur, chunk, caches, id_generator, config, dry_run, resume_mode, chunk_info
                    )
                    total_processed += processed
                    total_skipped += skipped
                
                if not dry_run:
                    conn.commit()
                    logging.debug(f"Chunk {chunk_num} committed to database")
                else:
                    logging.debug(f"[DRY RUN] Chunk {chunk_num} completed (would be committed)")
            
            logging.info(f"Phase 2 completed: {total_processed} transactions processed, {total_skipped} skipped")
        else:
            logging.info("Phase 2: No transactions to process")
        
        # PHASE 3: Cleanup Operations (Single Transaction)
        logging.info("Phase 3: Processing cleanup operations...")
        with conn.cursor() as cur:
            if other_entries:
                logging.info("Processing other directives...")
                process_other_directives(cur, other_entries, caches, dry_run)
            
            # Update import state hash
            if not dry_run:
                state_hash = calculate_file_state_hash(entries, id_generator, config)
                update_import_state(cur, state_hash, dry_run)
        
        if not dry_run:
            conn.commit()
            logging.info("Phase 3 completed and committed")
        else:
            logging.info("[DRY RUN] Phase 3 completed (would be committed)")
        
        logging.info("Import process completed successfully")
        
    except Exception as e:
        logging.error(f"A critical error occurred during import: {e}")
        logging.error(f"Files being processed: {file_paths}")
        if 'chunk_num' in locals():
            logging.error(f"Error occurred during chunk {chunk_num} processing")
        raise  # Re-raise to trigger rollback in main()


def main() -> int:
    """Main function to run the import process."""
    # Parse arguments first (before any other setup)
    args = parse_arguments()
    
    # Validate input files before doing anything else
    validate_input_files(args.input_file)
    
    # Load configuration
    config = load_config(args.config_file)
    
    # Setup logging
    log_level = 'DEBUG' if args.verbose else config.get('logging', {}).get('level', 'INFO')
    log_file = config.get('logging', {}).get('file')
    log_mode = config.get('logging', {}).get('mode', 'append')
    setup_logging(log_level, log_file, log_mode)
    
    logging.debug(f"Arguments: {args}")
    logging.debug(f"Configuration: {config}")
    
    # Connect to database
    conn = connect_to_db(config.get('database', {}))
    
    try:
        # Check schema
        if not check_schema(conn):
            return 1
        
        # Initialize transaction ID generator
        id_generator = TransactionIdGenerator()
        
        # Handle different modes
        if args.check:
            handle_check(conn, args.input_file, id_generator)
            return 0
        
        # Handle rollback partial first (if requested)
        if args.rollback_partial:
            handle_rollback_partial(conn, args.input_file, args.dry_run)
        
        # Handle rebuild if requested
        if args.rebuild:
            handle_rebuild(conn, args.dry_run)
        
        # Main import process (with resume capability)
        process_and_import(conn, args.input_file, id_generator, config, args.dry_run, args.resume)
        
        logging.info("Process finished successfully")
        return 0
        
    except KeyboardInterrupt:
        logging.error("Process interrupted by user")
        conn.rollback()
        return 1
    except Exception as e:
        logging.error(f"A critical error occurred: {e}")
        if hasattr(args, 'input_file'):
            logging.error(f"Input files: {args.input_file}")
        if hasattr(args, 'resume') and args.resume:
            logging.error("Operation was in --resume mode")
        conn.rollback()
        logging.error("Database transaction has been rolled back")
        return 1
    finally:
        conn.close()
        logging.debug("Database connection closed")


if __name__ == "__main__":
    sys.exit(main())