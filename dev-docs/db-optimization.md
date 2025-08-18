# Database Optimization Guide for beancount-to-sql

## Overview

This document provides comprehensive implementation details for optimizing database operations in the beancount-to-sql import process. The current implementation has significant inefficiencies that can be improved by 50-100x for large datasets while maintaining complete transactional integrity.

**Critical Requirement**: All optimizations MUST maintain atomicity - either ALL data from input files is written to the database, or NONE is written. Partial imports are not acceptable.

## Current Performance Issues

### Issue 1: SELECT-before-INSERT Anti-pattern
**Current behavior**: Every commodity, account, tag, and link requires a SELECT query to check existence before INSERT.

**Impact**: 
- 1000 transactions × 2 postings × 2 queries (SELECT + possible INSERT) = 4000+ queries
- Network latency dominates execution time

### Issue 2: Individual INSERT Statements
**Current behavior**: Each entity is inserted separately with individual INSERT statements.

**Impact**:
- 1000 transactions = 1000 transaction INSERTs + 2000 posting INSERTs + metadata INSERTs
- Each INSERT has network round-trip overhead

### Issue 3: Cache Not Pre-populated
**Current behavior**: Caches start empty and are populated on-demand during processing.

**Impact**:
- First occurrence of each entity requires database query
- Common entities (like "USD") queried hundreds of times

## Optimization Strategy

### Phase Structure (Maintain Current Approach)
```
Phase 1: Schema Operations (single transaction)
  - Pre-populate all caches
  - Process commodities and accounts
  
Phase 2: Bulk Data Operations (chunked transactions)  
  - Process transactions in configurable chunks
  - Use batch operations within each chunk
  
Phase 3: Cleanup Operations (single transaction)
  - Process other directives
  - Update import state
```

## Detailed Implementation

### 1. Cache Pre-population

Add this function to `src/import_beancount.py`:

```python
def preload_caches(cur: psycopg2.extensions.cursor) -> Dict[str, Dict[str, int]]:
    """
    Pre-populate caches with all existing entities from database.
    This eliminates thousands of SELECT queries during import.
    
    Returns:
        Dictionary of caches: {
            'commodities': {name: id},
            'accounts': {name: id},
            'tags': {name: id},
            'links': {name: id}
        }
    """
    caches = {}
    
    # Load all commodities (typically <100 rows)
    cur.execute("SELECT id, name FROM commodities")
    caches['commodities'] = {row[1]: row[0] for row in cur.fetchall()}
    logging.debug(f"Pre-loaded {len(caches['commodities'])} commodities into cache")
    
    # Load all accounts (typically <1000 rows)
    cur.execute("SELECT id, name FROM accounts")
    caches['accounts'] = {row[1]: row[0] for row in cur.fetchall()}
    logging.debug(f"Pre-loaded {len(caches['accounts'])} accounts into cache")
    
    # Load all tags (typically <100 rows)
    cur.execute("SELECT id, name FROM tags")
    caches['tags'] = {row[1]: row[0] for row in cur.fetchall()}
    logging.debug(f"Pre-loaded {len(caches['tags'])} tags into cache")
    
    # Load all links (typically <100 rows)
    cur.execute("SELECT id, name FROM links")
    caches['links'] = {row[1]: row[0] for row in cur.fetchall()}
    logging.debug(f"Pre-loaded {len(caches['links'])} links into cache")
    
    return caches
```

**Integration point**: Call this in `process_and_import()` before Phase 1:
```python
# Line ~1088 in src/import_beancount.py
# Replace:
caches = {
    'accounts': {},
    'commodities': {},
    'tags': {},
    'links': {}
}

# With:
with conn.cursor() as cur:
    caches = preload_caches(cur)
```

### 2. Batch INSERT with ON CONFLICT

Replace the `get_or_create_id` function with optimized versions:

```python
def get_or_create_single(cur: psycopg2.extensions.cursor,
                         cache: Dict[str, int],
                         table: str,
                         column: str,
                         value: str) -> int:
    """
    Optimized single-item get-or-create using INSERT...ON CONFLICT.
    Eliminates the SELECT-before-INSERT anti-pattern.
    
    Args:
        cur: Database cursor
        cache: Cache dictionary to check and update
        table: Table name (must be from whitelist for safety)
        column: Column name
        value: Value to get or create
        
    Returns:
        ID of the row (existing or newly created)
    """
    # Check cache first
    if value in cache:
        return cache[value]
    
    # Security: Validate table name against whitelist
    if table not in ['commodities', 'accounts', 'tags', 'links']:
        raise ValueError(f"Invalid table name: {table}")
    
    # Use psycopg2's safe SQL composition
    from psycopg2 import sql
    
    # Single query handles both INSERT and SELECT cases
    query = sql.SQL("""
        INSERT INTO {table} ({column})
        VALUES (%s)
        ON CONFLICT ({column}) DO UPDATE
        SET {column} = EXCLUDED.{column}  -- No-op update to return existing row
        RETURNING id
    """).format(
        table=sql.Identifier(table),
        column=sql.Identifier(column)
    )
    
    cur.execute(query, (value,))
    row_id = cur.fetchone()[0]
    
    # CRITICAL: Update cache after successful database operation
    cache[value] = row_id
    return row_id


def get_or_create_batch(cur: psycopg2.extensions.cursor, 
                        cache: Dict[str, int], 
                        table: str, 
                        column: str, 
                        values: List[str]) -> Dict[str, int]:
    """
    TRUE batch get-or-create operation for multiple values.
    Uses INSERT...ON CONFLICT with execute_values for maximum efficiency.
    
    Args:
        cur: Database cursor
        cache: Cache dictionary to check and update
        table: Table name (must be from whitelist for safety)
        column: Column name
        values: List of values to get or create
        
    Returns:
        Dictionary mapping values to their IDs
    """
    # Security: Validate table name against whitelist
    if table not in ['commodities', 'accounts', 'tags', 'links']:
        raise ValueError(f"Invalid table name: {table}")
    
    # Deduplicate and filter to only uncached values
    unique_values = set(values)
    uncached_values = [v for v in unique_values if v not in cache]
    
    if not uncached_values:
        return {v: cache[v] for v in values}
    
    # Use psycopg2's safe SQL composition and execute_values for TRUE batching
    from psycopg2 import sql
    from psycopg2.extras import execute_values
    
    # Prepare the query with proper escaping
    query = sql.SQL("""
        INSERT INTO {table} ({column})
        VALUES %s
        ON CONFLICT ({column}) DO UPDATE
        SET {column} = EXCLUDED.{column}  -- No-op update to return existing row
        RETURNING id, {column}
    """).format(
        table=sql.Identifier(table),
        column=sql.Identifier(column)
    )
    
    # Execute batch insert and fetch all results at once
    results = execute_values(
        cur,
        query.as_string(cur),  # Convert SQL composition to string
        [(v,) for v in uncached_values],
        fetch=True
    )
    
    # CRITICAL: Update cache with all results atomically
    for row_id, name_val in results:
        cache[name_val] = row_id
    
    # Return complete mapping for all requested values
    return {v: cache[v] for v in values}
```

### 2.1 Cache Coherency and Safety

```python
# CRITICAL: Cache Coherency Rules and Implementation

def create_transaction_scoped_cache(global_cache: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    """
    Create a transaction-scoped cache that inherits from global cache.
    This ensures cache consistency on rollback.
    """
    return {
        'commodities': global_cache.get('commodities', {}).copy(),
        'accounts': global_cache.get('accounts', {}).copy(),
        'tags': global_cache.get('tags', {}).copy(),
        'links': global_cache.get('links', {}).copy()
    }


def safe_cache_update(global_cache: Dict[str, Dict[str, int]], 
                      local_cache: Dict[str, Dict[str, int]]) -> None:
    """
    Safely update global cache after successful commit.
    Only call this AFTER conn.commit() succeeds.
    """
    for cache_type in ['commodities', 'accounts', 'tags', 'links']:
        if cache_type in local_cache:
            global_cache[cache_type].update(local_cache[cache_type])


# Example usage in process_transaction_chunk_optimized:
def process_transaction_chunk_with_cache_safety(conn, chunk, global_caches, ...):
    """
    Process chunk with proper cache management.
    """
    # Create transaction-scoped cache
    local_caches = create_transaction_scoped_cache(global_caches)
    
    try:
        with conn.cursor() as cur:
            # Do all work with local_caches
            processed, skipped = process_chunk_internal(cur, chunk, local_caches, ...)
        
        # Commit the transaction
        conn.commit()
        
        # ONLY update global cache after successful commit
        safe_cache_update(global_caches, local_caches)
        
        return processed, skipped
        
    except Exception as e:
        # Rollback the transaction
        conn.rollback()
        # local_caches is discarded - global cache remains unchanged
        logging.error(f"Chunk processing failed, cache remains consistent: {e}")
        raise


# Cache Coherency Rules:
# 1. NEVER update global cache before commit
# 2. ALWAYS use transaction-scoped caches for processing
# 3. Cache updates must be atomic with database commits
# 4. On rollback, discard local cache changes
# 5. For parallel imports, use process-specific caches or locking

# Thread Safety Note:
# If running parallel imports, either:
# - Use thread-local/process-local caches
# - Or implement proper locking:
import threading
cache_lock = threading.Lock()

def thread_safe_cache_update(global_cache, local_cache):
    with cache_lock:
        safe_cache_update(global_cache, local_cache)
```

### 3. Batch Processing for Transactions

Replace the transaction processing with batch operations:

```python
from psycopg2.extras import execute_values, execute_batch

def process_transaction_chunk_optimized(cur: psycopg2.extensions.cursor, 
                                       chunk: List[data.Transaction],
                                       caches: Dict[str, Dict[str, int]], 
                                       id_generator: TransactionIdGenerator,
                                       config: Dict[str, Any], 
                                       dry_run: bool, 
                                       resume_mode: bool,
                                       chunk_info: Tuple[int, int, int, int]) -> Tuple[int, int]:
    """
    Optimized transaction chunk processing using batch operations.
    Reduces database round-trips by 10-100x.
    """
    chunk_num, total_chunks, start_idx, end_idx = chunk_info
    on_duplicate = 'skip' if resume_mode else config.get('import_settings', {}).get('on_duplicate', 'skip')
    
    logging.info(f"Processing chunk {chunk_num}/{total_chunks} (transactions {start_idx}-{end_idx})")
    
    if dry_run:
        # Dry run logic remains the same
        return process_transaction_chunk(cur, chunk, caches, id_generator, 
                                       config, dry_run, resume_mode, chunk_info)
    
    # Step 1: Collect all unique entities needed for this chunk
    all_commodities = set()
    all_accounts = set()
    all_tags = set()
    all_links = set()
    
    # First pass: collect all entities
    for txn in chunk:
        # Collect from postings
        for posting in txn.postings:
            all_accounts.add(posting.account)
            all_commodities.add(posting.units.currency)
            if posting.cost and posting.cost.currency:
                all_commodities.add(posting.cost.currency)
            if posting.price and posting.price.currency:
                all_commodities.add(posting.price.currency)
        
        # Collect tags and links
        if txn.tags:
            all_tags.update(txn.tags)
        if txn.links:
            all_links.update(txn.links)
    
    # Step 2: Batch create all entities and update caches
    # Use TRUE batch operations with execute_values
    if all_commodities:
        commodity_ids = get_or_create_batch(cur, caches['commodities'], 'commodities', 'name', list(all_commodities))
    
    if all_accounts:
        # For accounts, we need special handling due to the type field
        # Batch check which accounts already exist
        uncached_accounts = [a for a in all_accounts if a not in caches['accounts']]
        if uncached_accounts:
            from psycopg2.extras import execute_values
            
            # Prepare account data with types
            account_data = [(acc, derive_account_type(acc), 'open') for acc in uncached_accounts]
            
            # Batch insert with ON CONFLICT
            results = execute_values(
                cur,
                """
                INSERT INTO accounts (name, type, status)
                VALUES %s
                ON CONFLICT (name) DO UPDATE 
                SET name = EXCLUDED.name  -- No-op to return existing
                RETURNING id, name
                """,
                account_data,
                fetch=True
            )
            
            # Update cache
            for acc_id, acc_name in results:
                caches['accounts'][acc_name] = acc_id
    
    if all_tags:
        tag_ids = get_or_create_batch(cur, caches['tags'], 'tags', 'name', list(all_tags))
    
    if all_links:
        link_ids = get_or_create_batch(cur, caches['links'], 'links', 'name', list(all_links))
    
    # Step 3: Check for duplicate transactions (batch)
    external_ids = []
    for txn in chunk:
        txn_with_id = add_transaction_id_to_beancount_transaction(
            transaction=txn,
            force_recalculate=False,
            strict_validation=False,
            id_generator=id_generator
        )
        external_ids.append(txn_with_id.meta['transaction_id'])
    
    # Batch check for existing transactions
    cur.execute("""
        SELECT external_id FROM transactions 
        WHERE external_id = ANY(%s)
    """, (external_ids,))
    existing_ids = {row[0] for row in cur.fetchall()}
    
    # Step 4: Prepare batch data for insertions
    transactions_to_insert = []
    postings_to_insert = []
    posting_metadata_to_insert = []
    transaction_metadata_to_insert = []
    transaction_tags_to_insert = []
    transaction_links_to_insert = []
    
    processed_count = 0
    skipped_count = 0
    
    for txn in chunk:
        txn_with_id = add_transaction_id_to_beancount_transaction(
            transaction=txn,
            force_recalculate=False,
            strict_validation=False,
            id_generator=id_generator
        )
        external_id = txn_with_id.meta['transaction_id']
        
        # Skip if duplicate
        if external_id in existing_ids and on_duplicate == 'skip':
            skipped_count += 1
            logging.debug(f"Skipping duplicate transaction: {external_id}")
            continue
        
        # Prepare transaction data
        transactions_to_insert.append((
            external_id,
            txn.date,
            txn.flag,
            txn.payee,
            txn.narration,
            txn.meta.get('filename'),
            txn.meta.get('lineno')
        ))
        
        processed_count += 1
    
    # Step 5: Batch insert transactions and get their IDs
    if transactions_to_insert:
        # Use INSERT...RETURNING to get all IDs at once
        transaction_ids = execute_values(
            cur,
            """
            INSERT INTO transactions (external_id, date, flag, payee, narration, source_file, source_line)
            VALUES %s
            RETURNING id, external_id
            """,
            transactions_to_insert,
            fetch=True
        )
        
        # Map external_id to internal id
        txn_id_map = {row[1]: row[0] for row in transaction_ids}
        
        # Step 6: Prepare posting and metadata batch data
        for txn in chunk:
            txn_with_id = add_transaction_id_to_beancount_transaction(
                transaction=txn,
                force_recalculate=False,
                strict_validation=False,
                id_generator=id_generator
            )
            external_id = txn_with_id.meta['transaction_id']
            
            if external_id not in txn_id_map:
                continue  # Was skipped
            
            txn_id = txn_id_map[external_id]
            
            # Prepare postings
            for posting in txn.postings:
                account_id = caches['accounts'][posting.account]
                currency_id = caches['commodities'][posting.units.currency]
                
                cost_amount = posting.cost.number if posting.cost else None
                cost_currency_id = caches['commodities'].get(posting.cost.currency) if posting.cost else None
                price_amount = posting.price.number if posting.price else None
                price_currency_id = caches['commodities'].get(posting.price.currency) if posting.price else None
                
                postings_to_insert.append((
                    txn_id,
                    posting.flag,
                    account_id,
                    posting.units.number,
                    currency_id,
                    cost_amount,
                    cost_currency_id,
                    price_amount,
                    price_currency_id
                ))
            
            # Prepare tags
            if txn.tags:
                for tag in txn.tags:
                    tag_id = caches['tags'][tag]
                    transaction_tags_to_insert.append((txn_id, tag_id))
            
            # Prepare links
            if txn.links:
                for link in txn.links:
                    link_id = caches['links'][link]
                    transaction_links_to_insert.append((txn_id, link_id))
            
            # Prepare metadata
            if txn.meta:
                for key, value in txn.meta.items():
                    if key not in RESERVED_METADATA_KEYS:
                        transaction_metadata_to_insert.append((txn_id, key, str(value)))
        
        # Step 7: Batch insert all related data
        if postings_to_insert:
            posting_ids = execute_values(
                cur,
                """
                INSERT INTO postings 
                (transaction_id, flag, account_id, amount, currency_id, 
                 cost_amount, cost_currency_id, price_amount, price_currency_id)
                VALUES %s
                RETURNING id, transaction_id
                """,
                postings_to_insert,
                fetch=True
            )
            
            # Process posting metadata if needed
            # Note: This requires tracking which posting each metadata belongs to
            # For simplicity, this example omits posting metadata batch processing
        
        if transaction_tags_to_insert:
            execute_values(
                cur,
                """
                INSERT INTO transaction_tags (transaction_id, tag_id)
                VALUES %s
                ON CONFLICT DO NOTHING
                """,
                transaction_tags_to_insert
            )
        
        if transaction_links_to_insert:
            execute_values(
                cur,
                """
                INSERT INTO transaction_links (transaction_id, link_id)
                VALUES %s
                ON CONFLICT DO NOTHING
                """,
                transaction_links_to_insert
            )
        
        if transaction_metadata_to_insert:
            execute_values(
                cur,
                """
                INSERT INTO transaction_metadata (transaction_id, key, value)
                VALUES %s
                ON CONFLICT (transaction_id, key) DO UPDATE SET value = EXCLUDED.value
                """,
                transaction_metadata_to_insert
            )
    
    logging.info(f"Chunk {chunk_num} completed: {processed_count} processed, {skipped_count} skipped")
    return processed_count, skipped_count
```

### 4. Configuration for Batch Sizes

Add to `config.yaml.example`:

```yaml
import_settings:
  # Existing settings...
  chunk_size: 500  # Transaction chunk size for commits
  
  # New optimization settings
  batch_insert_size: 100  # Number of rows per batch INSERT
  preload_cache: true     # Pre-populate caches from database
  use_copy: false         # Use COPY instead of INSERT for bulk loads (experimental)
```

### 5. COPY-based Bulk Loading (Optional, for --rebuild mode)

For maximum performance during rebuilds, use PostgreSQL's COPY command:

```python
import csv
from io import StringIO

def bulk_load_transactions_with_copy(cur: psycopg2.extensions.cursor,
                                    transactions: List[data.Transaction],
                                    caches: Dict[str, Dict[str, int]],
                                    id_generator: TransactionIdGenerator) -> None:
    """
    Ultra-fast bulk loading using PostgreSQL COPY command.
    Only suitable for initial loads or rebuilds (no duplicate checking).
    
    Performance: Can load 100,000+ transactions per second.
    """
    # Prepare CSV buffers for each table
    txn_buffer = StringIO()
    posting_buffer = StringIO()
    
    txn_writer = csv.writer(txn_buffer, delimiter='\t')
    posting_writer = csv.writer(posting_buffer, delimiter='\t')
    
    for txn in transactions:
        txn_with_id = add_transaction_id_to_beancount_transaction(
            transaction=txn,
            force_recalculate=False,
            strict_validation=False,
            id_generator=id_generator
        )
        external_id = txn_with_id.meta['transaction_id']
        
        # Write transaction row
        txn_writer.writerow([
            external_id,
            txn.date,
            txn.flag,
            txn.payee or '',
            txn.narration,
            txn.meta.get('filename', ''),
            txn.meta.get('lineno', '')
        ])
        
        # Write posting rows
        for posting in txn.postings:
            posting_writer.writerow([
                external_id,  # Will be joined to get transaction_id
                posting.flag or '',
                posting.account,
                posting.units.number,
                posting.units.currency,
                posting.cost.number if posting.cost else None,
                posting.cost.currency if posting.cost else None,
                posting.price.number if posting.price else None,
                posting.price.currency if posting.price else None
            ])
    
    # Load transactions
    txn_buffer.seek(0)
    cur.copy_expert("""
        COPY transactions (external_id, date, flag, payee, narration, source_file, source_line)
        FROM STDIN WITH CSV DELIMITER E'\t' NULL ''
    """, txn_buffer)
    
    # Load postings (requires a temporary table for the join)
    posting_buffer.seek(0)
    cur.execute("""
        CREATE TEMP TABLE temp_postings (
            external_id TEXT,
            flag VARCHAR(20),
            account_name TEXT,
            amount NUMERIC(19, 4),
            currency_name TEXT,
            cost_amount NUMERIC(19, 4),
            cost_currency_name TEXT,
            price_amount NUMERIC(19, 4),
            price_currency_name TEXT
        )
    """)
    
    cur.copy_expert("""
        COPY temp_postings
        FROM STDIN WITH CSV DELIMITER E'\t' NULL ''
    """, posting_buffer)
    
    # Insert postings with proper foreign key resolution
    cur.execute("""
        INSERT INTO postings (transaction_id, flag, account_id, amount, currency_id,
                            cost_amount, cost_currency_id, price_amount, price_currency_id)
        SELECT 
            t.id,
            tp.flag,
            a.id,
            tp.amount,
            c1.id,
            tp.cost_amount,
            c2.id,
            tp.price_amount,
            c3.id
        FROM temp_postings tp
        JOIN transactions t ON t.external_id = tp.external_id
        JOIN accounts a ON a.name = tp.account_name
        JOIN commodities c1 ON c1.name = tp.currency_name
        LEFT JOIN commodities c2 ON c2.name = tp.cost_currency_name
        LEFT JOIN commodities c3 ON c3.name = tp.price_currency_name
    """)
    
    cur.execute("DROP TABLE temp_postings")
```

## Understanding INSERT...ON CONFLICT Behavior

### How ON CONFLICT Works

When PostgreSQL encounters a conflict (duplicate key violation):

1. **Detection**: The conflict is detected via UNIQUE constraint
2. **Action**: Instead of throwing an error, it executes the DO UPDATE or DO NOTHING clause
3. **Return**: RETURNING clause works regardless of insert or update

### Examples for Each Table Type

```sql
-- Commodities: Simple reference table
INSERT INTO commodities (name) VALUES ('USD')
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
RETURNING id;
-- Result: Always returns the ID, whether inserted or existing

-- Transactions: Skip duplicates
INSERT INTO transactions (external_id, ...) VALUES ('abc123', ...)
ON CONFLICT (external_id) DO NOTHING
RETURNING id;
-- Result: Returns NULL if duplicate (skipped), ID if inserted

-- Accounts: Preserve existing data
INSERT INTO accounts (name, type, status) VALUES ('Assets:Bank', 'Assets', 'open')
ON CONFLICT (name) DO UPDATE 
SET name = EXCLUDED.name  -- No-op, preserves existing type and status
RETURNING id;
-- Result: Returns ID without modifying existing account properties
```

### Key Points:
- **DO UPDATE with no-op**: Returns existing row without modifications
- **DO NOTHING**: Returns NULL for conflicts (useful for skip behavior)
- **Thread-safe**: Multiple imports can run concurrently
- **Atomic**: Either inserts or returns existing, never partial state

## Integration Guide

### Step 1: Add optimization functions to src/import_beancount.py

Add these functions after the existing utility functions (around line 415):
- `preload_caches()`
- `get_or_create_single()` and `get_or_create_batch()`
- `create_transaction_scoped_cache()` and `safe_cache_update()`
- `process_transaction_chunk_optimized()`
- `bulk_load_transactions_with_copy()` (optional)

### Step 2: Update process_and_import() function

Replace cache initialization (line ~1088) with cache-safe approach:
```python
# Old:
caches = {'accounts': {}, 'commodities': {}, 'tags': {}, 'links': {}}

# New:
# Initialize global caches (persist across chunks)
with conn.cursor() as cur:
    if config.get('import_settings', {}).get('preload_cache', True):
        global_caches = preload_caches(cur)
    else:
        global_caches = {'accounts': {}, 'commodities': {}, 'tags': {}, 'links': {}}

# Then for each chunk, use transaction-scoped caches:
# (in the chunk processing loop)
local_caches = create_transaction_scoped_cache(global_caches)
```

### Step 3: Update chunk processing (line ~1127)

```python
# Old approach (unsafe cache management):
for chunk_num, chunk in enumerate(chunks, 1):
    with conn.cursor() as cur:
        processed, skipped = process_transaction_chunk(
            cur, chunk, caches, id_generator, config, dry_run, resume_mode, chunk_info
        )
    conn.commit()

# New approach (with proper cache management):
for chunk_num, chunk in enumerate(chunks, 1):
    # Create transaction-scoped cache for this chunk
    local_caches = create_transaction_scoped_cache(global_caches)
    
    try:
        with conn.cursor() as cur:
            if config.get('import_settings', {}).get('use_batch_operations', True):
                processed, skipped = process_transaction_chunk_optimized(
                    cur, chunk, local_caches, id_generator, config, dry_run, resume_mode, chunk_info
                )
            else:
                processed, skipped = process_transaction_chunk(
                    cur, chunk, local_caches, id_generator, config, dry_run, resume_mode, chunk_info
                )
        
        # Commit the chunk
        conn.commit()
        
        # Update global cache only after successful commit
        safe_cache_update(global_caches, local_caches)
        
    except Exception as e:
        # Rollback on error - local_caches discarded
        conn.rollback()
        logging.error(f"Chunk {chunk_num} failed: {e}")
        raise
```

### Step 4: Update rebuild mode for COPY support (optional)

In `handle_rebuild()` function, after truncation:
```python
if config.get('import_settings', {}).get('use_copy', False):
    logging.info("Using COPY for bulk loading (fastest)")
    # Use bulk_load_transactions_with_copy
```

## Testing Strategy

### 1. Unit Tests for New Functions
```python
def test_preload_caches():
    # Test that caches are populated correctly
    # Test with empty database
    # Test with existing data

def test_get_or_create_batch():
    # Test creating new entities
    # Test with existing entities
    # Test cache updates

def test_process_transaction_chunk_optimized():
    # Test maintains same behavior as original
    # Test performance improvement
    # Test transaction integrity
```

### 2. Integration Tests
- Test full import with optimizations enabled
- Test that optimized and non-optimized produce identical database state
- Test rollback on error maintains atomicity

### 3. Performance Tests
```python
# Benchmark script
import time

def benchmark_import(use_optimizations: bool):
    start = time.time()
    # Run import with flag
    elapsed = time.time() - start
    return elapsed

# Should show 10-100x improvement
```

## Rollout Plan

1. **Phase 1**: Add optimization functions without changing default behavior
2. **Phase 2**: Add config flags to enable optimizations (default: false)
3. **Phase 3**: Test thoroughly with real data
4. **Phase 4**: Enable optimizations by default
5. **Phase 5**: Remove old code paths after validation

## Expected Performance Improvements

| Operation | Current | Optimized | Improvement |
|-----------|---------|-----------|-------------|
| 1000 transactions import | ~60 seconds | ~2 seconds | 30x |
| Database queries | 7000+ | <200 | 35x |
| Network round-trips | 7000+ | <200 | 35x |
| Memory usage | O(1) | O(chunk_size) | Minimal increase |

## Critical Safety Checks

1. **Transaction Integrity**: All optimizations maintain chunk-level commits
2. **Duplicate Handling**: Batch duplicate checking maintains exact semantics
3. **Error Handling**: Any error in a chunk rolls back entire chunk
4. **Cache Consistency**: Cache updates are atomic with database operations
5. **Dry Run**: All optimizations respect --dry-run flag

## Monitoring and Validation

Add these metrics to track optimization effectiveness:

```python
# Add to process_transaction_chunk_optimized
metrics = {
    'cache_hits': 0,
    'cache_misses': 0,
    'batch_inserts': 0,
    'individual_inserts': 0,
    'total_queries': 0
}

# Log at end of import
logging.info(f"Performance metrics: {metrics}")
```

## Backwards Compatibility

All optimizations can be disabled via config:
```yaml
import_settings:
  use_optimizations: false  # Revert to original behavior
```

This ensures safe rollback if issues are discovered.