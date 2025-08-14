# Implementation Plan: Hybrid Chunked Processing for Beancount Import

## Overview
Implement chunked transaction processing with progress tracking, resume capability, and partial rollback options to handle large beancount file imports efficiently.

## Current State Analysis
- **Existing Implementation**: Single transaction processing in `src/import_beancount.py`
- **Problem**: 6604 transactions taking 46+ minutes, no progress visibility, memory issues
- **Solution**: Hybrid chunked approach with 4 new features

## Features to Implement

### 1. Progress Tracking
**Goal**: Log detailed progress during chunk processing
**Implementation**:
```python
# Add progress logging for each chunk
logging.info(f"Processing chunk {chunk_num}/{total_chunks} (transactions {start_idx}-{end_idx})")
logging.info(f"Chunk {chunk_num} completed: {processed_count} processed, {skipped_count} skipped")
```

### 2. Resume Capability (`--resume` flag)
**Goal**: Skip already-imported transactions based on external_id
**Implementation**:
- Use existing duplicate detection logic
- When `--resume` flag present, always set `on_duplicate: skip` behavior
- Log resume progress clearly

### 3. Duplicate Detection (Enhanced)
**Goal**: Leverage existing external_id logic for chunked processing
**Implementation**:
- Keep existing `external_id` uniqueness constraint
- Enhance logging to show skip reasons
- Optimize duplicate checking queries

### 4. Rollback Partial (`--rollback-partial` flag)
**Goal**: Clean up partial imports before retry
**Implementation**:
- Identify transactions from current file set using source_file metadata
- Remove related data (postings, metadata, tags, links) in dependency order
- Provide confirmation prompt

## Implementation Steps

### Step 1: Configuration Changes
**File**: `config.yaml.example`
```yaml
import_settings:
  chunk_size: 500  # Add this parameter
```

### Step 2: CLI Argument Updates
**File**: `src/import_beancount.py` - `parse_arguments()`
```python
parser.add_argument("--resume", action="store_true", 
                   help="Skip transactions already imported (based on external_id)")
parser.add_argument("--rollback-partial", action="store_true",
                   help="Remove partial import data before starting fresh")
```

### Step 3: Core Function Modifications

#### A. Update `process_and_import()` - Hybrid Transaction Logic
```python
def process_and_import(conn, file_paths, id_generator, config, dry_run, resume_mode):
    # Phase 1: Schema operations (single transaction)
    with conn.cursor() as cur:
        process_commodities(cur, entries, caches['commodities'], dry_run)
        process_accounts(cur, entries, caches['accounts'], dry_run)
        conn.commit()
    
    # Phase 2: Chunked transaction processing
    chunk_size = config.get('import_settings', {}).get('chunk_size', 500)
    transaction_entries = [e for e in entries if isinstance(e, data.Transaction)]
    
    for chunk_num, chunk in enumerate(chunk_transactions(transaction_entries, chunk_size), 1):
        with conn.cursor() as cur:
            processed, skipped = process_transaction_chunk(cur, chunk, caches, id_generator, config, dry_run, resume_mode)
            conn.commit()
            logging.info(f"Chunk {chunk_num} completed: {processed} processed, {skipped} skipped")
    
    # Phase 3: Cleanup operations (single transaction)
    with conn.cursor() as cur:
        process_other_directives(cur, entries, caches, dry_run)
        if not dry_run:
            state_hash = calculate_file_state_hash(entries, id_generator, config)
            update_import_state(cur, state_hash, dry_run)
        conn.commit()
```

#### B. Create `chunk_transactions()` Helper
```python
def chunk_transactions(transactions, chunk_size):
    """Split transactions into chunks of specified size."""
    for i in range(0, len(transactions), chunk_size):
        yield transactions[i:i + chunk_size]
```

#### C. Create `process_transaction_chunk()` Function
```python
def process_transaction_chunk(cur, chunk, caches, id_generator, config, dry_run, resume_mode):
    """Process a single chunk of transactions."""
    processed_count = 0
    skipped_count = 0
    
    on_duplicate = 'skip' if resume_mode else config.get('import_settings', {}).get('on_duplicate', 'skip')
    
    for entry in chunk:
        # ... existing transaction processing logic
        # Track processed vs skipped counts
    
    return processed_count, skipped_count
```

### Step 4: New Functions for Rollback Partial

#### A. `handle_rollback_partial()` Function
```python
def handle_rollback_partial(conn, file_paths, dry_run):
    """Remove all data from previous partial imports of these files."""
    if dry_run:
        logging.info("[DRY RUN] Would remove partial import data")
        return
    
    # Get source file names
    source_files = [os.path.basename(f) for f in file_paths]
    
    # Confirmation prompt
    print(f"\n*** WARNING ***")
    print(f"This will permanently delete all imported data from files: {source_files}")
    confirm = input("Are you sure you want to continue? (yes/no): ")
    
    if confirm.lower() != 'yes':
        logging.info("Rollback partial aborted by user")
        sys.exit(0)
    
    # Delete in dependency order
    with conn.cursor() as cur:
        # 1. Delete posting metadata
        cur.execute("""
            DELETE FROM posting_metadata 
            WHERE posting_id IN (
                SELECT p.id FROM postings p
                JOIN transactions t ON p.transaction_id = t.id
                WHERE t.source_file = ANY(%s)
            )
        """, (source_files,))
        
        # 2. Delete postings
        cur.execute("""
            DELETE FROM postings 
            WHERE transaction_id IN (
                SELECT id FROM transactions WHERE source_file = ANY(%s)
            )
        """, (source_files,))
        
        # 3. Delete transaction metadata, tags, links
        for table in ['transaction_metadata', 'transaction_tags', 'transaction_links']:
            cur.execute(f"""
                DELETE FROM {table}
                WHERE transaction_id IN (
                    SELECT id FROM transactions WHERE source_file = ANY(%s)
                )
            """, (source_files,))
        
        # 4. Delete transactions
        cur.execute("DELETE FROM transactions WHERE source_file = ANY(%s)", (source_files,))
        
        # 5. Delete other directives
        for table in ['balance_assertions', 'prices', 'events', 'documents']:
            cur.execute(f"DELETE FROM {table} WHERE source_file = ANY(%s)", (source_files,))
    
    conn.commit()
    logging.info(f"Removed partial import data for files: {source_files}")
```

### Step 5: Enhanced Progress Logging

#### A. Update Transaction Processing to Show Progress
```python
def process_transaction_chunk(cur, chunk, caches, id_generator, config, dry_run, resume_mode, chunk_info):
    """Process a single chunk with detailed progress logging."""
    chunk_num, total_chunks, start_idx, end_idx = chunk_info
    
    logging.info(f"Processing chunk {chunk_num}/{total_chunks} (transactions {start_idx}-{end_idx})")
    
    # ... existing processing logic
    
    logging.info(f"Chunk {chunk_num} completed: {processed_count} processed, {skipped_count} skipped")
    if skipped_count > 0 and resume_mode:
        logging.info(f"Resume mode: skipped {skipped_count} already-imported transactions")
```

### Step 6: Update Main Function Logic
```python
def main():
    # ... existing setup logic
    
    # Handle rollback partial first (if requested)
    if args.rollback_partial:
        handle_rollback_partial(conn, args.input_file, args.dry_run)
    
    # Handle rebuild
    if args.rebuild:
        handle_rebuild(conn, args.dry_run)
    
    # Main import with resume capability
    process_and_import(conn, args.input_file, id_generator, config, args.dry_run, args.resume)
```

## Testing Strategy

### Test Cases
1. **Normal Import**: Large file without chunking flags
2. **Chunked Import**: Large file with progress logging
3. **Resume Import**: Simulate failure mid-import, then resume
4. **Rollback Partial**: Import partial data, then clean and retry
5. **Dry Run**: All modes with --dry-run flag
6. **Edge Cases**: Empty chunks, single-transaction chunks

### Expected Behavior
```bash
# Normal chunked import (500 transactions per chunk)
./import_beancount.py -c config.yaml -i large.beancount
# Output: 13 progress updates for 6604 transactions

# Failed import (simulated at chunk 7)
# Resume import
./import_beancount.py -c config.yaml -i large.beancount --resume
# Output: Chunks 1-6 skipped, chunks 7-13 processed

# Clean partial and retry
./import_beancount.py -c config.yaml -i large.beancount --rollback-partial
# Output: Removes partial data, then imports all chunks fresh
```

## Performance Expectations

### Before (Single Transaction)
- 6604 transactions: 46+ minutes (stuck)
- No progress visibility
- Memory issues with large transactions

### After (Chunked Processing)
- 6604 transactions: ~5-15 minutes expected
- Progress update every 30-60 seconds (per chunk)
- Memory efficient (commit after each chunk)
- Resume capability for failed imports

## File Changes Required

### Modified Files
1. `src/import_beancount.py` - Core implementation
2. `config.yaml.example` - Add chunk_size parameter
3. `dev-docs/specification.md` - Updated with new features (already done)

### New Files
None - all functionality added to existing files

## Dependencies
- No new external dependencies required
- Uses existing psycopg2, beancount, yaml libraries

## Backward Compatibility
- Existing config files work (chunk_size defaults to 500)
- Existing CLI usage works (new flags are optional)
- Database schema unchanged
- Existing duplicate detection logic enhanced but not changed

## Implementation Order
1. CLI argument parsing updates
2. Configuration parameter addition
3. Helper functions (chunk_transactions, handle_rollback_partial)
4. Core logic refactoring (process_and_import hybrid approach)
5. Enhanced progress logging
6. Testing and validation

This plan provides complete context for implementing the hybrid chunked processing approach with all requested features.