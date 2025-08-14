# Pending Tests for Chunked Processing Features

## Overview
This document contains test procedures for the new hybrid chunked processing features that were implemented but not yet tested. These tests verify the progress tracking, resume capability, and partial rollback functionality.

## Test Environment Setup
- **File**: `/Users/sagar/Documents/beancount/ided/all.beancount`
- **Config**: `config.yaml` 
- **Expected data**: ~6485 transactions, 81 accounts, 13 balance directives
- **Expected chunks**: 13 chunks of 500 transactions each (with default chunk_size: 500)

---

## Test 1: Chunked Processing with Progress Tracking

### Purpose
Verify that the new chunked approach shows detailed progress and completes faster than the old single-transaction method (which took 48 minutes).

### Procedure
1. **Clean the database first:**
   ```bash
   ./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount --rebuild
   ```

2. **Observe expected output:**
   ```
   Phase 1: Processing schema operations (commodities and accounts)...
   Found X commodities, 81 accounts, 6485 transactions, 13 other directives
   Phase 1 completed and committed
   Phase 2: Processing 6485 transactions in 13 chunks of 500
   Processing chunk 1/13 (transactions 1-500)
   Chunk 1 completed: 500 processed, 0 skipped
   Processing chunk 2/13 (transactions 501-1000)
   Chunk 2 completed: 500 processed, 0 skipped
   ...
   Processing chunk 13/13 (transactions 6001-6485)
   Chunk 13 completed: 485 processed, 0 skipped
   Phase 2 completed: 6485 transactions processed, 0 skipped
   Phase 3: Processing cleanup operations...
   Phase 3 completed and committed
   Import process completed successfully
   ```

### Success Criteria
- ✅ Progress updates every 30-60 seconds (not 47-minute silence)
- ✅ Total import time significantly less than 48 minutes
- ✅ Shows chunk-by-chunk progress (13 chunks expected)
- ✅ All 6485 transactions imported successfully
- ✅ Final check passes: `./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount --check`

---

## Test 2: Resume Capability

### Purpose
Verify that the `--resume` flag can continue a partially completed import without duplicating data.

### Procedure
1. **Start a fresh import:**
   ```bash
   ./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount --rebuild
   ```

2. **Interrupt the import partway through** (use Ctrl+C after 3-5 chunks have completed)

3. **Verify partial data exists:**
   ```bash
   # Check database has some but not all transactions
   # Should show ~1500-2500 transactions if interrupted after 3-5 chunks
   ```

4. **Resume the import:**
   ```bash
   ./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount --resume
   ```

5. **Observe expected resume output:**
   ```
   Processing chunk 1/13 (transactions 1-500)
   Chunk 1 completed: 0 processed, 500 skipped
   Resume mode: skipped 500 already-imported transactions
   Processing chunk 2/13 (transactions 501-1000) 
   Chunk 2 completed: 0 processed, 500 skipped
   Resume mode: skipped 500 already-imported transactions
   ...
   Processing chunk 6/13 (transactions 2501-3000)
   Chunk 6 completed: 500 processed, 0 skipped
   ...
   ```

### Success Criteria
- ✅ Early chunks show "0 processed, 500 skipped" for already-imported data
- ✅ Later chunks show normal processing for new data
- ✅ Resume-specific logging: "Resume mode: skipped X already-imported transactions"
- ✅ Final result has all 6485 transactions (no duplicates)
- ✅ Final check passes: `./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount --check`

---

## Test 3: Rollback Partial Upload

### Purpose
Verify that `--rollback-partial` can clean up partial import data and allow a fresh start.

### Procedure
1. **Create partial import** (follow steps 1-2 from Test 2 to interrupt an import)

2. **Verify partial data exists in database**

3. **Use rollback partial:**
   ```bash
   ./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount --rollback-partial
   ```

4. **Observe expected rollback output:**
   ```
   *** WARNING ***
   This will permanently delete 2500 transactions and related data from files:
     - all.beancount
   This action cannot be undone.
   Are you sure you want to continue? (yes/no): yes
   
   Removing partial import data for 2500 transactions...
   Partial import data removed successfully:
     - 2500 transactions
     - 5000 postings
     - 150 posting metadata entries
     - 75 transaction metadata entries
     - 25 transaction tag links
     - 10 transaction links
   ```

5. **Verify database is clean:**
   ```bash
   # Check that no transactions remain from the file
   # Database should be empty or contain only other files' data
   ```

6. **Run fresh import:**
   ```bash
   ./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount
   ```

### Success Criteria
- ✅ Rollback shows warning and requires "yes" confirmation
- ✅ Detailed deletion summary shows transactions, postings, metadata removed
- ✅ Database is properly cleaned (no orphaned data)
- ✅ Fresh import works normally after rollback
- ✅ Final result has all 6485 transactions
- ✅ Final check passes: `./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount --check`

---

## Test 4: Dry Run with New Features

### Purpose
Verify that `--dry-run` works correctly with chunked processing.

### Procedure
1. **Clean database:**
   ```bash
   ./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount --rebuild
   ```

2. **Run dry-run import:**
   ```bash
   ./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount --dry-run
   ```

3. **Observe expected dry-run output:**
   ```
   [DRY RUN] Phase 1 completed (would be committed)
   Processing chunk 1/13 (transactions 1-500)
   Chunk 1 completed: 500 processed, 0 skipped
   [DRY RUN] Chunk 1 completed (would be committed)
   ...
   [DRY RUN] Phase 3 completed (would be committed)
   ```

4. **Verify no data was actually inserted:**
   ```bash
   # Database should remain empty
   # Check should show out of sync since nothing was actually imported
   ./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount --check
   ```

### Success Criteria
- ✅ All operations show "[DRY RUN]" prefix
- ✅ Progress tracking works in dry-run mode
- ✅ No actual data is inserted into database
- ✅ Check command shows database is out of sync (as expected)

---

## Test 5: Configuration Parameter

### Purpose
Verify that `chunk_size` configuration parameter works correctly.

### Procedure
1. **Modify config.yaml:**
   ```yaml
   import_settings:
     chunk_size: 1000  # Change from default 500 to 1000
   ```

2. **Run import:**
   ```bash
   ./import_beancount.py -c config.yaml -i /Users/sagar/Documents/beancount/ided/all.beancount --rebuild
   ```

3. **Observe expected output:**
   ```
   Phase 2: Processing 6485 transactions in 7 chunks of 1000
   Processing chunk 1/7 (transactions 1-1000)
   Chunk 1 completed: 1000 processed, 0 skipped
   ...
   Processing chunk 7/7 (transactions 6001-6485)
   Chunk 7 completed: 485 processed, 0 skipped
   ```

4. **Restore original config:**
   ```yaml
   import_settings:
     chunk_size: 500  # Restore to original value
   ```

### Success Criteria
- ✅ Shows 7 chunks of 1000 instead of 13 chunks of 500
- ✅ Import completes successfully with larger chunks
- ✅ Configuration parameter is properly read and applied

---

## Performance Benchmarks

### Old Implementation (Baseline)
- **Total time**: 48 minutes (14:09:50 to 14:57:57)
- **Progress visibility**: Single 47-minute gap with no updates
- **Memory usage**: High (single massive transaction)

### New Implementation (Target)
- **Total time**: < 15 minutes (goal)
- **Progress visibility**: Updates every 30-60 seconds
- **Memory usage**: Low (chunked commits)
- **Resume capability**: Can restart from partial completion

---

## Troubleshooting

### If Import Seems Stuck
- Look for chunk progress messages every 30-60 seconds
- If no progress for >5 minutes, may indicate database connectivity issues

### If Resume Doesn't Work
- Check that `external_id` values are being generated consistently
- Verify `on_duplicate: skip` is in configuration

### If Rollback Partial Fails
- Check database permissions for DELETE operations
- Verify foreign key constraints are properly defined

### If Performance Is Still Slow
- Check database server performance and network latency
- Consider increasing `chunk_size` to 1000 or higher
- Verify database indexes are present (run `utils/create_schema.sql`)

## Notes
- All tests should be run with the same beancount file to ensure consistent results
- Keep the original config.yaml backed up when testing configuration changes
- Tests can be run in any order, but Test 1 provides the baseline for comparison