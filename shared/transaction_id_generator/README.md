# Transaction ID Generator - Shared Library

This is a self-contained library for generating deterministic, collision-resistant transaction IDs for financial applications. It's designed to be shared across multiple projects using git subtree.

## Overview

The Transaction ID Generator creates SHA256-based unique identifiers for financial transactions with the following features:

- **Deterministic**: Same transaction data always produces the same ID
- **Collision-resistant**: Handles hash collisions with automatic suffixes
- **Self-contained**: Only requires Python standard library
- **Framework-agnostic**: Works with any Python project
- **Beancount integration**: Native support for Beancount transaction objects

## Quick Start

```python
from transaction_id_generator import TransactionIdGenerator

# Create generator instance
generator = TransactionIdGenerator()

# Generate transaction ID
txn_id = generator.generate_id(
    date="2024-01-15",
    payee="GROCERY STORE", 
    amount="-85.50 USD",
    mapped_account="Liabilities:CreditCard",
    narration="Weekly shopping"
)

print(txn_id)  # e.g., "a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"
```

## Key Functions

### `TransactionIdGenerator`
Main class for generating transaction IDs with collision tracking.

### `generate_single_transaction_id()`
Convenience function for one-off ID generation without state tracking.

### `add_transaction_id_to_beancount_transaction()`
Adds transaction_id metadata to Beancount transaction objects.

### `validate_single_ofx_id()`
Validates and cleans OFX transaction IDs.

## Hash Input Format

Transaction IDs are generated from: `{date}|{payee}|{narration}|{amount}|{account}`

Example: `"2024-01-15|GROCERY STORE|Weekly shopping|-85.50 USD|Liabilities:CreditCard"`

## Installation in Other Projects

This library is distributed via git subtree. To add it to a project:

### Initial Setup

```bash
# Add subtree to your project
git subtree add --prefix=shared/transaction-id-generator \
  https://github.com/yourusername/ofx-to-beancount.git main \
  --squash
```

### Usage in Your Project

```python
# Import from the subtree location
from shared.transaction_id_generator import TransactionIdGenerator

generator = TransactionIdGenerator()
txn_id = generator.generate_id(date, payee, amount, account)
```

### Updating the Library

When the source library is updated:

```bash
# Pull latest changes from source
git subtree pull --prefix=shared/transaction-id-generator \
  https://github.com/yourusername/ofx-to-beancount.git main \
  --squash
```

### Contributing Changes Back

If you improve the library while working in a consumer project:

```bash
# Push changes back to source
git subtree push --prefix=shared/transaction-id-generator \
  https://github.com/yourusername/ofx-to-beancount.git main
```

## Source Repository

This library is maintained as part of the [ofx-to-beancount](https://github.com/yourusername/ofx-to-beancount) project.

**Source location**: `shared-libs/transaction-id-generator/`

## Version

Current version: 1.0.0

## Dependencies

- Python 3.6+
- Standard library only (hashlib, secrets, typing, datetime)
- Optional: beancount library (for Beancount-specific functions)

## License

Same as the parent ofx-to-beancount project.