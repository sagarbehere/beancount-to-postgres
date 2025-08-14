#!/usr/bin/env python3

"""
Beancount to PostgreSQL Import Script - Wrapper

This is a convenience wrapper that calls the main implementation in src/import_beancount.py.
It ensures the src module is properly imported and passes all arguments through.
"""

import sys
import os

# Add the src directory to Python path so we can import the main module
script_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(script_dir, 'src')
sys.path.insert(0, src_dir)

# Import and run the main implementation
if __name__ == "__main__":
    try:
        from import_beancount import main
        sys.exit(main())
    except ImportError as e:
        print(f"Error: Could not import main implementation from src/import_beancount.py: {e}", file=sys.stderr)
        print("Please ensure the src/import_beancount.py file exists and is readable.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)