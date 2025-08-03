#!/usr/bin/env python3
"""Script to delete rows from ktem__index table based on provided IDs"""

import sys
import os
import argparse

# Add the project path to Python path
sys.path.insert(0, 'libs/ktem')
sys.path.insert(0, 'libs/kotaemon')

from sqlalchemy.orm import Session
from sqlalchemy import select, delete
from ktem.db.engine import engine
from ktem.index.models import Index

def get_existing_records(session, ids):
    """Get existing records for the given IDs"""
    stmt = select(Index).where(Index.id.in_(ids))
    result = session.execute(stmt)
    return [record for (record,) in result]

def delete_records_by_ids(ids, confirm=True):
    """Delete records from ktem__index table by IDs"""
    
    if not ids:
        print("No IDs provided. Nothing to delete.")
        return
    
    # Convert to integers and remove duplicates
    try:
        ids = list(set(int(id_val) for id_val in ids))
    except ValueError as e:
        print(f"Error: Invalid ID format. All IDs must be integers. {e}")
        return
    
    print(f"Attempting to delete records with IDs: {ids}")
    
    try:
        with Session(engine) as session:
            # First, check which records exist
            existing_records = get_existing_records(session, ids)
            
            if not existing_records:
                print("No records found with the provided IDs.")
                return
            
            print(f"\nFound {len(existing_records)} record(s) to delete:")
            for record in existing_records:
                print(f"  ID: {record.id}, Name: {record.name}, Type: {record.index_type}")
            
            # Confirmation
            if confirm:
                response = input(f"\nAre you sure you want to delete these {len(existing_records)} record(s)? (yes/no): ")
                if response.lower() not in ['yes', 'y']:
                    print("Deletion cancelled.")
                    return
            
            # Perform deletion
            existing_ids = [record.id for record in existing_records]
            stmt = delete(Index).where(Index.id.in_(existing_ids))
            result = session.execute(stmt)
            session.commit()
            
            print(f"\nSuccessfully deleted {result.rowcount} record(s).")
            
            # Check for IDs that didn't exist
            not_found_ids = set(ids) - set(existing_ids)
            if not_found_ids:
                print(f"Note: The following IDs were not found: {sorted(not_found_ids)}")
                
    except Exception as e:
        print(f"Error during deletion: {e}")

def list_all_records():
    """List all records in the table for reference"""
    print("=== Current Records in ktem__index ===")
    
    try:
        with Session(engine) as session:
            stmt = select(Index)
            result = session.execute(stmt)
            
            records = [record for (record,) in result]
            
            if not records:
                print("No records found in ktem__index table.")
                return
            
            print(f"Total records: {len(records)}\n")
            
            for record in records:
                print(f"ID: {record.id:>3} | Name: {record.name:<20} | Type: {record.index_type}")
                
    except Exception as e:
        print(f"Error accessing database: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="Delete rows from ktem__index table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --ids 1 2 3           # Delete records with IDs 1, 2, and 3
  %(prog)s --ids 1,2,3           # Same as above (comma-separated)
  %(prog)s --interactive         # Interactive mode
  %(prog)s --list                # List all current records
  %(prog)s --list --ids 1,2      # List records then delete IDs 1,2
        """
    )
    
    parser.add_argument(
        '--ids', 
        nargs='*', 
        help='List of IDs to delete (space or comma separated)'
    )
    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List all current records before deletion'
    )
    
    args = parser.parse_args()
    
    # Show current records if requested
    if args.list:
        list_all_records()
        print()
    
    # Handle different modes
    if args.ids:
        # Handle comma-separated IDs in a single argument
        ids = []
        for id_arg in args.ids:
            ids.extend(id_arg.split(','))
        delete_records_by_ids(ids)
    elif not args.list:
        # No specific action, show help
        parser.print_help()

if __name__ == "__main__":
    main()