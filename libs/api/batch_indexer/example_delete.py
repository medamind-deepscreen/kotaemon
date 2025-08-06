#!/usr/bin/env python3
"""
Test script to verify the delete_file_from_index function works correctly
"""

from pathlib import Path

from batch_indexer_module import BatchIndexerModule

print("Testing Delete File from Index...")

indexer = BatchIndexerModule()

test_file = Path("/Users/harshitm/Developer/Kotaemon/data/Berkshire AGM 2025.pdf")

try:
    files = indexer.get_files_for_user(user_id="5c1d0b271cf34c95b65063c0bb313af0", index_name="File Collection")
    print(f"Harshit files: {files}")

    # Test deleting the file from the index
    result = indexer.delete_file_from_index(
        file_path=test_file,
        index_name="File Collection",
        user_id="5c1d0b271cf34c95b65063c0bb313af0"
    )
    
    print(f"Delete result: {result}")

    files = indexer.get_files_for_user(user_id="5c1d0b271cf34c95b65063c0bb313af0", index_name="File Collection")
    print(f"Harshit files: {files}")
    
    if result['success']:
        print("✅ Test passed! File deletion works correctly.")
        print(f"   Deleted file: {result.get('file_name', 'Unknown')}")
        print(f"   File ID: {result.get('file_id', 'Unknown')}")
    else:
        print(f"⚠️ Test completed with message: {result['message']}")
        
        if "not found" in result['message'].lower():
            print("   This is expected if the file was not previously indexed.")
        else:
            print("   This might indicate an actual error.")
        
except Exception as e:
    print(f"❌ Test failed with error: {e}")
    import traceback
    traceback.print_exc()


