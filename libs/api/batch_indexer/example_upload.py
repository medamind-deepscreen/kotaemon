#!/usr/bin/env python3
"""
Test script to verify the stream-based approach works correctly
"""

from pathlib import Path

from batch_indexer_module import BatchIndexerModule



"""Test the new stream-based approach"""

print("Testing Stream-Based Approach...")

# Create an indexer instance
indexer = BatchIndexerModule()


# Test with a folder that exists
test_folder = Path("/Users/harshitm/Developer/Kotaemon/data")

try:
    # Test indexing the folder
    result = indexer.index_folder(
        folder_path=test_folder,
        index_name="File Collection",
        reindex=True,
        user_id="5c1d0b271cf34c95b65063c0bb313af0"
    )
    
    print(f"Indexing result: {result}")
    
    if result['success']:
        print("✅ Test passed! Stream-based indexing works correctly.")
    else:
        print(f"⚠️ Test completed with {result['errors']} errors.")
        
        # Show detailed error information
        if result['errors'] > 0:
            print("\nError details:")
            for file_result in result['results']:
                if file_result['status'] in ['failed', 'error']:
                    print(f"  - {file_result['file_name']}: {file_result['message']}")
        
except Exception as e:
    print(f"❌ Test failed with error: {e}")
    import traceback
    traceback.print_exc()