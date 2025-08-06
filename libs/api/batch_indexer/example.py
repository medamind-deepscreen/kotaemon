#!/usr/bin/env python3
"""
Test script to verify the stream-based approach works correctly
"""

import sys
from pathlib import Path

# Add the parent directory to the path to import ktem modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from batch_indexer_module import BatchIndexerModule


def test_stream_approach():
    """Test the new stream-based approach"""
    
    print("Testing Stream-Based Approach...")
    
    # Create an indexer instance
    indexer = BatchIndexerModule()
    
    # List available indices
    indices = indexer.list_indices()
    print(f"Available indices: {indices}")
    
    if not indices:
        print("No indices available. Please create an index in the Kotaemon UI first.")
        return
    
    # Use the first available index
    index_name = indices[0]['name']
    print(f"Using index: {index_name}")
    
    # Test with a folder that exists
    test_folder = Path("/Users/harshitm/Developer/Kotaemon/data")
    
    # if not test_folder.exists():
    #     print(f"Test folder {test_folder} does not exist. Creating it...")
    #     test_folder.mkdir(parents=True, exist_ok=True)
        
    #     # Create a test file
    #     test_file = test_folder / "test_stream.txt"
    #     test_file.write_text("This is a test file for stream-based indexing.")
    #     print(f"Created test file: {test_file}")
    
    try:
        # Test indexing the folder
        result = indexer.index_folder(
            folder_path=test_folder,
            index_name=index_name,
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


if __name__ == "__main__":
    test_stream_approach()