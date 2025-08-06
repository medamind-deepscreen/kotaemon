#!/usr/bin/env python3
"""
Test script to list files in the index
"""

from batch_indexer_module import BatchIndexerModule

indexer = BatchIndexerModule()

files = indexer.get_files_for_user(user_id="5c1d0b271cf34c95b65063c0bb313af0", index_name="File Collection")

print(f"Harshit files: {files}")