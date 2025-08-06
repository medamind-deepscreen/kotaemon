"""
Batch Indexer Module for Kotaemon

This module provides programmatic access to batch indexing functionality.
It can be imported and used in other Python scripts to index files programmatically.
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

from ktem.app import BaseApp
from ktem.db.engine import engine
from ktem.utils.generator import Generator as GeneratorWrapper
from sqlalchemy import select
from sqlalchemy.orm import Session


class BatchIndexerModule:
    def __init__(self, app: Optional[BaseApp] = None):
        if app is None:
            self.app = BaseApp()
        else:
            self.app = app
        
        self.index_manager = self.app.index_manager
        self.logger = logging.getLogger(__name__)
    
    def get_index(self, index_name: Optional[str] = None, index_id: Optional[int] = None):
        """
        Get an index by name or ID
            
        Returns:
            Index object or None if not found
        """
        if index_name:
            for index in self.index_manager.indices:
                if index.name == index_name:
                    return index
        elif index_id:
            for index in self.index_manager.indices:
                if index.id == index_id:
                    return index
        
        return None
    
    def get_supported_extensions(self, index) -> List[str]:
        """Get supported file extensions from index config"""

        supported_types = index.config.get("supported_file_types", ".pdf, .txt")
        return [ext.strip() for ext in supported_types.split(",")]
    
    def get_files_to_index(self, folder_path: Path, supported_extensions: List[str]) -> List[Path]:
        """Get all files in the folder that match supported extensions"""

        files = []
        if not folder_path.exists():
            raise FileNotFoundError(f"Folder {folder_path} does not exist")
        
        if not folder_path.is_dir():
            raise NotADirectoryError(f"{folder_path} is not a directory")
        
        for file_path in folder_path.rglob("*"):
            if file_path.is_file():
                # Check if file extension is supported
                if any(file_path.suffix.lower() in ext.lower() for ext in supported_extensions):
                    files.append(file_path)
        
        return sorted(files)
    
    def index_single_file(self, file_path: Path, index_name: Optional[str] = None, 
                         index_id: Optional[int] = None, reindex: bool = False, 
                         user_id: str = "default") -> Dict[str, Any]:
        index = self.get_index(index_name, index_id)
        if not index:
            if index_name:
                raise ValueError(f"Index with name '{index_name}' not found")
            elif index_id:
                raise ValueError(f"Index with ID {index_id} not found")
            else:
                raise ValueError("No index specified")
        
        pipeline = index.get_indexing_pipeline({}, user_id)
        
        try:
            stream_gen = pipeline.stream(str(file_path), reindex=reindex)
            wrapped_stream = GeneratorWrapper(stream_gen)
            docs = []
            
            try:
                for doc in wrapped_stream:
                    docs.append(doc)
            except ValueError as e:
                # This exception is raised when file already exists and reindex=False
                if "already indexed" in str(e) and not reindex:
                    return {
                        "success": True,
                        "file_id": None,
                        "status": "skipped",
                        "message": str(e)
                    }
                else:
                    raise e
            
            if hasattr(wrapped_stream, 'value') and wrapped_stream.value:
                file_ids, errors, all_docs = wrapped_stream.value
                
                if file_ids and len(file_ids) > 0 and file_ids[0] is not None:
                    file_id = file_ids[0]
                    return {
                        "success": True,
                        "file_id": file_id,
                        "status": "indexed",
                        "message": "File indexed successfully"
                    }
                else:
                    error_msg = errors[0] if errors and len(errors) > 0 and errors[0] else "Unknown error"
                    return {
                        "success": False,
                        "file_id": None,
                        "status": "failed",
                        "message": f"Failed to index file: {error_msg}"
                    }
            else:
                return {
                    "success": False,
                    "file_id": None,
                    "status": "failed",
                    "message": "Failed to index file - no return value from stream"
                }
                
        except Exception as e:
            return {
                "success": False,
                "file_id": None,
                "status": "error",
                "message": str(e)
            }
    
    def index_folder(self, folder_path: Path, index_name: Optional[str] = None, 
                    index_id: Optional[int] = None, reindex: bool = False, 
                    user_id: str = "default", progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """
        Index all files in the specified folder
        
        Args:
            folder_path: Path to the folder containing files to index
            index_name: Name of the index to use
            index_id: ID of the index to use
            reindex: Whether to reindex existing files
            user_id: User ID for the indexing operation
            progress_callback: Optional callback function for progress updates
            
        Returns:
            Dictionary with indexing results
        """
        # Get the target index
        index = self.get_index(index_name, index_id)
        if not index:
            if index_name:
                raise ValueError(f"Index with name '{index_name}' not found")
            elif index_id:
                raise ValueError(f"Index with ID {index_id} not found")
            else:
                # Use the first available index
                if not self.index_manager.indices:
                    raise ValueError("No indices available")
                index = self.index_manager.indices[0]
                self.logger.info(f"Using default index: {index.name} (ID: {index.id})")
        
        self.logger.info(f"Using index: {index.name} (ID: {index.id})")
        
        # Get supported file extensions
        supported_extensions = self.get_supported_extensions(index)
        self.logger.info(f"Supported file types: {', '.join(supported_extensions)}")
        
        # Get files to index
        files = self.get_files_to_index(folder_path, supported_extensions)
        self.logger.info(f"Found {len(files)} files to index")
        
        # Index files
        indexed_count = 0
        skipped_count = 0
        error_count = 0
        results = []
        
        for i, file_path in enumerate(files, 1):
            if progress_callback:
                progress_callback(i, len(files), str(file_path))
            
            result = self.index_single_file(
                file_path=file_path,
                index_name=index_name,
                index_id=index_id,
                reindex=reindex,
                user_id=user_id
            )
            
            result["file_path"] = str(file_path)
            result["file_name"] = file_path.name
            results.append(result)
            
            if result["status"] == "indexed":
                indexed_count += 1
            elif result["status"] == "skipped":
                skipped_count += 1
            else:
                error_count += 1
        
        return {
            "success": error_count == 0,
            "indexed": indexed_count,
            "skipped": skipped_count,
            "errors": error_count,
            "total_files": len(files),
            "results": results
        }
    
    def get_files_for_user(self, user_id: str, index_name: Optional[str] = None, index_id: Optional[int] = None) -> list[dict]:
        """
        Retrieve files uploaded by a specific user.

        Args:
            user_id: The user ID whose files to retrieve.
            index_name: Name of the index to use (optional).
            index_id: ID of the index to use (optional).

        Returns:
            List of dictionaries with file information.
        """
        index = self.get_index(index_name, index_id)
        if not index:
            if index_name:
                raise ValueError(f"Index with name '{index_name}' not found")
            elif index_id:
                raise ValueError(f"Index with ID {index_id} not found")
            else:
                raise ValueError("No index specified")

        files = []
        try:
            with Session(engine) as session:
                Source = index._resources["Source"]
                # Try to filter by user_id if the column exists
                if hasattr(Source, "user") and user_id:
                    query = select(Source).where(Source.user == user_id)
                else:
                    # If user_id is not tracked, return all files (or empty)
                    query = select(Source)
                for row in session.execute(query):
                    source = row[0]
                    file_info = {
                        "id": getattr(source, "id", None),
                        "name": getattr(source, "name", None),
                        "path": getattr(source, "path", None),
                        "type": getattr(source, "type", None),
                        "user": getattr(source, "user", None)
                    }
                    # Only include if user_id matches (if user_id column exists)
                    if not hasattr(source, "user") or source.user == user_id:
                        files.append(file_info)
        except Exception as e:
            self.logger.error(f"Error retrieving files for user {user_id}: {e}")
            return []

        return files
    
    
    def delete_file_from_index(self, file_path: Path, index_name: Optional[str] = None, 
                              index_id: Optional[int] = None, user_id: str = "default") -> Dict[str, Any]:
        """
        Delete a file from the index
        
        Args:
            file_path: Path to the file to delete
            index_name: Name of the index to use
            index_id: ID of the index to use
            user_id: User ID for the operation
            
        Returns:
            Dictionary with deletion result
        """

        index = self.get_index(index_name, index_id)
        if not index:
            if index_name:
                raise ValueError(f"Index with name '{index_name}' not found")
            elif index_id:
                raise ValueError(f"Index with ID {index_id} not found")
            else:
                raise ValueError("No index specified")
        
        try:
            with Session(engine) as session:
                Source = index._resources["Source"]
                # Try to filter by user_id if the column exists
                if hasattr(Source, "user") and user_id:
                    query = select(Source).where(Source.user == user_id)
                else:
                    # If user_id is not tracked, return all files (or empty)
                    query = select(Source)

                sources = session.execute(query).all()

                source = None

                for s in sources:
                    if s[0].name == file_path.name:
                        source = s[0]
                        break
                
                if not source:
                    return {
                        "success": False,
                        "message": "File not found in index"
                    }
                
                file_id = source.id
                file_name = source.name
                
                # Delete the source record
                session.delete(source)
                
                # Find and delete related index records
                vs_ids, ds_ids = [], []
                index_records = session.execute(
                    select(index._resources["Index"]).where(
                        index._resources["Index"].source_id == file_id
                    )
                ).all()
                
                for each in index_records:
                    if each[0].relation_type == "vector":
                        vs_ids.append(each[0].target_id)
                    elif each[0].relation_type == "document":
                        ds_ids.append(each[0].target_id)
                    session.delete(each[0])
                
                session.commit()
            
            # Delete from vector store and document store
            if vs_ids:
                index._vs.delete(vs_ids)
            index._docstore.delete(ds_ids)
            
            self.logger.info(f"File {file_name} has been deleted from index")
            
            return {
                "success": True,
                "file_id": file_id,
                "file_name": file_name,
                "message": f"File {file_name} deleted from index successfully"
            }
            
        except Exception as e:
            self.logger.error(f"Error deleting file {file_path}: {str(e)}")
            return {
                "success": False,
                "message": f"Error deleting file: {str(e)}"
            }