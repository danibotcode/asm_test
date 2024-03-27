# Copyright Exafunction, Inc.

"""Remote file replicator between a source and target directory."""

import posixpath
from typing import Any, Callable

from file_system import FileSystem
from file_system import FileSystemEvent
from file_system import FileSystemEventType

# If you're completing this task in an online assessment, you can increment this
# constant to enable more unit tests relevant to the task you are on (1-5).
TASK_NUM = 1

class ReplicatorSource:
    """Class representing the source side of a file replicator."""
    def __init__(self, fs: FileSystem, dir_path: str, rpc_handle: Callable[[Any], Any]):
        self._fs = fs
        self._dir_path = posixpath.normpath(dir_path)  # Normalize the directory path
        self._rpc_handle = rpc_handle

        # Initial synchronization
        self._initial_sync()
        # Watch for file system events
        self._fs.watchdir(self._dir_path, self.handle_event)  # Use self._dir_path here

    def _initial_sync(self):
        """Perform initial synchronization."""
        # Recursive function to copy files and subdirectories
        def copy_files_and_subdirs(source_path, target_path):
            items = self._fs.listdir(source_path)
            for item in items:
                item_source_path = posixpath.join(source_path, item)
                item_target_path = posixpath.join(target_path, item)
                try:
                    if self._fs.isfile(item_source_path):
                        # Check if the file exists in the target directory
                        if not self._fs.exists(item_target_path):
                            file_content = self._fs.readfile(item_source_path)
                            self._rpc_handle({'action': 'copy', 'path': item_target_path, 'content': file_content})
                    elif self._fs.isdir(item_source_path):
                        # Create the directory in the target directory if it doesn't exist
                        if not self._fs.exists(item_target_path):
                            self._fs.makedirs(item_target_path)
                        copy_files_and_subdirs(item_source_path, item_target_path)
                except Exception as e:
                    print(f"Error copying {item_source_path}: {e}")  # For debugging

        # Start initial synchronization
        copy_files_and_subdirs(self._dir_path, self._dir_path)

    def handle_event(self, event: FileSystemEvent):
        """Handle a file system event."""
        if event.type == FileSystemEventType.CREATED:
            file_path = posixpath.join(self._dir_path, event.file_name)
            self._rpc_handle({'action': 'copy', 'path': file_path})
        elif event.type == FileSystemEventType.MODIFIED:
            file_path = posixpath.join(self._dir_path, event.file_name)
            self._rpc_handle({'action': 'modify', 'path': file_path})
        elif event.type == FileSystemEventType.DELETED:
            file_path = posixpath.join(self._dir_path, event.file_name)
            self._rpc_handle({'action': 'delete', 'path': file_path})
    
    def stop_watching(self):
        """Stop watching for file system events."""
        self._fs.unwatchdir(self._dir_path)  # Use self._dir_path here


    def handle_event(self, event: FileSystemEvent):
        """Handle a file system event.

        Used as the callback provided to FileSystem.watchdir().
        """
        if event.type == FileSystemEventType.CREATED:
            file_path = posixpath.join(self._dir_path, event.file_name)
            self._rpc_handle({'action': 'copy', 'path': file_path})
        elif event.type == FileSystemEventType.MODIFIED:
            file_path = posixpath.join(self._dir_path, event.file_name)
            # Assuming the rpc_handle method can also handle a 'modify' request
            # This might involve re-copying the file or sending a differential update
            self._rpc_handle({'action': 'modify', 'path': file_path})
        elif event.type == FileSystemEventType.DELETED:
            file_path = posixpath.join(self._dir_path, event.file_name)
            # Assuming the rpc_handle method can handle a 'delete' request
            self._rpc_handle({'action': 'delete', 'path': file_path})
        # Additional handling for other event types can be added here
   
    def stop_watching(self):
        """Stop watching for file system events."""
        if self._fs.exists(self._dir_path):  # Check if directory exists
            self._fs.unwatchdir(self._dir_path)


class ReplicatorTarget:
    """Class representing the target side of a file replicator."""
    
    def __init__(self, fs: FileSystem, dir_path: str):
        self._fs = fs
        self._dir_path = dir_path

    def handle_request(self, request: Any) -> Any:
        if request['action'] == 'copy':
            # Extract the file name from the path
            _, file_name = posixpath.split(request['path'])
            target_path = posixpath.join(self._dir_path, file_name)
            # Copy the file content from the request to the target directory
            # This is a placeholder; actual implementation will depend on how the file content is sent
            self._fs.writefile(target_path, request['content'])