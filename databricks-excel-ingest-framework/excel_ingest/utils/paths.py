from enum import Enum


class FileLocationType(Enum):
    VOLUME = "VOLUME"          # /Volumes/catalog/schema/volume/...
    DBFS = "DBFS"              # /dbfs/... or dbfs:/...
    AZURE_STORAGE = "AZURE"    # abfss://, wasbs://, *.blob.core.windows.net
    WORKSPACE = "WORKSPACE"    # /Workspace/...
    LOCAL = "LOCAL"            # /tmp/, /local/, relative paths
    UNKNOWN = "UNKNOWN"


def detect_location_type(file_path: str) -> FileLocationType:
    p = file_path.strip()
    if p.startswith("/Volumes/"):
        return FileLocationType.VOLUME
    if p.startswith("/dbfs/") or p.startswith("dbfs:/"):
        return FileLocationType.DBFS
    if any(p.startswith(s) for s in ("abfss://", "wasbs://", "wasb://")) or (
        ".blob.core.windows.net" in p or ".dfs.core.windows.net" in p
    ):
        return FileLocationType.AZURE_STORAGE
    if p.startswith("/Workspace/"):
        return FileLocationType.WORKSPACE
    if p.startswith(("/tmp/", "/local/", "/home/", "./")):
        return FileLocationType.LOCAL
    return FileLocationType.UNKNOWN
