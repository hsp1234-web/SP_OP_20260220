import hashlib
from pathlib import Path

def compute_md5(file_path: Path) -> str:
    """
    Compute the MD5 checksum of a file efficiently.
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
