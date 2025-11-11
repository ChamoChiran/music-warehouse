"""
Pytest configuration and shared fixtures for music_warehouse tests.
"""

import pytest
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


@pytest.fixture(autouse=True)
def reset_environment(monkeypatch):
    """Reset environment variables for each test"""
    # You can add any environment setup here
    pass


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create a temporary directory for test data"""
    data_dir = tmp_path / "data" / "raw" / "geo"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir

