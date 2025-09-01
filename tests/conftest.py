"""
Pytest configuration and shared fixtures for enrollment analytics tests.
"""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture(scope="session")
def temp_output_dir():
    """Create a temporary directory for test outputs."""
    temp_dir = tempfile.mkdtemp(prefix="enrollment_test_")
    yield Path(temp_dir)
    # Cleanup after all tests
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_enrollment_snapshot():
    """Standard enrollment snapshot for testing."""
    return pd.DataFrame(
        {
            "student_id": ["S001001", "S001002", "S001003", "S001004", "S001005"],
            "student_name": [
                "John Smith",
                "Maria Garcia",
                "David Chen",
                "Sarah Johnson",
                "Michael Brown",
            ],
            "email": [
                "john.smith@university.edu",
                "maria.garcia@university.edu",
                "david.chen@university.edu",
                "sarah.johnson@university.edu",
                "michael.brown@university.edu",
            ],
            "division": [
                "Engineering",
                "Engineering",
                "Business",
                "Arts & Sciences",
                "Engineering",
            ],
            "program": [
                "Computer Science",
                "Electrical Engineering",
                "Finance",
                "Biology",
                "Mechanical Engineering",
            ],
            "level": [
                "Undergraduate",
                "Undergraduate",
                "Undergraduate",
                "Undergraduate",
                "Graduate",
            ],
            "status": ["Enrolled", "Enrolled", "Enrolled", "Enrolled", "Enrolled"],
        }
    )


@pytest.fixture
def sample_enrollment_metrics():
    """Standard enrollment metrics data for testing."""
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-12-01", periods=7, freq="D"),
            "total_enrollment": [20, 22, 21, 23, 24, 22, 25],
            "new_students": [0, 2, 1, 3, 2, 0, 4],
            "dropped_students": [0, 0, 2, 1, 1, 2, 1],
            "net_change": [0, 2, -1, 2, 1, -2, 3],
            "retention_rate": [100.0, 100.0, 90.91, 95.24, 95.65, 91.67, 95.45],
            "semester_term": ["Fall 2024"] * 7,
            "notes": [
                "baseline run",
                "Analysis for 2024-12-02",
                "Analysis for 2024-12-03",
                "Analysis for 2024-12-04",
                "Analysis for 2024-12-05",
                "Analysis for 2024-12-06",
                "Analysis for 2024-12-07",
            ],
        }
    )


@pytest.fixture
def mock_airflow_context():
    """Mock Airflow context for testing DAG tasks."""
    context = {
        "dag": MagicMock(),
        "task": MagicMock(),
        "execution_date": pd.Timestamp("2024-12-02"),
        "run_id": "test_run_123",
        "task_instance": MagicMock(),
        "params": {},
        "var": {"json": MagicMock(return_value={})},
        "conf": MagicMock(),
    }

    # Configure mocks
    context["dag"].dag_id = "enrollment_analytics_pipeline"
    context["task"].task_id = "test_task"
    context["task_instance"].xcom_pull = MagicMock(return_value=None)
    context["task_instance"].xcom_push = MagicMock()

    return context


@pytest.fixture
def analytics_config():
    """Standard configuration for analytics testing."""
    return {
        "institution_name": "Test University",
        "semester_term": "Test Semester 2024",
        "database": {"connection_id": "test_db_conn", "table_name": "test_enrollment_data"},
        "email": {"recipients": ["test@university.edu"], "sender": "noreply@university.edu"},
        "output": {
            "base_directory": "/tmp/enrollment_test",
            "reports_directory": "/tmp/enrollment_test/reports",
            "charts_directory": "/tmp/enrollment_test/charts",
        },
        "analysis": {"retention_target": 95.0, "anomaly_threshold": 10.0, "trend_window_days": 7},
    }


@pytest.fixture
def visualization_config():
    """Standard configuration for visualization testing."""
    return {
        "institution_name": "Test University",
        "semester_term": "Test Semester 2024",
        "chart_style": "professional",
        "color_scheme": "institutional",
        "figure_size": (12, 8),
        "dpi": 100,
        "output_format": "png",
    }


# Test data generators for different scenarios
@pytest.fixture
def large_enrollment_dataset():
    """Generate larger dataset for performance testing."""
    import numpy as np

    n_students = 1000
    divisions = ["Engineering", "Business", "Arts & Sciences", "Medicine", "Law"]
    programs = ["Program A", "Program B", "Program C", "Program D", "Program E"]
    levels = ["Undergraduate", "Graduate", "Doctorate"]

    return pd.DataFrame(
        {
            "student_id": [f"S{i:06d}" for i in range(1, n_students + 1)],
            "student_name": [f"Student {i}" for i in range(1, n_students + 1)],
            "email": [f"student{i}@university.edu" for i in range(1, n_students + 1)],
            "division": np.random.choice(divisions, n_students),
            "program": np.random.choice(programs, n_students),
            "level": np.random.choice(levels, n_students),
            "status": ["Enrolled"] * n_students,
        }
    )


@pytest.fixture
def enrollment_with_missing_data():
    """Enrollment data with some missing/null values for testing data quality."""
    return pd.DataFrame(
        {
            "student_id": ["S001", "S002", "S003", "", "S005"],
            "student_name": ["John Smith", "", "David Chen", "Sarah Johnson", "Michael Brown"],
            "email": ["john.smith@edu", "maria.garcia@edu", None, "sarah@edu", "michael@edu"],
            "division": ["Engineering", "Engineering", None, "Arts", "Engineering"],
            "program": ["CS", "EE", "Finance", "Biology", ""],
            "level": ["Undergrad", "Undergrad", "Undergrad", None, "Graduate"],
            "status": ["Enrolled", "Enrolled", "Enrolled", "Enrolled", "Enrolled"],
        }
    )


# Mock database connections for testing
@pytest.fixture
def mock_database_hook():
    """Mock database hook for testing database operations."""
    mock_hook = MagicMock()
    mock_hook.get_pandas_df.return_value = pd.DataFrame()
    mock_hook.insert_rows.return_value = None
    mock_hook.run.return_value = None
    return mock_hook


# Performance testing utilities
@pytest.fixture
def performance_timer():
    """Simple timer for performance testing."""
    import time

    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None

        def start(self):
            self.start_time = time.time()

        def stop(self):
            self.end_time = time.time()
            return self.duration

        @property
        def duration(self):
            if self.start_time and self.end_time:
                return self.end_time - self.start_time
            return None

    return Timer()


# Configuration for different test environments
@pytest.fixture(params=["development", "testing", "production"])
def environment_config(request):
    """Configuration for different environments."""
    base_config = {"institution_name": "Test University", "semester_term": "Test Semester 2024"}

    env_configs = {
        "development": {
            **base_config,
            "debug": True,
            "output_directory": "/tmp/dev_enrollment",
            "email_enabled": False,
        },
        "testing": {
            **base_config,
            "debug": False,
            "output_directory": "/tmp/test_enrollment",
            "email_enabled": False,
        },
        "production": {
            **base_config,
            "debug": False,
            "output_directory": "/opt/enrollment_data",
            "email_enabled": True,
            "email_recipients": ["admin@university.edu"],
        },
    }

    return env_configs[request.param]
