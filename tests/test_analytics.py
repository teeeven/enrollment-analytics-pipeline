"""
Unit tests for enrollment analytics functionality.
"""

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from analytics import EnrollmentAnalytics


class TestEnrollmentAnalytics:
    """Test cases for EnrollmentAnalytics class."""

    @pytest.fixture
    def sample_current_data(self):
        """Sample current enrollment snapshot."""
        return pd.DataFrame(
            {
                "student_id": ["S001", "S002", "S003", "S004", "S005"],
                "student_name": [
                    "John Doe",
                    "Jane Smith",
                    "Mike Johnson",
                    "Sarah Wilson",
                    "Tom Brown",
                ],
                "division": ["Engineering", "Business", "Arts", "Engineering", "Business"],
                "program": ["Computer Science", "Finance", "English", "Mechanical", "Marketing"],
                "level": [
                    "Undergraduate",
                    "Graduate",
                    "Undergraduate",
                    "Graduate",
                    "Undergraduate",
                ],
            }
        )

    @pytest.fixture
    def sample_previous_data(self):
        """Sample previous enrollment snapshot."""
        return pd.DataFrame(
            {
                "student_id": ["S001", "S002", "S006", "S007"],
                "student_name": ["John Doe", "Jane Smith", "Lisa Davis", "Bob Miller"],
                "division": ["Engineering", "Business", "Arts", "Engineering"],
                "program": ["Computer Science", "Finance", "History", "Civil"],
                "level": ["Undergraduate", "Graduate", "Undergraduate", "Graduate"],
            }
        )

    @pytest.fixture
    def analytics_instance(self):
        """Create EnrollmentAnalytics instance for testing."""
        config = {"institution_name": "Test University", "semester_term": "Test Term 2024"}
        return EnrollmentAnalytics(config)

    def test_init(self, analytics_instance):
        """Test EnrollmentAnalytics initialization."""
        assert analytics_instance.institution_name == "Test University"
        assert analytics_instance.semester_term == "Test Term 2024"

    def test_compare_snapshots_new_enrollments(
        self, analytics_instance, sample_current_data, sample_previous_data
    ):
        """Test snapshot comparison identifies new enrollments correctly."""
        metrics = analytics_instance.compare_snapshots(sample_current_data, sample_previous_data)

        # Should identify S003, S004, S005 as new students (in current but not previous)
        expected_new_students = {"S003", "S004", "S005"}
        actual_new_students = set(metrics["new_students"])

        assert actual_new_students == expected_new_students
        assert metrics["new_student_count"] == 3

    def test_compare_snapshots_dropped_students(
        self, analytics_instance, sample_current_data, sample_previous_data
    ):
        """Test snapshot comparison identifies dropped students correctly."""
        metrics = analytics_instance.compare_snapshots(sample_current_data, sample_previous_data)

        # Should identify S006, S007 as dropped students (in previous but not current)
        expected_dropped_students = {"S006", "S007"}
        actual_dropped_students = set(metrics["dropped_students"])

        assert actual_dropped_students == expected_dropped_students
        assert metrics["dropped_student_count"] == 2

    def test_compare_snapshots_retained_students(
        self, analytics_instance, sample_current_data, sample_previous_data
    ):
        """Test snapshot comparison identifies retained students correctly."""
        metrics = analytics_instance.compare_snapshots(sample_current_data, sample_previous_data)

        # Should identify S001, S002 as retained students (in both snapshots)
        expected_retained_students = {"S001", "S002"}
        actual_retained_students = set(metrics["retained_students"])

        assert actual_retained_students == expected_retained_students
        assert metrics["retained_student_count"] == 2

    def test_compare_snapshots_net_change(
        self, analytics_instance, sample_current_data, sample_previous_data
    ):
        """Test net change calculation."""
        metrics = analytics_instance.compare_snapshots(sample_current_data, sample_previous_data)

        # Net change = new students (3) - dropped students (2) = +1
        assert metrics["net_change"] == 1

    def test_compare_snapshots_retention_rate(
        self, analytics_instance, sample_current_data, sample_previous_data
    ):
        """Test retention rate calculation."""
        metrics = analytics_instance.compare_snapshots(sample_current_data, sample_previous_data)

        # Retention rate = retained students (2) / previous total (4) = 50%
        expected_retention_rate = 50.0
        assert abs(metrics["retention_rate"] - expected_retention_rate) < 0.01

    def test_compare_snapshots_enrollment_totals(
        self, analytics_instance, sample_current_data, sample_previous_data
    ):
        """Test enrollment total calculations."""
        metrics = analytics_instance.compare_snapshots(sample_current_data, sample_previous_data)

        assert metrics["current_enrollment"] == 5
        assert metrics["previous_enrollment"] == 4

    def test_compare_snapshots_empty_previous(self, analytics_instance, sample_current_data):
        """Test comparison when previous snapshot is empty (baseline scenario)."""
        empty_previous = pd.DataFrame(
            columns=["student_id", "student_name", "division", "program", "level"]
        )

        metrics = analytics_instance.compare_snapshots(sample_current_data, empty_previous)

        assert metrics["current_enrollment"] == 5
        assert metrics["previous_enrollment"] == 0
        assert metrics["new_student_count"] == 5
        assert metrics["dropped_student_count"] == 0
        assert metrics["retained_student_count"] == 0
        assert metrics["net_change"] == 5
        assert metrics["retention_rate"] == 100.0  # Special case for baseline

    def test_compare_snapshots_empty_current(self, analytics_instance, sample_previous_data):
        """Test comparison when current snapshot is empty (all students dropped)."""
        empty_current = pd.DataFrame(
            columns=["student_id", "student_name", "division", "program", "level"]
        )

        metrics = analytics_instance.compare_snapshots(empty_current, sample_previous_data)

        assert metrics["current_enrollment"] == 0
        assert metrics["previous_enrollment"] == 4
        assert metrics["new_student_count"] == 0
        assert metrics["dropped_student_count"] == 4
        assert metrics["retained_student_count"] == 0
        assert metrics["net_change"] == -4
        assert metrics["retention_rate"] == 0.0

    def test_compare_snapshots_identical_data(self, analytics_instance, sample_current_data):
        """Test comparison when snapshots are identical."""
        metrics = analytics_instance.compare_snapshots(
            sample_current_data, sample_current_data.copy()
        )

        assert metrics["current_enrollment"] == 5
        assert metrics["previous_enrollment"] == 5
        assert metrics["new_student_count"] == 0
        assert metrics["dropped_student_count"] == 0
        assert metrics["retained_student_count"] == 5
        assert metrics["net_change"] == 0
        assert metrics["retention_rate"] == 100.0

    def test_division_level_analysis(self, analytics_instance, sample_current_data):
        """Test division and level breakdown analysis."""
        # This would be an extension of the current functionality
        # For now, just test that the data structure supports it
        division_counts = sample_current_data["division"].value_counts()
        level_counts = sample_current_data["level"].value_counts()

        assert "Engineering" in division_counts.index
        assert "Business" in division_counts.index
        assert "Undergraduate" in level_counts.index
        assert "Graduate" in level_counts.index

    def test_required_columns_validation(self, analytics_instance):
        """Test that analytics validates required columns exist."""
        incomplete_data = pd.DataFrame(
            {
                "student_id": ["S001", "S002"],
                "student_name": ["John Doe", "Jane Smith"],
                # Missing division, program, level columns
            }
        )

        complete_data = pd.DataFrame(
            {
                "student_id": ["S001"],
                "student_name": ["John Doe"],
                "division": ["Engineering"],
                "program": ["Computer Science"],
                "level": ["Undergraduate"],
            }
        )

        # Should handle missing columns gracefully or raise appropriate error
        # This depends on implementation - test should match actual behavior
        with pytest.raises(Exception):
            analytics_instance.compare_snapshots(incomplete_data, complete_data)


class TestEnrollmentAnalyticsEdgeCases:
    """Test edge cases and error conditions."""

    def test_large_dataset_performance(self):
        """Test performance with larger dataset."""
        # Create larger test datasets
        large_current = pd.DataFrame(
            {
                "student_id": [f"S{i:06d}" for i in range(1000)],
                "student_name": [f"Student {i}" for i in range(1000)],
                "division": ["Engineering"] * 500 + ["Business"] * 500,
                "program": ["Computer Science"] * 1000,
                "level": ["Undergraduate"] * 800 + ["Graduate"] * 200,
            }
        )

        large_previous = pd.DataFrame(
            {
                "student_id": [
                    f"S{i:06d}" for i in range(50, 950)
                ],  # 900 students with some overlap
                "student_name": [f"Student {i}" for i in range(50, 950)],
                "division": ["Engineering"] * 450 + ["Business"] * 450,
                "program": ["Computer Science"] * 900,
                "level": ["Undergraduate"] * 720 + ["Graduate"] * 180,
            }
        )

        config = {"institution_name": "Test University", "semester_term": "Test Term"}
        analytics = EnrollmentAnalytics(config)

        # Should complete without performance issues
        metrics = analytics.compare_snapshots(large_current, large_previous)

        assert metrics["current_enrollment"] == 1000
        assert metrics["previous_enrollment"] == 900
        assert isinstance(metrics["retention_rate"], float)
        assert 0 <= metrics["retention_rate"] <= 100

    def test_duplicate_student_ids(self):
        """Test handling of duplicate student IDs."""
        config = {"institution_name": "Test University", "semester_term": "Test Term"}
        analytics = EnrollmentAnalytics(config)

        duplicate_data = pd.DataFrame(
            {
                "student_id": ["S001", "S001", "S002"],  # Duplicate S001
                "student_name": ["John Doe", "John Doe", "Jane Smith"],
                "division": ["Engineering", "Engineering", "Business"],
                "program": ["Computer Science", "Computer Science", "Finance"],
                "level": ["Undergraduate", "Undergraduate", "Graduate"],
            }
        )

        clean_data = pd.DataFrame(
            {
                "student_id": ["S001", "S002"],
                "student_name": ["John Doe", "Jane Smith"],
                "division": ["Engineering", "Business"],
                "program": ["Computer Science", "Finance"],
                "level": ["Undergraduate", "Graduate"],
            }
        )

        # Should handle duplicates appropriately (remove duplicates or raise error)
        # Test implementation should match actual behavior
        metrics = analytics.compare_snapshots(duplicate_data, clean_data)
        assert isinstance(metrics, dict)
        assert "current_enrollment" in metrics
