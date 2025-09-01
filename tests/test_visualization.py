"""
Unit tests for enrollment visualization functionality.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import pytest

# Use non-interactive backend for testing
matplotlib.use("Agg")

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from visualization import ChartGenerator


class TestChartGenerator:
    """Test cases for ChartGenerator class."""

    @pytest.fixture
    def sample_metrics_data(self):
        """Sample enrollment metrics data for testing."""
        return pd.DataFrame(
            {
                "date": ["2024-12-01", "2024-12-02", "2024-12-03", "2024-12-04", "2024-12-05"],
                "total_enrollment": [100, 102, 101, 103, 104],
                "new_students": [0, 2, 1, 3, 2],
                "dropped_students": [0, 0, 2, 1, 1],
                "net_change": [0, 2, -1, 2, 1],
                "retention_rate": [100.0, 100.0, 98.0, 99.0, 99.5],
            }
        )

    @pytest.fixture
    def chart_generator(self):
        """Create ChartGenerator instance for testing."""
        config = {
            "institution_name": "Test University",
            "semester_term": "Test Semester 2024",
            "chart_style": "professional",
            "color_scheme": "institutional",
        }
        return ChartGenerator(config)

    def test_init(self, chart_generator):
        """Test ChartGenerator initialization."""
        assert chart_generator.institution_name == "Test University"
        assert chart_generator.semester_term == "Test Semester 2024"
        assert chart_generator.chart_style == "professional"
        assert chart_generator.color_scheme == "institutional"

    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.show")
    def test_create_enrollment_trend_chart(
        self, mock_show, mock_savefig, chart_generator, sample_metrics_data
    ):
        """Test enrollment trend chart creation."""
        output_path = "/tmp/test_trend_chart.png"

        result_path = chart_generator.create_enrollment_trend_chart(
            sample_metrics_data, output_path
        )

        assert result_path == output_path
        mock_savefig.assert_called_once()

        # Verify the chart was configured properly
        fig = plt.gcf()
        assert fig is not None

        # Check that we have the expected number of data points
        ax = fig.get_axes()[0]
        lines = ax.get_lines()
        assert len(lines) > 0  # Should have at least one line (enrollment trend)

    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.show")
    def test_create_daily_change_chart(
        self, mock_show, mock_savefig, chart_generator, sample_metrics_data
    ):
        """Test daily change bar chart creation."""
        output_path = "/tmp/test_change_chart.png"

        result_path = chart_generator.create_daily_change_chart(sample_metrics_data, output_path)

        assert result_path == output_path
        mock_savefig.assert_called_once()

        # Verify chart structure
        fig = plt.gcf()
        assert fig is not None

        ax = fig.get_axes()[0]
        bars = ax.patches
        assert len(bars) > 0  # Should have bars for new/dropped students

    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.show")
    def test_create_retention_chart(
        self, mock_show, mock_savefig, chart_generator, sample_metrics_data
    ):
        """Test retention rate chart creation."""
        output_path = "/tmp/test_retention_chart.png"

        result_path = chart_generator.create_retention_chart(sample_metrics_data, output_path)

        assert result_path == output_path
        mock_savefig.assert_called_once()

        # Verify chart structure
        fig = plt.gcf()
        assert fig is not None

    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.show")
    def test_create_executive_dashboard(
        self, mock_show, mock_savefig, chart_generator, sample_metrics_data
    ):
        """Test executive dashboard creation with multiple subplots."""
        output_path = "/tmp/test_dashboard.png"

        result_path = chart_generator.create_executive_dashboard(sample_metrics_data, output_path)

        assert result_path == output_path
        mock_savefig.assert_called_once()

        # Verify dashboard structure
        fig = plt.gcf()
        assert fig is not None

        # Should have multiple subplots for executive dashboard
        axes = fig.get_axes()
        assert len(axes) >= 2  # At least enrollment trend + daily changes

    def test_apply_institutional_styling(self, chart_generator):
        """Test that institutional styling is applied to charts."""
        fig, ax = plt.subplots(figsize=(10, 6))

        chart_generator._apply_institutional_styling(fig, ax, "Test Chart Title")

        # Verify styling elements
        assert ax.get_title() != ""  # Should have a title
        assert fig.get_size_inches()[0] == 10  # Should maintain figure size
        assert fig.get_size_inches()[1] == 6

    def test_format_dates_for_display(self, chart_generator, sample_metrics_data):
        """Test date formatting for chart display."""
        formatted_dates = chart_generator._format_dates_for_display(sample_metrics_data["date"])

        # Should return formatted date strings
        assert len(formatted_dates) == len(sample_metrics_data)
        assert all(isinstance(date, str) for date in formatted_dates)

    def test_get_color_palette(self, chart_generator):
        """Test color palette retrieval."""
        colors = chart_generator._get_color_palette()

        assert isinstance(colors, dict)
        assert "primary" in colors
        assert "secondary" in colors
        assert "accent" in colors

        # Colors should be valid hex codes or named colors
        for color_name, color_value in colors.items():
            assert isinstance(color_value, str)
            assert len(color_value) > 0

    def test_empty_data_handling(self, chart_generator):
        """Test handling of empty datasets."""
        empty_data = pd.DataFrame(
            columns=[
                "date",
                "total_enrollment",
                "new_students",
                "dropped_students",
                "net_change",
                "retention_rate",
            ]
        )

        # Should handle empty data gracefully without crashing
        with patch("matplotlib.pyplot.savefig"):
            try:
                chart_generator.create_enrollment_trend_chart(empty_data, "/tmp/empty_test.png")
                # If it doesn't raise an exception, that's good
                success = True
            except Exception as e:
                # Should raise a meaningful error message
                assert "empty" in str(e).lower() or "no data" in str(e).lower()
                success = True

            assert success

    def test_single_data_point(self, chart_generator):
        """Test handling of single data point."""
        single_point_data = pd.DataFrame(
            {
                "date": ["2024-12-01"],
                "total_enrollment": [100],
                "new_students": [0],
                "dropped_students": [0],
                "net_change": [0],
                "retention_rate": [100.0],
            }
        )

        with patch("matplotlib.pyplot.savefig"):
            # Should handle single data point without crashing
            result_path = chart_generator.create_enrollment_trend_chart(
                single_point_data, "/tmp/single_point_test.png"
            )
            assert result_path is not None

    @patch("matplotlib.pyplot.savefig")
    def test_custom_output_directory(self, mock_savefig, chart_generator, sample_metrics_data):
        """Test charts are saved to custom output directories."""
        output_path = "/custom/path/test_chart.png"

        result_path = chart_generator.create_enrollment_trend_chart(
            sample_metrics_data, output_path
        )

        assert result_path == output_path
        mock_savefig.assert_called_once()

        # Verify savefig was called with correct path
        args, kwargs = mock_savefig.call_args
        assert args[0] == output_path


class TestChartGeneratorConfiguration:
    """Test different configuration options for ChartGenerator."""

    def test_different_color_schemes(self):
        """Test different color scheme configurations."""
        schemes = ["institutional", "modern", "accessible", "monochrome"]

        for scheme in schemes:
            config = {
                "institution_name": "Test University",
                "semester_term": "Test Semester",
                "color_scheme": scheme,
            }
            generator = ChartGenerator(config)
            colors = generator._get_color_palette()

            assert isinstance(colors, dict)
            assert len(colors) > 0

    def test_different_chart_styles(self):
        """Test different chart style configurations."""
        styles = ["professional", "academic", "minimal", "detailed"]

        for style in styles:
            config = {
                "institution_name": "Test University",
                "semester_term": "Test Semester",
                "chart_style": style,
            }
            generator = ChartGenerator(config)

            # Should initialize without errors
            assert generator.chart_style == style

    def test_missing_configuration_defaults(self):
        """Test that missing configuration uses appropriate defaults."""
        minimal_config = {"institution_name": "Test University", "semester_term": "Test Semester"}

        generator = ChartGenerator(minimal_config)

        # Should have reasonable defaults
        assert hasattr(generator, "chart_style")
        assert hasattr(generator, "color_scheme")

        # Should still be able to create charts
        colors = generator._get_color_palette()
        assert isinstance(colors, dict)

    def test_configuration_validation(self):
        """Test configuration validation."""
        # Test required fields
        with pytest.raises(KeyError):
            ChartGenerator({})  # Missing required fields

        # Test with minimal required config
        config = {"institution_name": "Test University", "semester_term": "Test Semester"}
        generator = ChartGenerator(config)
        assert generator.institution_name == "Test University"
