"""
Enrollment Visualization Engine

Professional chart generation utilities for enrollment analytics,
designed for executive reporting and stakeholder communication.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

try:
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.figure import Figure

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False


class ChartGenerator:
    """
    Professional chart generator for enrollment analytics.

    Creates publication-ready visualizations with institutional branding
    and smart formatting for various time ranges and data patterns.
    """

    def __init__(
        self,
        institution_name: str = "University",
        semester_term: str = "Fall 2024",
        primary_color: str = "#2E4057",
        accent_color: str = "#4caf50",
    ):
        """
        Initialize chart generator with institutional branding.

        Args:
            institution_name: Name of the institution for chart titles
            semester_term: Current semester/term for context
            primary_color: Primary color for chart elements
            accent_color: Accent color for highlights
        """
        self.institution_name = institution_name
        self.semester_term = semester_term
        self.primary_color = primary_color
        self.accent_color = accent_color
        self.logger = logging.getLogger(__name__)

        if not MATPLOTLIB_AVAILABLE:
            self.logger.warning("Matplotlib not available - chart generation will be disabled")

    def create_enrollment_trend_chart(
        self, metrics_df: pd.DataFrame, output_file: str, figsize: tuple = (14, 8)
    ) -> bool:
        """
        Create a professional enrollment trend chart.

        Args:
            metrics_df: DataFrame with enrollment metrics over time
            output_file: Path to save the chart image
            figsize: Figure size in inches (width, height)

        Returns:
            True if chart was created successfully, False otherwise
        """
        if not MATPLOTLIB_AVAILABLE:
            self.logger.error("Matplotlib not available for chart generation")
            return False

        try:
            # Prepare data
            df = metrics_df.copy()
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")

            # Create figure and axis
            plt.style.use("default")
            fig, ax = plt.subplots(figsize=figsize)

            # Main enrollment line
            ax.plot(
                df["date"],
                df["total_enrollment"],
                marker="o",
                linewidth=3,
                markersize=8,
                color=self.primary_color,
                markerfacecolor=self.primary_color,
                label="Total Enrollment",
            )

            # Add net change indicators
            self._add_change_indicators(ax, df)

            # Styling
            self._style_enrollment_chart(ax, df)

            # Save chart
            plt.tight_layout()
            plt.savefig(
                output_file, dpi=300, bbox_inches="tight", facecolor="white", edgecolor="none"
            )
            plt.close()

            self.logger.info(f"Enrollment trend chart saved to {output_file}")
            return True

        except Exception as e:
            self.logger.error(f"Chart generation failed: {e}")
            return False

    def create_daily_changes_chart(
        self, metrics_df: pd.DataFrame, output_file: str, figsize: tuple = (14, 6)
    ) -> bool:
        """
        Create a chart showing daily enrollment changes (adds/drops).

        Args:
            metrics_df: DataFrame with enrollment metrics
            output_file: Path to save the chart image
            figsize: Figure size in inches

        Returns:
            True if successful, False otherwise
        """
        if not MATPLOTLIB_AVAILABLE:
            return False

        try:
            df = metrics_df.copy()
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")

            fig, ax = plt.subplots(figsize=figsize)

            # Stacked bar chart for adds/drops
            ax.bar(
                df["date"],
                df["new_students"],
                color=self.accent_color,
                alpha=0.8,
                label="New Students",
            )
            ax.bar(
                df["date"],
                -df["dropped_students"],
                color="#f44336",
                alpha=0.8,
                label="Dropped Students",
            )

            # Net change line
            ax2 = ax.twinx()
            ax2.plot(
                df["date"],
                df["net_change"],
                color=self.primary_color,
                linewidth=2,
                marker="o",
                label="Net Change",
            )

            # Styling
            ax.set_title(
                f"{self.institution_name} Daily Enrollment Changes\n{self.semester_term}",
                fontsize=16,
                fontweight="bold",
                pad=20,
            )
            ax.set_ylabel("Students", fontsize=12)
            ax2.set_ylabel("Net Change", fontsize=12, color=self.primary_color)

            # Legends
            lines1, labels1 = ax.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

            # Grid and formatting
            ax.grid(True, alpha=0.3)
            ax.axhline(y=0, color="black", linestyle="-", alpha=0.3)

            plt.tight_layout()
            plt.savefig(
                output_file, dpi=300, bbox_inches="tight", facecolor="white", edgecolor="none"
            )
            plt.close()

            self.logger.info(f"Daily changes chart saved to {output_file}")
            return True

        except Exception as e:
            self.logger.error(f"Daily changes chart generation failed: {e}")
            return False

    def create_retention_analysis_chart(
        self, metrics_df: pd.DataFrame, output_file: str, figsize: tuple = (14, 6)
    ) -> bool:
        """
        Create a chart showing retention rate trends.

        Args:
            metrics_df: DataFrame with enrollment metrics
            output_file: Path to save the chart image
            figsize: Figure size in inches

        Returns:
            True if successful, False otherwise
        """
        if not MATPLOTLIB_AVAILABLE:
            return False

        try:
            df = metrics_df.copy()
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")

            # Skip if insufficient data
            if len(df) < 2:
                self.logger.warning("Insufficient data for retention analysis chart")
                return False

            fig, ax = plt.subplots(figsize=figsize)

            # Retention rate line
            ax.plot(
                df["date"],
                df["retention_rate"],
                marker="o",
                linewidth=3,
                markersize=6,
                color=self.primary_color,
                markerfacecolor=self.accent_color,
            )

            # Target line (e.g., 95% retention)
            target_retention = 95.0
            ax.axhline(
                y=target_retention,
                color="#ff9800",
                linestyle="--",
                alpha=0.8,
                label=f"Target ({target_retention}%)",
            )

            # Styling
            ax.set_title(
                f"{self.institution_name} Student Retention Rate\n{self.semester_term}",
                fontsize=16,
                fontweight="bold",
                pad=20,
            )
            ax.set_ylabel("Retention Rate (%)", fontsize=12)
            ax.set_xlabel("Date", fontsize=12)

            # Y-axis formatting
            ax.set_ylim(max(0, df["retention_rate"].min() - 5), 100)
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x:.1f}%"))

            # Grid and legend
            ax.grid(True, alpha=0.3)
            ax.legend()

            # Date formatting
            self._format_date_axis(ax, df)

            plt.tight_layout()
            plt.savefig(
                output_file, dpi=300, bbox_inches="tight", facecolor="white", edgecolor="none"
            )
            plt.close()

            self.logger.info(f"Retention analysis chart saved to {output_file}")
            return True

        except Exception as e:
            self.logger.error(f"Retention analysis chart generation failed: {e}")
            return False

    def create_summary_dashboard(
        self,
        metrics_df: pd.DataFrame,
        current_analysis: Dict[str, Any],
        output_file: str,
        figsize: tuple = (16, 12),
    ) -> bool:
        """
        Create a comprehensive dashboard with multiple charts.

        Args:
            metrics_df: Historical enrollment metrics
            current_analysis: Current day analysis results
            output_file: Path to save dashboard image
            figsize: Figure size in inches

        Returns:
            True if successful, False otherwise
        """
        if not MATPLOTLIB_AVAILABLE:
            return False

        try:
            fig = plt.figure(figsize=figsize)

            # Create subplots
            gs = fig.add_gridspec(3, 2, height_ratios=[2, 1, 1], hspace=0.3, wspace=0.3)

            # Main enrollment trend (top, spanning both columns)
            ax1 = fig.add_subplot(gs[0, :])
            self._plot_enrollment_trend(ax1, metrics_df, title=False)
            ax1.set_title(
                f"{self.institution_name} Enrollment Dashboard - {self.semester_term}",
                fontsize=18,
                fontweight="bold",
                pad=20,
            )

            # Daily changes (bottom left)
            ax2 = fig.add_subplot(gs[1, 0])
            self._plot_daily_changes(ax2, metrics_df)

            # Retention rate (bottom right)
            ax3 = fig.add_subplot(gs[1, 1])
            if len(metrics_df) > 1:
                self._plot_retention_rate(ax3, metrics_df)
            else:
                ax3.text(
                    0.5,
                    0.5,
                    "Insufficient data\nfor retention analysis",
                    ha="center",
                    va="center",
                    transform=ax3.transAxes,
                    fontsize=12,
                    color="gray",
                )
                ax3.set_title("Retention Rate", fontsize=12, fontweight="bold")

            # Summary statistics (bottom)
            ax4 = fig.add_subplot(gs[2, :])
            self._add_summary_statistics(ax4, current_analysis)

            plt.savefig(
                output_file, dpi=300, bbox_inches="tight", facecolor="white", edgecolor="none"
            )
            plt.close()

            self.logger.info(f"Summary dashboard saved to {output_file}")
            return True

        except Exception as e:
            self.logger.error(f"Dashboard generation failed: {e}")
            return False

    def _add_change_indicators(self, ax, df: pd.DataFrame) -> None:
        """Add visual indicators for enrollment changes."""
        for i, (_, row) in enumerate(df.iterrows()):
            if i == 0:  # Skip first point (no previous data)
                continue

            net_change = row["net_change"]
            if net_change > 0:
                ax.scatter(
                    row["date"],
                    row["total_enrollment"],
                    s=100,
                    color=self.accent_color,
                    alpha=0.7,
                    zorder=5,
                )
            elif net_change < 0:
                ax.scatter(
                    row["date"],
                    row["total_enrollment"],
                    s=100,
                    color="#f44336",
                    alpha=0.7,
                    zorder=5,
                )

    def _style_enrollment_chart(self, ax, df: pd.DataFrame) -> None:
        """Apply professional styling to enrollment chart."""
        # Title and labels
        ax.set_title(
            f"{self.institution_name} Enrollment Trend\n{self.semester_term}",
            fontsize=16,
            fontweight="bold",
            pad=20,
        )
        ax.set_ylabel("Total Enrollment", fontsize=12, fontweight="bold")
        ax.set_xlabel("Date", fontsize=12, fontweight="bold")

        # Grid and spines
        ax.grid(True, alpha=0.3, linestyle="-", linewidth=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#CCCCCC")
        ax.spines["bottom"].set_color("#CCCCCC")

        # Y-axis limits with padding
        min_enroll = df["total_enrollment"].min()
        max_enroll = df["total_enrollment"].max()
        y_range = max_enroll - min_enroll
        y_pad = max(10, y_range * 0.1)
        ax.set_ylim(min_enroll - y_pad, max_enroll + y_pad)

        # Date formatting
        self._format_date_axis(ax, df)

    def _format_date_axis(self, ax, df: pd.DataFrame) -> None:
        """Smart date axis formatting based on data range."""
        num_days = len(df)

        if num_days <= 7:
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
            rotation = 45
        elif num_days <= 21:
            interval = 2 if num_days <= 14 else 3
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=interval))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
            rotation = 45
        elif num_days <= 42:
            ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
            rotation = 45
        else:
            ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
            rotation = 30

        plt.setp(ax.xaxis.get_majorticklabels(), rotation=rotation)

    def _plot_enrollment_trend(self, ax, df: pd.DataFrame, title: bool = True) -> None:
        """Plot enrollment trend on given axis."""
        df_plot = df.copy()
        df_plot["date"] = pd.to_datetime(df_plot["date"])
        df_plot = df_plot.sort_values("date")

        ax.plot(
            df_plot["date"],
            df_plot["total_enrollment"],
            marker="o",
            linewidth=3,
            markersize=6,
            color=self.primary_color,
            markerfacecolor=self.primary_color,
        )

        if title:
            ax.set_title("Enrollment Trend", fontsize=14, fontweight="bold")
        ax.set_ylabel("Total Enrollment", fontsize=10)
        ax.grid(True, alpha=0.3)

    def _plot_daily_changes(self, ax, df: pd.DataFrame) -> None:
        """Plot daily changes on given axis."""
        df_plot = df.copy()
        df_plot["date"] = pd.to_datetime(df_plot["date"])
        df_plot = df_plot.sort_values("date")

        ax.bar(
            df_plot["date"],
            df_plot["net_change"],
            color=[self.accent_color if x >= 0 else "#f44336" for x in df_plot["net_change"]],
            alpha=0.7,
        )

        ax.set_title("Daily Net Changes", fontsize=12, fontweight="bold")
        ax.set_ylabel("Net Change", fontsize=10)
        ax.axhline(y=0, color="black", linestyle="-", alpha=0.3)
        ax.grid(True, alpha=0.3)

    def _plot_retention_rate(self, ax, df: pd.DataFrame) -> None:
        """Plot retention rate on given axis."""
        df_plot = df.copy()
        df_plot["date"] = pd.to_datetime(df_plot["date"])
        df_plot = df_plot.sort_values("date")

        ax.plot(
            df_plot["date"],
            df_plot["retention_rate"],
            marker="o",
            linewidth=2,
            markersize=4,
            color=self.primary_color,
        )

        ax.set_title("Retention Rate", fontsize=12, fontweight="bold")
        ax.set_ylabel("Retention (%)", fontsize=10)
        ax.set_ylim(max(0, df_plot["retention_rate"].min() - 5), 100)
        ax.grid(True, alpha=0.3)

    def _add_summary_statistics(self, ax, analysis: Dict[str, Any]) -> None:
        """Add summary statistics to axis."""
        ax.axis("off")

        # Create summary text
        summary_text = f"""
        Current Total: {analysis.get('current_total', 'N/A'):,}
        New Students: {analysis.get('new_students', 0):,}
        Dropped Students: {analysis.get('dropped_students', 0):,}
        Net Change: {analysis.get('net_change', 0):+d}
        Retention Rate: {analysis.get('retention_rate', 0):.2f}%
        """

        ax.text(
            0.1,
            0.5,
            summary_text,
            transform=ax.transAxes,
            fontsize=14,
            verticalalignment="center",
            bbox=dict(boxstyle="round,pad=0.5", facecolor=self.accent_color, alpha=0.1),
        )

        ax.set_title("Current Summary", fontsize=12, fontweight="bold")
