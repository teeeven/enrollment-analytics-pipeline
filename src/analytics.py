"""
Enrollment Analytics Engine

Core analytics utilities for processing enrollment data, detecting changes,
and calculating key metrics for institutional planning and reporting.
"""

import logging
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd


class EnrollmentAnalytics:
    """
    Core analytics engine for enrollment data processing.

    Provides utilities for:
    - Snapshot comparison and delta detection
    - Retention rate calculations
    - Student change tracking
    - Time-series metrics preparation
    """

    def __init__(
        self, student_id_column: str = "student_id", student_name_column: str = "student_name"
    ):
        """
        Initialize the analytics engine.

        Args:
            student_id_column: Name of the student ID column
            student_name_column: Name of the student name column
        """
        self.student_id_column = student_id_column
        self.student_name_column = student_name_column
        self.logger = logging.getLogger(__name__)

    def compare_snapshots(
        self, baseline_df: pd.DataFrame, current_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Compare two enrollment snapshots and calculate key metrics.

        Args:
            baseline_df: Previous enrollment snapshot
            current_df: Current enrollment snapshot

        Returns:
            Dictionary containing comparison metrics
        """
        try:
            # Convert DataFrames to sets for efficient comparison
            baseline_ids = set(baseline_df[self.student_id_column].astype(str))
            current_ids = set(current_df[self.student_id_column].astype(str))

            # Calculate set differences
            dropped_ids = baseline_ids - current_ids  # In baseline but not current
            new_ids = current_ids - baseline_ids  # In current but not baseline
            retained_ids = baseline_ids & current_ids  # In both snapshots

            # Calculate metrics
            baseline_total = len(baseline_ids)
            current_total = len(current_ids)
            new_students = len(new_ids)
            dropped_students = len(dropped_ids)
            retained_students = len(retained_ids)
            net_change = new_students - dropped_students
            retention_rate = (
                (retained_students / baseline_total * 100) if baseline_total > 0 else 0.0
            )

            analysis_result = {
                "status": "comparison_complete",
                "baseline_total": baseline_total,
                "current_total": current_total,
                "new_students": new_students,
                "dropped_students": dropped_students,
                "retained_students": retained_students,
                "net_change": net_change,
                "retention_rate": round(retention_rate, 2),
                "growth_rate": (
                    round((net_change / baseline_total * 100), 2) if baseline_total > 0 else 0.0
                ),
            }

            self.logger.info(
                f"Snapshot comparison: {current_total} total "
                f"({new_students} new, {dropped_students} dropped, {net_change:+d} net)"
            )

            return analysis_result

        except Exception as e:
            self.logger.error(f"Snapshot comparison failed: {e}")
            raise

    def extract_student_changes(
        self, baseline_df: pd.DataFrame, current_df: pd.DataFrame
    ) -> Dict[str, pd.DataFrame]:
        """
        Extract detailed information about students who were added or dropped.

        Args:
            baseline_df: Previous enrollment snapshot
            current_df: Current enrollment snapshot

        Returns:
            Dictionary containing DataFrames of dropped and added students
        """
        try:
            # Convert student IDs to strings for consistent comparison
            baseline_df = baseline_df.copy()
            current_df = current_df.copy()
            baseline_df[self.student_id_column] = baseline_df[self.student_id_column].astype(str)
            current_df[self.student_id_column] = current_df[self.student_id_column].astype(str)

            # Get ID sets
            baseline_ids = set(baseline_df[self.student_id_column])
            current_ids = set(current_df[self.student_id_column])

            # Find changes
            dropped_ids = baseline_ids - current_ids
            new_ids = current_ids - baseline_ids

            # Extract student records
            dropped_students = baseline_df[
                baseline_df[self.student_id_column].isin(dropped_ids)
            ].copy()

            added_students = current_df[current_df[self.student_id_column].isin(new_ids)].copy()

            self.logger.info(f"Extracted {len(dropped_students)} drops, {len(added_students)} adds")

            return {
                "dropped_students": dropped_students,
                "added_students": added_students,
                "dropped_ids": list(dropped_ids),
                "added_ids": list(new_ids),
            }

        except Exception as e:
            self.logger.error(f"Student changes extraction failed: {e}")
            raise

    def calculate_trend_metrics(self, metrics_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate trend analysis metrics from historical data.

        Args:
            metrics_df: DataFrame with historical enrollment metrics

        Returns:
            Dictionary containing trend analysis results
        """
        try:
            if len(metrics_df) < 2:
                return {
                    "trend_analysis": "insufficient_data",
                    "trend_direction": "unknown",
                    "average_daily_change": 0.0,
                    "volatility": 0.0,
                }

            # Ensure date column is datetime
            metrics_df = metrics_df.copy()
            metrics_df["date"] = pd.to_datetime(metrics_df["date"])
            metrics_df = metrics_df.sort_values("date")

            # Calculate trend metrics
            enrollment_values = metrics_df["total_enrollment"].values
            daily_changes = metrics_df["net_change"].values

            # Linear trend
            days = np.arange(len(enrollment_values))
            trend_slope = np.polyfit(days, enrollment_values, 1)[0]

            # Metrics
            avg_daily_change = np.mean(daily_changes)
            volatility = np.std(daily_changes)
            total_change = enrollment_values[-1] - enrollment_values[0]
            days_elapsed = len(enrollment_values) - 1

            # Trend direction
            if trend_slope > 1:
                trend_direction = "increasing"
            elif trend_slope < -1:
                trend_direction = "decreasing"
            else:
                trend_direction = "stable"

            trend_analysis = {
                "trend_analysis": "complete",
                "trend_direction": trend_direction,
                "trend_slope": round(trend_slope, 2),
                "average_daily_change": round(avg_daily_change, 1),
                "volatility": round(volatility, 1),
                "total_change": int(total_change),
                "days_analyzed": days_elapsed,
                "current_enrollment": int(enrollment_values[-1]),
                "starting_enrollment": int(enrollment_values[0]),
            }

            self.logger.info(
                f"Trend analysis: {trend_direction} trend, {avg_daily_change:+.1f} avg daily change"
            )

            return trend_analysis

        except Exception as e:
            self.logger.error(f"Trend analysis failed: {e}")
            return {"trend_analysis": "error", "error": str(e)}

    def detect_anomalies(
        self, metrics_df: pd.DataFrame, sensitivity: float = 2.0
    ) -> Dict[str, Any]:
        """
        Detect anomalies in enrollment patterns.

        Args:
            metrics_df: DataFrame with historical enrollment metrics
            sensitivity: Standard deviations for anomaly detection threshold

        Returns:
            Dictionary containing anomaly detection results
        """
        try:
            if len(metrics_df) < 5:
                return {"anomaly_detection": "insufficient_data"}

            # Calculate rolling statistics
            metrics_df = metrics_df.copy()
            metrics_df["date"] = pd.to_datetime(metrics_df["date"])
            metrics_df = metrics_df.sort_values("date")

            # Focus on net changes for anomaly detection
            net_changes = metrics_df["net_change"].values

            # Calculate thresholds
            mean_change = np.mean(net_changes[:-1])  # Exclude today for threshold
            std_change = np.std(net_changes[:-1])

            upper_threshold = mean_change + (sensitivity * std_change)
            lower_threshold = mean_change - (sensitivity * std_change)

            # Check latest change
            latest_change = net_changes[-1]
            is_anomaly = (latest_change > upper_threshold) or (latest_change < lower_threshold)

            anomaly_result = {
                "anomaly_detection": "complete",
                "is_anomaly": is_anomaly,
                "latest_change": int(latest_change),
                "expected_range": (round(lower_threshold, 1), round(upper_threshold, 1)),
                "historical_mean": round(mean_change, 1),
                "historical_std": round(std_change, 1),
            }

            if is_anomaly:
                anomaly_type = "spike" if latest_change > upper_threshold else "drop"
                anomaly_result["anomaly_type"] = anomaly_type
                self.logger.warning(
                    f"Anomaly detected: {anomaly_type} with change of {latest_change}"
                )
            else:
                self.logger.info("No anomalies detected in enrollment pattern")

            return anomaly_result

        except Exception as e:
            self.logger.error(f"Anomaly detection failed: {e}")
            return {"anomaly_detection": "error", "error": str(e)}

    def generate_summary_statistics(self, current_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate summary statistics for current enrollment snapshot.

        Args:
            current_df: Current enrollment DataFrame

        Returns:
            Dictionary containing summary statistics
        """
        try:
            total_students = len(current_df)

            # Basic statistics
            summary = {
                "total_enrollment": total_students,
                "data_quality": {
                    "total_records": total_students,
                    "unique_students": current_df[self.student_id_column].nunique(),
                    "missing_names": current_df[self.student_name_column].isna().sum(),
                    "missing_ids": current_df[self.student_id_column].isna().sum(),
                },
            }

            # Division/Department breakdown if available
            if "division" in current_df.columns:
                division_counts = current_df["division"].value_counts().to_dict()
                summary["division_breakdown"] = division_counts

            # Additional demographics if available
            demographic_columns = ["program", "level", "status", "department"]
            for col in demographic_columns:
                if col in current_df.columns:
                    breakdown = current_df[col].value_counts().head(10).to_dict()
                    summary[f"{col}_breakdown"] = breakdown

            self.logger.info(f"Summary statistics generated for {total_students} students")
            return summary

        except Exception as e:
            self.logger.error(f"Summary statistics generation failed: {e}")
            return {"error": str(e)}

    def prepare_forecasting_data(self, metrics_df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare enrollment data for time-series forecasting.

        Args:
            metrics_df: Historical enrollment metrics DataFrame

        Returns:
            DataFrame prepared for forecasting models
        """
        try:
            # Ensure proper data types and sorting
            forecast_df = metrics_df.copy()
            forecast_df["date"] = pd.to_datetime(forecast_df["date"])
            forecast_df = forecast_df.sort_values("date").reset_index(drop=True)

            # Add time-based features
            forecast_df["day_of_week"] = forecast_df["date"].dt.dayofweek
            forecast_df["day_of_year"] = forecast_df["date"].dt.dayofyear
            forecast_df["week_of_year"] = forecast_df["date"].dt.isocalendar().week
            forecast_df["days_since_start"] = (
                forecast_df["date"] - forecast_df["date"].min()
            ).dt.days

            # Add rolling averages for smoothing
            forecast_df["enrollment_ma_3"] = (
                forecast_df["total_enrollment"].rolling(3, min_periods=1).mean()
            )
            forecast_df["enrollment_ma_7"] = (
                forecast_df["total_enrollment"].rolling(7, min_periods=1).mean()
            )

            # Add lag features
            forecast_df["enrollment_lag_1"] = forecast_df["total_enrollment"].shift(1)
            forecast_df["change_lag_1"] = forecast_df["net_change"].shift(1)

            self.logger.info(
                f"Forecasting data prepared: {len(forecast_df)} records with {len(forecast_df.columns)} features"
            )
            return forecast_df

        except Exception as e:
            self.logger.error(f"Forecasting data preparation failed: {e}")
            raise
