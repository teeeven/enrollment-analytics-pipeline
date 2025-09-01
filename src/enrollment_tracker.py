"""
Enrollment Tracking Pipeline

A generalized Airflow DAG for tracking student enrollment changes over time.
Provides time-series analytics, delta detection, and automated reporting.

This pipeline demonstrates:
- Time-series data modeling patterns
- Delta detection using set operations
- Automated visualization and reporting
- Production analytics engineering practices
"""

import logging
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from .analytics import EnrollmentAnalytics
from .visualization import ChartGenerator


# Configuration from Airflow Variables
def get_config() -> Dict[str, Any]:
    """Load configuration from Airflow Variables."""
    return {
        "db_conn_id": Variable.get("ENROLLMENT_DB_CONN_ID", "postgres_default"),
        "db_type": Variable.get("ENROLLMENT_DB_TYPE", "postgres"),
        "email_recipients": Variable.get("ENROLLMENT_EMAIL_RECIPIENTS", "").split(","),
        "sql_query": Variable.get("ENROLLMENT_SQL_QUERY", ""),
        "output_dir": Variable.get("ENROLLMENT_OUTPUT_DIR", "/tmp/enrollment"),
        "reports_dir": Variable.get("ENROLLMENT_REPORTS_DIR", "/tmp/reports"),
        "institution_name": Variable.get("INSTITUTION_NAME", "University"),
        "semester_term": Variable.get("ENROLLMENT_SEMESTER", "Fall 2025"),
    }


def get_database_hook(db_type: str, conn_id: str):
    """Get appropriate database hook based on type."""
    if db_type.lower() == "postgres":
        return PostgresHook(postgres_conn_id=conn_id)
    elif db_type.lower() == "mssql":
        return MsSqlHook(mssql_conn_id=conn_id)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="enrollment_analytics_pipeline",
    default_args=default_args,
    description="Time-series enrollment analytics with automated reporting",
    schedule="0 8 * * *",  # Daily at 8 AM
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["enrollment", "analytics", "time-series", "production"],
    doc_md=__doc__,
)
def enrollment_analytics_pipeline():
    """Enrollment Analytics Pipeline DAG."""

    @task
    def load_configuration() -> Dict[str, Any]:
        """Load and validate pipeline configuration."""
        config = get_config()

        # Validate required configuration
        required_fields = ["db_conn_id", "sql_query", "email_recipients"]
        missing_fields = [field for field in required_fields if not config.get(field)]

        if missing_fields:
            raise ValueError(f"Missing required configuration: {missing_fields}")

        logging.info(
            f"✅ Configuration loaded for {config['institution_name']} {config['semester_term']}"
        )
        return config

    @task
    def extract_enrollment_data(config: Dict[str, Any]) -> str:
        """Extract current enrollment data from database."""
        logging.info("🔄 Extracting enrollment data")

        try:
            # Get database hook
            hook = get_database_hook(config["db_type"], config["db_conn_id"])

            # Execute query
            df = hook.get_pandas_df(sql=config["sql_query"])

            # Create output directory
            output_dir = Path(config["output_dir"])
            output_dir.mkdir(parents=True, exist_ok=True)

            # Save with timestamp
            timestamp = pendulum.now().format("YYYYMMDD_HHmm")
            current_file = output_dir / f"enrollment_snapshot_{timestamp}.csv"
            df.to_csv(current_file, index=False)

            logging.info(f"✅ Extracted {len(df)} enrollment records to {current_file}")
            return str(current_file)

        except Exception as e:
            logging.error(f"❌ Enrollment extraction failed: {e}")
            raise

    @task
    def find_baseline_snapshot(config: Dict[str, Any], current_file: str) -> Optional[str]:
        """Find the most recent baseline snapshot for comparison."""
        logging.info("🔍 Looking for baseline snapshot")

        try:
            output_dir = Path(config["output_dir"])
            current_path = Path(current_file)

            # Find all snapshot files except the current one
            snapshot_files = list(output_dir.glob("enrollment_snapshot_*.csv"))
            snapshot_files = [f for f in snapshot_files if f != current_path]

            if not snapshot_files:
                logging.info("📂 No baseline found - this is the first run")
                return None

            # Get most recent baseline
            baseline_file = max(snapshot_files, key=lambda f: f.stat().st_mtime)
            logging.info(f"📂 Found baseline: {baseline_file}")
            return str(baseline_file)

        except Exception as e:
            logging.error(f"❌ Baseline search failed: {e}")
            return None

    @task
    def analyze_enrollment_changes(
        config: Dict[str, Any], current_file: str, baseline_file: Optional[str]
    ) -> Dict[str, Any]:
        """Analyze enrollment changes between snapshots."""
        logging.info("🔧 Analyzing enrollment changes")

        try:
            analytics = EnrollmentAnalytics()

            # Load current data
            current_df = pd.read_csv(current_file)

            if not baseline_file:
                # First run - no comparison possible
                analysis = {
                    "status": "baseline_created",
                    "current_total": len(current_df),
                    "new_students": 0,
                    "dropped_students": 0,
                    "net_change": 0,
                    "retention_rate": 100.0,
                }
                logging.info(f"📊 Baseline created with {len(current_df)} students")
                return analysis

            # Load baseline data
            baseline_df = pd.read_csv(baseline_file)

            # Perform delta analysis
            analysis = analytics.compare_snapshots(baseline_df, current_df)

            logging.info(
                f"📊 Analysis complete: {analysis['current_total']} total, "
                f"{analysis['new_students']} new, {analysis['dropped_students']} dropped, "
                f"{analysis['net_change']:+d} net change"
            )

            return analysis

        except Exception as e:
            logging.error(f"❌ Enrollment analysis failed: {e}")
            raise

    @task
    def extract_student_changes(
        config: Dict[str, Any],
        current_file: str,
        baseline_file: Optional[str],
        analysis: Dict[str, Any],
    ) -> Dict[str, str]:
        """Extract detailed student add/drop information."""
        logging.info("📋 Extracting student changes")

        try:
            output_dir = Path(config["output_dir"])
            timestamp = pendulum.now().format("YYYYMMDD_HHmm")

            current_df = pd.read_csv(current_file)

            if not baseline_file or analysis["status"] == "baseline_created":
                # Create empty change files for first run
                empty_df = pd.DataFrame(columns=["student_id", "student_name"])

                dropped_file = output_dir / f"dropped_students_{timestamp}.csv"
                added_file = output_dir / f"added_students_{timestamp}.csv"

                empty_df.to_csv(dropped_file, index=False)
                empty_df.to_csv(added_file, index=False)

                logging.info("📋 Created empty change files for baseline run")
                return {
                    "dropped_file": str(dropped_file),
                    "added_file": str(added_file),
                    "dropped_count": 0,
                    "added_count": 0,
                }

            # Load baseline and extract changes
            baseline_df = pd.read_csv(baseline_file)

            analytics = EnrollmentAnalytics()
            changes = analytics.extract_student_changes(baseline_df, current_df)

            # Save change files
            dropped_file = output_dir / f"dropped_students_{timestamp}.csv"
            added_file = output_dir / f"added_students_{timestamp}.csv"

            changes["dropped_students"].to_csv(dropped_file, index=False)
            changes["added_students"].to_csv(added_file, index=False)

            logging.info(
                f"📋 Saved {len(changes['dropped_students'])} drops, {len(changes['added_students'])} adds"
            )

            return {
                "dropped_file": str(dropped_file),
                "added_file": str(added_file),
                "dropped_count": len(changes["dropped_students"]),
                "added_count": len(changes["added_students"]),
            }

        except Exception as e:
            logging.error(f"❌ Student changes extraction failed: {e}")
            raise

    @task
    def update_metrics_history(config: Dict[str, Any], analysis: Dict[str, Any]) -> str:
        """Update historical metrics for trend analysis."""
        logging.info("📈 Updating metrics history")

        try:
            reports_dir = Path(config["reports_dir"])
            reports_dir.mkdir(parents=True, exist_ok=True)
            metrics_file = reports_dir / "enrollment_metrics.csv"

            # Create new metrics record
            current_date = pendulum.now().format("YYYY-MM-DD")
            new_metrics = {
                "date": current_date,
                "total_enrollment": analysis["current_total"],
                "new_students": analysis["new_students"],
                "dropped_students": analysis["dropped_students"],
                "net_change": analysis["net_change"],
                "retention_rate": analysis["retention_rate"],
                "semester_term": config["semester_term"],
                "notes": f"Analysis for {current_date}",
            }

            # Load existing metrics or create new DataFrame
            if metrics_file.exists():
                metrics_df = pd.read_csv(metrics_file)
                # Remove any existing record for today (reprocessing)
                metrics_df = metrics_df[metrics_df["date"] != current_date]
            else:
                metrics_df = pd.DataFrame()

            # Add new metrics and sort by date
            metrics_df = pd.concat([metrics_df, pd.DataFrame([new_metrics])], ignore_index=True)
            metrics_df["date"] = pd.to_datetime(metrics_df["date"])
            metrics_df = metrics_df.sort_values("date")
            metrics_df["date"] = metrics_df["date"].dt.strftime("%Y-%m-%d")

            # Save updated metrics
            metrics_df.to_csv(metrics_file, index=False)

            logging.info(f"📊 Metrics updated: {len(metrics_df)} historical records")
            return str(metrics_file)

        except Exception as e:
            logging.error(f"❌ Metrics update failed: {e}")
            raise

    @task
    def generate_trend_visualization(config: Dict[str, Any], metrics_file: str) -> str:
        """Generate enrollment trend visualization."""
        logging.info("📊 Generating trend visualization")

        try:
            chart_generator = ChartGenerator(
                institution_name=config["institution_name"], semester_term=config["semester_term"]
            )

            # Load metrics data
            metrics_df = pd.read_csv(metrics_file)

            # Generate chart
            reports_dir = Path(config["reports_dir"])
            timestamp = pendulum.now().format("YYYYMMDD")
            chart_file = reports_dir / f"enrollment_trend_{timestamp}.png"

            chart_generator.create_enrollment_trend_chart(
                metrics_df=metrics_df, output_file=str(chart_file)
            )

            logging.info(f"📊 Chart generated: {chart_file}")
            return str(chart_file)

        except Exception as e:
            logging.error(f"❌ Chart generation failed: {e}")
            return ""  # Non-critical failure

    @task
    def send_analytics_report(
        config: Dict[str, Any], analysis: Dict[str, Any], changes: Dict[str, str], chart_file: str
    ) -> None:
        """Send comprehensive analytics report via email."""
        logging.info("📧 Sending analytics report")

        try:
            # Import email utility (would need to be implemented)
            # from .utils.email_utils import send_email

            # Build email content
            current_date = pendulum.now().format("YYYY-MM-DD")

            html_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 800px;">
                <h1 style="color: #2e7d32; border-bottom: 3px solid #4caf50;">
                    📊 {config['institution_name']} Enrollment Report - {current_date}
                </h1>
                
                <div style="background:#e8f5e8;padding:15px;border-radius:5px;margin:15px 0;">
                    <h2 style="color:#2e7d32;margin:0 0 10px 0;">✅ Daily Summary</h2>
                    <ul style="font-size:16px; margin:0; padding-left:1.2em;">
                        <li><strong>Total Enrollment:</strong> {analysis['current_total']:,}</li>
                        <li><strong>New Students:</strong> {analysis['new_students']:,}</li>
                        <li><strong>Dropped Students:</strong> {analysis['dropped_students']:,}</li>
                        <li><strong>Net Change:</strong> {analysis['net_change']:+d}</li>
                        <li><strong>Retention Rate:</strong> {analysis['retention_rate']:.2f}%</li>
                    </ul>
                </div>
                
                <div style="background:#e3f2fd;padding:15px;border-radius:5px;margin:15px 0;">
                    <h2 style="color:#1976d2;margin:0 0 10px 0;">📈 Trend Analysis</h2>
                    <p style="margin:0;">
                        {'Positive enrollment trend' if analysis['net_change'] >= 0 else 'Declining enrollment trend'} 
                        for {config['semester_term']}. See attached chart for detailed trend analysis.
                    </p>
                </div>
                
                <div style="margin-top:20px;padding:10px;background:#f5f5f5;border-radius:5px;">
                    <p style="margin:0;font-size:14px;color:#666;">
                        🚀 <em>Generated at: {pendulum.now().format('YYYY-MM-DD HH:mm:ss')} UTC</em><br>
                        <small>Enrollment Analytics Pipeline - Time Series Analysis</small>
                    </p>
                </div>
            </div>
            """

            # In a real implementation, would send email here
            # send_email(
            #     to=config['email_recipients'],
            #     subject=f"📈 {config['institution_name']} Enrollment Report - {current_date}",
            #     html_content=html_content,
            #     attachments=[chart_file] if chart_file and Path(chart_file).exists() else []
            # )

            logging.info(
                f"✅ Analytics report prepared for {len(config['email_recipients'])} recipients"
            )

        except Exception as e:
            logging.error(f"❌ Report sending failed: {e}")
            # Don't fail the pipeline for notification errors

    # Task orchestration
    config = load_configuration()

    # Extract current enrollment data
    current_snapshot = extract_enrollment_data(config)

    # Find baseline for comparison
    baseline_snapshot = find_baseline_snapshot(config, current_snapshot)

    # Analyze changes
    enrollment_analysis = analyze_enrollment_changes(config, current_snapshot, baseline_snapshot)

    # Extract detailed student changes
    student_changes = extract_student_changes(
        config, current_snapshot, baseline_snapshot, enrollment_analysis
    )

    # Update historical metrics
    metrics_file = update_metrics_history(config, enrollment_analysis)

    # Generate visualization
    chart_file = generate_trend_visualization(config, metrics_file)

    # Send report
    send_analytics_report(config, enrollment_analysis, student_changes, chart_file)


# Create the DAG instance
dag_instance = enrollment_analytics_pipeline()
