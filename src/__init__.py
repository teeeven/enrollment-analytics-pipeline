"""
Enrollment Analytics Pipeline

A production-ready pipeline for tracking student enrollment changes over time,
generating analytics, and providing operational intelligence for educational institutions.

Key Features:
- Time-series enrollment tracking with daily snapshots
- Delta detection for student adds/drops
- Retention rate analysis and trending
- Automated visualization and reporting
- Integration-ready outputs for BI tools
"""

__version__ = "1.0.0"
__author__ = "Steven Orizaga"

from .analytics import EnrollmentAnalytics
from .enrollment_tracker import EnrollmentTracker
from .visualization import ChartGenerator

__all__ = [
    "EnrollmentTracker",
    "EnrollmentAnalytics",
    "ChartGenerator",
]
