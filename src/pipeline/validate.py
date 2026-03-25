"""
TalentScope AI — Data Validation
Key field validations using Great Expectations patterns.
Scoped to high-impact checks only.
"""

import pandas as pd
from sqlalchemy import text
from src.database.connection import get_engine
from src.utils.logger import get_logger

logger = get_logger("pipeline.validate")


class DataValidator:
    """Validates clean_jobs data quality."""

    def __init__(self):
        self.engine = get_engine()
        self.results = []

    def expect_not_null(self, column: str, table: str = "clean_jobs"):
        """Check that a column has no null values."""
        with self.engine.connect() as conn:
            total = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()[0]
            nulls = conn.execute(text(
                f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL OR {column} = ''"
            )).fetchone()[0]

        pct = round((1 - nulls / max(total, 1)) * 100, 1)
        passed = pct >= 95.0

        result = {
            "check": f"not_null({column})",
            "total": total,
            "failures": nulls,
            "success_pct": pct,
            "passed": passed,
        }
        self.results.append(result)
        return result

    def expect_unique(self, column: str, table: str = "clean_jobs"):
        """Check that a column has unique values."""
        with self.engine.connect() as conn:
            total = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()[0]
            distinct = conn.execute(text(
                f"SELECT COUNT(DISTINCT {column}) FROM {table} WHERE {column} IS NOT NULL"
            )).fetchone()[0]

        pct = round(distinct / max(total, 1) * 100, 1)
        passed = pct >= 90.0

        result = {
            "check": f"unique({column})",
            "total": total,
            "distinct": distinct,
            "uniqueness_pct": pct,
            "passed": passed,
        }
        self.results.append(result)
        return result

    def expect_salary_sanity(self):
        """Check salary_min < salary_max and within reasonable bounds."""
        with self.engine.connect() as conn:
            total_with_salary = conn.execute(text(
                "SELECT COUNT(*) FROM clean_jobs WHERE salary_min IS NOT NULL"
            )).fetchone()[0]

            bad_range = conn.execute(text(
                "SELECT COUNT(*) FROM clean_jobs WHERE salary_min > salary_max AND salary_min IS NOT NULL AND salary_max IS NOT NULL"
            )).fetchone()[0]

            too_low = conn.execute(text(
                "SELECT COUNT(*) FROM clean_jobs WHERE salary_min IS NOT NULL AND salary_min < 15000"
            )).fetchone()[0]

            too_high = conn.execute(text(
                "SELECT COUNT(*) FROM clean_jobs WHERE salary_max IS NOT NULL AND salary_max > 1000000"
            )).fetchone()[0]

        failures = bad_range + too_low + too_high
        pct = round((1 - failures / max(total_with_salary, 1)) * 100, 1)
        passed = pct >= 95.0

        result = {
            "check": "salary_sanity",
            "total_with_salary": total_with_salary,
            "bad_range": bad_range,
            "too_low": too_low,
            "too_high": too_high,
            "success_pct": pct,
            "passed": passed,
        }
        self.results.append(result)
        return result

    def expect_date_validity(self):
        """Check posted_date is not in the future and within expected range."""
        with self.engine.connect() as conn:
            total = conn.execute(text(
                "SELECT COUNT(*) FROM clean_jobs WHERE posted_date IS NOT NULL"
            )).fetchone()[0]

            future = conn.execute(text(
                "SELECT COUNT(*) FROM clean_jobs WHERE posted_date > CURRENT_DATE + INTERVAL '1 day'"
            )).fetchone()[0]

            too_old = conn.execute(text(
                "SELECT COUNT(*) FROM clean_jobs WHERE posted_date < '2020-01-01'"
            )).fetchone()[0]

        failures = future + too_old
        pct = round((1 - failures / max(total, 1)) * 100, 1)
        passed = pct >= 95.0

        result = {
            "check": "date_validity",
            "total_with_date": total,
            "future_dates": future,
            "too_old": too_old,
            "success_pct": pct,
            "passed": passed,
        }
        self.results.append(result)
        return result

    def run_all_checks(self) -> list:
        """Run all validation checks."""
        logger.info("Running data validation suite...")

        self.expect_not_null("title")
        self.expect_not_null("company")
        self.expect_not_null("description")
        self.expect_unique("dedup_hash")
        self.expect_salary_sanity()
        self.expect_date_validity()

        passed = sum(1 for r in self.results if r["passed"])
        total = len(self.results)

        logger.info(f"Validation complete: {passed}/{total} checks passed")
        return self.results

    def print_report(self):
        """Print formatted validation report."""
        print("\n" + "=" * 60)
        print("DATA VALIDATION REPORT")
        print("=" * 60)

        for r in self.results:
            status = "PASS" if r["passed"] else "FAIL"
            check = r["check"]

            if "success_pct" in r:
                pct = r["success_pct"]
            elif "uniqueness_pct" in r:
                pct = r["uniqueness_pct"]
            else:
                pct = 0

            bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
            print(f"\n  [{status}] {check}")
            print(f"       {bar} {pct}%")

            for k, v in r.items():
                if k not in ["check", "passed", "success_pct", "uniqueness_pct"]:
                    print(f"       {k}: {v}")

        passed = sum(1 for r in self.results if r["passed"])
        total = len(self.results)
        print(f"\n{'=' * 60}")
        print(f"  RESULT: {passed}/{total} checks passed")
        if passed == total:
            print(f"  STATUS: ALL VALIDATIONS PASSED")
        else:
            print(f"  STATUS: {total - passed} CHECKS FAILED — review above")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    validator = DataValidator()
    validator.run_all_checks()
    validator.print_report()