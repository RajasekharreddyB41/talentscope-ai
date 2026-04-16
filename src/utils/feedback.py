"""
TalentScope AI — User feedback collector
Fire-and-forget save of thumbs-up/down + optional comment per page.

Design rules (same as analytics.py):
- NEVER raises an exception up to the caller
- Anonymous: session_id only, no PII
- One public function: save_feedback()
"""

import json
from typing import Optional

from sqlalchemy import text

from src.database.connection import get_engine
from src.utils.logger import get_logger

logger = get_logger("utils.feedback")

# Valid rating values
THUMBS_UP = 1
THUMBS_DOWN = -1
VALID_RATINGS = (THUMBS_UP, THUMBS_DOWN, None)


def save_feedback(
    session_id: str,
    page_name: str,
    rating: Optional[int] = None,
    comment: Optional[str] = None,
    context: Optional[dict] = None,
) -> bool:
    """
    Save user feedback. Never raises — returns False on any failure.

    Args:
        session_id: Anonymous session ID (from analytics.get_session_id())
        page_name: e.g. 'market_dashboard', 'salary_predictor'
        rating: 1 (thumbs up), -1 (thumbs down), or None (text only)
        comment: Optional free-text comment (trimmed to 2000 chars)
        context: Optional dict snapshot of filters / inputs at time of feedback

    Returns:
        True on success, False on failure or if both rating and comment are empty.
    """
    # Nothing to save
    if rating is None and (not comment or not comment.strip()):
        logger.info(f"save_feedback skipped: empty for page={page_name}")
        return False

    # Validate rating
    if rating not in VALID_RATINGS:
        logger.warning(f"save_feedback invalid rating={rating}, coercing to None")
        rating = None

    # Trim comment
    clean_comment = comment.strip()[:2000] if comment else None

    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO user_feedback
                        (session_id, page_name, rating, comment, context)
                    VALUES
                        (:session_id, :page_name, :rating, :comment, CAST(:context AS JSONB))
                """),
                {
                    "session_id": session_id[:64],
                    "page_name": page_name[:100],
                    "rating": rating,
                    "comment": clean_comment,
                    "context": json.dumps(context) if context else None,
                },
            )
            conn.commit()
        return True
    except Exception as e:
        logger.warning(f"save_feedback failed for {page_name}: {e}")
        return False


# ============================================================
# Self-test
# ============================================================

if __name__ == "__main__":
    import uuid

    print("=" * 60)
    print("FEEDBACK SELF-TEST")
    print("=" * 60)

    test_session = str(uuid.uuid4())
    print(f"Session ID: {test_session}")

    # Test 1: thumbs up only
    ok1 = save_feedback(test_session, "self_test_page", rating=THUMBS_UP)
    print(f"  thumbs up only:      {'OK' if ok1 else 'FAIL'}")

    # Test 2: thumbs down + comment + context
    ok2 = save_feedback(
        test_session,
        "self_test_page",
        rating=THUMBS_DOWN,
        comment="Salary range felt too wide for junior roles.",
        context={"role": "Data Analyst", "experience": "junior"},
    )
    print(f"  thumbs down + text:  {'OK' if ok2 else 'FAIL'}")

    # Test 3: comment only, no rating
    ok3 = save_feedback(
        test_session,
        "self_test_page",
        comment="Would love a CSV export.",
    )
    print(f"  comment only:        {'OK' if ok3 else 'FAIL'}")

    # Test 4: should SKIP (nothing provided)
    ok4 = save_feedback(test_session, "self_test_page")
    print(f"  empty (should skip): {'SKIP' if not ok4 else 'UNEXPECTED OK'}")

    # Test 5: invalid rating should coerce and still save if comment present
    ok5 = save_feedback(
        test_session,
        "self_test_page",
        rating=7,
        comment="Invalid rating test",
    )
    print(f"  invalid rating:      {'OK (coerced)' if ok5 else 'FAIL'}")

    # Read back
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT page_name, rating, comment, context, created_at
                    FROM user_feedback
                    WHERE session_id = :sid
                    ORDER BY created_at ASC
                """),
                {"sid": test_session},
            )
            rows = result.fetchall()
            print(f"\nRows written for test session: {len(rows)}  (expected 4)")
            for row in rows:
                print(f"  [{row[4]}] rating={row[1]} comment={row[2]!r} ctx={row[3]}")

            # Cleanup
            conn.execute(
                text("DELETE FROM user_feedback WHERE session_id = :sid"),
                {"sid": test_session},
            )
            conn.commit()
            print(f"\nCleanup: test rows deleted.")
    except Exception as e:
        print(f"Read-back failed: {e}")