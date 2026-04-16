
"""
TalentScope AI — Analytics event tracker
Fire-and-forget logging of page views and feature usage.

Design rules:
- NEVER raises an exception up to the caller (UI must never break on analytics)
- Anonymous: session_id is client-generated, no PII collected
- One public function: track_event()
- Optional helper: get_session_id() for Streamlit pages
"""

import json
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import text

from src.database.connection import get_engine
from src.utils.logger import get_logger

logger = get_logger("utils.analytics")


def track_event(
    event_name: str,
    session_id: str,
    event_type: str = "page_view",
    properties: Optional[dict] = None,
) -> bool:
    """
    Log an analytics event. Never raises — returns False on failure.

    Args:
        event_name: Short identifier, e.g. 'market_dashboard', 'salary_predict'
        session_id: Anonymous session ID from get_session_id()
        event_type: 'page_view' | 'feature_use' | 'error'
        properties: Optional dict of structured context (role, city, etc.)

    Returns:
        True on success, False on any failure.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO analytics_events
                        (session_id, event_type, event_name, properties)
                    VALUES
                        (:session_id, :event_type, :event_name, CAST(:properties AS JSONB))
                """),
                {
                    "session_id": session_id[:64],
                    "event_type": event_type[:50],
                    "event_name": event_name[:100],
                    "properties": json.dumps(properties) if properties else None,
                },
            )
            conn.commit()
        return True
    except Exception as e:
        # Never propagate — analytics must not break the app
        logger.warning(f"track_event failed for {event_name}: {e}")
        return False


def get_session_id() -> str:
    """
    Return a stable per-session ID for Streamlit.
    Stored in st.session_state, persists for the browser session.
    Falls back to a fresh UUID if Streamlit isn't available (e.g. in tests).
    """
    try:
        import streamlit as st
        if "analytics_session_id" not in st.session_state:
            st.session_state.analytics_session_id = str(uuid.uuid4())
        return st.session_state.analytics_session_id
    except Exception:
        return str(uuid.uuid4())


# ============================================================
# Self-test
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("ANALYTICS SELF-TEST")
    print("=" * 60)

    test_session = str(uuid.uuid4())
    print(f"Session ID: {test_session}")

    # Test 1: simple page view
    ok1 = track_event("self_test_page_view", test_session, "page_view")
    print(f"  page_view event:    {'OK' if ok1 else 'FAIL'}")

    # Test 2: feature use with properties
    ok2 = track_event(
        "self_test_feature",
        test_session,
        event_type="feature_use",
        properties={"role": "Data Engineer", "city": "Boston"},
    )
    print(f"  feature_use event:  {'OK' if ok2 else 'FAIL'}")

    # Test 3: read back
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT event_type, event_name, properties, created_at
                    FROM analytics_events
                    WHERE session_id = :sid
                    ORDER BY created_at DESC
                """),
                {"sid": test_session},
            )
            rows = result.fetchall()
            print(f"\nRows written for test session: {len(rows)}")
            for row in rows:
                print(f"  [{row[3]}] {row[0]} / {row[1]} -> {row[2]}")

            # Cleanup test rows so we don't pollute the table
            conn.execute(
                text("DELETE FROM analytics_events WHERE session_id = :sid"),
                {"sid": test_session},
            )
            conn.commit()
            print(f"\nCleanup: test rows deleted.")
    except Exception as e:
        print(f"Read-back failed: {e}")
