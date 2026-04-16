"""
TalentScope AI — Salary Prediction Model v2
Enhanced features + SHAP explainability
Progressive: Linear Regression → Random Forest → XGBoost
"""

import pandas as pd
import numpy as np
import pickle
import os
from datetime import datetime
from sqlalchemy import text
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
from src.database.connection import get_engine
from src.utils.logger import get_logger

try:
    from xgboost import XGBRegressor
    HAS_XGB = True
except ImportError:
    HAS_XGB = False

logger = get_logger("models.salary")

MODEL_DIR = "data/models"

# Top skills to create binary features for
TOP_SKILLS = ["python", "sql", "aws", "excel", "java", "azure", "git", "scala"]


def load_training_data() -> pd.DataFrame:
    """Load enriched feature data for model training."""
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT 
                f.skill_count,
                f.experience_encoded,
                f.location_encoded,
                f.title_category,
                f.skills,
                c.salary_min,
                c.salary_max,
                c.is_remote,
                c.description,
                c.company,
                c.posted_date,
                c.source
            FROM job_features f
            JOIN clean_jobs c ON c.id = f.clean_job_id
            WHERE c.salary_min IS NOT NULL
              AND c.salary_max IS NOT NULL
              AND c.salary_min > 20000
              AND c.salary_max > 20000
              AND c.salary_min < 500000
        """), conn)

    logger.info(f"Loaded {len(df)} records with salary data")
    return df


def engineer_features(df: pd.DataFrame):
    """
    Build v2 feature set with 10+ features.
    Returns X, y, encoders, and feature column names.
    """
    # Target: midpoint salary
    df["salary_mid"] = (df["salary_min"] + df["salary_max"]) / 2

    # --- Feature engineering ---

    # 1. Is remote (boolean → int)
    df["is_remote_flag"] = df["is_remote"].astype(int)

    # 2. Has salary range (min != max means negotiable)
    df["has_salary_range"] = (df["salary_min"] != df["salary_max"]).astype(int)

    # 3. Description length (proxy for role seniority/complexity)
    df["desc_length"] = df["description"].fillna("").str.len()
    df["desc_length_log"] = np.log1p(df["desc_length"])

    # 4. Company job count (large employer signal)
    company_counts = df["company"].value_counts().to_dict()
    df["company_job_count"] = df["company"].map(company_counts).fillna(1)
    df["company_job_count_log"] = np.log1p(df["company_job_count"])

    # 5. Days since posted (freshness)
    today = datetime.now().date()
    df["posted_date_parsed"] = pd.to_datetime(df["posted_date"], errors="coerce")
    df["days_since_posted"] = (pd.Timestamp(today) - df["posted_date_parsed"]).dt.days
    df["days_since_posted"] = df["days_since_posted"].fillna(30).clip(0, 365)

    # 6. Top skill binary flags
    for skill in TOP_SKILLS:
        df[f"has_{skill}"] = df["skills"].apply(
            lambda s: 1 if s and skill in [x.lower() for x in s] else 0
        )

    # 7. Salary spread (max - min, as feature for range prediction)
    df["salary_spread"] = df["salary_max"] - df["salary_min"]

    # 8. Encode categoricals
    le_loc = LabelEncoder()
    le_title = LabelEncoder()

    df["loc_code"] = le_loc.fit_transform(df["location_encoded"].fillna("unknown"))
    df["title_code"] = le_title.fit_transform(df["title_category"].fillna("Other"))

    # Build feature list
    feature_cols = [
        "skill_count",
        "experience_encoded",
        "loc_code",
        "title_code",
        "is_remote_flag",
        "has_salary_range",
        "desc_length_log",
        "company_job_count_log",
        "days_since_posted",
    ] + [f"has_{s}" for s in TOP_SKILLS]

    X = df[feature_cols].fillna(0)
    y = df["salary_mid"]

    return X, y, le_loc, le_title, feature_cols


def evaluate_model(name: str, model, X_test, y_test) -> dict:
    """Evaluate a model and return metrics."""
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    metrics = {"model": name, "MAE": round(mae), "RMSE": round(rmse), "R2": round(r2, 3)}
    logger.info(f"{name}: MAE=${mae:,.0f} | RMSE=${rmse:,.0f} | R²={r2:.3f}")
    return metrics


def train_models():
    """Train and compare all models with v2 features."""
    df = load_training_data()

    if len(df) < 50:
        logger.error(f"Not enough salary data: {len(df)} records")
        return None

    X, y, le_loc, le_title, feature_cols = engineer_features(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    logger.info(f"Train: {len(X_train)} | Test: {len(X_test)} | Features: {len(feature_cols)}")

    # ---- Model 1: Linear Regression ----
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    lr_metrics = evaluate_model("Linear Regression", lr, X_test, y_test)

    # ---- Model 2: Random Forest ----
    rf = RandomForestRegressor(
        n_estimators=200, max_depth=12, min_samples_leaf=5,
        random_state=42, n_jobs=-1
    )
    rf.fit(X_train, y_train)
    rf_metrics = evaluate_model("Random Forest", rf, X_test, y_test)

    # ---- Model 3: XGBoost ----
    all_metrics = [lr_metrics, rf_metrics]
    all_models = [
        ("Linear Regression", lr, lr_metrics),
        ("Random Forest", rf, rf_metrics),
    ]

    if HAS_XGB:
        xgb = XGBRegressor(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.08,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42,
            verbosity=0,
        )
        xgb.fit(X_train, y_train)
        xgb_metrics = evaluate_model("XGBoost", xgb, X_test, y_test)
        all_metrics.append(xgb_metrics)
        all_models.append(("XGBoost", xgb, xgb_metrics))

    # ---- Results ----
    results = pd.DataFrame(all_metrics)

    print("\n" + "=" * 60)
    print("SALARY PREDICTION v2 — MODEL COMPARISON")
    print("=" * 60)
    print(f"\nTraining: {len(X_train)} | Test: {len(X_test)} | Features: {len(feature_cols)}")
    print(f"Features: {feature_cols}\n")
    print(results.to_string(index=False))

    # ---- Feature Importance ----
    best_name, best_model, best_metrics = min(all_models, key=lambda x: x[2]["MAE"])

    if hasattr(best_model, "feature_importances_"):
        importance = pd.DataFrame({
            "feature": feature_cols,
            "importance": best_model.feature_importances_
        }).sort_values("importance", ascending=False)

        print(f"\n--- Feature Importance ({best_name}) ---")
        for _, row in importance.iterrows():
            bar = "█" * int(row["importance"] * 50)
            print(f"  {row['feature']:25s}: {row['importance']:.3f} {bar}")

    # ---- Save model ----
    os.makedirs(MODEL_DIR, exist_ok=True)

    # Compute SHAP values for explainability
    shap_data = None
    try:
        import shap
        explainer = shap.TreeExplainer(best_model)
        shap_values = explainer.shap_values(X_test)
        
        # Average absolute SHAP per feature
        shap_importance = pd.DataFrame({
            "feature": feature_cols,
            "shap_impact": np.abs(shap_values).mean(axis=0)
        }).sort_values("shap_impact", ascending=False)
        
        shap_data = shap_importance.to_dict("records")
        
        print(f"\n--- SHAP Feature Impact ---")
        for _, row in shap_importance.iterrows():
            bar = "█" * int(row["shap_impact"] / shap_importance["shap_impact"].max() * 30)
            print(f"  {row['feature']:25s}: ${row['shap_impact']:>10,.0f} {bar}")
    except Exception as e:
        logger.warning(f"SHAP computation skipped: {e}")

    # ---- Quantile regression for confidence intervals ----
    logger.info("Training quantile models for 90% confidence intervals...")
    quantile_lower = GradientBoostingRegressor(
        loss="quantile", alpha=0.05,
        n_estimators=200, max_depth=5, learning_rate=0.08,
        min_samples_leaf=5, random_state=42,
    )
    quantile_upper = GradientBoostingRegressor(
        loss="quantile", alpha=0.95,
        n_estimators=200, max_depth=5, learning_rate=0.08,
        min_samples_leaf=5, random_state=42,
    )
    quantile_lower.fit(X_train, y_train)
    quantile_upper.fit(X_train, y_train)

    # Sanity check on test set
    q_lower_pred = quantile_lower.predict(X_test)
    q_upper_pred = quantile_upper.predict(X_test)
    coverage = ((y_test >= q_lower_pred) & (y_test <= q_upper_pred)).mean()
    logger.info(f"Quantile 90% interval coverage on test: {coverage:.1%}")
    print(f"\n✓ Confidence interval coverage (target 90%): {coverage:.1%}")

    model_package = {
        "model": best_model,
        "quantile_lower": quantile_lower,
        "quantile_upper": quantile_upper,
        "le_loc": le_loc,
        "le_title": le_title,
        "feature_cols": feature_cols,
        "metrics": best_metrics,
        "all_results": results.to_dict("records"),
        "top_skills": TOP_SKILLS,
        "version": "v2.1",
        "shap_data": shap_data,
        "quantile_coverage": round(float(coverage), 3),
    }

    model_path = os.path.join(MODEL_DIR, "salary_model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(model_package, f)

    logger.info(f"Best model saved: {best_name} -> {model_path}")
    print(f"\n✓ Best model: {best_name} (saved to {model_path})")
    print(f"  MAE: ${best_metrics['MAE']:,} | RMSE: ${best_metrics['RMSE']:,} | R²: {best_metrics['R2']}")

    return results


def predict_salary(skill_count: int, experience: str, location_tier: str,
                   title_category: str, is_remote: bool = False,
                   user_skills: list = None) -> dict:
    """
    Predict salary with v2 model.
    Returns prediction with range and feature contributions.
    """
    model_path = os.path.join(MODEL_DIR, "salary_model.pkl")

    if not os.path.exists(model_path):
        logger.info("Model not found, training automatically...")
        try:
            train_models()
        except Exception as e:
            logger.error(f"Auto-train failed: {e}")
            return None

    if not os.path.exists(model_path):
        return None

    with open(model_path, "rb") as f:
        pkg = pickle.load(f)

    model = pkg["model"]
    le_loc = pkg["le_loc"]
    le_title = pkg["le_title"]
    feature_cols = pkg["feature_cols"]
    top_skills = pkg.get("top_skills", TOP_SKILLS)

    # Encode location
    try:
        loc_code = le_loc.transform([location_tier])[0]
    except ValueError:
        loc_code = 0

    # Encode title
    try:
        title_code = le_title.transform([title_category])[0]
    except ValueError:
        title_code = 0

    # Experience
    exp_map = {"junior": 0, "mid": 1, "senior": 2, "lead": 3}
    exp_code = exp_map.get(experience, 1)

    # Build skill flags
    user_skills_lower = [s.lower() for s in (user_skills or [])]
    skill_flags = {f"has_{s}": 1 if s in user_skills_lower else 0 for s in top_skills}

    # Build feature vector
    features = {
        "skill_count": skill_count,
        "experience_encoded": exp_code,
        "loc_code": loc_code,
        "title_code": title_code,
        "is_remote_flag": 1 if is_remote else 0,
        "has_salary_range": 1,
        "desc_length_log": np.log1p(2000),  # Assume average description
        "company_job_count_log": np.log1p(5),  # Assume mid-size employer
        "days_since_posted": 7,  # Assume recent posting
    }
    features.update(skill_flags)

    # Ensure correct column order
    X = pd.DataFrame([features])[feature_cols]

    predicted_mid = model.predict(X)[0]

    # Real confidence intervals from quantile models (fallback to ±15% for old pickles)
    q_lower_model = pkg.get("quantile_lower")
    q_upper_model = pkg.get("quantile_upper")
    if q_lower_model is not None and q_upper_model is not None:
        predicted_low = float(q_lower_model.predict(X)[0])
        predicted_high = float(q_upper_model.predict(X)[0])
        # Safety: ensure ordering (quantile crossing can occasionally happen)
        if predicted_low > predicted_high:
            predicted_low, predicted_high = predicted_high, predicted_low
        confidence_source = "quantile_regression"
        confidence_level = 0.90
    else:
        predicted_low = predicted_mid * 0.85
        predicted_high = predicted_mid * 1.15
        confidence_source = "heuristic"
        confidence_level = None

    # Feature contributions (approximate via feature importance)
    # SHAP-based contributions
    contributions = {}
    shap_data = pkg.get("shap_data")
    if shap_data:
        for item in shap_data:
            if item["shap_impact"] > 1000:  # Only show impactful features
                feat = item["feature"]
                readable = feat.replace("_encoded", "").replace("_code", "").replace("_flag", "").replace("_log", "").replace("has_", "").replace("company_job_count", "company_size").replace("desc_length", "role_complexity").replace("skill_count", "skills")
                contributions[readable] = round(item["shap_impact"])
    elif hasattr(model, "feature_importances_"):
        for feat, imp in zip(feature_cols, model.feature_importances_):
            if imp > 0.05:
                readable = feat.replace("_encoded", "").replace("_code", "").replace("_flag", "").replace("_log", "").replace("has_", "")
                contributions[readable] = round(imp * 100, 1)

    return {
        "predicted_min": round(predicted_low),
        "predicted_mid": round(predicted_mid),
        "predicted_max": round(predicted_high),
        "model_version": pkg.get("version", "v1"),
        "model_name": pkg["metrics"]["model"],
        "contributions": contributions,
        "confidence_source": confidence_source,
        "confidence_level": confidence_level,
    }


if __name__ == "__main__":
    results = train_models()

    if results is not None:
        print("\n" + "=" * 60)
        print("SAMPLE PREDICTIONS (v2)")
        print("=" * 60)

        test_cases = [
            {"skill_count": 5, "experience": "junior", "location_tier": "tier1",
             "title_category": "Data Analyst", "user_skills": ["python", "sql", "excel"]},
            {"skill_count": 8, "experience": "mid", "location_tier": "tier1",
             "title_category": "Data Engineer", "user_skills": ["python", "sql", "aws", "scala"]},
            {"skill_count": 10, "experience": "senior", "location_tier": "tier1",
             "title_category": "Data Scientist", "is_remote": True,
             "user_skills": ["python", "sql", "aws", "git", "java"]},
            {"skill_count": 12, "experience": "senior", "location_tier": "tier2",
             "title_category": "ML Engineer", "user_skills": ["python", "aws", "azure"]},
        ]

        for tc in test_cases:
            result = predict_salary(**tc)
            if result:
                print(f"\n  {tc['title_category']} | {tc['experience']} | {tc['location_tier']}")
                print(f"  Skills: {tc.get('user_skills', [])}")
                print(f"  → ${result['predicted_min']:,} - ${result['predicted_mid']:,} - ${result['predicted_max']:,}")
                if result.get("contributions"):
                    print(f"  Top factors: {result['contributions']}")