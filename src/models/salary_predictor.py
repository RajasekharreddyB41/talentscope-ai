"""
TalentScope AI — Salary Prediction Model
Progressive comparison: Linear Regression → Random Forest → XGBoost
"""

import pandas as pd
import numpy as np
import pickle
import os
from sqlalchemy import text
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
from src.database.connection import get_engine
from src.utils.logger import get_logger
from xgboost import XGBRegressor

logger = get_logger("models.salary")

MODEL_DIR = "data/models"


def load_training_data() -> pd.DataFrame:
    """Load feature data with salary for model training."""
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT 
                f.skill_count,
                f.experience_encoded,
                f.location_encoded,
                f.title_category,
                f.salary_normalized,
                c.salary_min,
                c.salary_max
            FROM job_features f
            JOIN clean_jobs c ON c.id = f.clean_job_id
            WHERE c.salary_min IS NOT NULL
              AND c.salary_max IS NOT NULL
              AND c.salary_min > 20000
              AND c.salary_max > 20000
        """), conn)

    logger.info(f"Loaded {len(df)} records with salary data")
    return df


def prepare_features(df: pd.DataFrame):
    """Prepare X and y for training."""
    # Target: midpoint salary
    df["salary_mid"] = (df["salary_min"] + df["salary_max"]) / 2

    # Encode categoricals
    le_loc = LabelEncoder()
    le_title = LabelEncoder()

    df["loc_code"] = le_loc.fit_transform(df["location_encoded"].fillna("unknown"))
    df["title_code"] = le_title.fit_transform(df["title_category"].fillna("Other"))

    feature_cols = ["skill_count", "experience_encoded", "loc_code", "title_code"]
    X = df[feature_cols]
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
    """Train and compare all models."""
    # Load data
    df = load_training_data()

    if len(df) < 50:
        logger.error(f"Not enough salary data to train: {len(df)} records")
        return

    # Prepare features
    X, y, le_loc, le_title, feature_cols = prepare_features(df)

    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    logger.info(f"Train: {len(X_train)} | Test: {len(X_test)}")

    # ---- Model 1: Linear Regression (Baseline) ----
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    lr_metrics = evaluate_model("Linear Regression", lr, X_test, y_test)

    # ---- Model 2: Random Forest ----
    rf = RandomForestRegressor(
        n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
    )
    rf.fit(X_train, y_train)
    rf_metrics = evaluate_model("Random Forest", rf, X_test, y_test)

    # ---- Model 3: XGBoost ----
    xgb = XGBRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=0,
    )
    xgb.fit(X_train, y_train)
    xgb_metrics = evaluate_model("XGBoost", xgb, X_test, y_test)

    # ---- Results Comparison ----
    results = pd.DataFrame([lr_metrics, rf_metrics, xgb_metrics])

    print("\n" + "=" * 60)
    print("SALARY PREDICTION — MODEL COMPARISON")
    print("=" * 60)
    print(f"\nTraining samples: {len(X_train)} | Test samples: {len(X_test)}")
    print(f"Features: {feature_cols}\n")
    print(results.to_string(index=False))

    # ---- Feature Importance (XGBoost) ----
    importance = pd.DataFrame({
        "feature": feature_cols,
        "importance": xgb.feature_importances_
    }).sort_values("importance", ascending=False)

    print(f"\n--- Feature Importance (XGBoost) ---")
    for _, row in importance.iterrows():
        bar = "█" * int(row["importance"] * 50)
        print(f"  {row['feature']:20s}: {row['importance']:.3f} {bar}")

    # ---- Save best model ----
    os.makedirs(MODEL_DIR, exist_ok=True)

    # Pick best model by MAE
    all_models = [
        ("Linear Regression", lr, lr_metrics),
        ("Random Forest", rf, rf_metrics),
        ("XGBoost", xgb, xgb_metrics),
    ]
    best_name, best_model, best_metrics = min(all_models, key=lambda x: x[2]["MAE"])

    model_package = {
        "model": best_model,
        "le_loc": le_loc,
        "le_title": le_title,
        "feature_cols": feature_cols,
        "metrics": best_metrics,
        "all_results": results.to_dict("records"),
    }

    model_path = os.path.join(MODEL_DIR, "salary_model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(model_package, f)

    logger.info(f"Best model saved: {best_name} -> {model_path}")
    print(f"\n✓ Best model: {best_name} (saved to {model_path})")
    print(f"  MAE: ${best_metrics['MAE']:,} | RMSE: ${best_metrics['RMSE']:,} | R²: {best_metrics['R2']}")

    return results


def predict_salary(skill_count: int, experience: str, location_tier: str, title_category: str) -> dict:
    """
    Predict salary for given inputs.
    
    Returns: {"predicted_min": float, "predicted_mid": float, "predicted_max": float}
    """
    model_path = os.path.join(MODEL_DIR, "salary_model.pkl")

    if not os.path.exists(model_path):
        logger.error("Model not found. Run train_models() first.")
        return None

    with open(model_path, "rb") as f:
        pkg = pickle.load(f)

    model = pkg["model"]
    le_loc = pkg["le_loc"]
    le_title = pkg["le_title"]

    # Encode inputs (handle unseen labels)
    try:
        loc_code = le_loc.transform([location_tier])[0]
    except ValueError:
        loc_code = le_loc.transform(["tier3"])[0]

    try:
        title_code = le_title.transform([title_category])[0]
    except ValueError:
        title_code = le_title.transform(["Other Tech"])[0]

    exp_map = {"junior": 0, "mid": 1, "senior": 2, "lead": 3}
    exp_code = exp_map.get(experience, 1)

    X = pd.DataFrame([{
        "skill_count": skill_count,
        "experience_encoded": exp_code,
        "loc_code": loc_code,
        "title_code": title_code,
    }])

    predicted_mid = model.predict(X)[0]

    # Estimate range (±15% spread)
    return {
        "predicted_min": round(predicted_mid * 0.85),
        "predicted_mid": round(predicted_mid),
        "predicted_max": round(predicted_mid * 1.15),
    }


if __name__ == "__main__":
    # Train models
    train_models()

    # Test prediction
    print("\n" + "=" * 60)
    print("SAMPLE PREDICTIONS")
    print("=" * 60)

    test_cases = [
        {"skill_count": 5, "experience": "junior", "location_tier": "tier1", "title_category": "Data Analyst"},
        {"skill_count": 8, "experience": "mid", "location_tier": "tier1", "title_category": "Data Engineer"},
        {"skill_count": 10, "experience": "senior", "location_tier": "tier1", "title_category": "Data Scientist"},
        {"skill_count": 12, "experience": "senior", "location_tier": "tier2", "title_category": "ML Engineer"},
        {"skill_count": 6, "experience": "mid", "location_tier": "tier3", "title_category": "Software Engineer"},
    ]

    for tc in test_cases:
        result = predict_salary(**tc)
        if result:
            print(f"\n  {tc['title_category']} | {tc['experience']} | {tc['location_tier']} | {tc['skill_count']} skills")
            print(f"  → ${result['predicted_min']:,} - ${result['predicted_mid']:,} - ${result['predicted_max']:,}")