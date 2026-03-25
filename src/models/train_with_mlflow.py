"""
TalentScope AI — Model Training with MLflow
Logs all experiments for reproducibility and comparison.
"""

import os
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from src.models.salary_predictor import load_training_data, prepare_features
from src.utils.logger import get_logger

logger = get_logger("models.mlflow")

# MLflow setup
MLFLOW_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "mlruns")
mlflow.set_tracking_uri(f"file:///{os.path.abspath(MLFLOW_DIR)}")
EXPERIMENT_NAME = "TalentScope_Salary_Prediction"


def log_model_run(name, model, params, X_train, X_test, y_train, y_test, feature_cols):
    """Train a model and log everything to MLflow."""
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name=name):
        # Log parameters
        mlflow.log_param("model_type", name)
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        mlflow.log_param("features", str(feature_cols))
        mlflow.log_param("n_features", len(feature_cols))

        for k, v in params.items():
            mlflow.log_param(k, v)

        # Train
        model.fit(X_train, y_train)

        # Predict
        y_pred = model.predict(X_test)

        # Metrics
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)

        mlflow.log_metric("MAE", round(mae))
        mlflow.log_metric("RMSE", round(rmse))
        mlflow.log_metric("R2", round(r2, 4))

        # Log model
        mlflow.sklearn.log_model(model, "model")

        # Log feature importance if available
        if hasattr(model, "feature_importances_"):
            importance = dict(zip(feature_cols, model.feature_importances_))
            for feat, imp in importance.items():
                mlflow.log_metric(f"importance_{feat}", round(float(imp), 4))

        logger.info(f"[MLflow] {name}: MAE=${mae:,.0f} | RMSE=${rmse:,.0f} | R²={r2:.4f}")

        return {"model": name, "MAE": round(mae), "RMSE": round(rmse), "R2": round(r2, 4)}


def run_experiment():
    """Run full experiment with all models logged to MLflow."""
    print("=" * 60)
    print("MLFLOW EXPERIMENT — SALARY PREDICTION")
    print("=" * 60)

    # Load and prepare data
    df = load_training_data()
    X, y, le_loc, le_title, feature_cols = prepare_features(df)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print(f"\nTrain: {len(X_train)} | Test: {len(X_test)}")
    print(f"Features: {feature_cols}")
    print(f"MLflow tracking: {mlflow.get_tracking_uri()}\n")

    results = []

    # Model 1: Linear Regression
    results.append(log_model_run(
        "Linear Regression",
        LinearRegression(),
        {"regularization": "none"},
        X_train, X_test, y_train, y_test, feature_cols
    ))

    # Model 2: Random Forest
    rf_params = {"n_estimators": 100, "max_depth": 10}
    results.append(log_model_run(
        "Random Forest",
        RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1),
        rf_params,
        X_train, X_test, y_train, y_test, feature_cols
    ))

    # Model 3: XGBoost default
    xgb_params = {"n_estimators": 200, "max_depth": 6, "learning_rate": 0.1}
    results.append(log_model_run(
        "XGBoost_v1",
        XGBRegressor(n_estimators=200, max_depth=6, learning_rate=0.1, random_state=42, verbosity=0),
        xgb_params,
        X_train, X_test, y_train, y_test, feature_cols
    ))

    # Model 4: XGBoost tuned
    xgb_tuned_params = {"n_estimators": 300, "max_depth": 4, "learning_rate": 0.05, "subsample": 0.8}
    results.append(log_model_run(
        "XGBoost_v2_tuned",
        XGBRegressor(
            n_estimators=300, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, random_state=42, verbosity=0
        ),
        xgb_tuned_params,
        X_train, X_test, y_train, y_test, feature_cols
    ))

    # Summary
    df_results = pd.DataFrame(results)
    print("\n" + "=" * 60)
    print("EXPERIMENT RESULTS")
    print("=" * 60)
    print(df_results.to_string(index=False))

    best = min(results, key=lambda x: x["MAE"])
    print(f"\nBest model: {best['model']} (MAE=${best['MAE']:,})")
    print(f"\nMLflow UI: run 'mlflow ui' to view experiments")
    print(f"Tracking dir: {MLFLOW_DIR}")

    return df_results


if __name__ == "__main__":
    run_experiment()