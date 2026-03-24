"""
TalentScope AI — Job Clustering
Groups similar jobs using TF-IDF + dimensionality reduction.
Uses PCA for MVP (UMAP in Phase 2).
"""

import pandas as pd
import numpy as np
import pickle
import os
from sqlalchemy import text
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from src.database.connection import get_engine
from src.utils.logger import get_logger

logger = get_logger("models.clustering")

MODEL_DIR = "data/models"


def build_clusters(n_clusters: int = 8) -> pd.DataFrame:
    """
    Build job clusters using TF-IDF + PCA + KMeans.

    Returns:
        DataFrame with job info, cluster labels, and 2D coordinates
    """
    engine = get_engine()

    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT c.id, c.title, c.company, c.location_city,
                   c.salary_min, c.salary_max, c.experience_level,
                   c.description, f.title_category, f.skill_count
            FROM clean_jobs c
            JOIN job_features f ON f.clean_job_id = c.id
            WHERE c.description IS NOT NULL 
              AND c.description != ''
              AND LENGTH(c.description) > 100
        """), conn)

    logger.info(f"Building clusters from {len(df)} jobs")

    if len(df) < 50:
        logger.error("Not enough jobs for clustering")
        return pd.DataFrame()

    # TF-IDF on descriptions
    tfidf = TfidfVectorizer(
        max_features=3000,
        stop_words="english",
        ngram_range=(1, 2),
        min_df=3,
        max_df=0.95,
    )
    tfidf_matrix = tfidf.fit_transform(df["description"])

    logger.info(f"TF-IDF matrix: {tfidf_matrix.shape}")

    # PCA for 2D visualization
    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(tfidf_matrix.toarray())

    df["x"] = coords[:, 0]
    df["y"] = coords[:, 1]

    logger.info(f"PCA variance explained: {pca.explained_variance_ratio_.sum():.2%}")

    # KMeans clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df["cluster"] = kmeans.fit_predict(tfidf_matrix)

    # Label clusters by most common title category
    cluster_labels = {}
    for c in range(n_clusters):
        cluster_df = df[df["cluster"] == c]
        if not cluster_df.empty:
            top_category = cluster_df["title_category"].mode().iloc[0]
            count = len(cluster_df)
            cluster_labels[c] = f"{top_category} ({count} jobs)"

    df["cluster_label"] = df["cluster"].map(cluster_labels)

    # Get top keywords per cluster
    cluster_keywords = {}
    feature_names = tfidf.get_feature_names_out()
    for c in range(n_clusters):
        cluster_indices = df[df["cluster"] == c].index
        if len(cluster_indices) > 0:
            cluster_tfidf = tfidf_matrix[cluster_indices].mean(axis=0).A1
            top_idx = cluster_tfidf.argsort()[-5:][::-1]
            keywords = [feature_names[i] for i in top_idx]
            cluster_keywords[c] = ", ".join(keywords)

    df["cluster_keywords"] = df["cluster"].map(cluster_keywords)

    # Save clustering artifacts
    os.makedirs(MODEL_DIR, exist_ok=True)
    cluster_pkg = {
        "tfidf": tfidf,
        "pca": pca,
        "kmeans": kmeans,
        "cluster_labels": cluster_labels,
        "cluster_keywords": cluster_keywords,
    }
    with open(os.path.join(MODEL_DIR, "cluster_model.pkl"), "wb") as f:
        pickle.dump(cluster_pkg, f)

    logger.info(f"Clustering complete: {n_clusters} clusters created")

    # Summary
    print(f"\n--- Clustering Summary ---")
    print(f"  Total jobs clustered: {len(df)}")
    print(f"  Number of clusters:   {n_clusters}")
    print(f"  PCA variance:         {pca.explained_variance_ratio_.sum():.2%}")
    print(f"\n--- Cluster Breakdown ---")
    for c in range(n_clusters):
        label = cluster_labels.get(c, "Unknown")
        keywords = cluster_keywords.get(c, "")
        print(f"  Cluster {c}: {label}")
        print(f"    Keywords: {keywords}")

    return df


def get_cluster_data() -> pd.DataFrame:
    """Load pre-built cluster data for visualization."""
    model_path = os.path.join(MODEL_DIR, "cluster_model.pkl")

    if not os.path.exists(model_path):
        logger.info("No cluster model found, building now...")
        return build_clusters()

    return build_clusters()


if __name__ == "__main__":
    print("=" * 60)
    print("TALENTSCOPE AI — JOB CLUSTERING")
    print("=" * 60)

    df = build_clusters()

    if not df.empty:
        print(f"\n--- Sample Clustered Jobs ---")
        sample = df[["title", "company", "cluster_label", "x", "y"]].head(10)
        print(sample.to_string(index=False))