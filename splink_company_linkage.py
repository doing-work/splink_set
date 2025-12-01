"""
Splink Company Record Linkage Pipeline

This script performs entity resolution on company records using Splink,
grouping similar companies based on normalized names and country codes.

Uses DuckDB backend (default, no Spark required).
Optimized for 1-5 million records with efficient blocking strategies.
"""

import os
import json
from typing import Optional, Tuple
import pandas as pd

# Import Splink DuckDB linker (default backend, no Spark needed)
try:
    from splink.duckdb.linker import DuckDBLinker
    Linker = DuckDBLinker
except ImportError:
    try:
        from splink.linker import Linker
    except ImportError:
        raise ImportError(
            "Splink not installed. Install with: pip install splink"
        )

from splink.comparison import Comparison, ComparisonLevel
from splink.comparison_library import (
    exact_match,
    jaro_winkler_at_thresholds,
    jaccard_at_thresholds
)


# No Spark session needed - DuckDB handles everything internally


def load_and_prepare_data(
    csv_path: str,
    unique_id_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Load CSV data and prepare it for Splink processing.
    
    Args:
        csv_path: Path to input CSV file
        unique_id_col: Optional column name for unique ID (if None, will generate)
        
    Returns:
        Prepared pandas DataFrame
    """
    print(f"Loading data from {csv_path}...")
    
    # Read CSV
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    
    # Validate required columns
    required_cols = ["tv_name_regulatized", "tv_country_code"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Generate unique_id if not present
    if unique_id_col and unique_id_col in df.columns:
        df = df.rename(columns={unique_id_col: "unique_id"})
    else:
        df["unique_id"] = range(len(df))
    
    # Clean and normalize data
    df["tv_name_regulatized"] = df["tv_name_regulatized"].astype(str).str.lower().str.replace(r"[^\w\s]", "", regex=True).str.strip()
    df["tv_country_code"] = df["tv_country_code"].astype(str).str.upper().str.strip()
    
    # Filter out records with null or empty names
    df = df[
        (df["tv_name_regulatized"].notna()) &
        (df["tv_name_regulatized"] != "") &
        (df["tv_country_code"].notna()) &
        (df["tv_country_code"] != "")
    ].copy()
    
    # Ensure unique_id is unique and not null
    df = df[df["unique_id"].notna()].copy()
    
    record_count = len(df)
    print(f"Loaded {record_count:,} records after cleaning")
    
    return df[["unique_id", "tv_name_regulatized", "tv_country_code"]]


def create_splink_settings() -> dict:
    """
    Create Splink settings dictionary optimized for company matching.
    
    Returns:
        Splink settings dictionary
    """
    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "unique_id",
        "blocking_rules": [
            # Primary blocking: first 2 chars of name + country
            # Note: Splink automatically adds _l and _r suffixes to column names
            "substr(tv_name_regulatized_l, 1, 2) = substr(tv_name_regulatized_r, 1, 2) AND tv_country_code_l = tv_country_code_r",
            # Secondary blocking: first 3 chars of name + country (for better recall)
            "substr(tv_name_regulatized_l, 1, 3) = substr(tv_name_regulatized_r, 1, 3) AND tv_country_code_l = tv_country_code_r",
            # Fallback: country only (for edge cases)
            "tv_country_code_l = tv_country_code_r",
        ],
        "comparisons": [
            # Company name comparison with multiple similarity metrics
            Comparison(
                col_name="tv_name_regulatized",
                comparison_levels=[
                    ComparisonLevel(
                        sql_condition="tv_name_regulatized_l = tv_name_regulatized_r",
                        label_for_charts="Exact match",
                        m_probability=0.95
                    ),
                    *jaro_winkler_at_thresholds(
                        "tv_name_regulatized",
                        [0.9, 0.85, 0.75],
                        higher_is_more_similar=True
                    ),
                    *jaccard_at_thresholds(
                        "tv_name_regulatized",
                        [0.9, 0.8, 0.7],
                        higher_is_more_similar=True
                    ),
                    ComparisonLevel(
                        sql_condition="ELSE",
                        label_for_charts="All other comparisons",
                        m_probability=0.01
                    )
                ]
            ),
            # Country code comparison (exact match heavily weighted)
            Comparison(
                col_name="tv_country_code",
                comparison_levels=[
                    ComparisonLevel(
                        sql_condition="tv_country_code_l = tv_country_code_r",
                        label_for_charts="Exact match",
                        m_probability=0.9
                    ),
                    ComparisonLevel(
                        sql_condition="ELSE",
                        label_for_charts="Different country",
                        m_probability=0.1
                    )
                ]
            )
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": False,
        "em_convergence": 0.0001,
        "max_iterations": 50,
        "probability_two_random_records_match": 0.01
    }
    
    return settings


def train_and_predict(
    linker: Linker,
    df: pd.DataFrame,
    sample_size: Optional[int] = None
) -> Linker:
    """
    Train the Splink model.
    
    Args:
        linker: Configured Splink linker
        df: Input DataFrame
        sample_size: Optional sample size for training (if None, uses full dataset)
        
    Returns:
        Trained Splink linker
    """
    print("Training Splink model...")
    
    # Determine training dataset
    record_count = len(df)
    if sample_size and record_count > sample_size:
        print(f"Sampling {sample_size:,} records for training (out of {record_count:,} total)...")
        training_df = df.sample(n=sample_size, random_state=42)
    else:
        training_df = df
        print(f"Using full dataset ({record_count:,} records) for training...")
    
    # Train the model
    print("Estimating u probabilities using random sampling...")
    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    
    print("Estimating m probabilities using Expectation-Maximization...")
    linker.estimate_parameters_using_expectation_maximisation(training_df)
    
    print("Model training completed.")
    
    return linker


def convert_to_clusters(
    linker: Linker,
    predictions,
    match_probability_threshold: float = 0.5
) -> pd.DataFrame:
    """
    Convert pairwise predictions to clusters using Splink's built-in clustering.
    
    Args:
        linker: Trained Splink linker
        predictions: DataFrame with pairwise predictions
        match_probability_threshold: Minimum probability to consider a match
        
    Returns:
        DataFrame with cluster assignments (unique_id, cluster_id)
    """
    print(f"Converting predictions to clusters (threshold: {match_probability_threshold})...")
    
    # Try to use Splink's built-in clustering method
    try:
        # Method 1: cluster_pairwise_predictions_at_threshold (Splink 4.x)
        clusters = linker.cluster_pairwise_predictions_at_threshold(
            match_probability_threshold
        )
        # Convert to pandas if needed
        if hasattr(clusters, 'to_pandas'):
            clusters = clusters.to_pandas()
        # Ensure we have the right columns
        if "cluster_id" in clusters.columns and "unique_id" in clusters.columns:
            return clusters[["unique_id", "cluster_id"]]
    except (AttributeError, TypeError) as e:
        print(f"Method 1 failed: {e}")
    
    try:
        # Method 2: cluster_pairwise_predictions with filtered predictions
        matches = predictions[predictions["match_probability"] >= match_probability_threshold]
        clusters = linker.cluster_pairwise_predictions(matches)
        if hasattr(clusters, 'to_pandas'):
            clusters = clusters.to_pandas()
        if "cluster_id" in clusters.columns and "unique_id" in clusters.columns:
            return clusters[["unique_id", "cluster_id"]]
    except (AttributeError, TypeError) as e:
        print(f"Method 2 failed: {e}")
    
    # Method 3: Manual clustering using connected components
    print("Using manual connected components clustering...")
    return _manual_clustering(predictions, match_probability_threshold)


def _manual_clustering(predictions: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """
    Manual clustering implementation using iterative connected components.
    
    Args:
        predictions: DataFrame with pairwise predictions
        threshold: Match probability threshold
        
    Returns:
        DataFrame with cluster assignments
    """
    # Convert to pandas if needed
    if hasattr(predictions, 'to_pandas'):
        predictions = predictions.to_pandas()
    
    # Filter to matches above threshold
    matches = predictions[predictions["match_probability"] >= threshold][
        ["unique_id_l", "unique_id_r"]
    ].drop_duplicates()
    
    # Get all unique IDs
    all_ids = pd.concat([
        matches[["unique_id_l"]].rename(columns={"unique_id_l": "unique_id"}),
        matches[["unique_id_r"]].rename(columns={"unique_id_r": "unique_id"})
    ]).drop_duplicates().reset_index(drop=True)
    
    # Initialize: each ID is its own cluster
    clusters = all_ids.copy()
    clusters["cluster_id"] = clusters["unique_id"]
    
    # Iteratively merge clusters (connected components)
    max_iterations = 20
    for iteration in range(max_iterations):
        # Merge clusters based on matches
        merged = matches.merge(
            clusters,
            left_on="unique_id_l",
            right_on="unique_id",
            how="inner"
        )[["unique_id_r", "cluster_id"]].rename(columns={"unique_id_r": "unique_id"})
        
        merged2 = matches.merge(
            clusters,
            left_on="unique_id_r",
            right_on="unique_id",
            how="inner"
        )[["unique_id_l", "cluster_id"]].rename(columns={"unique_id_l": "unique_id"})
        
        all_merged = pd.concat([merged, merged2]).groupby("unique_id")["cluster_id"].min().reset_index()
        
        # Update clusters
        clusters = all_ids.merge(all_merged, on="unique_id", how="left")
        clusters["cluster_id"] = clusters["cluster_id_y"].fillna(clusters["unique_id"])
        clusters = clusters[["unique_id", "cluster_id"]]
        
        # Check convergence
        old_count = clusters["cluster_id"].nunique()
        new_count = clusters["cluster_id"].nunique()
        
        if old_count == new_count:
            print(f"Clustering converged after {iteration + 1} iterations")
            break
    
    return clusters


def add_representative_records(
    clusters: pd.DataFrame,
    original_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Add representative record information to clusters.
    
    Args:
        clusters: DataFrame with cluster assignments
        original_df: Original DataFrame with company data
        
    Returns:
        DataFrame with cluster info and representative records
    """
    print("Determining representative records per cluster...")
    
    # Convert to pandas if needed
    if hasattr(clusters, 'to_pandas'):
        clusters = clusters.to_pandas()
    if hasattr(original_df, 'to_pandas'):
        original_df = original_df.to_pandas()
    
    # Join clusters with original data
    clustered_df = clusters.merge(
        original_df,
        on="unique_id",
        how="inner"
    )
    
    # Select representative record (longest name, or first if tie)
    clustered_df["name_length"] = clustered_df["tv_name_regulatized"].str.len()
    clustered_df = clustered_df.sort_values(
        by=["cluster_id", "name_length", "unique_id"],
        ascending=[True, False, True]
    )
    clustered_df["is_representative"] = (
        clustered_df.groupby("cluster_id").cumcount() == 0
    )
    
    # Get representative info
    representatives = clustered_df[clustered_df["is_representative"]][
        ["cluster_id", "tv_name_regulatized", "tv_country_code"]
    ].rename(columns={
        "tv_name_regulatized": "representative_name",
        "tv_country_code": "representative_country"
    })
    
    # Add representative info to all records
    final_df = clustered_df.merge(
        representatives,
        on="cluster_id",
        how="left"
    )[
        ["cluster_id", "unique_id", "tv_name_regulatized", "tv_country_code",
         "representative_name", "representative_country", "is_representative"]
    ]
    
    return final_df


def run_pipeline(
    input_csv: str,
    output_csv: str,
    match_probability_threshold: float = 0.5,
    training_sample_size: Optional[int] = 100000,
    unique_id_col: Optional[str] = None
) -> DataFrame:
    """
    Run the complete Splink record linkage pipeline.
    
    Args:
        input_csv: Path to input CSV file
        output_csv: Path to output CSV file
        match_probability_threshold: Minimum match probability (0-1)
        training_sample_size: Sample size for EM training (None = full dataset)
        unique_id_col: Optional unique ID column name
        
    Returns:
        Final clustered DataFrame
    """
    try:
        # Load and prepare data
        df = load_and_prepare_data(input_csv, unique_id_col)
        
        # Create Splink linker
        print("Initializing Splink linker...")
        settings = create_splink_settings()
        linker = Linker(df, settings)
        
        # Train the model
        trained_linker = train_and_predict(linker, df, training_sample_size)
        
        # Generate predictions
        print("Generating pairwise predictions...")
        predictions = trained_linker.predict()
        
        # Convert to pandas if needed
        if hasattr(predictions, 'to_pandas'):
            predictions = predictions.to_pandas()
        
        # Convert to clusters using Splink's built-in clustering
        clusters = convert_to_clusters(trained_linker, predictions, match_probability_threshold)
        
        # Add representative records
        final_df = add_representative_records(clusters, df)
        
        # Save to CSV
        print(f"Saving results to {output_csv}...")
        final_df.to_csv(output_csv, index=False)
        print(f"Results saved to {output_csv}")
        
        # Show summary statistics
        cluster_count = final_df["cluster_id"].nunique()
        total_records = len(final_df)
        avg_cluster_size = total_records / cluster_count if cluster_count > 0 else 0
        
        print("\n" + "="*50)
        print("PIPELINE SUMMARY")
        print("="*50)
        print(f"Total records processed: {total_records:,}")
        print(f"Number of clusters: {cluster_count:,}")
        print(f"Average cluster size: {avg_cluster_size:.2f}")
        print(f"Match probability threshold: {match_probability_threshold}")
        print("="*50)
        
        return final_df
        
    except Exception as e:
        print(f"Error running pipeline: {e}")
        raise


def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Splink Company Record Linkage Pipeline"
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to input CSV file"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to output CSV file"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Match probability threshold (default: 0.5)"
    )
    parser.add_argument(
        "--training-sample",
        type=int,
        default=100000,
        help="Sample size for EM training (default: 100000, use 0 for full dataset)"
    )
    parser.add_argument(
        "--unique-id-col",
        type=str,
        default=None,
        help="Name of unique ID column (if not provided, will generate)"
    )
    
    args = parser.parse_args()
    
    training_sample = None if args.training_sample == 0 else args.training_sample
    
    run_pipeline(
        input_csv=args.input,
        output_csv=args.output,
        match_probability_threshold=args.threshold,
        training_sample_size=training_sample,
        unique_id_col=args.unique_id_col
    )


if __name__ == "__main__":
    main()

