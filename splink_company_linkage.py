"""
Splink Company Record Linkage Pipeline

This script performs entity resolution on company records using Splink,
grouping similar companies based on normalized names and country codes.

Optimized for 1-5 million records with efficient blocking strategies.
"""

import os
import json
from typing import Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, isnan, isnull, length, max as spark_max,
    substring, lower, trim, regexp_replace, monotonically_increasing_id,
    collect_list, first, row_number, concat_ws
)
from pyspark.sql.window import Window

# Import Splink with fallback for different versions
try:
    # Splink 4.x import path
    from splink.spark.spark_linker import SparkLinker
except ImportError:
    try:
        # Alternative import path (older versions)
        from splink.spark.linker import SparkLinker
    except ImportError:
        try:
            from splink.spark import SparkLinker
        except ImportError:
            raise ImportError(
                "Splink Spark backend not found. Please install with: pip install 'splink[spark]'"
            )

from splink.comparison import Comparison, ComparisonLevel
from splink.comparison_library import (
    exact_match,
    jaro_winkler_at_thresholds,
    jaccard_at_thresholds
)

# Import for Spark similarity JAR (required for similarity functions)
try:
    from splink.backends.spark import similarity_jar_location
    SIMILARITY_JAR_AVAILABLE = True
except ImportError:
    try:
        # Alternative import path
        from splink.spark import similarity_jar_location
        SIMILARITY_JAR_AVAILABLE = True
    except ImportError:
        SIMILARITY_JAR_AVAILABLE = False
        similarity_jar_location = None


def create_spark_session(
    app_name: str = "SplinkCompanyLinkage",
    master: str = "local[*]",
    driver_memory: str = "8g",
    executor_memory: str = "8g",
    max_result_size: str = "4g",
    parallelism: int = 8
) -> SparkSession:
    """
    Create and configure Spark session for Splink with similarity JAR support.
    
    Args:
        app_name: Application name
        master: Spark master URL
        driver_memory: Driver memory allocation
        executor_memory: Executor memory allocation
        max_result_size: Maximum result size
        parallelism: Default parallelism (adjust based on cluster size)
        
    Returns:
        Configured SparkSession with Splink similarity functions
    """
    from pyspark import SparkConf, SparkContext
    
    # Create Spark configuration
    conf = SparkConf()
    conf.set("spark.driver.memory", driver_memory)
    conf.set("spark.executor.memory", executor_memory)
    conf.set("spark.driver.maxResultSize", max_result_size)
    conf.set("spark.sql.shuffle.partitions", str(parallelism * 25))  # ~25 partitions per core
    conf.set("spark.default.parallelism", str(parallelism))
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.codegen.wholeStage", "false")  # Can help with stability
    
    # Add Splink similarity JAR (required for Jaro-Winkler, Jaccard, etc.)
    if SIMILARITY_JAR_AVAILABLE and similarity_jar_location is not None:
        try:
            jar_path = similarity_jar_location()
            conf.set("spark.jars", jar_path)
            print(f"Splink similarity JAR loaded: {jar_path}")
        except Exception as e:
            print(f"Warning: Could not load Splink similarity JAR: {e}")
            print("Similarity functions may not work correctly.")
    else:
        print("Warning: Splink similarity JAR not available. Similarity functions may not work.")
    
    # Create or get SparkContext
    sc = SparkContext.getOrCreate(conf=conf)
    
    # Create SparkSession
    spark = SparkSession(sc)
    
    # Set checkpoint directory for iterative algorithms
    import os
    checkpoint_dir = "./tmp_checkpoints"
    os.makedirs(checkpoint_dir, exist_ok=True)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_and_prepare_data(
    spark: SparkSession,
    csv_path: str,
    unique_id_col: Optional[str] = None
) -> DataFrame:
    """
    Load CSV data and prepare it for Splink processing.
    
    Args:
        spark: SparkSession
        csv_path: Path to input CSV file
        unique_id_col: Optional column name for unique ID (if None, will generate)
        
    Returns:
        Prepared Spark DataFrame
    """
    print(f"Loading data from {csv_path}...")
    
    # Read CSV
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("nullValue", "") \
        .option("emptyValue", "") \
        .csv(csv_path)
    
    # Validate required columns
    required_cols = ["tv_name_regulatized", "tv_country_code"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Generate unique_id if not present
    if unique_id_col and unique_id_col in df.columns:
        df = df.withColumnRenamed(unique_id_col, "unique_id")
    else:
        df = df.withColumn("unique_id", monotonically_increasing_id())
    
    # Clean and normalize data
    df = df.withColumn(
        "tv_name_regulatized",
        trim(lower(regexp_replace(
            col("tv_name_regulatized"),
            r"[^\w\s]", ""
        )))
    )
    
    df = df.withColumn(
        "tv_country_code",
        trim(upper(col("tv_country_code")))
    )
    
    # Filter out records with null or empty names
    df = df.filter(
        col("tv_name_regulatized").isNotNull() &
        (col("tv_name_regulatized") != "") &
        col("tv_country_code").isNotNull() &
        (col("tv_country_code") != "")
    )
    
    # Ensure unique_id is unique and not null
    df = df.filter(col("unique_id").isNotNull())
    
    record_count = df.count()
    print(f"Loaded {record_count:,} records after cleaning")
    
    return df.select("unique_id", "tv_name_regulatized", "tv_country_code")


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
            "substring(tv_name_regulatized_l, 1, 2) = substring(tv_name_regulatized_r, 1, 2) AND tv_country_code_l = tv_country_code_r",
            # Secondary blocking: first 3 chars of name + country (for better recall)
            "substring(tv_name_regulatized_l, 1, 3) = substring(tv_name_regulatized_r, 1, 3) AND tv_country_code_l = tv_country_code_r",
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
    linker: SparkLinker,
    df: DataFrame,
    sample_size: Optional[int] = None
) -> SparkLinker:
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
    record_count = df.count()
    if sample_size and record_count > sample_size:
        print(f"Sampling {sample_size:,} records for training (out of {record_count:,} total)...")
        training_df = df.sample(fraction=sample_size / record_count, seed=42)
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
    linker: SparkLinker,
    predictions: DataFrame,
    match_probability_threshold: float = 0.5
) -> DataFrame:
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
        # Ensure we have the right columns
        if "cluster_id" in clusters.columns and "unique_id" in clusters.columns:
            return clusters.select("unique_id", "cluster_id")
    except (AttributeError, TypeError):
        pass
    
    try:
        # Method 2: cluster_pairwise_predictions with filtered predictions
        matches = predictions.filter(
            col("match_probability") >= match_probability_threshold
        )
        clusters = linker.cluster_pairwise_predictions(matches)
        if "cluster_id" in clusters.columns and "unique_id" in clusters.columns:
            return clusters.select("unique_id", "cluster_id")
    except (AttributeError, TypeError):
        pass
    
    # Method 3: Manual clustering using connected components
    print("Using manual connected components clustering...")
    return _manual_clustering(predictions, match_probability_threshold)


def _manual_clustering(predictions: DataFrame, threshold: float) -> DataFrame:
    """
    Manual clustering implementation using iterative connected components.
    
    Args:
        predictions: DataFrame with pairwise predictions
        threshold: Match probability threshold
        
    Returns:
        DataFrame with cluster assignments
    """
    from pyspark.sql.functions import min as spark_min, max as spark_max
    
    # Filter to matches above threshold
    matches = predictions.filter(
        col("match_probability") >= threshold
    ).select(
        col("unique_id_l").alias("id1"),
        col("unique_id_r").alias("id2")
    ).distinct()
    
    # Get all unique IDs
    all_ids = matches.select(col("id1").alias("unique_id")).union(
        matches.select(col("id2").alias("unique_id"))
    ).distinct()
    
    # Initialize: each ID is its own cluster
    clusters = all_ids.withColumn("cluster_id", col("unique_id"))
    
    # Iteratively merge clusters (connected components)
    max_iterations = 20
    for iteration in range(max_iterations):
        # Join clusters with matches to find connected components
        updated = clusters.alias("c1").join(
            matches.alias("m"),
            (col("c1.unique_id") == col("m.id1")) | (col("c1.unique_id") == col("m.id2")),
            "inner"
        ).select(
            when(col("c1.unique_id") == col("m.id1"), col("m.id2"))
            .otherwise(col("m.id1")).alias("unique_id"),
            spark_min(col("c1.cluster_id")).alias("cluster_id")
        ).groupBy("unique_id").agg(
            spark_min("cluster_id").alias("cluster_id")
        )
        
        # Also keep IDs that weren't in matches
        all_updated = all_ids.alias("ids").join(
            updated.alias("upd"),
            col("ids.unique_id") == col("upd.unique_id"),
            "left"
        ).select(
            col("ids.unique_id").alias("unique_id"),
            when(col("upd.cluster_id").isNotNull(), col("upd.cluster_id"))
            .otherwise(col("ids.unique_id")).alias("cluster_id")
        )
        
        # Check convergence
        old_clusters = clusters.select("cluster_id").distinct().count()
        new_clusters = all_updated.select("cluster_id").distinct().count()
        
        clusters = all_updated
        
        if old_clusters == new_clusters:
            print(f"Clustering converged after {iteration + 1} iterations")
            break
    
    return clusters


def add_representative_records(
    clusters: DataFrame,
    original_df: DataFrame
) -> DataFrame:
    """
    Add representative record information to clusters.
    
    Args:
        clusters: DataFrame with cluster assignments
        original_df: Original DataFrame with company data
        
    Returns:
        DataFrame with cluster info and representative records
    """
    print("Determining representative records per cluster...")
    
    # Join clusters with original data
    clustered_df = clusters.join(
        original_df,
        on="unique_id",
        how="inner"
    )
    
    # Select representative record (longest name, or first if tie)
    window_spec = Window.partitionBy("cluster_id").orderBy(
        length(col("tv_name_regulatized")).desc(),
        col("unique_id").asc()
    )
    
    clustered_with_rep = clustered_df.withColumn(
        "is_representative",
        row_number().over(window_spec) == 1
    )
    
    # Get representative info
    representatives = clustered_with_rep.filter(
        col("is_representative") == True
    ).select(
        col("cluster_id"),
        col("tv_name_regulatized").alias("representative_name"),
        col("tv_country_code").alias("representative_country")
    )
    
    # Add representative info to all records
    final_df = clustered_df.join(
        representatives,
        on="cluster_id",
        how="left"
    ).select(
        col("cluster_id"),
        col("unique_id"),
        col("tv_name_regulatized"),
        col("tv_country_code"),
        col("representative_name"),
        col("representative_country"),
        col("is_representative")
    )
    
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
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load and prepare data
        df = load_and_prepare_data(spark, input_csv, unique_id_col)
        
        # Create Splink linker
        print("Initializing Splink linker...")
        settings = create_splink_settings()
        linker = SparkLinker(df, settings)
        
        # Train the model
        trained_linker = train_and_predict(linker, df, training_sample_size)
        
        # Generate predictions
        print("Generating pairwise predictions...")
        predictions = trained_linker.predict(df)
        
        # Convert to clusters using Splink's built-in clustering
        clusters = convert_to_clusters(trained_linker, predictions, match_probability_threshold)
        
        # Add representative records
        final_df = add_representative_records(clusters, df)
        
        # Save to CSV
        print(f"Saving results to {output_csv}...")
        final_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_csv.replace(".csv", "_temp"))
        
        # Rename the output file (Spark writes to directory)
        # Note: In production, you may want to handle this differently
        print(f"Results saved. Check {output_csv.replace('.csv', '_temp')} directory for output files.")
        
        # Show summary statistics
        cluster_count = final_df.select("cluster_id").distinct().count()
        total_records = final_df.count()
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
        
    finally:
        spark.stop()


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

