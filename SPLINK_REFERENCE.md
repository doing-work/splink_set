# Splink Spark Implementation Reference

This implementation follows the official Splink Spark example patterns from:
**https://moj-analytical-services.github.io/splink/demos/examples/spark/deduplicate_1k_synthetic.html**

## Key Implementation Patterns (Based on Official Example)

### 1. Spark Session Setup

Our `create_spark_session()` function follows the official pattern:

```python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from splink.backends.spark import similarity_jar_location

conf = SparkConf()
conf.set("spark.driver.memory", "12g")
conf.set("spark.default.parallelism", "8")
conf.set("spark.sql.codegen.wholeStage", "false")

# Add Splink similarity JAR (required for similarity functions)
path = similarity_jar_location()
conf.set("spark.jars", path)

sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
spark.sparkContext.setCheckpointDir("./tmp_checkpoints")
```

**Reference**: [Official Spark Example](https://moj-analytical-services.github.io/splink/demos/examples/spark/deduplicate_1k_synthetic.html?h=spark#linking-in-spark)

### 2. SparkLinker Initialization

```python
from splink.spark.spark_linker import SparkLinker

linker = SparkLinker(df, settings)
```

This matches the official pattern where SparkLinker takes:
- First argument: Spark DataFrame
- Second argument: Settings dictionary

### 3. Settings Structure

Our settings use the Comparison API which is the recommended approach:

```python
from splink.comparison import Comparison, ComparisonLevel
from splink.comparison_library import (
    jaro_winkler_at_thresholds,
    jaccard_at_thresholds
)

settings = {
    "link_type": "dedupe_only",
    "unique_id_column_name": "unique_id",
    "blocking_rules": [...],
    "comparisons": [
        Comparison(...),
        Comparison(...)
    ]
}
```

### 4. Training Flow

```python
# Estimate u probabilities
linker.estimate_u_using_random_sampling(max_pairs=1e6)

# Estimate m probabilities using EM
linker.estimate_parameters_using_expectation_maximisation(training_df)

# Generate predictions
predictions = linker.predict(df)
```

### 5. Clustering

```python
# Convert predictions to clusters
clusters = linker.cluster_pairwise_predictions_at_threshold(threshold)
```

## Differences from Official Example

1. **Blocking Rules**: Our implementation uses multi-level blocking optimized for company names:
   - First 2 chars + country
   - First 3 chars + country  
   - Country only (fallback)

2. **Comparison Levels**: We use multiple similarity thresholds:
   - Exact match
   - Jaro-Winkler at 0.9, 0.85, 0.75
   - Jaccard at 0.9, 0.8, 0.7

3. **Country-Aware Matching**: Strong weighting for same-country matches (m_probability=0.9)

## Official Documentation Links

- **Main Spark Example**: https://moj-analytical-services.github.io/splink/demos/examples/spark/deduplicate_1k_synthetic.html
- **Spark Performance Guide**: https://moj-analytical-services.github.io/splink/user_guide/performance/spark_performance.html
- **Blocking Rules**: https://moj-analytical-services.github.io/splink/user_guide/blocking/blocking_rules.html
- **Comparison Library**: https://moj-analytical-services.github.io/splink/api_reference/comparisons/comparison_library.html

## Verification

To verify our implementation matches official patterns:

1. Check that similarity JAR is loaded (required for Jaro-Winkler, Jaccard)
2. Verify blocking rules use SQL conditions with `_l` and `_r` suffixes
3. Confirm Comparison objects are used (not raw dictionaries)
4. Ensure checkpoint directory is set for iterative algorithms

All of these are implemented in our `splink_company_linkage.py` script.

