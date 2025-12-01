# Splink Company Record Linkage Pipeline

A production-ready entity resolution pipeline for grouping similar companies based on normalized names and country codes. Optimized for datasets with 1-5 million records using DuckDB and Splink 4.x (no Spark required).

## Features

- **Efficient Blocking**: Multi-level blocking strategies to reduce comparisons from O(n²) to manageable sizes
- **Advanced Matching**: Uses exact match, Jaro-Winkler, and Jaccard similarity for company names
- **Country-Aware**: Strongly weights same-country matches while allowing cross-country matches
- **Scalable**: Handles 1-5M records efficiently with DuckDB (no Spark needed)
- **Simple Setup**: No Java, Spark, or complex dependencies required
- **Production-Ready**: Includes error handling, logging, and comprehensive configuration

## Installation

### Prerequisites

1. **Python 3.8+**
   - Verify: `python --version`

### Install Dependencies

```bash
pip install -r requirements.txt
```

That's it! No Java, Spark, or other complex setup needed. DuckDB is included with Splink.

## Usage

### Basic Usage

```bash
python splink_company_linkage.py \
    --input data/companies.csv \
    --output results/clustered_companies.csv \
    --threshold 0.5
```

### Command-Line Arguments

- `--input` (required): Path to input CSV file
- `--output` (required): Path to output CSV file
- `--threshold` (optional): Match probability threshold (0-1, default: 0.5)
- `--training-sample` (optional): Sample size for EM training (default: 100000, use 0 for full dataset)
- `--unique-id-col` (optional): Name of unique ID column (if not provided, will generate)

### Example with Custom Parameters

```bash
python splink_company_linkage.py \
    --input data/large_companies.csv \
    --output results/clusters.csv \
    --threshold 0.6 \
    --training-sample 200000 \
    --unique-id-col company_id
```

### Input CSV Format

Your input CSV must contain at least these columns:

- `tv_name_regulatized`: Normalized company name (string)
- `tv_country_code`: 2-letter country code (string)

Optional:
- Any unique identifier column (specify with `--unique-id-col`)

Example:
```csv
tv_name_regulatized,tv_country_code
microsoft corporation,US
microsoft corp,US
apple inc,US
apple incorporated,US
```

### Output Format

The output CSV contains:

- `cluster_id`: Unique identifier for each cluster
- `unique_id`: Original unique identifier for each record
- `tv_name_regulatized`: Company name
- `tv_country_code`: Country code
- `representative_name`: Representative name for the cluster
- `representative_country`: Representative country for the cluster
- `is_representative`: Boolean indicating if this is the representative record

## Tuning Guide

### Match Probability Threshold

The `--threshold` parameter controls how similar records must be to be considered a match:

- **Lower threshold (0.3-0.4)**: Higher recall, more matches, but may include false positives
- **Medium threshold (0.5-0.6)**: Balanced recall and precision (recommended starting point)
- **Higher threshold (0.7-0.9)**: Higher precision, fewer matches, but may miss some true matches

**Recommendation**: Start with 0.5 and adjust based on your data quality and requirements.

### Training Sample Size

For large datasets (>500K records), use sampling for faster training:

- **<100K records**: Use full dataset (`--training-sample 0`)
- **100K-1M records**: Use 100K-200K samples
- **>1M records**: Use 200K-500K samples

Larger samples improve model accuracy but increase training time.

### Blocking Rules

The pipeline uses three blocking rules (in order of priority):

1. **First 2 characters of name + country**: Most restrictive, fastest
2. **First 3 characters of name + country**: Better recall
3. **Country only**: Fallback for edge cases

These are configured in `splink_company_linkage.py` in the `create_splink_settings()` function.

### Comparison Levels

Company name matching uses multiple similarity metrics:

- **Exact match**: Perfect string match
- **Jaro-Winkler ≥0.9**: Very similar names
- **Jaro-Winkler ≥0.85**: Similar names
- **Jaro-Winkler ≥0.75**: Moderately similar names
- **Jaccard ≥0.9, 0.8, 0.7**: Token-based similarity

Country matching:
- **Exact match**: Same country (high weight: 0.9)
- **Different country**: Different country (low weight: 0.1)

## Performance Tips

### For Large Datasets (1-5M records)

1. **Increase Spark Memory**:
   Edit `create_spark_session()` in `splink_company_linkage.py`:
   ```python
   driver_memory="16g"
   executor_memory="16g"
   ```

2. **Adjust Partitions**:
   ```python
   .config("spark.sql.shuffle.partitions", "400")
   .config("spark.default.parallelism", "400")
   ```

3. **Use Sampling for Training**:
   ```bash
   --training-sample 200000
   ```

4. **Monitor Resource Usage**:
   - Watch CPU and memory usage
   - Adjust Spark configs if you see OOM errors

### Expected Performance

- **100K records**: ~5-10 minutes
- **500K records**: ~20-40 minutes
- **1M records**: ~40-80 minutes
- **5M records**: ~3-6 hours

*Times vary based on hardware, data characteristics, and Spark configuration.*

## Troubleshooting

### Out of Memory Errors

**Symptom**: `java.lang.OutOfMemoryError`

**Solutions**:
1. Increase Spark memory in `create_spark_session()`
2. Reduce training sample size
3. Increase blocking restrictiveness (use first 3 chars instead of 2)

### Slow Performance

**Solutions**:
1. Check blocking effectiveness - should reduce comparisons to <1% of cartesian
2. Increase Spark partitions
3. Use smaller training sample
4. Ensure Spark is using all available cores

### Low Match Rate

**Solutions**:
1. Lower the match probability threshold
2. Adjust blocking rules to be less restrictive
3. Check data quality (normalization, nulls)
4. Review comparison level thresholds

### High False Positive Rate

**Solutions**:
1. Increase the match probability threshold
2. Make blocking rules more restrictive
3. Adjust comparison level thresholds (increase Jaro-Winkler/Jaccard thresholds)

## Configuration Files

### `splink_settings.json`

This file contains the Splink configuration template. The actual settings are generated programmatically in `create_splink_settings()` to ensure proper integration with the SparkLinker API.

### Modifying Settings

To customize matching behavior, edit the `create_splink_settings()` function in `splink_company_linkage.py`:

- **Blocking rules**: Modify the `blocking_rules` list
- **Comparison levels**: Adjust thresholds in `ComparisonLevel` objects
- **M-probabilities**: Tune the `m_probability` values (probability of match given a match)

## Advanced Usage

### Programmatic Usage

```python
from splink_company_linkage import run_pipeline

result_df = run_pipeline(
    input_csv="data/companies.csv",
    output_csv="results/clusters.csv",
    match_probability_threshold=0.6,
    training_sample_size=150000,
    unique_id_col="company_id"
)

# result_df is a Spark DataFrame - you can continue processing
result_df.show(10)
```

### Custom Spark Configuration

Modify `create_spark_session()` to add custom Spark configurations:

```python
spark = SparkSession.builder \
    .appName("CustomApp") \
    .config("spark.custom.setting", "value") \
    # ... other configs
    .getOrCreate()
```

## Output Analysis

After running the pipeline, analyze the results:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg

spark = SparkSession.builder.appName("Analysis").getOrCreate()
df = spark.read.csv("results/clusters.csv", header=True, inferSchema=True)

# Cluster statistics
df.groupBy("cluster_id").agg(
    count("*").alias("cluster_size")
).agg(
    avg("cluster_size").alias("avg_cluster_size")
).show()
```

## References

- [Splink Documentation](https://moj-analytical-services.github.io/splink/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Entity Resolution Best Practices](https://en.wikipedia.org/wiki/Record_linkage)

## License

This pipeline is provided as-is for entity resolution tasks.

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review Splink documentation
3. Verify your data format matches requirements
4. Check Spark/Java installation

