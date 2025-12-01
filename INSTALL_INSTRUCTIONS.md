# Installation Instructions for Splink Spark Backend

## The Problem
You're getting `ModuleNotFoundError: No module named 'splink.spark'` because the Splink Spark backend is not installed.

## Solution

### Step 1: Install Splink with Spark Support

Run this command in your Colab/Jupyter cell or terminal:

```bash
pip install 'splink[spark]'
```

**Important**: Use quotes around `splink[spark]` because the square brackets have special meaning in shell.

### Step 2: Restart Runtime (if in Colab)

After installation, restart your Colab runtime:
- Go to **Runtime → Restart runtime**

This is necessary because Python caches imported modules.

### Step 3: Verify Installation

Run this in a new cell to verify:

```python
try:
    from splink.spark.spark_linker import SparkLinker
    print("✓ Splink Spark backend installed successfully!")
except ImportError as e:
    print(f"✗ Installation failed: {e}")
    print("Please run: pip install 'splink[spark]'")
```

### Step 4: Install Other Dependencies

```bash
pip install pyspark>=3.5.0 pandas>=2.0.0 numpy>=1.24.0
```

Or install everything at once:

```bash
pip install 'splink[spark]' pyspark>=3.5.0 pandas>=2.0.0 numpy>=1.24.0
```

## Alternative: Install from requirements.txt

If you have the requirements.txt file:

```bash
pip install -r requirements.txt
```

## Troubleshooting

### If installation fails:

1. **Upgrade pip first:**
   ```bash
   pip install --upgrade pip
   ```

2. **Uninstall and reinstall:**
   ```bash
   pip uninstall splink -y
   pip install 'splink[spark]'
   ```

3. **Check Python version:**
   ```python
   import sys
   print(sys.version)
   ```
   Splink requires Python 3.8+

### If import still fails after installation:

1. Make sure you restarted the runtime/kernel
2. Check if splink is installed:
   ```python
   import splink
   print(splink.__version__)
   ```

3. Check if spark backend is available:
   ```python
   import splink.spark
   ```

## Note for Colab Users

Colab environments reset when you disconnect. You'll need to reinstall `splink[spark]` each time you start a new session, or add this to the first cell:

```python
!pip install 'splink[spark]' pyspark pandas numpy
```

