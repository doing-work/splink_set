"""
Diagnostic script to check Splink Spark installation.
Run this to understand what's installed and what's missing.
"""

import sys
import importlib

print("=" * 70)
print("SPLINK INSTALLATION DIAGNOSTIC")
print("=" * 70)

# 1. Check Python version
print(f"\n1. Python version: {sys.version}")

# 2. Check if splink base package is installed
print("\n2. Checking Splink base package...")
try:
    import splink
    print(f"   ✓ splink imported successfully")
    print(f"   Version: {getattr(splink, '__version__', 'unknown')}")
    
    # Check what's in splink
    print(f"   Available attributes: {[x for x in dir(splink) if not x.startswith('_')][:10]}...")
except ImportError as e:
    print(f"   ✗ splink not found: {e}")
    print("   → Run: pip install 'splink[spark]'")
    sys.exit(1)

# 3. Check for spark module
print("\n3. Checking for splink.spark module...")
try:
    import splink.spark
    print("   ✓ splink.spark module found")
    print(f"   Available in splink.spark: {[x for x in dir(splink.spark) if not x.startswith('_')]}")
except ImportError as e:
    print(f"   ✗ splink.spark not found: {e}")
    print("   → This means splink[spark] extra is not installed")

# 4. Check for backends
print("\n4. Checking for splink.backends...")
try:
    import splink.backends
    print("   ✓ splink.backends found")
    print(f"   Available backends: {[x for x in dir(splink.backends) if not x.startswith('_')]}")
    
    # Check for spark in backends
    try:
        import splink.backends.spark
        print("   ✓ splink.backends.spark found")
        print(f"   Available: {[x for x in dir(splink.backends.spark) if not x.startswith('_')]}")
    except ImportError:
        print("   ✗ splink.backends.spark not found")
except ImportError as e:
    print(f"   ✗ splink.backends not found: {e}")

# 5. Try different import paths
print("\n5. Trying different import paths for SparkLinker...")
import_paths = [
    ("splink.spark.spark_linker", "SparkLinker"),
    ("splink.spark.linker", "SparkLinker"),
    ("splink.spark", "SparkLinker"),
    ("splink.backends.spark.spark_linker", "SparkLinker"),
    ("splink.backends.spark", "SparkLinker"),
]

found = False
for module_path, class_name in import_paths:
    try:
        module = __import__(module_path, fromlist=[class_name])
        if hasattr(module, class_name):
            print(f"   ✓ Found: from {module_path} import {class_name}")
            found = True
            break
        else:
            print(f"   - Module {module_path} exists but no {class_name}")
    except (ImportError, ModuleNotFoundError) as e:
        print(f"   ✗ {module_path}: {e}")

# 6. Check pip installation
print("\n6. Checking pip installation...")
try:
    import subprocess
    result = subprocess.run(
        [sys.executable, "-m", "pip", "show", "splink"],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print("   ✓ splink package info:")
        for line in result.stdout.split('\n')[:10]:
            if line.strip():
                print(f"     {line}")
    else:
        print("   ✗ Could not get package info")
except Exception as e:
    print(f"   ✗ Error checking pip: {e}")

# 7. Check if spark extra is installed
print("\n7. Checking for spark dependencies...")
try:
    import pyspark
    print(f"   ✓ pyspark installed: {pyspark.__version__}")
except ImportError:
    print("   ✗ pyspark not installed")

# Summary
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)

if found:
    print("✓ SparkLinker can be imported!")
    print("\nNext steps:")
    print("  1. Note which import path worked above")
    print("  2. Update your script to use that import")
    print("  3. Restart runtime and try again")
else:
    print("✗ SparkLinker cannot be imported")
    print("\nSOLUTION:")
    print("  1. Run: !pip uninstall splink -y")
    print("  2. Run: !pip install 'splink[spark]' --no-cache-dir")
    print("  3. RESTART RUNTIME (Runtime → Restart runtime)")
    print("  4. Run this diagnostic again to verify")

print("=" * 70)

