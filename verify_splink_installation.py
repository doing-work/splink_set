"""
Verification script for Splink Spark installation.
Run this AFTER restarting your runtime.
"""

import sys

print("=" * 60)
print("Verifying Splink Spark Installation")
print("=" * 60)

# Check if splink is installed
try:
    import splink
    print(f"✓ Splink base package found: version {splink.__version__}")
except ImportError as e:
    print(f"✗ Splink base package not found: {e}")
    sys.exit(1)

# Check module structure
print("\nChecking Splink module structure...")
try:
    import splink.spark
    print("✓ splink.spark module found")
except ImportError:
    print("✗ splink.spark module not found")
    print("\nTrying to find alternative import paths...")
    
    # Check what's actually available
    import splink
    print(f"\nAvailable in splink: {dir(splink)}")
    
    # Check backends
    try:
        import splink.backends
        print(f"Available backends: {dir(splink.backends)}")
    except:
        pass
    
    sys.exit(1)

# Try different import paths
print("\nTrying import paths...")

import_paths = [
    ("splink.spark.spark_linker", "SparkLinker"),
    ("splink.spark.linker", "SparkLinker"),
    ("splink.spark", "SparkLinker"),
]

success = False
for module_path, class_name in import_paths:
    try:
        module = __import__(module_path, fromlist=[class_name])
        linker_class = getattr(module, class_name)
        print(f"✓ Successfully imported from: {module_path}")
        print(f"  Class: {linker_class}")
        success = True
        break
    except (ImportError, AttributeError) as e:
        print(f"✗ Failed to import from {module_path}: {e}")

if success:
    print("\n" + "=" * 60)
    print("✓ Splink Spark installation verified successfully!")
    print("=" * 60)
    print("\nYou can now use splink_company_linkage.py")
else:
    print("\n" + "=" * 60)
    print("✗ Import failed. Please try:")
    print("=" * 60)
    print("1. Restart your runtime/kernel")
    print("2. Run: pip install 'splink[spark]' --force-reinstall")
    print("3. Restart again")
    sys.exit(1)

