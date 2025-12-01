"""
Quick installation script for Splink with Spark backend.
Run this before using splink_company_linkage.py
"""

import subprocess
import sys

def install_splink_spark():
    """Install Splink with Spark backend."""
    print("Installing Splink with Spark backend...")
    print("This may take a few minutes...\n")
    
    try:
        # Install splink[spark]
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            "splink[spark]", "pyspark>=3.5.0", "pandas>=2.0.0", "numpy>=1.24.0"
        ])
        print("\n✓ Installation successful!")
        
        # Verify installation
        print("\nVerifying installation...")
        try:
            from splink.spark.spark_linker import SparkLinker
            print("✓ Splink Spark backend imported successfully!")
            return True
        except ImportError as e:
            print(f"✗ Import failed: {e}")
            print("\n⚠️  Please RESTART your runtime/kernel and try again.")
            return False
            
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Installation failed: {e}")
        print("\nTry running manually:")
        print("  pip install 'splink[spark]' pyspark pandas numpy")
        return False

if __name__ == "__main__":
    success = install_splink_spark()
    if success:
        print("\n" + "="*60)
        print("Installation complete! You can now use splink_company_linkage.py")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("Installation had issues. Please check the error messages above.")
        print("="*60)
        sys.exit(1)

