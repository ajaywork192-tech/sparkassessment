#!/usr/bin/env python3

try:
    import pytest
    import ipytest
except Exception:
    pytest = None
    ipytest = None


def main():
    spark = None
    try:
        # Placeholder for main logic — avoid using undefined variables
        print("Running main")
    finally:
        if spark:
            print(">>> Stopping Spark session...")
            spark.stop()


if __name__ == "__main__":
    main()
