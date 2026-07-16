"""
Root conftest.py – adds the project root to sys.path so imports work without install.
"""
import sys
import os

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(__file__))
