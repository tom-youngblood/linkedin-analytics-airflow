"""
Basic tests for the LinkedIn Lead Generation Pipeline.
Add more comprehensive tests as needed.
"""

import pytest
import sys
import os

# Add the include directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'include'))

def test_imports():
    """Test that all modules can be imported successfully."""
    try:
        import gs_sql
        import enrich_posts
        import scrape
        import sql_hs
        import airflow_utils
        import utils
        assert True
    except ImportError as e:
        pytest.fail(f"Failed to import module: {e}")

def test_airflow_utils():
    """Test the airflow_utils module basic functionality."""
    import airflow_utils
    
    # Test that the module has the expected functions
    assert hasattr(airflow_utils, 'get_env_var')
    assert hasattr(airflow_utils, 'get_required_env_var')
    assert hasattr(airflow_utils, 'get_optional_env_var')

def test_dag_structure():
    """Test that DAG files exist and are valid Python."""
    dag_dir = os.path.join(os.path.dirname(__file__), '..', 'dags')
    dag_files = [f for f in os.listdir(dag_dir) if f.endswith('.py') and f != '__init__.py']
    
    assert len(dag_files) > 0, "No DAG files found"
    
    # Test that pipeline_dag.py exists
    assert 'pipeline_dag.py' in dag_files, "pipeline_dag.py not found"

def test_requirements():
    """Test that requirements.txt exists and is readable."""
    requirements_file = os.path.join(os.path.dirname(__file__), '..', 'requirements.txt')
    assert os.path.exists(requirements_file), "requirements.txt not found"
    
    with open(requirements_file, 'r') as f:
        requirements = f.read()
        assert len(requirements) > 0, "requirements.txt is empty"

def test_project_structure():
    """Test that essential project files exist."""
    project_root = os.path.join(os.path.dirname(__file__), '..')
    
    essential_files = [
        'requirements.txt',
        'Dockerfile',
        'README.md',
        'dags/pipeline_dag.py',
        'include/airflow_utils.py',
        'include/utils.py'
    ]
    
    for file_path in essential_files:
        full_path = os.path.join(project_root, file_path)
        assert os.path.exists(full_path), f"Essential file missing: {file_path}" 