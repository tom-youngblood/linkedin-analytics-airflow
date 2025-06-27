"""
Utility module for accessing environment variables in both Airflow and non-Airflow contexts.
This allows scripts to work both when run directly and when run as Airflow tasks.
"""

import os
from typing import Optional

def get_env_var(var_name: str, default: Optional[str] = None) -> str:
    """
    Get environment variable from either Airflow Variables or regular environment variables.
    
    Args:
        var_name: Name of the environment variable
        default: Default value if variable is not found
        
    Returns:
        The environment variable value
        
    Raises:
        ValueError: If variable is required but not found
    """
    # First try to get from Airflow Variables (if running in Airflow context)
    try:
        from airflow.models import Variable
        value = Variable.get(var_name, default_var=default)
        if value is not None:
            return value
    except (ImportError, Exception):
        # Not running in Airflow context or variable not found
        pass
    
    # Fall back to regular environment variables
    value = os.getenv(var_name, default)
    
    if value is None:
        raise ValueError(f"Environment variable {var_name} is required but not found")
    
    return value

def get_required_env_var(var_name: str) -> str:
    """
    Get a required environment variable.
    
    Args:
        var_name: Name of the environment variable
        
    Returns:
        The environment variable value
        
    Raises:
        ValueError: If variable is not found
    """
    return get_env_var(var_name)

def get_optional_env_var(var_name: str, default: str = None) -> Optional[str]:
    """
    Get an optional environment variable.
    
    Args:
        var_name: Name of the environment variable
        default: Default value if variable is not found
        
    Returns:
        The environment variable value or default
    """
    try:
        return get_env_var(var_name, default)
    except ValueError:
        return default 