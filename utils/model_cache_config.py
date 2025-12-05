"""
Model Cache Configuration for Deployment
Handles model cache directory setup for different deployment environments
"""

import os
import logging

logger = logging.getLogger(__name__)

def setup_model_cache():
    """
    Configure model cache directories based on deployment environment
    
    Priority:
    1. App models directory (if models downloaded to app/)
    2. Railway Volume (if mounted)
    3. Docker volume (if set)
    4. Environment variables (if set)
    5. Local user cache (development)
    
    Returns:
        str: Base directory for model cache
    """
    # Check for app models directory (models downloaded to app directory)
    app_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    app_models_dir = os.path.join(app_dir, 'models')
    
    # Check for Railway Volume
    railway_volume = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', '/data')
    
    # Check for Docker volume or custom cache directory
    docker_volume = os.environ.get('MODEL_CACHE_DIR', None)
    
    # Check for explicit environment variables
    explicit_cache = os.environ.get('MODEL_CACHE_BASE', None)
    
    # Determine cache base directory
    if explicit_cache and os.path.exists(explicit_cache):
        base_dir = explicit_cache
        logger.info(f"Using explicit model cache: {base_dir}")
    elif os.path.exists(app_models_dir):
        # App models directory exists - use it (models included in build)
        base_dir = app_models_dir
        logger.info(f"Using app models directory: {base_dir}")
    elif os.path.exists(railway_volume):
        # Railway Volume exists - use it for models
        base_dir = os.path.join(railway_volume, 'models')
        logger.info(f"Using Railway Volume for model cache: {base_dir}")
    elif docker_volume and os.path.exists(docker_volume):
        base_dir = docker_volume
        logger.info(f"Using Docker volume for model cache: {base_dir}")
    else:
        # Local development - use user cache
        base_dir = os.path.expanduser('~/.cache')
        logger.info(f"Using local user cache: {base_dir}")
    
    # Set up cache directories
    # If using app models directory, use it directly
    if base_dir.endswith('models'):
        cache_dirs = {
            'HF_HOME': os.path.join(base_dir, 'huggingface'),
            'UNSTRUCTURED_CACHE_DIR': os.path.join(base_dir, 'unstructured'),
            'LAYOUTPARSER_CACHE_DIR': os.path.join(base_dir, 'layoutparser')
        }
    else:
        cache_dirs = {
            'HF_HOME': os.path.join(base_dir, 'huggingface', 'hub'),
            'UNSTRUCTURED_CACHE_DIR': os.path.join(base_dir, 'unstructured'),
            'LAYOUTPARSER_CACHE_DIR': os.path.join(base_dir, 'layoutparser')
        }
    
    # Set environment variables (if not already set)
    for env_var, cache_path in cache_dirs.items():
        if env_var not in os.environ:
            os.environ[env_var] = cache_path
        else:
            # Use existing environment variable
            cache_path = os.environ[env_var]
            cache_dirs[env_var] = cache_path
    
    # Create directories
    for env_var, cache_path in cache_dirs.items():
        try:
            os.makedirs(cache_path, exist_ok=True)
            logger.debug(f"Created/verified cache directory: {cache_path} ({env_var})")
        except Exception as e:
            logger.warning(f"Could not create cache directory {cache_path}: {e}")
    
    # Log final configuration
    logger.info("Model cache configuration:")
    logger.info(f"  Hugging Face: {cache_dirs['HF_HOME']}")
    logger.info(f"  Unstructured.io: {cache_dirs['UNSTRUCTURED_CACHE_DIR']}")
    logger.info(f"  LayoutParser: {cache_dirs['LAYOUTPARSER_CACHE_DIR']}")
    
    return base_dir

def get_cache_info():
    """Get information about current cache configuration"""
    return {
        'hf_home': os.environ.get('HF_HOME', 'Not set'),
        'unstructured_cache': os.environ.get('UNSTRUCTURED_CACHE_DIR', 'Not set'),
        'layoutparser_cache': os.environ.get('LAYOUTPARSER_CACHE_DIR', 'Not set'),
        'railway_volume': os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', 'Not set'),
        'model_cache_dir': os.environ.get('MODEL_CACHE_DIR', 'Not set')
    }

