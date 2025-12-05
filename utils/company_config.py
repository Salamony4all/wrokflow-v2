"""
Company Configuration Manager
Loads company branding settings from environment variables or config file
"""
import os
import json
import logging

logger = logging.getLogger(__name__)

class CompanyConfig:
    """Manages company branding configuration"""
    
    _config = None
    _config_file = 'config/company_config.json'
    
    @classmethod
    def _load_config(cls):
        """Load configuration from environment variables or config file"""
        if cls._config is not None:
            return cls._config
        
        config = {}
        
        # First, try environment variables
        config['logo_path'] = os.environ.get('COMPANY_LOGO_PATH', '')
        config['name'] = os.environ.get('COMPANY_NAME', '')
        config['website'] = os.environ.get('COMPANY_WEBSITE', '')
        config['email'] = os.environ.get('COMPANY_EMAIL', '')
        config['phone'] = os.environ.get('COMPANY_PHONE', '')
        config['address'] = os.environ.get('COMPANY_ADDRESS', '')
        
        # If config file exists, load it and use as fallback for empty env vars
        if os.path.exists(cls._config_file):
            try:
                with open(cls._config_file, 'r', encoding='utf-8') as f:
                    file_config = json.load(f)
                    
                # Only use file config if env var is empty
                if not config['logo_path']:
                    config['logo_path'] = file_config.get('logo_path', '')
                if not config['name']:
                    config['name'] = file_config.get('name', '')
                if not config['website']:
                    config['website'] = file_config.get('website', '')
                if not config['email']:
                    config['email'] = file_config.get('email', '')
                if not config['phone']:
                    config['phone'] = file_config.get('phone', '')
                if not config['address']:
                    config['address'] = file_config.get('address', '')
            except Exception as e:
                logger.warning(f"Failed to load config file: {e}")
        
        cls._config = config
        return config
    
    @classmethod
    def get_logo_path(cls):
        """Get company logo path - ONLY returns logo if explicitly configured"""
        config = cls._load_config()
        logo_path = config.get('logo_path', '').strip()
        
        # Only return logo if explicitly configured (no fallbacks to old logos)
        if not logo_path:
            return None
        
        # Check if it's an absolute path
        if os.path.isabs(logo_path) and os.path.exists(logo_path):
            return logo_path
        
        # Check if it's relative to static/images
        static_path = os.path.join('static', 'images', logo_path)
        if os.path.exists(static_path):
            return static_path
        
        # Check if logo_path is already a full path from static/images
        if logo_path.startswith('static/images/'):
            if os.path.exists(logo_path):
                return logo_path
        
        # Try direct path
        if os.path.exists(logo_path):
            return logo_path
        
        # If configured but not found, return None (don't fallback to old logos)
        return None
    
    @classmethod
    def get_company_name(cls):
        """Get company name - returns empty string if not configured"""
        config = cls._load_config()
        name = config.get('name', '').strip()
        return name if name else ''
    
    @classmethod
    def get_company_website(cls):
        """Get company website URL"""
        config = cls._load_config()
        return config.get('website', '')
    
    @classmethod
    def get_company_email(cls):
        """Get company email"""
        config = cls._load_config()
        return config.get('email', '')
    
    @classmethod
    def get_company_phone(cls):
        """Get company phone"""
        config = cls._load_config()
        return config.get('phone', '')
    
    @classmethod
    def get_company_address(cls):
        """Get company address"""
        config = cls._load_config()
        return config.get('address', '')
    
    @classmethod
    def get_all_config(cls):
        """Get all configuration as dictionary"""
        config = cls._load_config()
        return {
            'logo_path': config.get('logo_path', ''),
            'name': config.get('name', ''),
            'website': config.get('website', ''),
            'email': config.get('email', ''),
            'phone': config.get('phone', ''),
            'address': config.get('address', '')
        }
    
    @classmethod
    def save_config(cls, config_data):
        """Save configuration to JSON file"""
        try:
            # Ensure config directory exists
            os.makedirs('config', exist_ok=True)
            
            # Save to file
            with open(cls._config_file, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
            
            # Reload config
            cls._config = None
            cls._load_config()
            
            logger.info(f"Company configuration saved to {cls._config_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
            return False
    
    @classmethod
    def reload_config(cls):
        """Force reload configuration (useful after updates)"""
        cls._config = None
        return cls._load_config()

