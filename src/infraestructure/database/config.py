import os
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import validator


class DatabaseSettings(BaseSettings):
    """Configuración de la base de datos."""
    host: str
    user: str  
    password: str  # Sin valor por defecto
    database: str
    port: int = 3306
    
    # URLs completas (se generan automáticamente)
    database_url: Optional[str] = None
    database_url_async: Optional[str] = None
    
    # Connection pool settings
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600
    
    # Database settings
    echo_sql: bool = False
    
    # MySQL specific settings
    charset: str = "utf8mb4"
    collation: str = "utf8mb4_unicode_ci"
    # Environment
    environment: str = "development"
    
    class Config:
        env_file = ".env"
        env_prefix = "DB_"
        case_sensitive = False
        extra = "allow"
    
    @validator("database_url", always=True)
    def build_database_url(cls, v, values):
        if v:
            return v
        return f"mysql://{values['user']}:{values['password']}@{values['host']}:{values['port']}/{values['database']}"
    
    @validator("database_url_async", always=True)
    def build_async_url(cls, v, values):
        if v:
            return v
        url = values.get('database_url') or f"mysql://{values['user']}:{values['password']}@{values['host']}:{values['port']}/{values['database']}"
        return url.replace("mysql://", "mysql+aiomysql://", 1)
    
    @validator("echo_sql", always=True)
    def set_echo_sql(cls, v, values):
        """Activa el echo SQL en desarrollo."""
        if "environment" in values and values["environment"] == "development":
            return True
        return v


class Settings(BaseSettings):
    """Configuración general de la aplicación."""
    
    # App settings
    app_name: str = "PLANTAS API"
    app_version: str = "1.0.0"
    debug: bool = False
    environment: str = "development"  # NUEVO CAMPO
    # API settings
    api_v1_prefix: str = "/api/v1"
    
    # Security
    secret_key: str 
    access_token_expire_minutes: int = 30
    
    # CORS
    allowed_origins: List[str] = []  
    allow_credentials: bool = False
    allow_methods: List[str] = ["GET", "POST", "PUT", "DELETE"]
    allow_headers: List[str] = ["Content-Type", "Authorization"]
    # Database
    db_settings: DatabaseSettings = None
    
    # Logging
    log_level: str = "INFO"
    
    # File storage
    uploads_directory: str = "uploads"
    max_upload_size_mb: int = 10
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "allow"
        
    def __init__(self, **data):
        # Inicializar db_settings si no se proporciona
        if 'db_settings' not in data or data['db_settings'] is None:
            data['db_settings'] = DatabaseSettings()
        super().__init__(**data)

    @validator("secret_key")
    def validate_secret_key(cls, v):
        """Valida que la clave secreta no sea la predeterminada en producción."""
        default_key = "tu-clave-secreta-muy-segura-aqui"
        if v == default_key and os.getenv("ENVIRONMENT") == "production":
            raise ValueError(
                "La clave secreta predeterminada no debe usarse en producción. "
                "Configura una clave secreta segura con la variable de entorno SECRET_KEY."
            )
        return v
    
    @validator("debug")
    def set_debug_by_environment(cls, v):
        """Desactiva el debug en producción."""
        if os.getenv("ENVIRONMENT") == "production":
            return False
        return v


# Instancia para uso en la aplicación
settings = Settings()
database_settings = settings.db_settings