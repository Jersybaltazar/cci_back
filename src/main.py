import uvicorn
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Importar configuraciones
from src.infraestructure.database.config import settings
from src.infraestructure.web.controllers.agricultor_controller import agricultor_controller
from src.infraestructure.web.dependencies import close_db_pool

# Configurar logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Maneja el ciclo de vida de la aplicación."""
    # Startup
    logger.info("Iniciando aplicación...")
    yield
    # Shutdown
    logger.info("Cerrando aplicación...")
    await close_db_pool()

app = FastAPI(
    title=getattr(settings, 'app_name', 'Plantas API'),
    version=getattr(settings, 'app_version', '1.0.0'),
    debug=getattr(settings, 'debug', False),
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=getattr(settings, 'allowed_origins', ["*"]),
    allow_credentials=getattr(settings, 'allow_credentials', True),
    allow_methods=getattr(settings, 'allow_methods', ["*"]),
    allow_headers=getattr(settings, 'allow_headers', ["*"]),
)

# Routes
app.include_router(
    agricultor_controller,
    prefix=getattr(settings, 'api_v1_prefix', '/api/v1'),
    tags=["agricultores"]
)

@app.get("/")
async def root():
    return {
        "message": f"Bienvenido a {getattr(settings, 'app_name', 'Plantas API')}",
        "version": getattr(settings, 'app_version', '1.0.0'),
        "environment": getattr(settings, 'environment', 'development')
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "environment": getattr(settings, 'environment', 'development')
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=getattr(settings, 'debug', False)
    )