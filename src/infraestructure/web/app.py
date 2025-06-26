"""
Punto de entrada principal para la aplicación PLANTAS API.
"""
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.infraestructure.database.config import settings
from src.infraestructure.web.controllers.agricultor_controller import agricultor_controller
from src.domain.exceptions.domain_exceptions import AgricultorNotFoundException, DomainException

# Crear aplicación FastAPI
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="API para el sistema de trazabilidad agrícola PLANTAS",
    openapi_url="/api/openapi.json",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=settings.allow_credentials,
    allow_methods=settings.allow_methods,
    allow_headers=settings.allow_headers,
)

# Registrar routers
app.include_router(agricultor_controller, prefix=settings.api_v1_prefix)

# Manejador de excepciones global
@app.exception_handler(DomainException)
async def domain_exception_handler(request, exc):
    """Manejador para excepciones de dominio."""
    status_code = 400
    if isinstance(exc, AgricultorNotFoundException):
        status_code = 404
    
    return JSONResponse(
        status_code=status_code,
        content={"error": exc.__class__.__name__, "detail": str(exc)},
    )

# Endpoint raíz
@app.get("/")
async def root():
    """Información sobre la API."""
    return {
        "app": settings.app_name,
        "version": settings.app_version,
        "docs": "/api/docs"
    }

