"""
Dependencias para la inyección de dependencias de FastAPI.
"""
import aiomysql
import asyncio
import logging
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import Depends

from src.domain.repositories.agricultor_repository import AgricultorRepository
from src.domain.services.agricultor_service import AgricultorService
from src.infraestructure.database.repositories.mysql_agricultor_repository import MySQLAgricultorRepository
from src.infraestructure.database.config import database_settings
from src.domain.exceptions.domain_exceptions import DatabaseConnectionException

logger = logging.getLogger(__name__)

# Pool de conexiones global
_connection_pool: Optional[aiomysql.Pool] = None
_pool_lock = asyncio.Lock()

async def get_db_pool() -> aiomysql.Pool:
    """Obtiene el pool de conexiones a la base de datos con manejo seguro."""
    global _connection_pool
    
    async with _pool_lock:
        if _connection_pool is None or _connection_pool.closed:
            try:
                logger.info("Creando nuevo pool de conexiones...")
                _connection_pool = await aiomysql.create_pool(
                    host=database_settings.host,
                    user=database_settings.user,
                    password=database_settings.password,
                    db=database_settings.database,
                    port=database_settings.port,
                    minsize=2,  # Reducido para desarrollo
                    maxsize=5,  # Reducido para desarrollo
                    pool_recycle=3600,  # Reciclar conexiones cada hora
                    charset=database_settings.charset,
                    autocommit=False,
                    echo=database_settings.echo_sql
                )
                logger.info("Pool de conexiones creado exitosamente")
            except Exception as e:
                logger.error(f"Error creando pool de conexiones: {e}")
                raise DatabaseConnectionException(f"No se pudo conectar a la base de datos: {e}")
    
    return _connection_pool

@asynccontextmanager
async def get_db_connection():
    """Context manager para obtener una conexión del pool de forma segura."""
    pool = await get_db_pool()
    connection = None
    
    try:
        connection = await pool.acquire()
        yield connection
    except Exception as e:
        logger.error(f"Error en conexión de base de datos: {e}")
        raise DatabaseConnectionException(f"Error de conexión: {e}")
    finally:
        if connection and not connection.closed:
            try:
                await pool.release(connection)
            except Exception as e:
                logger.warning(f"Error liberando conexión: {e}")

async def get_agricultor_repository() -> AgricultorRepository:
    """Inyección de dependencia para el repositorio de agricultores."""
    pool = await get_db_pool()
    return MySQLAgricultorRepository(pool)

async def get_agricultor_service(
    repository: AgricultorRepository = Depends(get_agricultor_repository)
) -> AgricultorService:
    """Inyección de dependencia para el servicio de agricultores."""
    return AgricultorService(repository)

async def close_db_pool():
    """Cierra el pool de conexiones de forma segura."""
    global _connection_pool
    
    if _connection_pool and not _connection_pool.closed:
        try:
            logger.info("Cerrando pool de conexiones...")
            _connection_pool.close()
            await _connection_pool.wait_closed()
            logger.info("Pool de conexiones cerrado correctamente")
        except Exception as e:
            logger.error(f"Error cerrando pool: {e}")
        finally:
            _connection_pool = None

async def health_check_db() -> bool:
    """Verifica la salud de la conexión a la base de datos."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                result = await cursor.fetchone()
                return result[0] == 1
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False