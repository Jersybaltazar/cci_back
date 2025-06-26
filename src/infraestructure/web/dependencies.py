import aiomysql
import logging
from fastapi import Depends

from src.domain.exceptions.domain_exceptions import DatabaseConnectionException
from src.domain.repositories.agricultor_repository import AgricultorRepository
from src.domain.services.agricultor_service import AgricultorService
from src.infraestructure.database.config import database_settings
from src.infraestructure.database.repositories.mysql_agricultor_repository import MySQLAgricultorRepository
# Configuración del logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
# Singleton para el pool de conexiones
_connection_pool = None

async def get_db_pool():
    """Obtiene el pool de conexiones a la base de datos."""
    global _connection_pool
    if _connection_pool is None:
        try:
            _connection_pool = await aiomysql.create_pool(
                host=database_settings.host,
                user=database_settings.user,
                password=database_settings.password,
                db=database_settings.database,
                port=database_settings.port,
                minsize=database_settings.pool_size,
                maxsize=database_settings.max_overflow,
                charset=database_settings.charset,
                autocommit=False,
                echo=database_settings.echo_sql
        )
            logger.info("Pool de conexiones creado exitosamente")
        except Exception as e:
            logger.error(f"Error creando pool de conexiones: {e}")
            raise DatabaseConnectionException(f"No se pudo conectar a la base de datos: {e}")
    return _connection_pool

async def get_agricultor_repository() -> AgricultorRepository:
    """Inyección de dependencia para el repositorio de agricultores."""
    pool = await get_db_pool()
    return MySQLAgricultorRepository(pool)

async def get_agricultor_service() -> AgricultorService:
    """Inyección de dependencia para el servicio de agricultores."""
    repository = await get_agricultor_repository()
    return AgricultorService(repository)

# Función para cerrar el pool al finalizar la aplicación
async def close_db_pool():
    """Cierra el pool de conexiones."""
    global _connection_pool
    if _connection_pool:
        _connection_pool.close()
        await _connection_pool.wait_closed()
        _connection_pool = None
        logger.info("Pool de conexiones cerrado")