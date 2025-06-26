"""
Implementación del repositorio de Agricultor con PostgreSQL.
"""
from typing import List, Optional
import asyncpg
from datetime import date

from src.domain.entities.agricultor import Agricultor
from src.domain.repositories.agricultor_repository import AgricultorRepository
from src.domain.exceptions.domain_exceptions import AgricultorNotFoundException, RepositoryException, DatabaseConnectionException


class PostgreSQLAgricultorRepository(AgricultorRepository):
    """Implementación del repositorio de Agricultor con PostgreSQL."""
    
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
    
    async def find_by_dni(self, dni: str) -> Optional[Agricultor]:
        """Busca un agricultor por su DNI."""
        try:
            async with self.connection_pool.acquire() as connection:
                query = """
                    SELECT * FROM agricultores 
                    WHERE dni = $1
                """
                row = await connection.fetchrow(query, dni)
                
                if row is None:
                    return None
                    
                return self._map_to_entity(row)
                
        except asyncpg.PostgresError as e:
            raise DatabaseConnectionException(f"Error al consultar agricultor: {str(e)}")
            
    async def save(self, agricultor: Agricultor) -> Agricultor:
        """Guarda un nuevo agricultor."""
        try:
            async with self.connection_pool.acquire() as connection:
                query = """
                    INSERT INTO agricultores (
                        dni, fecha_censo, apellidos, nombres, nombre_completo, sexo, edad,
                        esparrago, granada, maiz, palta, papa, pecano, vid,
                        dpto, provincia, distrito, centro_poblado,
                        senasa, sispa, codigo_autogene_sispa, regimen_tenencia_sispa, 
                        area_total_declarada, fecha_actualizacion_sispa,
                        toma, edad_cultivo, total_ha_sembrada, productividad_x_ha,
                        tipo_riego, nivel_alcance_venta, jornales_por_ha,
                        practica_economica_sost, porcentaje_prac_economica_sost
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                        $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
                        $27, $28, $29, $30, $31, $32, $33
                    )
                    RETURNING id
                """
                
                values = (
                    agricultor.dni, agricultor.fecha_censo, agricultor.apellidos, agricultor.nombres,
                    agricultor.nombre_completo, agricultor.sexo, agricultor.edad, agricultor.esparrago,
                    agricultor.granada, agricultor.maiz, agricultor.palta, agricultor.papa,
                    agricultor.pecano, agricultor.vid, agricultor.dpto, agricultor.provincia,
                    agricultor.distrito, agricultor.centro_poblado, agricultor.senasa, agricultor.sispa,
                    agricultor.codigo_autogene_sispa, agricultor.regimen_tenencia_sispa,
                    agricultor.area_total_declarada, agricultor.fecha_actualizacion_sispa,
                    agricultor.toma, agricultor.edad_cultivo, agricultor.total_ha_sembrada,
                    agricultor.productividad_x_ha, agricultor.tipo_riego, agricultor.nivel_alcance_venta,
                    agricultor.jornales_por_ha, agricultor.practica_economica_sost,
                    agricultor.porcentaje_prac_economica_sost
                )
                
                record = await connection.fetchrow(query, *values)
                
                # El agricultor ahora tiene un ID
                return agricultor
                
        except asyncpg.PostgresError as e:
            raise RepositoryException(f"Error al guardar agricultor: {str(e)}")
    
    async def update(self, agricultor: Agricultor) -> Agricultor:
        """Actualiza un agricultor existente."""
        try:
            async with self.connection_pool.acquire() as connection:
                # Verificar que exista
                exists = await self.exists_by_dni(agricultor.dni)
                if not exists:
                    raise AgricultorNotFoundException(agricultor.dni)
                
                query = """
                    UPDATE agricultores SET
                        fecha_censo = $2, apellidos = $3, nombres = $4, nombre_completo = $5,
                        sexo = $6, edad = $7, esparrago = $8, granada = $9, maiz = $10,
                        palta = $11, papa = $12, pecano = $13, vid = $14, dpto = $15,
                        provincia = $16, distrito = $17, centro_poblado = $18, senasa = $19,
                        sispa = $20, codigo_autogene_sispa = $21, regimen_tenencia_sispa = $22,
                        area_total_declarada = $23, fecha_actualizacion_sispa = $24, toma = $25,
                        edad_cultivo = $26, total_ha_sembrada = $27, productividad_x_ha = $28,
                        tipo_riego = $29, nivel_alcance_venta = $30, jornales_por_ha = $31,
                        practica_economica_sost = $32, porcentaje_prac_economica_sost = $33,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE dni = $1
                """
                
                values = (
                    agricultor.dni, agricultor.fecha_censo, agricultor.apellidos, agricultor.nombres,
                    agricultor.nombre_completo, agricultor.sexo, agricultor.edad, agricultor.esparrago,
                    agricultor.granada, agricultor.maiz, agricultor.palta, agricultor.papa,
                    agricultor.pecano, agricultor.vid, agricultor.dpto, agricultor.provincia,
                    agricultor.distrito, agricultor.centro_poblado, agricultor.senasa, agricultor.sispa,
                    agricultor.codigo_autogene_sispa, agricultor.regimen_tenencia_sispa,
                    agricultor.area_total_declarada, agricultor.fecha_actualizacion_sispa,
                    agricultor.toma, agricultor.edad_cultivo, agricultor.total_ha_sembrada,
                    agricultor.productividad_x_ha, agricultor.tipo_riego, agricultor.nivel_alcance_venta,
                    agricultor.jornales_por_ha, agricultor.practica_economica_sost,
                    agricultor.porcentaje_prac_economica_sost
                )
                
                await connection.execute(query, *values)
                return agricultor
                
        except asyncpg.PostgresError as e:
            raise RepositoryException(f"Error al actualizar agricultor: {str(e)}")
    
    async def delete_by_dni(self, dni: str) -> bool:
        """Elimina un agricultor por su DNI."""
        try:
            async with self.connection_pool.acquire() as connection:
                query = "DELETE FROM agricultores WHERE dni = $1"
                result = await connection.execute(query, dni)
                
                # Si contiene "DELETE 0", no se eliminó ningún registro
                return "DELETE 0" not in result
                
        except asyncpg.PostgresError as e:
            raise RepositoryException(f"Error al eliminar agricultor: {str(e)}")
    
    async def find_all(self, limit: int = 100, offset: int = 0) -> List[Agricultor]:
        """Obtiene una lista paginada de agricultores."""
        try:
            async with self.connection_pool.acquire() as connection:
                query = """
                    SELECT * FROM agricultores
                    ORDER BY apellidos, nombres
                    LIMIT $1 OFFSET $2
                """
                rows = await connection.fetch(query, limit, offset)
                return [self._map_to_entity(row) for row in rows]
                
        except asyncpg.PostgresError as e:
            raise DatabaseConnectionException(f"Error al consultar agricultores: {str(e)}")
    
    async def count_all(self) -> int:
        """Cuenta el número total de agricultores."""
        try:
            async with self.connection_pool.acquire() as connection:
                query = "SELECT COUNT(*) FROM agricultores"
                result = await connection.fetchval(query)
                return result
                
        except asyncpg.PostgresError as e:
            raise DatabaseConnectionException(f"Error al contar agricultores: {str(e)}")
    
    async def exists_by_dni(self, dni: str) -> bool:
        """Verifica si existe un agricultor con el DNI dado."""
        try:
            async with self.connection_pool.acquire() as connection:
                query = "SELECT EXISTS(SELECT 1 FROM agricultores WHERE dni = $1)"
                return await connection.fetchval(query, dni)
                
        except asyncpg.PostgresError as e:
            raise DatabaseConnectionException(f"Error al verificar existencia: {str(e)}")
    
    def _map_to_entity(self, row) -> Agricultor:
        """Mapea una fila de la base de datos a una entidad Agricultor."""
        return Agricultor(
            dni=row['dni'],
            fecha_censo=row['fecha_censo'],
            apellidos=row['apellidos'],
            nombres=row['nombres'],
            nombre_completo=row['nombre_completo'],
            sexo=row['sexo'],
            edad=row['edad'],
            esparrago=row['esparrago'],
            granada=row['granada'],
            maiz=row['maiz'],
            palta=row['palta'],
            papa=row['papa'],
            pecano=row['pecano'],
            vid=row['vid'],
            dpto=row['dpto'],
            provincia=row['provincia'],
            distrito=row['distrito'],
            centro_poblado=row['centro_poblado'],
            senasa=row['senasa'],
            sispa=row['sispa'],
            codigo_autogene_sispa=row['codigo_autogene_sispa'],
            regimen_tenencia_sispa=row['regimen_tenencia_sispa'],
            area_total_declarada=row['area_total_declarada'],
            fecha_actualizacion_sispa=row['fecha_actualizacion_sispa'],
            toma=row['toma'],
            edad_cultivo=row['edad_cultivo'],
            total_ha_sembrada=row['total_ha_sembrada'],
            productividad_x_ha=row['productividad_x_ha'],
            tipo_riego=row['tipo_riego'],
            nivel_alcance_venta=row['nivel_alcance_venta'],
            jornales_por_ha=row['jornales_por_ha'],
            practica_economica_sost=row['practica_economica_sost'],
            porcentaje_prac_economica_sost=row['porcentaje_prac_economica_sost']
        )