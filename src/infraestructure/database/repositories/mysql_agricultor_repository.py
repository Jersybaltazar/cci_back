"""
Implementación del repositorio de Agricultor con MySQL.
"""
from typing import List, Optional
import aiomysql
from datetime import date

from src.domain.entities.agricultor import Agricultor
from src.domain.repositories.agricultor_repository import AgricultorRepository
from src.domain.exceptions.domain_exceptions import RepositoryException, DatabaseConnectionException, AgricultorNotFoundException


class MySQLAgricultorRepository(AgricultorRepository):  
    """Implementación del repositorio de Agricultor con MySQL."""
    
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
    
    async def find_by_dni(self, dni: str) -> Optional[Agricultor]:
        """Busca un agricultor por su DNI."""
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    query = """
                        SELECT * FROM agricultores 
                        WHERE dni = %s
                    """
                    await cursor.execute(query, (dni,))
                    row = await cursor.fetchone()
                    
                    if row is None:
                        return None
                        
                    return self._map_to_entity(row)
                    
        except aiomysql.Error as e:
            raise DatabaseConnectionException(f"Error al consultar agricultor: {str(e)}")
            
    async def save(self, agricultor: Agricultor) -> Agricultor:
        """Guarda un nuevo agricultor."""
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    query = """
                        INSERT INTO agricultores (
                            dni, fecha_censo, apellidos, nombres, nombre_completo,
                            nombre_empresa_organizacion, pais,
                            sexo, edad, telefono, tamaño_empresa, sector, 
                            esparrago, granada, maiz, palta, papa, pecano, vid, castaña,
                            dpto, provincia, distrito, centro_poblado, coordenadas, ubicacion_maps,
                            senasa, cod_lugar_prod, area_solicitada, rendimiento_certificado,
                            predio, direccion, departamento_senasa, provincia_senasa, distrito_senasa,
                            sector_senasa, subsector_senasa, sispa, codigo_autogene_sispa, regimen_tenencia_sispa,
                            area_total_declarada, fecha_actualizacion_sispa,
                            programa_plantas, inia_programa_peru_2m, senasa_escuela_campo,
                            toma, edad_cultivo, total_ha_sembrada, productividad_x_ha,
                            tipo_riego, nivel_alcance_venta, jornales_por_ha,
                            practica_economica_sost, porcentaje_prac_economica_sost
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    
                    values = (
                        agricultor.dni, agricultor.fecha_censo, agricultor.apellidos, agricultor.nombres,
                        agricultor.nombre_completo, agricultor.nombre_empresa_organizacion, agricultor.pais,
                        agricultor.sexo, agricultor.edad, agricultor.telefono, agricultor.tamaño_empresa, 
                        agricultor.sector, agricultor.esparrago, agricultor.granada, agricultor.maiz, 
                        agricultor.palta, agricultor.papa, agricultor.pecano, agricultor.vid, agricultor.castaña,
                        agricultor.dpto, agricultor.provincia, agricultor.distrito, agricultor.centro_poblado,
                        agricultor.coordenadas, agricultor.ubicacion_maps, agricultor.senasa, agricultor.cod_lugar_prod,
                        agricultor.area_solicitada, agricultor.rendimiento_certificado, agricultor.predio,
                        agricultor.direccion, agricultor.departamento_senasa, agricultor.provincia_senasa,
                        agricultor.distrito_senasa, agricultor.sector_senasa, agricultor.subsector_senasa,
                        agricultor.sispa, agricultor.codigo_autogene_sispa, agricultor.regimen_tenencia_sispa,
                        agricultor.area_total_declarada, agricultor.fecha_actualizacion_sispa,
                        agricultor.programa_plantas, agricultor.inia_programa_peru_2m, agricultor.senasa_escuela_campo,
                        agricultor.toma, agricultor.edad_cultivo, agricultor.total_ha_sembrada,
                        agricultor.productividad_x_ha, agricultor.tipo_riego, agricultor.nivel_alcance_venta,
                        agricultor.jornales_por_ha, agricultor.practica_economica_sost,
                        agricultor.porcentaje_prac_economica_sost
                    )
                    
                    await cursor.execute(query, values)
                    await conn.commit()
                    
                    return agricultor
                    
        except aiomysql.Error as e:
            raise RepositoryException(f"Error al guardar agricultor: {str(e)}")
    
    async def update(self, agricultor: Agricultor) -> Agricultor:
        """Actualiza un agricultor existente."""
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Verificar que exista
                    exists = await self.exists_by_dni(agricultor.dni)
                    if not exists:
                        raise AgricultorNotFoundException(agricultor.dni)
                    
                    query = """
                        UPDATE agricultores SET
                            fecha_censo = %s, apellidos = %s, nombres = %s, nombre_completo = %s,
                            nombre_empresa_organizacion = %s, pais = %s,
                            sexo = %s, edad = %s, telefono = %s, tamaño_empresa = %s, sector = %s,
                            esparrago = %s, granada = %s, maiz = %s, palta = %s, papa = %s,
                            pecano = %s, vid = %s, castaña = %s, dpto = %s, provincia = %s, distrito = %s,
                            centro_poblado = %s, coordenadas = %s, ubicacion_maps = %s, senasa = %s,
                            cod_lugar_prod = %s, area_solicitada = %s, rendimiento_certificado = %s,
                            predio = %s, direccion = %s, departamento_senasa = %s, provincia_senasa = %s,
                            distrito_senasa = %s, sector_senasa = %s, subsector_senasa = %s,
                            sispa = %s, codigo_autogene_sispa = %s, regimen_tenencia_sispa = %s,
                            area_total_declarada = %s, fecha_actualizacion_sispa = %s,
                            programa_plantas = %s, inia_programa_peru_2m = %s, senasa_escuela_campo = %s,
                            toma = %s, edad_cultivo = %s, total_ha_sembrada = %s, productividad_x_ha = %s,
                            tipo_riego = %s, nivel_alcance_venta = %s, jornales_por_ha = %s,
                            practica_economica_sost = %s, porcentaje_prac_economica_sost = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE dni = %s
                    """
                    
                    values = (
                        agricultor.fecha_censo, agricultor.apellidos, agricultor.nombres,
                        agricultor.nombre_completo, agricultor.nombre_empresa_organizacion, agricultor.pais,
                        agricultor.sexo, agricultor.edad, agricultor.telefono, agricultor.tamaño_empresa, 
                        agricultor.sector, agricultor.esparrago, agricultor.granada, agricultor.maiz, 
                        agricultor.palta, agricultor.papa, agricultor.pecano, agricultor.vid, agricultor.castaña,
                        agricultor.dpto, agricultor.provincia, agricultor.distrito, agricultor.centro_poblado,
                        agricultor.coordenadas, agricultor.ubicacion_maps, agricultor.senasa, agricultor.cod_lugar_prod,
                        agricultor.area_solicitada, agricultor.rendimiento_certificado, agricultor.predio,
                        agricultor.direccion, agricultor.departamento_senasa, agricultor.provincia_senasa,
                        agricultor.distrito_senasa, agricultor.sector_senasa, agricultor.subsector_senasa,
                        agricultor.sispa, agricultor.codigo_autogene_sispa, agricultor.regimen_tenencia_sispa,
                        agricultor.area_total_declarada, agricultor.fecha_actualizacion_sispa,
                        agricultor.programa_plantas, agricultor.inia_programa_peru_2m, agricultor.senasa_escuela_campo,
                        agricultor.toma, agricultor.edad_cultivo, agricultor.total_ha_sembrada,
                        agricultor.productividad_x_ha, agricultor.tipo_riego, agricultor.nivel_alcance_venta,
                        agricultor.jornales_por_ha, agricultor.practica_economica_sost,
                        agricultor.porcentaje_prac_economica_sost, agricultor.dni
                    )
                    
                    await cursor.execute(query, values)
                    await conn.commit()
                    
                    return agricultor
                    
        except aiomysql.Error as e:
            raise RepositoryException(f"Error al actualizar agricultor: {str(e)}")
        
    async def delete_by_dni(self, dni: str) -> bool:
        """Elimina un agricultor por su DNI."""
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    query = "DELETE FROM agricultores WHERE dni = %s"
                    await cursor.execute(query, (dni,))
                    await conn.commit()
                    
                    # MySQL devuelve el número de filas afectadas
                    return cursor.rowcount > 0
                    
        except aiomysql.Error as e:
            raise RepositoryException(f"Error al eliminar agricultor: {str(e)}")
    
    async def find_all(self, limit: int = 100, offset: int = 0) -> List[Agricultor]:
        """Obtiene una lista paginada de agricultores."""
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    query = """
                        SELECT * FROM agricultores
                        ORDER BY apellidos, nombres
                        LIMIT %s OFFSET %s
                    """
                    await cursor.execute(query, (limit, offset))
                    rows = await cursor.fetchall()
                    
                    return [self._map_to_entity(row) for row in rows]
                    
        except aiomysql.Error as e:
            raise DatabaseConnectionException(f"Error al consultar agricultores: {str(e)}")
    
    async def count_all(self) -> int:
        """Cuenta el número total de agricultores."""
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    query = "SELECT COUNT(*) FROM agricultores"
                    await cursor.execute(query)
                    result = await cursor.fetchone()
                    
                    # MySQL devuelve una tupla, el primer elemento es el conteo
                    return result[0]
                    
        except aiomysql.Error as e:
            raise DatabaseConnectionException(f"Error al contar agricultores: {str(e)}")
    
    async def exists_by_dni(self, dni: str) -> bool:
        """Verifica si existe un agricultor con el DNI dado."""
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    query = "SELECT 1 FROM agricultores WHERE dni = %s LIMIT 1"
                    await cursor.execute(query, (dni,))
                    result = await cursor.fetchone()
                    
                    return result is not None
                    
        except aiomysql.Error as e:
            raise RepositoryException(f"Error al verificar existencia: {str(e)}")
    
    
    
    def _map_to_entity(self, row: dict) -> Agricultor:
        """Convierte una fila de la base de datos a una entidad Agricultor."""
        return Agricultor(
            dni=row["dni"],
            fecha_censo=row["fecha_censo"],
            apellidos=row["apellidos"],
            nombres=row["nombres"],
            nombre_completo=row["nombre_completo"],
            nombre_empresa_organizacion=row.get("nombre_empresa_organizacion"),
            pais=row.get("pais"),
            sexo=row["sexo"],
            edad=row["edad"],
            telefono=row.get("telefono"),
            tamaño_empresa=row.get("tamaño_empresa"),
            sector=row.get("sector"),
            
            esparrago=row["esparrago"],
            granada=row["granada"],
            maiz=row["maiz"],
            palta=row["palta"],
            papa=row["papa"],
            pecano=row["pecano"],
            vid=row["vid"],
            castaña=row.get("castaña"),

            dpto=row["dpto"],
            provincia=row["provincia"],
            distrito=row["distrito"],
            centro_poblado=row["centro_poblado"],
            coordenadas=row.get("coordenadas"),
            ubicacion_maps=row.get("ubicacion_maps"),
            
            senasa=row["senasa"],
            cod_lugar_prod=row.get("cod_lugar_prod"),
            area_solicitada=row.get("area_solicitada"),
            rendimiento_certificado=row.get("rendimiento_certificado"),
            predio=row.get("predio"),
            direccion=row.get("direccion"),
            departamento_senasa=row.get("departamento_senasa"),
            provincia_senasa=row.get("provincia_senasa"),
            distrito_senasa=row.get("distrito_senasa"),
            sector_senasa=row.get("sector_senasa"),
            subsector_senasa=row.get("subsector_senasa"),
            sispa=row["sispa"],
            codigo_autogene_sispa=row["codigo_autogene_sispa"],
            regimen_tenencia_sispa=row["regimen_tenencia_sispa"],
            area_total_declarada=row["area_total_declarada"],
            fecha_actualizacion_sispa=row["fecha_actualizacion_sispa"],
            
            programa_plantas=row.get("programa_plantas"),
            inia_programa_peru_2m=row.get("inia_programa_peru_2m"),
            senasa_escuela_campo=row.get("senasa_escuela_campo"),
            
            toma=row["toma"],
            edad_cultivo=row["edad_cultivo"],
            total_ha_sembrada=row["total_ha_sembrada"],
            productividad_x_ha=row["productividad_x_ha"],
            tipo_riego=row["tipo_riego"],
            nivel_alcance_venta=row["nivel_alcance_venta"],
            jornales_por_ha=row["jornales_por_ha"],
            
            practica_economica_sost=row["practica_economica_sost"],
            porcentaje_prac_economica_sost=row["porcentaje_prac_economica_sost"]
        )
    
    async def find_by_location(self, dpto: str = None, provincia: str = None, distrito: str = None) -> List[Agricultor]:
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    # Construir la consulta dinámicamente
                    query = "SELECT * FROM agricultores WHERE 1=1"
                    params = []
                    
                    if dpto:
                        query += " AND dpto = %s"
                        params.append(dpto)
                    
                    if provincia:
                        query += " AND provincia = %s"
                        params.append(provincia)
                    
                    if distrito:
                        query += " AND distrito = %s"
                        params.append(distrito)
                    
                    # Ordenar y limitar
                    query += " ORDER BY apellidos, nombres LIMIT 1000"
                    
                    await cursor.execute(query, tuple(params))
                    rows = await cursor.fetchall()
                    
                    return [self._map_to_entity(row) for row in rows]
                    
        except aiomysql.Error as e:
            raise DatabaseConnectionException(f"Error al buscar agricultores por ubicación: {str(e)}")