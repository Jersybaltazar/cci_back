"""
Implementación del repositorio de Agricultor con MySQL.
"""
from typing import List, Optional
import aiomysql
import logging

from src.domain.entities.agricultor import Agricultor
from src.domain.repositories.agricultor_repository import AgricultorRepository
from src.domain.exceptions.domain_exceptions import RepositoryException, DatabaseConnectionException, AgricultorNotFoundException
logger = logging.getLogger(__name__)

class MySQLAgricultorRepository(AgricultorRepository):  
    """Implementación del repositorio de Agricultor con MySQL."""
    
    def __init__(self, pool: aiomysql.Pool):
        self.pool = pool
    
    async def find_by_dni(self, dni: str) -> Optional[Agricultor]:
        """Busca un agricultor por DNI."""
        query = """
        SELECT dni, fecha_censo, apellidos, nombres, nombre_completo,
               nombre_empresa_organizacion, pais, sexo, edad, telefono, 
               tamaño_empresa, sector, esparrago, granada, maiz, palta, 
               papa, pecano, vid, castaña, dpto, provincia, distrito, 
               centro_poblado, coordenadas, ubicacion_maps, senasa, 
               cod_lugar_prod, area_solicitada, rendimiento_certificado,
               predio, direccion, departamento_senasa, provincia_senasa, 
               distrito_senasa, sector_senasa, subsector_senasa, sispa, 
               codigo_autogene_sispa, regimen_tenencia_sispa, 
               area_total_declarada, fecha_actualizacion_sispa,
               programa_plantas, inia_programa_peru_2m, senasa_escuela_campo,
               toma, edad_cultivo, total_ha_sembrada, productividad_x_ha,
               tipo_riego, nivel_alcance_venta, jornales_por_ha,
               practica_economica_sost, porcentaje_prac_economica_sost
        FROM agricultores 
        WHERE dni = %s
        """
        
        connection = None
        try:
            connection = await self.pool.acquire()
            
            # CRUCIAL: Usar autocommit=True para lecturas
            await connection.ensure_closed()
            await connection.ping()
            
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, (dni,))
                result = await cursor.fetchone()
                
                if result:
                    logger.info(f"Agricultor encontrado: {dni}")
                    return self._map_to_entity(result)
                else:
                    logger.info(f"Agricultor no encontrado: {dni}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error consultando agricultor por DNI {dni}: {e}")
            raise DatabaseConnectionException(f"Error consultando agricultor: {e}")
        finally:
            if connection:
                try:
                    await self.pool.release(connection)
                except Exception as e:
                    logger.warning(f"Error liberando conexión: {e}")

    async def create(self, agricultor: Agricultor) -> Agricultor:
        """Crea un nuevo agricultor."""
        query = """
        INSERT INTO agricultores (
            dni, fecha_censo, apellidos, nombres, nombre_completo,
            nombre_empresa_organizacion, pais, sexo, edad, telefono, 
            tamaño_empresa, sector, esparrago, granada, maiz, palta, 
            papa, pecano, vid, castaña, dpto, provincia, distrito, 
            centro_poblado, coordenadas, ubicacion_maps, senasa, 
            cod_lugar_prod, area_solicitada, rendimiento_certificado,
            predio, direccion, departamento_senasa, provincia_senasa, 
            distrito_senasa, sector_senasa, subsector_senasa, sispa, 
            codigo_autogene_sispa, regimen_tenencia_sispa, 
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
        
        connection = None
        try:
            connection = await self.pool.acquire()
            
            # CRUCIAL: Iniciar transacción explícita
            await connection.begin()
            
            async with connection.cursor() as cursor:
                values = (
                    agricultor.dni, agricultor.fecha_censo, agricultor.apellidos,
                    agricultor.nombres, agricultor.nombre_completo,
                    agricultor.nombre_empresa_organizacion, agricultor.pais,
                    agricultor.sexo, agricultor.edad, agricultor.telefono,
                    agricultor.tamaño_empresa, agricultor.sector,
                    agricultor.esparrago, agricultor.granada, agricultor.maiz,
                    agricultor.palta, agricultor.papa, agricultor.pecano,
                    agricultor.vid, agricultor.castaña, agricultor.dpto,
                    agricultor.provincia, agricultor.distrito, agricultor.centro_poblado,
                    agricultor.coordenadas, agricultor.ubicacion_maps,
                    agricultor.senasa, agricultor.cod_lugar_prod,
                    agricultor.area_solicitada, agricultor.rendimiento_certificado,
                    agricultor.predio, agricultor.direccion,
                    agricultor.departamento_senasa, agricultor.provincia_senasa,
                    agricultor.distrito_senasa, agricultor.sector_senasa,
                    agricultor.subsector_senasa, agricultor.sispa,
                    agricultor.codigo_autogene_sispa, agricultor.regimen_tenencia_sispa,
                    agricultor.area_total_declarada, agricultor.fecha_actualizacion_sispa,
                    agricultor.programa_plantas, agricultor.inia_programa_peru_2m,
                    agricultor.senasa_escuela_campo, agricultor.toma,
                    agricultor.edad_cultivo, agricultor.total_ha_sembrada,
                    agricultor.productividad_x_ha, agricultor.tipo_riego,
                    agricultor.nivel_alcance_venta, agricultor.jornales_por_ha,
                    agricultor.practica_economica_sost, agricultor.porcentaje_prac_economica_sost
                )
                
                await cursor.execute(query, values)
                
                # CRUCIAL: Commit explícito y verificación
                await connection.commit()
                
                # Verificar que se insertó correctamente
                if cursor.rowcount != 1:
                    raise DatabaseConnectionException(f"Error: No se insertó el agricultor {agricultor.dni}")
                
                logger.info(f"Agricultor creado exitosamente: {agricultor.dni}")
                return agricultor
                
        except Exception as e:
            # CRUCIAL: Rollback en caso de error
            if connection:
                try:
                    await connection.rollback()
                except:
                    pass
            logger.error(f"Error creando agricultor {agricultor.dni}: {e}")
            raise DatabaseConnectionException(f"Error creando agricultor: {e}")
        finally:
            if connection:
                try:
                    await self.pool.release(connection)
                except Exception as e:
                    logger.warning(f"Error liberando conexión: {e}")

    async def save(self, agricultor: Agricultor) -> Agricultor:
        """Guarda un agricultor (crear o actualizar)."""
        # Verificar si existe
        existing = await self.find_by_dni(agricultor.dni)
        
        if existing:
            logger.info(f"Actualizando agricultor existente: {agricultor.dni}")
            return await self.update(agricultor)
        else:
            logger.info(f"Creando nuevo agricultor: {agricultor.dni}")
            return await self.create(agricultor)
    async def update(self, agricultor: Agricultor) -> Agricultor:
        """Actualiza un agricultor existente."""
        query = """
        UPDATE agricultores SET 
            fecha_censo = %s, apellidos = %s, nombres = %s, nombre_completo = %s,
            nombre_empresa_organizacion = %s, pais = %s, sexo = %s, edad = %s, 
            telefono = %s, tamaño_empresa = %s, sector = %s, esparrago = %s, 
            granada = %s, maiz = %s, palta = %s, papa = %s, pecano = %s, 
            vid = %s, castaña = %s, dpto = %s, provincia = %s, distrito = %s, 
            centro_poblado = %s, coordenadas = %s, ubicacion_maps = %s, 
            senasa = %s, cod_lugar_prod = %s, area_solicitada = %s, 
            rendimiento_certificado = %s, predio = %s, direccion = %s,
            departamento_senasa = %s, provincia_senasa = %s, distrito_senasa = %s, 
            sector_senasa = %s, subsector_senasa = %s, sispa = %s, 
            codigo_autogene_sispa = %s, regimen_tenencia_sispa = %s, 
            area_total_declarada = %s, fecha_actualizacion_sispa = %s,
            programa_plantas = %s, inia_programa_peru_2m = %s, 
            senasa_escuela_campo = %s, toma = %s, edad_cultivo = %s, 
            total_ha_sembrada = %s, productividad_x_ha = %s, tipo_riego = %s, 
            nivel_alcance_venta = %s, jornales_por_ha = %s,
            practica_economica_sost = %s, porcentaje_prac_economica_sost = %s
        WHERE dni = %s
        """
        
        connection = None
        try:
            connection = await self.pool.acquire()
            
            # CRUCIAL: Iniciar transacción explícita
            await connection.begin()
            
            async with connection.cursor() as cursor:
                values = (
                    agricultor.fecha_censo, agricultor.apellidos, agricultor.nombres,
                    agricultor.nombre_completo, agricultor.nombre_empresa_organizacion,
                    agricultor.pais, agricultor.sexo, agricultor.edad,
                    agricultor.telefono, agricultor.tamaño_empresa, agricultor.sector,
                    agricultor.esparrago, agricultor.granada, agricultor.maiz,
                    agricultor.palta, agricultor.papa, agricultor.pecano,
                    agricultor.vid, agricultor.castaña, agricultor.dpto,
                    agricultor.provincia, agricultor.distrito, agricultor.centro_poblado,
                    agricultor.coordenadas, agricultor.ubicacion_maps,
                    agricultor.senasa, agricultor.cod_lugar_prod,
                    agricultor.area_solicitada, agricultor.rendimiento_certificado,
                    agricultor.predio, agricultor.direccion,
                    agricultor.departamento_senasa, agricultor.provincia_senasa,
                    agricultor.distrito_senasa, agricultor.sector_senasa,
                    agricultor.subsector_senasa, agricultor.sispa,
                    agricultor.codigo_autogene_sispa, agricultor.regimen_tenencia_sispa,
                    agricultor.area_total_declarada, agricultor.fecha_actualizacion_sispa,
                    agricultor.programa_plantas, agricultor.inia_programa_peru_2m,
                    agricultor.senasa_escuela_campo, agricultor.toma,
                    agricultor.edad_cultivo, agricultor.total_ha_sembrada,
                    agricultor.productividad_x_ha, agricultor.tipo_riego,
                    agricultor.nivel_alcance_venta, agricultor.jornales_por_ha,
                    agricultor.practica_economica_sost, agricultor.porcentaje_prac_economica_sost,
                    agricultor.dni  # WHERE condition
                )
                
                await cursor.execute(query, values)
                
                if cursor.rowcount == 0:
                    await connection.rollback()
                    raise AgricultorNotFoundException(agricultor.dni)
                
                # CRUCIAL: Commit explícito
                await connection.commit()
                
                logger.info(f"Agricultor actualizado exitosamente: {agricultor.dni}")
                return agricultor
                
        except AgricultorNotFoundException:
            raise
        except Exception as e:
            # CRUCIAL: Rollback en caso de error
            if connection:
                try:
                    await connection.rollback()
                except:
                    pass
            logger.error(f"Error actualizando agricultor {agricultor.dni}: {e}")
            raise DatabaseConnectionException(f"Error actualizando agricultor: {e}")
        finally:
            if connection:
                try:
                    await self.pool.release(connection)
                except Exception as e:
                    logger.warning(f"Error liberando conexión: {e}")

    async def delete_by_dni(self, dni: str) -> bool:
        """Elimina un agricultor por DNI."""
        query = "DELETE FROM agricultores WHERE dni = %s"
        
        connection = None
        try:
            connection = await self.pool.acquire()
            await connection.begin()
            
            async with connection.cursor() as cursor:
                await cursor.execute(query, (dni,))
                deleted = cursor.rowcount > 0
                
                await connection.commit()
                
                logger.info(f"Agricultor {'eliminado' if deleted else 'no encontrado'}: {dni}")
                return deleted
                    
        except Exception as e:
            if connection:
                try:
                    await connection.rollback()
                except:
                    pass
            logger.error(f"Error eliminando agricultor {dni}: {e}")
            raise DatabaseConnectionException(f"Error eliminando agricultor: {e}")
        finally:
            if connection:
                try:
                    await self.pool.release(connection)
                except Exception as e:
                    logger.warning(f"Error liberando conexión: {e}")

    async def find_all(self, limit: int = 100, offset: int = 0) -> List[Agricultor]:

        query = """
        SELECT dni, fecha_censo, apellidos, nombres, nombre_completo,
            nombre_empresa_organizacion, pais, sexo, edad, telefono, 
            tamaño_empresa, sector, esparrago, granada, maiz, palta, 
            papa, pecano, vid, castaña, dpto, provincia, distrito, 
            centro_poblado, coordenadas, ubicacion_maps, senasa, 
            cod_lugar_prod, area_solicitada, rendimiento_certificado,
            predio, direccion, departamento_senasa, provincia_senasa, 
            distrito_senasa, sector_senasa, subsector_senasa, sispa, 
            codigo_autogene_sispa, regimen_tenencia_sispa, 
            area_total_declarada, fecha_actualizacion_sispa,
            programa_plantas, inia_programa_peru_2m, senasa_escuela_campo,
            toma, edad_cultivo, total_ha_sembrada, productividad_x_ha,
            tipo_riego, nivel_alcance_venta, jornales_por_ha,
            practica_economica_sost, porcentaje_prac_economica_sost
        FROM agricultores 
        ORDER BY fecha_censo DESC
        LIMIT %s OFFSET %s
        """
        
        connection = None
        try:
            connection = await self.pool.acquire()
            
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, (limit, offset))
                results = await cursor.fetchall()
                
                logger.info(f"Total agricultores encontrados: {len(results)} (limit: {limit}, offset: {offset})")
                return [self._map_to_entity(row) for row in results]
                    
        except Exception as e:
            logger.error(f"Error consultando todos los agricultores: {e}")
            raise DatabaseConnectionException(f"Error consultando agricultores: {e}")
        finally:
            if connection:
                try:
                    await self.pool.release(connection)
                except Exception as e:
                    logger.warning(f"Error liberando conexión: {e}")

    async def count_all(self) -> int:
        connection = None
        try:
            connection = await self.pool.acquire()
            
            async with connection.cursor() as cursor:
                query = "SELECT COUNT(*) FROM agricultores"
                await cursor.execute(query)
                result = await cursor.fetchone()
                
                # MySQL devuelve una tupla, el primer elemento es el conteo
                count = result[0] if result else 0
                logger.info(f"Total de agricultores en BD: {count}")
                return count
                
        except Exception as e:
            logger.error(f"Error contando agricultores: {e}")
            raise DatabaseConnectionException(f"Error al contar agricultores: {e}")
        finally:
            if connection:
                try:
                    await self.pool.release(connection)
                except Exception as e:
                    logger.warning(f"Error liberando conexión: {e}")
    
    async def exists_by_dni(self, dni: str) -> bool:
        """Verifica si existe un agricultor con el DNI dado."""
        query = "SELECT 1 FROM agricultores WHERE dni = %s LIMIT 1"
        
        connection = None
        try:
            connection = await self.pool.acquire()
            
            async with connection.cursor() as cursor:
                await cursor.execute(query, (dni,))
                result = await cursor.fetchone()
                exists = result is not None
                
                logger.info(f"Agricultor {'existe' if exists else 'no existe'}: {dni}")
                return exists
                    
        except Exception as e:
            logger.error(f"Error verificando existencia del agricultor {dni}: {e}")
            raise DatabaseConnectionException(f"Error verificando agricultor: {e}")
        finally:
            if connection:
                try:
                    await self.pool.release(connection)
                except Exception as e:
                    logger.warning(f"Error liberando conexión: {e}")
    
    
    def _map_to_entity(self, row: dict) -> Agricultor:
        """Mapea una fila de la base de datos a una entidad Agricultor."""
        return Agricultor(
            dni=row['dni'],
            fecha_censo=row['fecha_censo'],
            apellidos=row['apellidos'],
            nombres=row['nombres'],
            nombre_completo=row['nombre_completo'],
            nombre_empresa_organizacion=row['nombre_empresa_organizacion'],
            pais=row['pais'],
            sexo=row['sexo'],
            edad=row['edad'],
            telefono=row['telefono'],
            tamaño_empresa=row['tamaño_empresa'],
            sector=row['sector'],
            esparrago=row['esparrago'],
            granada=row['granada'],
            maiz=row['maiz'],
            palta=row['palta'],
            papa=row['papa'],
            pecano=row['pecano'],
            vid=row['vid'],
            castaña=row['castaña'],
            dpto=row['dpto'],
            provincia=row['provincia'],
            distrito=row['distrito'],
            centro_poblado=row['centro_poblado'],
            coordenadas=row['coordenadas'],
            ubicacion_maps=row['ubicacion_maps'],
            senasa=row['senasa'],
            cod_lugar_prod=row['cod_lugar_prod'],
            area_solicitada=row['area_solicitada'],
            rendimiento_certificado=row['rendimiento_certificado'],
            predio=row['predio'],
            direccion=row['direccion'],
            departamento_senasa=row['departamento_senasa'],
            provincia_senasa=row['provincia_senasa'],
            distrito_senasa=row['distrito_senasa'],
            sector_senasa=row['sector_senasa'],
            subsector_senasa=row['subsector_senasa'],
            sispa=row['sispa'],
            codigo_autogene_sispa=row['codigo_autogene_sispa'],
            regimen_tenencia_sispa=row['regimen_tenencia_sispa'],
            area_total_declarada=row['area_total_declarada'],
            fecha_actualizacion_sispa=row['fecha_actualizacion_sispa'],
            programa_plantas=row['programa_plantas'],
            inia_programa_peru_2m=row['inia_programa_peru_2m'],
            senasa_escuela_campo=row['senasa_escuela_campo'],
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
    
    async def find_by_location(self, dpto: str = None, provincia: str = None, distrito: str = None) -> List[Agricultor]:

        connection = None
        try:
            connection = await self.pool.acquire()
            
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                # Construir la consulta dinámicamente
                query = """
                SELECT dni, fecha_censo, apellidos, nombres, nombre_completo,
                    nombre_empresa_organizacion, pais, sexo, edad, telefono, 
                    tamaño_empresa, sector, esparrago, granada, maiz, palta, 
                    papa, pecano, vid, castaña, dpto, provincia, distrito, 
                    centro_poblado, coordenadas, ubicacion_maps, senasa, 
                    cod_lugar_prod, area_solicitada, rendimiento_certificado,
                    predio, direccion, departamento_senasa, provincia_senasa, 
                    distrito_senasa, sector_senasa, subsector_senasa, sispa, 
                    codigo_autogene_sispa, regimen_tenencia_sispa, 
                    area_total_declarada, fecha_actualizacion_sispa,
                    programa_plantas, inia_programa_peru_2m, senasa_escuela_campo,
                    toma, edad_cultivo, total_ha_sembrada, productividad_x_ha,
                    tipo_riego, nivel_alcance_venta, jornales_por_ha,
                    practica_economica_sost, porcentaje_prac_economica_sost
                FROM agricultores WHERE 1=1
                """
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
                
                logger.info(f"Agricultores encontrados por ubicación: {len(rows)}")
                return [self._map_to_entity(row) for row in rows]
                    
        except Exception as e:
            logger.error(f"Error buscando por ubicación: {e}")
            raise DatabaseConnectionException(f"Error al buscar agricultores por ubicación: {e}")
        finally:
            if connection:
                try:
                    await self.pool.release(connection)
                except Exception as e:
                    logger.warning(f"Error liberando conexión: {e}")