"""
Controlador REST para agricultores.
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from fastapi.responses import JSONResponse, Response

from src.applicattion.dto.actualizarAgricultorDTO import ActualizarAgricultorDTO
from src.applicattion.use_cases.actualizar_agricultor import ActualizarAgricultorUseCase
from src.applicattion.dto.agricultor_response_dto import AgricultorDTO, CrearAgricultorDTO
from src.applicattion.use_cases.consultar_agricultor_por_dni import ConsultarAgricultorPorDniUseCase
from src.applicattion.use_cases.crear_agriculture_dni import CrearAgricultorUseCase
from src.domain.entities.agricultor import Agricultor
from src.domain.exceptions.domain_exceptions import (
    AgricultorNotFoundException, 
    InvalidDNIException, 
    AgricultorValidationException
)
from src.infraestructure.web.dependencies import get_agricultor_repository, get_agricultor_service

# Crear router
router = APIRouter(prefix="/agricultores", tags=["Agricultores"])

# Mappers
def agricultor_to_dto(agricultor: Agricultor) -> AgricultorDTO:
    """Convierte una entidad Agricultor a un DTO."""
    return AgricultorDTO(
        dni=agricultor.dni,
        fecha_censo=agricultor.fecha_censo,
        apellidos=agricultor.apellidos,
        nombres=agricultor.nombres,
        nombre_completo=agricultor.nombre_completo,
        nombre_empresa_organizacion=agricultor.nombre_empresa_organizacion,  # NUEVO
        pais=agricultor.pais,
        sexo=agricultor.sexo,
        edad=agricultor.edad,
        telefono=agricultor.telefono,
        tamaño_empresa=agricultor.tamaño_empresa,
        sector=agricultor.sector,
        
        # Cultivos
        esparrago=agricultor.esparrago,
        granada=agricultor.granada,
        maiz=agricultor.maiz,
        palta=agricultor.palta,
        papa=agricultor.papa,
        pecano=agricultor.pecano,
        vid=agricultor.vid,
        castaña=agricultor.castaña,
        
        # Ubicación
        dpto=agricultor.dpto,
        provincia=agricultor.provincia,
        distrito=agricultor.distrito,
        centro_poblado=agricultor.centro_poblado,
        coordenadas=agricultor.coordenadas,
        ubicacion_maps=agricultor.ubicacion_maps,
        ubicacion_completa=agricultor.ubicacion_completa,
        
        # Información SENASA/SISPA
        senasa=agricultor.senasa,
        cod_lugar_prod=agricultor.cod_lugar_prod,
        area_solicitada=agricultor.area_solicitada,
        rendimiento_certificado=agricultor.rendimiento_certificado,
        predio=agricultor.predio,
        direccion=agricultor.direccion,
        departamento_senasa=agricultor.departamento_senasa,
        provincia_senasa=agricultor.provincia_senasa,
        distrito_senasa=agricultor.distrito_senasa,
        sector_senasa=agricultor.sector_senasa,
        subsector_senasa=agricultor.subsector_senasa,
        sispa=agricultor.sispa,
        codigo_autogene_sispa=agricultor.codigo_autogene_sispa,
        regimen_tenencia_sispa=agricultor.regimen_tenencia_sispa,
        area_total_declarada=agricultor.area_total_declarada,
        fecha_actualizacion_sispa=agricultor.fecha_actualizacion_sispa,
        
        # Certificaciones
        programa_plantas=agricultor.programa_plantas,
        inia_programa_peru_2m=agricultor.inia_programa_peru_2m,
        senasa_escuela_campo=agricultor.senasa_escuela_campo,
        
        # Información técnica
        toma=agricultor.toma,
        edad_cultivo=agricultor.edad_cultivo,
        total_ha_sembrada=agricultor.total_ha_sembrada,
        productividad_x_ha=agricultor.productividad_x_ha,
        tipo_riego=agricultor.tipo_riego,
        nivel_alcance_venta=agricultor.nivel_alcance_venta,
        jornales_por_ha=agricultor.jornales_por_ha,
        
        # Prácticas sostenibles
        practica_economica_sost=agricultor.practica_economica_sost,
        porcentaje_prac_economica_sost=agricultor.porcentaje_prac_economica_sost,
        tiene_practicas_sostenibles=agricultor.tiene_practicas_sostenibles,
        tiene_certificaciones=agricultor.tiene_certificaciones,
        
        # Métricas derivadas
        cultivos_activos=agricultor.cultivos_activos
    )
def dto_to_agricultor(dto: CrearAgricultorDTO) -> Agricultor:
    """Convierte un DTO a una entidad Agricultor."""
    return Agricultor(
        dni=dto.dni,
        fecha_censo=dto.fecha_censo,
        apellidos=dto.apellidos,
        nombres=dto.nombres,
        nombre_completo=dto.nombre_completo,
        nombre_empresa_organizacion=dto.nombre_empresa_organizacion,  # NUEVO
        pais=dto.pais,
        sexo=dto.sexo,
        edad=dto.edad,
        telefono=dto.telefono,
        tamaño_empresa=dto.tamaño_empresa,
        sector=dto.sector,
        
        # Cultivos
        esparrago=dto.esparrago,
        granada=dto.granada,
        maiz=dto.maiz,
        palta=dto.palta,
        papa=dto.papa,
        pecano=dto.pecano,
        vid=dto.vid,
        castaña=dto.castaña,
        # Ubicación
        dpto=dto.dpto,
        provincia=dto.provincia,
        distrito=dto.distrito,
        centro_poblado=dto.centro_poblado,
        coordenadas=dto.coordenadas,
        ubicacion_maps=dto.ubicacion_maps,
        
        # Información SENASA/SISPA
        senasa=dto.senasa,
        cod_lugar_prod=dto.cod_lugar_prod,
        area_solicitada=dto.area_solicitada,
        rendimiento_certificado=dto.rendimiento_certificado,
        predio=dto.predio,
        direccion=dto.direccion,
        departamento_senasa=dto.departamento_senasa,
        provincia_senasa=dto.provincia_senasa,
        distrito_senasa=dto.distrito_senasa,
        sector_senasa=dto.sector_senasa,
        subsector_senasa=dto.subsector_senasa,
        sispa=dto.sispa,
        codigo_autogene_sispa=dto.codigo_autogene_sispa,
        regimen_tenencia_sispa=dto.regimen_tenencia_sispa,
        area_total_declarada=dto.area_total_declarada,
        fecha_actualizacion_sispa=dto.fecha_actualizacion_sispa,
        
        # Certificaciones
        programa_plantas=dto.programa_plantas,
        inia_programa_peru_2m=dto.inia_programa_peru_2m,
        senasa_escuela_campo=dto.senasa_escuela_campo,
        
        # Información técnica
        toma=dto.toma,
        edad_cultivo=dto.edad_cultivo,
        total_ha_sembrada=dto.total_ha_sembrada,
        productividad_x_ha=dto.productividad_x_ha,
        tipo_riego=dto.tipo_riego,
        nivel_alcance_venta=dto.nivel_alcance_venta,
        jornales_por_ha=dto.jornales_por_ha,
        
        # Prácticas sostenibles
        practica_economica_sost=dto.practica_economica_sost,
        porcentaje_prac_economica_sost=dto.porcentaje_prac_economica_sost
    )

def update_dto_to_agricultor(dni: str, dto: ActualizarAgricultorDTO) -> Agricultor:
    """Convierte un DTO de actualización a una entidad Agricultor."""
    return Agricultor(
        dni=dni,
        fecha_censo=dto.fecha_censo,
        apellidos=dto.apellidos,
        nombres=dto.nombres,
        nombre_completo=dto.nombre_completo,
        nombre_empresa_organizacion=dto.nombre_empresa_organizacion,  # NUEVO
        pais=dto.pais,
        sexo=dto.sexo,
        edad=dto.edad,
        telefono=dto.telefono,
        tamaño_empresa=dto.tamaño_empresa,
        sector=dto.sector,
        
        # Cultivos
        esparrago=dto.esparrago,
        granada=dto.granada,
        maiz=dto.maiz,
        palta=dto.palta,
        papa=dto.papa,
        pecano=dto.pecano,
        vid=dto.vid,
        castaña=dto.castaña,
        # Ubicación
        dpto=dto.dpto,
        provincia=dto.provincia,
        distrito=dto.distrito,
        centro_poblado=dto.centro_poblado,
        coordenadas=dto.coordenadas,
        ubicacion_maps=dto.ubicacion_maps,
        
        # Información SENASA/SISPA
        senasa=dto.senasa,
        cod_lugar_prod=dto.cod_lugar_prod,
        area_solicitada=dto.area_solicitada,
        rendimiento_certificado=dto.rendimiento_certificado,
        predio=dto.predio,
        direccion=dto.direccion,
        departamento_senasa=dto.departamento_senasa,
        provincia_senasa=dto.provincia_senasa,
        distrito_senasa=dto.distrito_senasa,
        sector_senasa=dto.sector_senasa,
        subsector_senasa=dto.subsector_senasa,
        sispa=dto.sispa,
        codigo_autogene_sispa=dto.codigo_autogene_sispa,
        regimen_tenencia_sispa=dto.regimen_tenencia_sispa,
        area_total_declarada=dto.area_total_declarada,
        fecha_actualizacion_sispa=dto.fecha_actualizacion_sispa,
        
        # Certificaciones
        programa_plantas=dto.programa_plantas,
        inia_programa_peru_2m=dto.inia_programa_peru_2m,
        senasa_escuela_campo=dto.senasa_escuela_campo,
        
        # Información técnica
        toma=dto.toma,
        edad_cultivo=dto.edad_cultivo,
        total_ha_sembrada=dto.total_ha_sembrada,
        productividad_x_ha=dto.productividad_x_ha,
        tipo_riego=dto.tipo_riego,
        nivel_alcance_venta=dto.nivel_alcance_venta,
        jornales_por_ha=dto.jornales_por_ha,
        
        # Prácticas sostenibles
        practica_economica_sost=dto.practica_economica_sost,
        porcentaje_prac_economica_sost=dto.porcentaje_prac_economica_sost
    )

# Endpoints
@router.get("", response_model=List[AgricultorDTO])
async def listar_agricultores(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    repository=Depends(get_agricultor_repository)
):
    """
    Lista de agricultores paginados.
    
    - **limit**: Número máximo de resultados (1-1000)
    - **offset**: Número de registros a saltar
    """
    try:
        agricultores = await repository.find_all(limit, offset)
        return [agricultor_to_dto(agricultor) for agricultor in agricultores]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{dni}", response_model=AgricultorDTO)
async def obtener_agricultor(
    dni: str = Path(..., regex="^[0-9]{8}$"),
    use_case=Depends(lambda repo=Depends(get_agricultor_repository), 
                    service=Depends(get_agricultor_service): 
                    ConsultarAgricultorPorDniUseCase(repo, service))
):
    """
    Obtiene un agricultor por su DNI.
    
    - **dni**: Documento Nacional de Identidad (8 dígitos)
    """
    try:
        agricultor = await use_case.execute(dni)
        return agricultor_to_dto(agricultor)
    except AgricultorNotFoundException:
        raise HTTPException(status_code=404, detail=f"Agricultor con DNI {dni} no encontrado")
    except InvalidDNIException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("", response_model=AgricultorDTO, status_code=201)
async def crear_agricultor(
    dto: CrearAgricultorDTO,
    use_case=Depends(lambda repo=Depends(get_agricultor_repository), 
                    service=Depends(get_agricultor_service): 
                    CrearAgricultorUseCase(repo, service))
):
    """
    Crea un nuevo agricultor.
    """
    try:
        agricultor = dto_to_agricultor(dto)
        agricultor_creado = await use_case.execute(agricultor)
        return agricultor_to_dto(agricultor_creado)
    except InvalidDNIException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except AgricultorValidationException as e:
        raise HTTPException(status_code=400, detail=f"Error de validación: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.put("/{dni}", response_model=AgricultorDTO)
async def actualizar_agricultor(
    dni: str = Path(..., regex="^[0-9]{8}$"),
    dto: ActualizarAgricultorDTO = None,
    use_case=Depends(lambda repo=Depends(get_agricultor_repository), 
                     service=Depends(get_agricultor_service): 
                     ActualizarAgricultorUseCase(repo, service))
):
    """
    Actualiza un agricultor existente identificado por su DNI.
    """
    try:
        agricultor = update_dto_to_agricultor(dni, dto)
        agricultor_actualizado = await use_case.execute(dni, agricultor)
        return agricultor_to_dto(agricultor_actualizado)
    except AgricultorNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidDNIException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except AgricultorValidationException as e:
        raise HTTPException(status_code=400, detail=f"Error de validación: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Añadimos un nuevo endpoint DELETE (para completar todas las operaciones CRUD)
@router.delete("/{dni}", status_code=204)
async def eliminar_agricultor(
    dni: str = Path(..., regex="^[0-9]{8}$"),
    repository=Depends(get_agricultor_repository),
    service=Depends(get_agricultor_service)
):
    """
    Elimina un agricultor por su DNI.
    """
    try:
        # Validar el DNI
        dni_limpio = service._validar_y_limpiar_dni(dni)
        
        # Verificar que exista
        exists = await repository.exists_by_dni(dni_limpio)
        if not exists:
            raise AgricultorNotFoundException(dni_limpio)
        
        # Eliminar
        await repository.delete_by_dni(dni_limpio)
        return Response(status_code=204)
    except AgricultorNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidDNIException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

agricultor_controller = router