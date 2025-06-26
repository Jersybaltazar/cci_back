"""
DTO para actualización de agricultores.
"""
from datetime import date
from typing import Optional
from pydantic import BaseModel, Field, validator


class ActualizarAgricultorDTO(BaseModel):
    """DTO para actualizar un agricultor existente."""
    fecha_censo: date
    apellidos: str = Field(..., min_length=2, max_length=100)
    nombres: str = Field(..., min_length=2, max_length=100)
    nombre_completo: str = Field(..., min_length=4, max_length=200)
    nombre_empresa_organizacion: Optional[str] = Field(None, max_length=200)  # NUEVO
    pais: Optional[str] = Field(None, max_length=100)  # NUEVO
    sexo: str = Field(..., min_length=1, max_length=20)
    edad: Optional[str] = Field(None, max_length=20)
    telefono: Optional[int] = Field(None, ge=900000000, le=999999999)
    tamaño_empresa: Optional[str] = Field(None, max_length=50)
    sector: Optional[str] = Field(None, max_length=100)
    
    # Cultivos
    esparrago: Optional[str] = None
    granada: Optional[str] = None
    maiz: Optional[str] = None
    palta: Optional[str] = None
    papa: Optional[str] = None
    pecano: Optional[str] = None
    vid: Optional[str] = None
    castaña: Optional[str] = None  # NUEVO CULTIVO
    
    # Ubicación
    dpto: str = Field(..., min_length=2, max_length=100)
    provincia: str = Field(..., min_length=2, max_length=100)
    distrito: str = Field(..., min_length=2, max_length=100)
    centro_poblado: Optional[str] = Field(None, max_length=100)
    coordenadas: Optional[str] = Field(None, max_length=100)
    ubicacion_maps: Optional[str] = Field(None, max_length=500)
    
    # Información SENASA/SISPA
    senasa: Optional[str] = Field(None, max_length=100)
    cod_lugar_prod: Optional[str] = Field(None, max_length=50)
    area_solicitada: Optional[float] = Field(None, ge=0)
    rendimiento_certificado: Optional[float] = Field(None, ge=0)
    predio: Optional[str] = Field(None, max_length=200)
    direccion: Optional[str] = Field(None, max_length=255)
    departamento_senasa: Optional[str] = Field(None, max_length=100)
    provincia_senasa: Optional[str] = Field(None, max_length=100)
    distrito_senasa: Optional[str] = Field(None, max_length=100)
    sector_senasa: Optional[str] = Field(None, max_length=100)
    subsector_senasa: Optional[str] = Field(None, max_length=100)
    sispa: Optional[str] = Field(None, max_length=100)
    codigo_autogene_sispa: Optional[str] = Field(None, max_length=20)
    regimen_tenencia_sispa: Optional[str] = Field(None, max_length=100)
    area_total_declarada: Optional[float] = Field(None, ge=0)
    fecha_actualizacion_sispa: Optional[date] = None
    
    # Certificaciones
    programa_plantas: Optional[str] = Field(None, max_length=50)
    inia_programa_peru_2m: Optional[str] = Field(None, max_length=50)
    senasa_escuela_campo: Optional[str] = Field(None, max_length=50)
    
    # Información técnica
    toma: Optional[str] = Field(None, max_length=20)
    edad_cultivo: Optional[str] = Field(None, max_length=20)
    total_ha_sembrada: Optional[float] = Field(None, ge=0)
    productividad_x_ha: Optional[float] = Field(None, ge=0)
    tipo_riego: Optional[str] = Field(None, max_length=50)
    nivel_alcance_venta: Optional[str] = Field(None, max_length=50)
    jornales_por_ha: Optional[float] = Field(None, ge=0)
    
    # Prácticas sostenibles
    practica_economica_sost: Optional[str] = Field(None, max_length=100)
    porcentaje_prac_economica_sost: Optional[str] = Field(None, max_length=20)

    class Config:
        json_schema_extra = {
            "example": {
                "fecha_censo": "2023-06-01",
                "apellidos": "García",
                "nombres": "Juan Carlos",
                "nombre_completo": "Juan Carlos García",
                "nombre_empresa_organizacion": "Cooperativa Agrícola San José",
                "pais": "Perú",
                "sexo": "Masculino",
                "edad": "46",
                "telefono": 987654321,
                "dpto": "Lima",
                "provincia": "Barranca",
                "distrito": "Supe"
            }
        }