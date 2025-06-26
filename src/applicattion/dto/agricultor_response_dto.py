"""
DTOs para la entidad Agricultor.
"""
from typing import Optional
from datetime import date
from pydantic import BaseModel, Field, validator


class AgricultorDTO(BaseModel):
    """DTO para respuestas con información de agricultor."""
    
    # Datos personales
    dni: str
    fecha_censo: date
    apellidos: str
    nombres: str
    nombre_completo: str
    nombre_empresa_organizacion: Optional[str] = None  # NUEVO
    pais: Optional[str] = None
    sexo: str
    edad: Optional[str] = None
    telefono: Optional[int] = None
    tamaño_empresa: Optional[str] = None
    sector: Optional[str] = None
    # Cultivos
    esparrago: Optional[str] = None
    granada: Optional[str] = None
    maiz: Optional[str] = None
    palta: Optional[str] = None
    papa: Optional[str] = None
    pecano: Optional[str] = None
    vid: Optional[str] = None
    castaña: Optional[str] = None 
    # Ubicación
    dpto: str
    provincia: str
    distrito: str
    centro_poblado: Optional[str] = None
    coordenadas: Optional[str] = None
    ubicacion_maps: Optional[str] = None
    ubicacion_completa: str
    
    # SENASA/SISPA
    senasa: Optional[str] = None
    cod_lugar_prod: Optional[str] = None
    area_solicitada: Optional[float] = None
    rendimiento_certificado: Optional[float] = None
    predio: Optional[str] = None
    direccion: Optional[str] = None
    departamento_senasa: Optional[str] = None
    provincia_senasa: Optional[str] = None
    distrito_senasa: Optional[str] = None
    sector_senasa: Optional[str] = None
    subsector_senasa: Optional[str] = None
    sispa: Optional[str] = None
    codigo_autogene_sispa: Optional[str] = None
    regimen_tenencia_sispa: Optional[str] = None
    area_total_declarada: Optional[float] = None
    fecha_actualizacion_sispa: Optional[date] = None
    # Certificaciones
    programa_plantas: Optional[str] = None
    inia_programa_peru_2m: Optional[str] = None
    senasa_escuela_campo: Optional[str] = None
    
    # Información agrícola
    toma: Optional[str] = None
    edad_cultivo: Optional[str] = None
    total_ha_sembrada: Optional[float] = None
    productividad_x_ha: Optional[float] = None
    tipo_riego: Optional[str] = None
    nivel_alcance_venta: Optional[str] = None
    jornales_por_ha: Optional[float] = None
    
    # Prácticas sostenibles
    practica_economica_sost: Optional[str] = None
    porcentaje_prac_economica_sost: Optional[str] = None
    tiene_practicas_sostenibles: bool
    tiene_certificaciones: bool
    # Métricas derivadas
    cultivos_activos: dict
    
    class Config:
        json_schema_extra = {
            "example": {
                "dni": "12345678",
                "fecha_censo": "2023-06-01",
                "apellidos": "García",
                "nombres": "Juan",
                "nombre_completo": "Juan García",
                "nombre_empresa_organizacion": "Cooperativa Agrícola San José",
                "pais": "Perú",
                "sexo": "Masculino",
                "edad": "45",
                "telefono": 987654321,
                "castaña": "SÍ",
                "dpto": "Lima",
                "provincia": "Barranca",
                "distrito": "Supe"
            }
        }


class CrearAgricultorDTO(BaseModel):
    """DTO para crear un nuevo agricultor."""
    dni: str
    fecha_censo: date
    apellidos: str
    nombres: str
    nombre_completo: str
    nombre_empresa_organizacion: Optional[str] = Field(None, max_length=200)  # NUEVO
    pais: Optional[str] = Field(None, max_length=100) 
    sexo: str
    edad: Optional[str] = None
    telefono: Optional[int] = None
    tamaño_empresa: Optional[str] = None
    sector: Optional[str] = None
    
    # Cultivos
    esparrago: Optional[str] = None
    granada: Optional[str] = None
    maiz: Optional[str] = None
    palta: Optional[str] = None
    papa: Optional[str] = None
    pecano: Optional[str] = None
    vid: Optional[str] = None
    castaña: Optional[str] = None
    # Ubicación
    dpto: str
    provincia: str
    distrito: str
    centro_poblado: Optional[str] = None
    coordenadas: Optional[str] = None
    ubicacion_maps: Optional[str] = None
    
    # Información SENASA/SISPA
    senasa: Optional[str] = None
    cod_lugar_prod: Optional[str] = None
    area_solicitada: Optional[float] = None
    rendimiento_certificado: Optional[float] = None
    predio: Optional[str] = None
    direccion: Optional[str] = None
    departamento_senasa: Optional[str] = None
    provincia_senasa: Optional[str] = None
    distrito_senasa: Optional[str] = None
    sector_senasa: Optional[str] = None
    subsector_senasa: Optional[str] = None
    sispa: Optional[str] = None
    codigo_autogene_sispa: Optional[str] = None
    regimen_tenencia_sispa: Optional[str] = None
    area_total_declarada: Optional[float] = None
    fecha_actualizacion_sispa: Optional[date] = None
    
    # Certificaciones
    programa_plantas: Optional[str] = None
    inia_programa_peru_2m: Optional[str] = None
    senasa_escuela_campo: Optional[str] = None
    
    # Información técnica del cultivo
    toma: Optional[str] = None
    edad_cultivo: Optional[str] = None
    total_ha_sembrada: Optional[float] = None
    productividad_x_ha: Optional[float] = None
    tipo_riego: Optional[str] = None
    nivel_alcance_venta: Optional[str] = None
    jornales_por_ha: Optional[float] = None
    
    # Prácticas sostenibles
    practica_economica_sost: Optional[str] = None
    porcentaje_prac_economica_sost: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "dni": "12345678",
                "fecha_censo": "2023-06-01",
                "apellidos": "García",
                "nombres": "Juan",
                "nombre_completo": "Juan García",
                "nombre_empresa_organizacion": "Cooperativa Agrícola San José",
                "pais": "Perú",
                "sexo": "Masculino",
                "edad": "45",
                "telefono": 987654321,
                "tamaño_empresa": "Pequeña",
                "sector": "Agrícola",
                "dpto": "Lima",
                "provincia": "Barranca",
                "distrito": "Supe",
                "coordenadas": "-12.345678,-76.789012",
                "ubicacion_maps": "https://maps.google.com/?q=-12.345678,-76.789012",
                "esparrago": "SÍ",
                "granada": "NO"
            }
        }
    @validator('dni')
    def validate_dni(cls, v):
        if not v.isdigit():
            raise ValueError('DNI debe contener solo números')
        return v
    
    @validator('nombre_completo', always=True)
    def set_nombre_completo(cls, v, values):
        if v is None and 'nombres' in values and 'apellidos' in values:
            return f"{values['nombres']} {values['apellidos']}"
        return v