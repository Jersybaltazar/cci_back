"""
Entidad Agricultor - Dominio del Proyecto PLANTAS
Representa a un agricultor registrado en el sistema de trazabilidad.
"""

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class Agricultor:
    """
    Entidad de dominio que representa a un agricultor en el sistema PLANTAS.
    
    Esta clase encapsula toda la información relevante de un agricultor,
    incluyendo datos personales, cultivos, ubicación y prácticas sostenibles.
    """
    
    # Identificación y datos personales
    dni: str
    fecha_censo: date
    apellidos: str
    nombres: str
    nombre_completo: str
    sexo: str

    nombre_empresa_organizacion: Optional[str] = None  # NUEVO CAMPO
    pais: Optional[str] = None  # NUEVO CAMPO
    edad: Optional[str] = None  # Cambiado a string
    telefono: Optional[int] = None  # Nuevo campo
    tamaño_empresa: Optional[str] = None  # Nuevo campo
    sector: Optional[str] = None

    # Cultivos (en hectáreas o estado)
    esparrago: Optional[str] = None
    granada: Optional[str] = None
    maiz: Optional[str] = None
    palta: Optional[str] = None
    papa: Optional[str] = None
    pecano: Optional[str] = None
    vid: Optional[str] = None
    castaña: Optional[str] = None
    # Ubicación geográfica
    dpto: str = ""
    provincia: str = ""
    distrito: str = ""
    centro_poblado: Optional[str] = None
    coordenadas: Optional[str] = None  # Nuevo campo
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
    
    #certificaciones
    programa_plantas: Optional[str] = None  # Nuevo campo
    inia_programa_peru_2m: Optional[str] = None  # Nuevo campo
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
    
    def __post_init__(self):
        """Validaciones básicas después de la inicialización."""
        self._validate_dni()
        self._validate_age()
    
    def _validate_dni(self):
        """Valida que el DNI tenga el formato correcto."""
        if not self.dni or len(self.dni.strip()) == 0:
            raise ValueError("DNI no puede estar vacío")
        
        # Remover espacios y validar longitud
        dni_clean = self.dni.strip()
        if len(dni_clean) != 8:
            raise ValueError("DNI debe tener exactamente 8 dígitos")
        
        # Validar que sea numérico
        if not dni_clean.isdigit():
            raise ValueError("DNI debe contener solo números")
        
        self.dni = dni_clean
    
    def _validate_age(self):
        """Valida que la edad esté dentro de rangos razonables."""
        if self.edad is not None:
            # Si es un string que contiene solo dígitos, convertirlo y validar
            if isinstance(self.edad, str) and self.edad.strip().isdigit():
                edad_num = int(self.edad.strip())
                if edad_num < 0 or edad_num > 120:
                    raise ValueError("Edad debe estar entre 0 y 120 años")
            # Si es un string que no es número, pero tiene contenido, aceptarlo
            elif isinstance(self.edad, str) and self.edad.strip():
                pass  # Aceptar texto descriptivo como "50 años", "N/A", etc.
            # Si es un número directamente
            elif isinstance(self.edad, (int, float)):
                if self.edad < 0 or self.edad > 120:
                    raise ValueError("Edad debe estar entre 0 y 120 años")
    
    @property
    def ubicacion_completa(self) -> str:
        """Retorna la ubicación completa como string."""
        ubicacion_parts = [self.dpto, self.provincia, self.distrito]
        if self.centro_poblado:
            ubicacion_parts.append(self.centro_poblado)
        return ", ".join(filter(None, ubicacion_parts))
    
    @property
    def cultivos_activos(self) -> dict:
        """Retorna un diccionario de cultivos que tienen información."""
        cultivos = {}
        
        cultivos_list = ['esparrago', 'granada', 'maiz', 'palta', 'papa', 'pecano', 'vid', 'castaña']
        for cultivo in cultivos_list:
            valor = getattr(self, cultivo)
            
            # Si es string "SÍ"/"NO"
            if isinstance(valor, str) and valor.strip() and valor.upper() in ["SÍ", "SI", "S"]:
                cultivos[cultivo] = valor
            # Si es valor numérico mayor a 0
            elif isinstance(valor, (int, float)) and valor > 0:
                cultivos[cultivo] = valor
        
        return cultivos
    
    @property
    def tiene_practicas_sostenibles(self) -> bool:
        """Indica si el agricultor implementa prácticas sostenibles."""
        return (
            self.practica_economica_sost is not None and
            self.practica_economica_sost.strip() != ""
        )
    
    @property
    def tiene_certificaciones(self) -> bool:
        """Indica si el agricultor cuenta con alguna certificación."""
        certificaciones = [self.programa_plantas, self.inia_programa_peru_2m, self.senasa_escuela_campo]
        return any(cert and cert.strip() for cert in certificaciones)
    def __str__(self) -> str:
        """Representación string del agricultor."""
        return f"Agricultor(dni={self.dni}, nombre={self.nombre_completo})"
    
    def __repr__(self) -> str:
        """Representación detallada del agricultor."""
        return (
            f"Agricultor(dni={self.dni}, "
            f"nombre={self.nombre_completo}, "
            f"ubicacion={self.ubicacion_completa})"
        )