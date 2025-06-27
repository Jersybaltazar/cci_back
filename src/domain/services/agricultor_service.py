"""
Servicio de dominio para Agricultor.
Contiene la lógica de negocio compleja que no pertenece a una entidad específica.
"""

from typing import Optional, Dict, Any
from ..entities.agricultor import Agricultor
from ..repositories.agricultor_repository import AgricultorRepository
from ..exceptions.domain_exceptions import (
    AgricultorNotFoundException,
    InvalidDNIException,
    AgricultorValidationException
)


class AgricultorService:
    """
    Servicio de dominio que encapsula la lógica de negocio
    relacionada con los agricultores.
    """
    
    def __init__(self, repository: AgricultorRepository):
        self._repository = repository
    
    async def obtener_agricultor_por_dni(self, dni: str) -> Agricultor:
        """
        Obtiene un agricultor por su DNI con validaciones de negocio.
        
        Args:
            dni: DNI del agricultor a buscar
            
        Returns:
            Agricultor encontrado
            
        Raises:
            InvalidDNIException: Si el DNI no es válido
            AgricultorNotFoundException: Si no se encuentra el agricultor
        """
        # Validar formato de DNI
        dni_limpio = self._validar_y_limpiar_dni(dni)
        
        # Buscar en el repositorio
        agricultor = await self._repository.find_by_dni(dni_limpio)
        
        if agricultor is None:
            raise AgricultorNotFoundException(dni_limpio)
        
        return agricultor
    
    def _validar_y_limpiar_dni(self, dni: str) -> str:
        """
        Valida y limpia el formato del DNI.
        
        Args:
            dni: DNI a validar
            
        Returns:
            DNI limpio y validado
            
        Raises:
            InvalidDNIException: Si el DNI no es válido
        """
        if not dni:
            raise InvalidDNIException(dni, "DNI no puede estar vacío")
        
        # Limpiar espacios y caracteres especiales
        dni_limpio = dni.strip().replace("-", "").replace(".", "")
        
        # Validar longitud
        if len(dni_limpio) != 8:
            raise InvalidDNIException(
                dni, 
                f"DNI debe tener 8 dígitos, recibido: {len(dni_limpio)}"
            )
        
        # Validar que sea numérico
        if not dni_limpio.isdigit():
            raise InvalidDNIException(dni, "DNI debe contener solo números")
        
        return dni_limpio
    
    async def verificar_existencia_agricultor(self, dni: str) -> bool:
        """
        Verifica si existe un agricultor con el DNI dado.
        
        Args:
            dni: DNI a verificar
            
        Returns:
            True si existe, False si no
        """
        try:
            dni_limpio = self._validar_y_limpiar_dni(dni)
            return await self._repository.exists_by_dni(dni_limpio)
        except InvalidDNIException:
            return False
    
    def generar_resumen_agricultor(self, agricultor: Agricultor) -> Dict[str, Any]:
        """
        Genera un resumen con información clave del agricultor.
        
        Args:
            agricultor: Entidad Agricultor
            
        Returns:
            Diccionario con resumen de información
        """
        return {
            "identificacion": {
                "dni": agricultor.dni,
                "nombre_completo": agricultor.nombre_completo,
                "edad": agricultor.edad,
                "sexo": agricultor.sexo
            },
            "ubicacion": {
                "departamento": agricultor.dpto,
                "provincia": agricultor.provincia,
                "distrito": agricultor.distrito,
                "centro_poblado": agricultor.centro_poblado,
                "ubicacion_completa": agricultor.ubicacion_completa
            },
            "actividad_agricola": {
                "cultivos_activos": agricultor.cultivos_activos,
                "total_ha_sembrada": agricultor.total_ha_sembrada,
                "productividad_x_ha": agricultor.productividad_x_ha,
                "tipo_riego": agricultor.tipo_riego,
                "nivel_alcance_venta": agricultor.nivel_alcance_venta
            },
            "sostenibilidad": {
                "tiene_practicas_sostenibles": agricultor.tiene_practicas_sostenibles,
                "practica_economica_sost": agricultor.practica_economica_sost,
                "porcentaje_prac_economica_sost": agricultor.porcentaje_prac_economica_sost
            },
            "informacion_tecnica": {
                "senasa": agricultor.senasa,
                "sispa": agricultor.sispa,
                "area_total_declarada": agricultor.area_total_declarada,
                "jornales_por_ha": agricultor.jornales_por_ha
            }
        }
    
    def validar_actualizacion(self, dni_url: str, agricultor: Agricultor) -> None:
        """
        Valida que una actualización sea consistente.
        
        Args:
            dni_url: DNI de la URL del endpoint
            agricultor: Entidad agricultor a validar
            
        Raises:
            ValueError: Si hay inconsistencias
        """
        # Validar que el DNI coincida
        if agricultor.dni != dni_url:
            raise ValueError(f"Inconsistencia: DNI en URL ({dni_url}) != DNI en entidad ({agricultor.dni})")
        
        # Validar datos generales
        self.validar_datos_agricultor(agricultor)

    def validar_datos_agricultor(self, agricultor: Agricultor) -> None:
        """
        Valida que los datos del agricultor cumplan con las reglas de negocio.
        
        Args:
            agricultor: Entidad Agricultor a validar
            
        Raises:
            AgricultorValidationException: Si los datos no son válidos
        """
        # Validar datos obligatorios
        if not agricultor.nombre_completo or agricultor.nombre_completo.strip() == "":
            raise AgricultorValidationException(
                "nombre_completo", 
                agricultor.nombre_completo, 
                "Nombre completo es obligatorio"
            )
        
        if not agricultor.dpto or agricultor.dpto.strip() == "":
            raise AgricultorValidationException(
                "dpto", 
                agricultor.dpto, 
                "Departamento es obligatorio"
            )
        
        # Validar rangos numéricos
        if agricultor.total_ha_sembrada is not None and agricultor.total_ha_sembrada < 0:
            raise AgricultorValidationException(
                "total_ha_sembrada",
                agricultor.total_ha_sembrada,
                "Hectáreas sembradas no puede ser negativo"
            )
        
        if agricultor.productividad_x_ha is not None and agricultor.productividad_x_ha < 0:
            raise AgricultorValidationException(
                "productividad_x_ha",
                agricultor.productividad_x_ha,
                "Productividad no puede ser negativa"
            )
    
    def calcular_metricas_agricultor(self, agricultor: Agricultor) -> Dict[str, Any]:
        """
        Calcula métricas derivadas del agricultor.
        
        Args:
            agricultor: Entidad Agricultor
            
        Returns:
            Diccionario con métricas calculadas
        """
        metricas = {
            "numero_cultivos_activos": len(agricultor.cultivos_activos),
            "tiene_informacion_completa": self._tiene_informacion_completa(agricultor),
            "score_sostenibilidad": self._calcular_score_sostenibilidad(agricultor)
        }
        
        # Calcular producción total estimada
        if (agricultor.total_ha_sembrada and 
            agricultor.productividad_x_ha and 
            agricultor.total_ha_sembrada > 0 and 
            agricultor.productividad_x_ha > 0):
            metricas["produccion_total_estimada"] = (
                agricultor.total_ha_sembrada * agricultor.productividad_x_ha
            )
        
        return metricas
    
    def _tiene_informacion_completa(self, agricultor: Agricultor) -> bool:
        """Verifica si el agricultor tiene información completa."""
        campos_clave = [
            agricultor.nombre_completo,
            agricultor.dpto,
            agricultor.provincia,
            agricultor.distrito,
            agricultor.total_ha_sembrada,
            agricultor.tipo_riego
        ]
        
        return all(campo is not None and str(campo).strip() != "" 
                  for campo in campos_clave)
    
    def _calcular_score_sostenibilidad(self, agricultor: Agricultor) -> int:
        """Calcula un score de sostenibilidad del 0 al 100."""
        score = 0
        
        # +30 puntos por tener prácticas sostenibles
        if agricultor.tiene_practicas_sostenibles:
            score += 30
        
        # +20 puntos por tener información de SENASA
        if agricultor.senasa and agricultor.senasa.strip():
            score += 20
        
        # +20 puntos por riego tecnificado
        if agricultor.tipo_riego and "goteo" in agricultor.tipo_riego.lower():
            score += 20
        elif agricultor.tipo_riego and "aspersion" in agricultor.tipo_riego.lower():
            score += 15
        
        # +30 puntos por diversificación de cultivos
        num_cultivos = len(agricultor.cultivos_activos)
        if num_cultivos >= 3:
            score += 30
        elif num_cultivos == 2:
            score += 20
        elif num_cultivos == 1:
            score += 10
        
        return min(score, 100)  # Máximo 100