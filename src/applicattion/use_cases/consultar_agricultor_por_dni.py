"""
Caso de uso: Consultar agricultor por DNI
"""
from typing import Optional

from src.domain.entities.agricultor import Agricultor
from src.domain.exceptions.domain_exceptions import AgricultorNotFoundException, InvalidDNIException
from src.domain.repositories.agricultor_repository import AgricultorRepository
from src.domain.services.agricultor_service import AgricultorService


class ConsultarAgricultorPorDniUseCase:
    """Caso de uso para consultar un agricultor por su DNI."""
    
    def __init__(self, repository: AgricultorRepository, service: AgricultorService):
        self.repository = repository
        self.service = service
        
    async def execute(self, dni: str) -> Agricultor:
        """
        Consulta un agricultor por su DNI.
        
        Args:
            dni: DNI del agricultor a consultar
            
        Returns:
            Entidad Agricultor
            
        Raises:
            InvalidDNIException: Si el formato del DNI es inválido
            AgricultorNotFoundException: Si no se encuentra el agricultor
        """
        # Validar formato del DNI
        try:
            dni_limpio = self.service._validar_y_limpiar_dni(dni)
        except ValueError as e:
            raise InvalidDNIException(dni, str(e))
        
        # Consultar en el repositorio
        agricultor = await self.repository.find_by_dni(dni_limpio)
        
        if agricultor is None:
            raise AgricultorNotFoundException(dni_limpio)
            
        return agricultor