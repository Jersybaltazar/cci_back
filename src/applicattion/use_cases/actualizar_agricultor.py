"""
Caso de uso: Actualizar un agricultor existente
"""
from src.domain.entities.agricultor import Agricultor
from src.domain.exceptions.domain_exceptions import (
    AgricultorNotFoundException, 
    InvalidDNIException, 
    AgricultorValidationException
)
from src.domain.repositories.agricultor_repository import AgricultorRepository
from src.domain.services.agricultor_service import AgricultorService


class ActualizarAgricultorUseCase:
    """Caso de uso para actualizar un agricultor existente."""
    
    def __init__(self, repository: AgricultorRepository, service: AgricultorService):
        self.repository = repository
        self.service = service
    
    async def execute(self, dni: str, agricultor: Agricultor) -> Agricultor:
        """
        Actualiza un agricultor existente.
        
        Args:
            dni: DNI del agricultor a actualizar
            agricultor: Entidad Agricultor con datos actualizados
            
        Returns:
            Agricultor actualizado con datos actualizados
            
        Raises:
            InvalidDNIException: Si el formato del DNI es inválido
            AgricultorNotFoundException: Si el agricultor no existe
            AgricultorValidationException: Si hay errores de validación
        """
        # Validar el DNI
        try:
            dni_limpio = self.service._validar_y_limpiar_dni(dni)
        except ValueError as e:
            raise InvalidDNIException(dni, str(e))
        
        # Verificar si el agricultor existe
        exists = await self.repository.exists_by_dni(dni_limpio)
        if not exists:
            raise AgricultorNotFoundException(dni_limpio)
        
        # Asegurarse de que el DNI en la entidad coincida con el DNI del path
        if agricultor.dni != dni_limpio:
            agricultor.dni = dni_limpio
        
        try:
            # Validar los datos del agricultor
            self.service.validar_datos_agricultor(agricultor)
        except ValueError as e:
            field = str(e).split(":")[0] if ":" in str(e) else "unknown"
            raise AgricultorValidationException(field, getattr(agricultor, field, None), str(e))
        
        # Actualizar en el repositorio
        agricultor_actualizado = await self.repository.update(agricultor)
        return agricultor_actualizado