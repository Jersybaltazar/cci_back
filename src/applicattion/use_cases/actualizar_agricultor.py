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

    async def execute(self, dni: str, agricultor_actualizado: Agricultor) -> Agricultor:
        """
        Actualiza un agricultor existente.
        
        Args:
            dni: DNI del agricultor a actualizar (de la URL)
            agricultor_actualizado: Entidad Agricultor con datos actualizados
            
        Returns:
            Agricultor actualizado
            
        Raises:
            InvalidDNIException: Si el formato del DNI es inválido
            AgricultorNotFoundException: Si el agricultor no existe
            AgricultorValidationException: Si hay errores de validación
        """
        # 1. Validar el DNI de la URL
        try:
            dni_limpio = self.service._validar_y_limpiar_dni(dni)
        except ValueError as e:
            raise InvalidDNIException(dni, str(e))
        
        # 2. Verificar que el agricultor existe
        agricultor_existente = await self.repository.find_by_dni(dni_limpio)
        if agricultor_existente is None:
            raise AgricultorNotFoundException(dni_limpio)
        
        # 3. CRÍTICO: Asegurar que el DNI no cambie
        # El DNI siempre debe ser el de la URL, ignorando cualquier DNI en el body
        if agricultor_actualizado.dni != dni_limpio:
            agricultor_actualizado.dni = dni_limpio
        
        # 4. Validar los nuevos datos
        try:
            self.service.validar_datos_agricultor(agricultor_actualizado)
        except ValueError as e:
            field = str(e).split(":")[0] if ":" in str(e) else "unknown"
            raise AgricultorValidationException(
                field, 
                getattr(agricultor_actualizado, field, None), 
                str(e)
            )
        
        # 5. Actualizar en el repositorio
        try:
            return await self.repository.update(agricultor_actualizado)
        except Exception as e:
            raise AgricultorValidationException(
                "general", 
                None, 
                f"Error actualizando agricultor: {str(e)}"
            )