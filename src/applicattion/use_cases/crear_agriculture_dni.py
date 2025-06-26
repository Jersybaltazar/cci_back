"""
Caso de uso: Crear agricultor
"""
from src.domain.entities.agricultor import Agricultor
from src.domain.exceptions.domain_exceptions import InvalidDNIException, AgricultorValidationException
from src.domain.repositories.agricultor_repository import AgricultorRepository
from src.domain.services.agricultor_service import AgricultorService


class CrearAgricultorUseCase:
    """Caso de uso para crear un nuevo agricultor."""
    
    def __init__(self, repository: AgricultorRepository, service: AgricultorService):
        self.repository = repository
        self.service = service
        
    async def execute(self, agricultor: Agricultor) -> Agricultor:
        """
        Crea un nuevo agricultor.
        
        Args:
            agricultor: Entidad Agricultor a crear
            
        Returns:
            Agricultor creado con datos actualizados
            
        Raises:
            InvalidDNIException: Si el formato del DNI es inválido
            AgricultorValidationException: Si hay errores de validación
        """
        try:
            # Validar el agricultor
            self.service.validar_datos_agricultor(agricultor)
        except ValueError as e:
            field = str(e).split(":")[0] if ":" in str(e) else "unknown"
            raise AgricultorValidationException(field, getattr(agricultor, field, None), str(e))
        
        # Verificar si ya existe
        exists = await self.repository.exists_by_dni(agricultor.dni)
        if exists:
            raise AgricultorValidationException("dni", agricultor.dni, "Ya existe un agricultor con este DNI")
        
        # Crear en el repositorio
        agricultor_creado = await self.repository.save(agricultor)
        return agricultor_creado