"""
Puerto (interfaz) del repositorio de Agricultor.
Define el contrato que debe implementar cualquier adaptador de persistencia.
"""

from abc import ABC, abstractmethod
from typing import Optional, List
from ..entities.agricultor import Agricultor


class AgricultorRepository(ABC):
    """
    Repositorio abstracto para la entidad Agricultor.
    
    Define las operaciones de persistencia que debe implementar
    cualquier adaptador concreto (SQLAlchemy, MongoDB, etc.).
    """
    
    @abstractmethod
    async def find_by_dni(self, dni: str) -> Optional[Agricultor]:
        """
        Busca un agricultor por su DNI.
        
        Args:
            dni: Documento Nacional de Identidad del agricultor
            
        Returns:
            Agricultor si existe, None si no se encuentra
            
        Raises:
            RepositoryException: Si hay errores en la consulta
        """
        pass
    
    @abstractmethod
    async def save(self, agricultor: Agricultor) -> Agricultor:
        """
        Guarda un agricultor en el repositorio.
        
        Args:
            agricultor: Entidad Agricultor a guardar
            
        Returns:
            Agricultor guardado con datos actualizados
            
        Raises:
            RepositoryException: Si hay errores al guardar
        """
        pass
    
    @abstractmethod
    async def update(self, agricultor: Agricultor) -> Agricultor:
        """
        Actualiza un agricultor existente.
        
        Args:
            agricultor: Entidad Agricultor con datos actualizados
            
        Returns:
            Agricultor actualizado
            
        Raises:
            RepositoryException: Si hay errores al actualizar
            AgricultorNotFoundException: Si el agricultor no existe
        """
        pass
    
    @abstractmethod
    async def delete_by_dni(self, dni: str) -> bool:
        """
        Elimina un agricultor por su DNI.
        
        Args:
            dni: DNI del agricultor a eliminar
            
        Returns:
            True si se eliminó, False si no existía
            
        Raises:
            RepositoryException: Si hay errores al eliminar
        """
        pass
    
    @abstractmethod
    async def find_all(self, limit: int = 100, offset: int = 0) -> List[Agricultor]:
        """
        Obtiene una lista paginada de agricultores.
        
        Args:
            limit: Número máximo de resultados
            offset: Número de registros a saltar
            
        Returns:
            Lista de agricultores
            
        Raises:
            RepositoryException: Si hay errores en la consulta
        """
        pass
    
    @abstractmethod
    async def count_all(self) -> int:
        """
        Cuenta el total de agricultores registrados.
        
        Returns:
            Número total de agricultores
            
        Raises:
            RepositoryException: Si hay errores en la consulta
        """
        pass
    
    @abstractmethod
    async def find_by_location(
        self, 
        dpto: Optional[str] = None,
        provincia: Optional[str] = None,
        distrito: Optional[str] = None
    ) -> List[Agricultor]:
        """
        Busca agricultores por ubicación geográfica.
        
        Args:
            dpto: Departamento (opcional)
            provincia: Provincia (opcional)
            distrito: Distrito (opcional)
            
        Returns:
            Lista de agricultores que coinciden con la ubicación
            
        Raises:
            RepositoryException: Si hay errores en la consulta
        """
        pass
    
    @abstractmethod
    async def exists_by_dni(self, dni: str) -> bool:
        """
        Verifica si existe un agricultor con el DNI dado.
        
        Args:
            dni: DNI a verificar
            
        Returns:
            True si existe, False si no
            
        Raises:
            RepositoryException: Si hay errores en la consulta
        """
        pass