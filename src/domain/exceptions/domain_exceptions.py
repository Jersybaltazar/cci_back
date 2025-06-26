"""
Excepciones del dominio para el Proyecto PLANTAS.
"""


class DomainException(Exception):
    """Excepción base para el dominio."""
    pass


class AgricultorNotFoundException(DomainException):
    """Se lanza cuando no se encuentra un agricultor por DNI."""
    
    def __init__(self, dni: str):
        self.dni = dni
        super().__init__(f"Agricultor con DNI {dni} no encontrado")


class InvalidDNIException(DomainException):
    """Se lanza cuando el DNI no tiene un formato válido."""
    
    def __init__(self, dni: str, reason: str = ""):
        self.dni = dni
        self.reason = reason
        message = f"DNI inválido: {dni}"
        if reason:
            message += f" - {reason}"
        super().__init__(message)


class AgricultorValidationException(DomainException):
    """Se lanza cuando la validación de un agricultor falla."""
    
    def __init__(self, field: str, value: any, reason: str):
        self.field = field
        self.value = value
        self.reason = reason
        super().__init__(f"Error de validación en campo '{field}': {reason}")


class RepositoryException(DomainException):
    """Se lanza cuando hay errores en el repositorio."""
    pass


class DatabaseConnectionException(RepositoryException):
    """Se lanza cuando hay problemas de conexión con la base de datos."""
    pass