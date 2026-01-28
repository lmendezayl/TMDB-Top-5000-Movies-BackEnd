"""Base Repository - Clase base para todos los repositorios."""

from typing import TypeVar, Generic, List, Optional
from sqlmodel import SQLModel, Session, select

T = TypeVar("T", bound=SQLModel)


class BaseRepository(Generic[T]):
    """Repositorio base con operaciones CRUD genéricas."""
    
    def __init__(self, session: Session, model: type[T]) -> None:
        """Inicializa el repositorio con una sesión y un modelo."""
        self.session = session
        self.model = model
    
    def create(self, obj: T) -> T:
        """Crea un nuevo registro en la BD."""
        self.session.add(obj)
        self.session.commit()
        self.session.refresh(obj)
        return obj
    
    def create_batch(self, objs: List[T]) -> List[T]:
        """Crea múltiples registros en lote."""
        self.session.add_all(objs)
        self.session.commit()
        return objs
    
    def get_by_id(self, id: int) -> Optional[T]:
        """Obtiene un registro por ID."""
        statement = select(self.model).where(self.model.id == id)
        return self.session.exec(statement).first()
    
    def get_all(self, skip: int = 0, limit: int = 100) -> List[T]:
        """Obtiene todos los registros con paginación."""
        statement = select(self.model).offset(skip).limit(limit)
        return self.session.exec(statement).all()
    
    def update(self, id: int, obj_update: dict) -> Optional[T]:
        """Actualiza un registro por ID."""
        obj = self.get_by_id(id)
        if obj:
            for key, value in obj_update.items():
                setattr(obj, key, value)
            self.session.add(obj)
            self.session.commit()
            self.session.refresh(obj)
        return obj
    
    def delete(self, id: int) -> bool:
        """Elimina un registro por ID."""
        obj = self.get_by_id(id)
        if obj:
            self.session.delete(obj)
            self.session.commit()
            return True
        return False
    
    def count(self) -> int:
        """Cuenta el total de registros."""
        statement = select(self.model)
        return len(self.session.exec(statement).all())
