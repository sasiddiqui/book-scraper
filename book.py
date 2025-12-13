from pydantic import BaseModel
from typing import Optional

class Book(BaseModel):
    title: str
    price: float
    url: str
    instock: bool
    image: Optional[str] = None
    source: str

    publisher: Optional[str] = None
    author: Optional[str] = None
    description: Optional[str] = None

    titleNormalized: Optional[str] = None
    authorNormalized: Optional[str] = None
    publisherNormalized: Optional[str] = None

    def __str__(self):
        return f"{self.title} by {self.author} - ${self.price}"
