from sprout.sprout import Sprout
from pydantic import BaseModel
from typing import Optional
from uuid import UUID, uuid4

class Request(BaseModel):
    id: UUID = uuid4()

app = Sprout()

@app.task()
def afunc(a: Optional[UUID] = None) -> Request:
    a = a or uuid4()
    return Request(id = a)

@app.task()
def fails():
    raise Exception

@app.task()
def infinite(a: Optional[UUID] = None):
    a = a or uuid4()
    i = 0
    while True:
        yield f"{a.hex} {i}"
        i += 1

if __name__=="__main__":
    app()