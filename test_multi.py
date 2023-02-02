from sprout.sprout import Sprout
from pydantic import BaseModel

class Request(BaseModel):
    file: str = "fsq/fsq/fqsss"

class ReturnVal(BaseModel):
    param: Request = Request()
    uwu: str = "uwu"

app = Sprout()

@app.task()
def afunc(a:Request) -> str:
    print(a)
    return a.file+"uwu"

@app.task()
def anotherfunc(a:str = "uwu") -> ReturnVal:
    return 1

@app.task()
def fails():
    raise Exception("lol")


@app.task()
def infinite(what:str):
    i = 0
    while True:
        yield f"{what} {i}"
        i += 1

if __name__=="__main__":
    app()