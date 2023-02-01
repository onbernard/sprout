from sprout.sprout import Sprout
from pydantic import BaseModel

class Request(BaseModel):
    file: str = "fsq/fsq/fqsss"

app = Sprout()

@app.task()
def afunc(a:Request) -> str:
    print(a)
    return a.file+"uwu"

@app.task()
def anotherfunc(a:str = "uwu"):
    return a*2

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