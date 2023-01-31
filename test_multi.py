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

if __name__=="__main__":
    app()