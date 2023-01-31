from src.async_sprout import Sprout

app = Sprout()

@app.task()
def afunc(a:int):
    return a*2

@app.task()
def anotherfunc(a:str = "uwu"):
    return a*2

@app.task()
def fails():
    raise Exception("lol")

if __name__=="__main__":
    app()