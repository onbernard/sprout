import time

from src.sprout import Sprout


app = Sprout()

@app.task(n_worker=2)
def test(x: int):
    time.sleep(x)
    return x

if __name__=="__main__":
    app.start()
    test(x=1)
