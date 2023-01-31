import asyncio
import time
from uuid import UUID, uuid4
from concurrent.futures.process import ProcessPoolExecutor
from typing import Optional, Dict, List
from http import HTTPStatus

from fastapi import Body, FastAPI, Form, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from pydantic import BaseModel


from test_multi import app, afunc, anotherfunc, fails
from sprout.future import FutureModel

app = FastAPI()

@app.get("/afunc")
async def get_afunc() -> List[FutureModel]:
    return [el async for el in afunc.all()]