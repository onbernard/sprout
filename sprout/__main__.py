logo = """
_____/\\\\\\\\\\\_________________________________________________________________________        
 ___/\\\/////////\\\_______________________________________________________________________       
  __\//\\\______\///____/\\\\\\\\\________________________________________________/\\\______      
   ___\////\\\__________/\\\/////\\\__/\\/\\\\\\\______/\\\\\_____/\\\____/\\\__/\\\\\\\\\\\_     
    ______\////\\\______\/\\\\\\\\\\__\/\\\/////\\\___/\\\///\\\__\/\\\___\/\\\_\////\\\////__    
     _________\////\\\___\/\\\//////___\/\\\___\///___/\\\__\//\\\_\/\\\___\/\\\____\/\\\______   
      __/\\\______\//\\\__\/\\\_________\/\\\_________\//\\\__/\\\__\/\\\___\/\\\____\/\\\_/\\__  
       _\///\\\\\\\\\\\/___\/\\\_________\/\\\__________\///\\\\\/___\//\\\\\\\\\_____\//\\\\\___ 
        ___\///////////_____\///__________\///_____________\/////______\/////////_______\/////____
"""

from typing import List, Coroutine
import sys
import time
import asyncio
import typer
import sys
import importlib
from rich import print as pprint
from rich import box
from rich.console import Console
from rich.live import Live
from rich.table import Table

from .sprout import Sprout
from .task import Task
from .future import Future
from .utils.helpers import merge_async_iters

typer_app = typer.Typer()

def make_table_coroutine(task: Task):
    tables = {
        "inprogress": (task.inprogress_queue, Table("key")),
        "failed": (task.failed_queue, Table()),
        "completed": (task.completed_queue, Table())
    }
    async def drain(queue_name: str):
        while True:
            future: Future = await tables[queue_name][0].get()
            tables[queue_name][1].add_column(future.key)
    return [table for _, table in tables.values()],[drain(name) for name in tables.keys()]

async def run_app(app: Sprout):
    async for _ in merge_async_iters(*(t.consumer() for t in app.task_index)):
        ...

def run(app: Sprout):
    all_tables: List[Table] = []
    all_drains: List[Coroutine] = []
    for task in app.task_index:
        tables, drains = make_table_coroutine(task) 
        all_tables.extend(tables)
        all_drains.extend(drains)
    console = Console()
    with Live(console=console, screen=True, auto_refresh=False) as live:
        async def update_display():
            while True:
                live.update(all_tables[0], refresh=True)
                await asyncio.sleep(1)
        async def launch():
            await asyncio.gather(update_display(), run_app(app), *all_drains)
        asyncio.get_running_loop().create_task(launch())


@typer_app.command()
def worker(cmd: str, path: str):
    try:
        module_name, app_name = path.split(":")
    except ValueError:
        raise typer.BadParameter("worker command expect a path of the form module.submodule..:app")
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        raise typer.BadParameter(f"module {module_name} cannot be found")
    try:
        app = getattr(module, app_name)
    except AttributeError:
        raise typer.BadParameter(f"module {module_name} does not contain the attribute {app_name}")
    if not isinstance(app, Sprout):
        raise typer.BadParameter(f"atrribute {app_name} of module {module_name} is not a Sprout instance")
    run(app)


def main():
    typer_app()

if __name__ == "__main__":
    main()