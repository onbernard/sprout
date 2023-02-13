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

from typing import List
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
from .sprout import Task

typer_app = typer.Typer()



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
    app()


def main():
    typer_app()

if __name__ == "__main__":
    main()