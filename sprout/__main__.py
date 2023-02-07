import sys

logo = """
                                                                                                                     
                                                                                                                     
   SSSSSSSSSSSSSSS                                                                                     tttt          
 SS:::::::::::::::S                                                                                 ttt:::t          
S:::::SSSSSS::::::S                                                                                 t:::::t          
S:::::S     SSSSSSS                                                                                 t:::::t          
S:::::S            ppppp   ppppppppp   rrrrr   rrrrrrrrr      ooooooooooo   uuuuuu    uuuuuu  ttttttt:::::ttttttt    
S:::::S            p::::ppp:::::::::p  r::::rrr:::::::::r   oo:::::::::::oo u::::u    u::::u  t:::::::::::::::::t    
 S::::SSSS         p:::::::::::::::::p r:::::::::::::::::r o:::::::::::::::ou::::u    u::::u  t:::::::::::::::::t    
  SS::::::SSSSS    pp::::::ppppp::::::prr::::::rrrrr::::::ro:::::ooooo:::::ou::::u    u::::u  tttttt:::::::tttttt    
    SSS::::::::SS   p:::::p     p:::::p r:::::r     r:::::ro::::o     o::::ou::::u    u::::u        t:::::t          
       SSSSSS::::S  p:::::p     p:::::p r:::::r     rrrrrrro::::o     o::::ou::::u    u::::u        t:::::t          
            S:::::S p:::::p     p:::::p r:::::r            o::::o     o::::ou::::u    u::::u        t:::::t          
            S:::::S p:::::p    p::::::p r:::::r            o::::o     o::::ou:::::uuuu:::::u        t:::::t    tttttt
SSSSSSS     S:::::S p:::::ppppp:::::::p r:::::r            o:::::ooooo:::::ou:::::::::::::::uu      t::::::tttt:::::t
S::::::SSSSSS:::::S p::::::::::::::::p  r:::::r            o:::::::::::::::o u:::::::::::::::u      tt::::::::::::::t
S:::::::::::::::SS  p::::::::::::::pp   r:::::r             oo:::::::::::oo   uu::::::::uu:::u        tt:::::::::::tt
 SSSSSSSSSSSSSSS    p::::::pppppppp     rrrrrrr               ooooooooooo       uuuuuuuu  uuuu          ttttttttttt  
                    p:::::p                                                                                          
                    p:::::p                                                                                          
                   p:::::::p                                                                                         
                   p:::::::p                                                                                         
                   p:::::::p                                                                                         
                   ppppppppp                                                                                         
                                                                                                                     
"""

import typer


def hello(name: str):
    print(f"Hello {name}")

def main():
    typer.run(hello)


if __name__ == "__main__":
    main()