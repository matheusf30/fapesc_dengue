###################################################
## Roteiro adaptado para estruturar dados        ##
## Dados: Regional de Saúde (.pdf) e D.TK5 (.csv)##
## Demanda: FAPESC edital nº 37/2024             ##
## Adaptado por: Matheus Ferreira de Souza       ##
##               e Domênica Tcacenco             ##
## Data: 29/07/2025                              ##
###################################################

##### Bibliotecas correlatas ####################################################
import pandas as pd
import os, sys

##### Padrão ANSI ###############################################################
bold = "\033[1m"
red = "\033[91m"
green = "\033[92m"
yellow = "\033[33m"
blue = "\033[34m"
magenta = "\033[35m"
cyan = "\033[36m"
white = "\033[37m"
reset = "\033[0m"

##### CAMINHOS E ARQUIVOS ########################################################
caminho_dados = "/home/sifapsc/scripts/matheus/fapesc_dengue/domenica/"
nome_arquivo = "regional_saude.csv"
caminho_arquivo = f"{caminho_dados}{nome_arquivo}"
resultado = "regional_sc_saude.csv"
caminho_resultado = f"/home/sifapsc/scripts/matheus/fapesc_dengue/matheus/{resultado}"

### PRÉ-PROCESSAMNETO ############################################################
regional = pd.read_csv(caminho_arquivo)
print(f"\n{green}ARQUIVO:\n{reset}{regional}\n")

