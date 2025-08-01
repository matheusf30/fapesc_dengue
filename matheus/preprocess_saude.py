###################################################
## Roteiro adaptado para pré-processar dados     ##
## Dados: Focos de _Aedes_ sp. e                 ##
##        Casos Prováveis de Dengue (DIVE/SC)    ##
## Demanda: FAPESC edital nº 37/2024             ##
## Adaptado por: Matheus Ferreira de Souza       ##
##               e Everton Weber Galliani        ##
## Data: 31/07/2025                              ##
###################################################

##### Bibliotecas correlatas ####################################################
#import dbf
import pandas as pd
import numpy as np
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
caminho_dados = "/home/sifapsc/scripts/matheus/fapesc_dengue/matheus/dados/"
casos = "dengue-2025.xlsx"
focos = "focos 2025.xlsx"


##### ABRINDO ARQUIVOS ###########################################################
#casos = pd.read_excel(f"{caminho_dados}{casos}")
focos = pd.read_excel(f"{caminho_dados}{focos}")


### PRÉ-PROCESSAMENTO ############################################################
### CASOS
#print(f"\n{green}CASOS:\n{reset}{casos}\n")
#print(f"\n{green}CASOS:\n{reset}{casos.columns}\n")
#sys.exit()
### FOCOS
print(f"\n{green}FOCOS:\n{reset}{focos}\n")
print(f"\n{green}FOCOS:\n{reset}{focos.columns}\n")
colunas_renomear = {"Unnamed: 0":"EXCLUIR",
					"Dengue - Quadro de focos":"regional",
					"Unnamed: 2":"municipio",
					"Unnamed: 3":"A.aegypti",
					"Unnamed: 4":"A.albopictus",
					"Unnamed: 5":"data"}
focos = focos.rename(columns = colunas_renomear)
print(f"\n{green}FOCOS:\n{reset}{focos}\n")
print(f"\n{green}FOCOS:\n{reset}{focos.columns}\n")
focos.drop(columns = ["EXCLUIR"], inplace = True)
focos.dropna(axis = 0, inplace = True)
focos["data"] = pd.to_datetime(focos["data"], format = "%d/%m/%Y", errors = "coerce")
focos.sort_values(by = ["data"], inplace = True)
focos.set_index("data", inplace=True)
focos = focos.drop(pd.to_datetime("NaT"))
colunas = [["A.aegypti", "A.albopictus"]]
for col in colunas:
	focos[col] = focos[col].astype(int)
focos["focos"] = focos["A.aegypti"] + focos["A.albopictus"] 
print(f"\n{green}FOCOS:\n{reset}{focos}\n")
print(f"\n{green}FOCOS:\n{reset}{focos.columns}\n")
fator_agregacao = {"A.aegypti":"sum", "A.albopictus":"sum", "focos":"sum"}
focos_semanal = focos.groupby("municipio").resample("W").agg(fator_agregacao)
focos_semanal.reset_index(inplace = True)
focos_semanal.sort_values(by = ["data"], inplace = True)
print(f"\n{green}FOCOS:\n{reset}{focos_semanal}\n")
print(f"\n{green}FOCOS:\n{reset}{focos_semanal.columns}\n")
focos_semanal_pivot = pd.pivot_table(focos_semanal, values = "focos", fill_value = 0,
									columns = "municipio", index = "data")
colunas = focos_semanal_pivot.columns
for c in colunas:
	focos_semanal_pivot[c] = focos_semanal_pivot[c].astype(int)
focos_semanal_pivot.reset_index(inplace = True)
focos_semanal_pivot = focos_semanal_pivot.rename(columns = {"data":"Semana"})
print(f"\n{green}FOCOS:\n{reset}{focos_semanal_pivot}\n")
print(f"\n{green}FOCOS:\n{reset}{focos_semanal_pivot.columns}\n")
focos_semanal_pivot.to_csv(f"{caminho_dados}focos_semanal_pivot.csv", index = False)
print(f"\n{green}SALVANDO FOCOS (semanal):\n{reset}{focos_semanal_pivot}\n")
sys.exit()


print(f"\n{green}INMET BD-MEP:\n{reset}{inmet}\n")
print(f"\n{green}INMET BD-MEP (diário):\n{reset}{inmet_diario}\n")
print(f"\n{green}INMET BD-MEP (semanal):\n{reset}{inmet_semanal}\n")
inmet.to_csv(f"{caminho_dados}inmet_horario.csv", index = False)
print(f"\n{green}SALVANDO INMET BD-MEP (horário):\n{reset}{inmet}\n")
inmet_diario.to_csv(f"{caminho_dados}inmet_diario.csv", index = False)
print(f"\n{green}SALVANDO INMET BD-MEP (diário):\n{reset}{inmet_diario}\n")
inmet_semanal.to_csv(f"{caminho_dados}inmet_semanal.csv", index = False)
print(f"\n{green}SALVANDO INMET BD-MEP (semanal):\n{reset}{inmet_semanal}\n")
