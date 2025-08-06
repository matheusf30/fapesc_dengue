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
#focos = "focos 2025.xlsx"
#casos = "casos_se32_25.xlsx"
focos = "focos_se32_25.xlsx"

##### ABRINDO ARQUIVOS ###########################################################
casos = pd.read_excel(f"{caminho_dados}{casos}", engine = "openpyxl")
focos = pd.read_excel(f"{caminho_dados}{focos}", engine = "openpyxl")

### PRÉ-PROCESSAMENTO ############################################################
### CASOS
print(f"\n{green}CASOS:\n{reset}{casos}\n")
print(f"\n{green}CASOS:\n{reset}{casos.columns}\n")
colunas_renomear = {"ID_AGRAVO":"doenca",
					"DT_NOTIFIC":"data_notificacao",
					"DT_SIN_PRI":"data_sintoma",
					"ID_MUNICIP":"municipio",
					"ID_REGIONA":"regional",
					"CLASSI_FIN":"classificacao",
					"CRITERIO":"criterio ",
					"SOROTIPO":"sorotipo"}
casos = casos.rename(columns = colunas_renomear)
casos = casos[["data_sintoma", "data_notificacao", "municipio", "regional",
				"doenca", "classificacao", "criterio ", "sorotipo"]]
print(f"\n{green}CASOS:\n{reset}{casos}\n")
print(f"\n{green}CASOS:\n{reset}{casos.columns}\n")
"""
REVER ESSAS DATAS (ANONIMIZADAS?)
CASOS:
        data_sintoma  data_notificacao  municipio  regional doenca  classificacao  criterio   sorotipo
0              45738             45743     420757    1546.0    A90            5.0        1.0       NaN
1              45746             45746     420910    1565.0    A90            5.0        1.0       NaN
2              45729             45731     420210    1565.0    A90            5.0        2.0       NaN
3              45735             45736     420820    1550.0    A90           10.0        1.0       NaN
4              45737             45740     421600    1553.0    A90            5.0        1.0       NaN
...              ...               ...        ...       ...    ...            ...        ...       ...
113082         45783             45784     420540    1476.0    A90            8.0        NaN       NaN
113083         45791             45792     420540    1476.0    A90            8.0        NaN       NaN
113084         45839             45839     420540    1476.0    A90            NaN        NaN       NaN
113085         45764             45766     420540    1476.0    A90            8.0        NaN       NaN
113086         45773             45777     420540    1476.0    A90            8.0        NaN       NaN
"""
casos["data_sintoma"] = pd.to_datetime(casos["data_sintoma"],
										unit = "D", origin = "1899-12-30")
casos["data_notificacao"] = pd.to_datetime(casos["data_notificacao"],
										unit = "D", origin = "1899-12-30")
casos["casos"] = np.ones(len(casos)).astype(int)
print(f"\n{green}CASOS:\n{reset}{casos}\n")
print(f"\n{green}CASOS:\n{reset}{casos.columns}\n")
casos["data_sintoma"] = pd.to_datetime(casos["data_sintoma"], format = "%Y-%m-%d", errors = "coerce")
casos_agrupados = casos.groupby(["data_sintoma", "municipio"]).sum(numeric_only = True)["casos"]
casos = casos_agrupados.reset_index()
casos.columns = ["data_sintoma", "municipio", "casos"]
"""
casos = casos.rename(columns = {"data_sintoma":"data"})
casos["data"] = pd.to_datetime(casos["data"], format = "%d/%m/%Y", errors = "coerce")

fator_agregacao = {"casos":"sum"}
casos_semanal = casos.groupby("municipio").resample("W").agg(fator_agregacao)
casos_semanal.reset_index(inplace = True)
casos_semanal.sort_values(by = ["data"], inplace = True)
casos_semanal_pivot = pd.pivot_table(casos_semanal, values = "casos", fill_value = 0,
									columns = "municipio", index = "data")
colunas = casos_semanal_pivot.columns
for c in colunas:
	casos_semanal_pivot[c] = casos_semanal_pivot[c].astype(int)
casos_semanal_pivot.reset_index(inplace = True)
casos_semanal_pivot = casos_semanal_pivot.rename(columns = {"data":"Semana"})
"""
print(f"\n{green}CASOS:\n{reset}{casos}\n")
print(f"\n{green}CASOS:\n{reset}{casos.columns}\n")
sys.exit()

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
