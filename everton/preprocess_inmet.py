###################################################
## Roteiro adaptado para pré-processar dados     ##
## Dados: meteorológicos (INMET_BDMEP)           ##
## Demanda: FAPESC edital nº 37/2024             ##
## Adaptado por: Matheus Ferreira de Souza       ##
##               e Everton Weber Galliani        ##
## Data: 30/07/2025                              ##
###################################################

##### Bibliotecas correlatas ####################################################
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
caminho_dados = "/home/sifapsc/scripts/matheus/fapesc_dengue/everton/dados/"
inmet = "dados_A806_H_2020-01-01_2025-06-30.csv"
#https://raw.githubusercontent.com/matheusf30/fapesc_dengue/refs/heads/main/everton/dados/dados_A806_H_2020-01-01_2025-06-30.csv

##### ABRINDO ARQUIVOS ###########################################################
inmet = pd.read_csv(f"{caminho_dados}{inmet}", skiprows = 10, sep = ";", decimal = ",")

### PRÉ-PROCESSAMENTO ############################################################
print(f"\n{green}INMET BD-MEP:\n{reset}{inmet.columns}\n")
colunas_renomear = {"Data Medicao":"dia", "Hora Medicao":"hora",
		"PRECIPITACAO TOTAL, HORARIO(mm)":"prec", "RADIACAO GLOBAL(Kj/m²)":"rad",
		"PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA(mB)":"p_estacao",
		"PRESSAO ATMOSFERICA REDUZIDA NIVEL DO MAR, AUT(mB)":"p_mar",
		"PRESSAO ATMOSFERICA MAX.NA HORA ANT. (AUT)(mB)":"pmax",
		"PRESSAO ATMOSFERICA MIN. NA HORA ANT. (AUT)(mB)":"pmin",
		"TEMPERATURA DO AR - BULBO SECO, HORARIA(°C)":"t_seco",
		"TEMPERATURA DO PONTO DE ORVALHO(°C)":"t_orvalho",
		"TEMPERATURA MAXIMA NA HORA ANT. (AUT)(°C)":"tmax",
		"TEMPERATURA MINIMA NA HORA ANT. (AUT)(°C)":"tmin",
		"TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT)(°C)":"tomax",
		"TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT)(°C)":"tomin",
		"UMIDADE REL. MAX. NA HORA ANT. (AUT)(%)":"urmax",
		"UMIDADE REL. MIN. NA HORA ANT. (AUT)(%)":"urmin",
		"UMIDADE RELATIVA DO AR, HORARIA(%)":"urar",
		"VENTO, DIRECAO HORARIA (gr)(° (gr))":"ventodir", "VENTO, VELOCIDADE HORARIA(m/s)":"ventovel",
		"VENTO, RAJADA MAXIMA(m/s)":"rajada", "Unnamed: 20":"EXCLUIR"}
inmet = inmet.rename(columns = colunas_renomear)
print(f"\n{green}INMET BD-MEP:\n{reset}{inmet.columns}\n")
inmet.drop(columns = ["EXCLUIR"], inplace = True)
inmet.dropna(inplace = True) # REVER ESSE PASSO  (48192 X 20) >> (45202 X 20)
inmet["data"] = pd.to_datetime(inmet["dia"] + " " + inmet["hora"].astype(str).str.zfill(4).str.slice_replace(2, 2, ":"))
inmet.set_index("data", inplace=True)
inmet.drop(["dia", "hora"], axis=1, inplace=True)
colunas = inmet.columns
for col in colunas:
	inmet[col] = inmet[col].astype(str).str.replace(",", ".").astype(float)
print(f"\n{green}INMET BD-MEP:\n{reset}{inmet.columns}\n")
print(f"\n{green}INMET BD-MEP:\n{reset}{inmet}\n")
fator_agregacao = {"rad":"mean", "p_estacao":"mean", "p_mar":"mean", "pmax":"max", "pmin":"min",
				"t_seco":"mean", "tmax":"max", "tmin":"min",
				"t_orvalho":"mean", "tomax":"max", "tomin":"min",
				"prec":"sum", "urar":"mean", "urmax":"max", "urmin":"min",
				"ventodir":"mean", "rajada":"max", "ventovel":"mean"}
inmet_diario = inmet.resample("D").agg(fator_agregacao)
inmet_semanal = inmet.resample("W").agg(fator_agregacao)
print(f"\n{green}INMET BD-MEP:\n{reset}{inmet}\n")
print(f"\n{green}INMET BD-MEP (diário):\n{reset}{inmet_diario}\n")
print(f"\n{green}INMET BD-MEP (semanal):\n{reset}{inmet_semanal}\n")
inmet.to_csv(f"{caminho_dados}inmet_horario.csv", index = False)
print(f"\n{green}SALVANDO INMET BD-MEP (horário):\n{reset}{inmet}\n")
inmet_diario.to_csv(f"{caminho_dados}inmet_diario.csv", index = False)
print(f"\n{green}SALVANDO INMET BD-MEP (diário):\n{reset}{inmet_diario}\n")
inmet_semanal.to_csv(f"{caminho_dados}inmet_semanal.csv", index = False)
print(f"\n{green}SALVANDO INMET BD-MEP (semanal):\n{reset}{inmet_semanal}\n")
