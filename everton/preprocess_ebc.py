###################################################
## Roteiro adaptado para pré-processar dados     ##
## Dados: meteorológicos (EBC_IFSC)              ##
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
jan25 = "IFLORI114-2025-01-01-2025-01-31.csv"
fev25 = "IFLORI114-2025-02-01-2025-02-28.csv"
mar25 = "IFLORI114-2025-03-01-2025-03-31.csv"
abr25 = "IFLORI114-2025-04-01-2025-04-30.csv"
mai25 = "IFLORI114-2025-05-01-2025-05-31.csv"
jun25 = "IFLORI114-2025-06-01-2025-06-30.csv"

##### ABRINDO ARQUIVOS ###########################################################
jan25 = pd.read_csv(f"{caminho_dados}{jan25}", sep = ";")#, skiprows = 10, sep = ";", decimal = ",")
fev25 = pd.read_csv(f"{caminho_dados}{fev25}", sep = ";")
mar25 = pd.read_csv(f"{caminho_dados}{mar25}", sep = ";")
abr25 = pd.read_csv(f"{caminho_dados}{abr25}", sep = ";")
mai25 = pd.read_csv(f"{caminho_dados}{mai25}", sep = ";")
jun25 = pd.read_csv(f"{caminho_dados}{jun25}", sep = ";")

### PRÉ-PROCESSAMENTO ############################################################
jan25["data"] = pd.to_datetime(jan25["Data"] + " " + jan25["Hora (Local)"].astype(str).str.zfill(4).str.slice_replace(2, 2, ":"), format="%d/%m/%Y %H:%M")
jan25.set_index("data", inplace = True)
jan25.drop(columns = ["Data", "Hora (Local)"], inplace = True)
print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{jan25}\n")

fev25["data"] = pd.to_datetime(fev25["Data"] + " " + fev25["Hora (Local)"].astype(str).str.zfill(4).str.slice_replace(2, 2, ":"), format="%d/%m/%Y %H:%M")
fev25.set_index("data", inplace = True)
fev25.drop(columns = ["Data", "Hora (Local)"], inplace = True)
print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{fev25}\n")

mar25["data"] = pd.to_datetime(mar25["Data"] + " " + mar25["Hora (Local)"].astype(str).str.zfill(4).str.slice_replace(2, 2, ":"), format="%d/%m/%Y %H:%M")
mar25.set_index("data", inplace = True)
mar25.drop(columns = ["Data", "Hora (Local)"], inplace = True)
print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{mar25}\n")

abr25["data"] = pd.to_datetime(abr25["Data"] + " " + abr25["Hora (Local)"].astype(str).str.zfill(4).str.slice_replace(2, 2, ":"), format="%d/%m/%Y %H:%M")
abr25.set_index("data", inplace = True)
abr25.drop(columns = ["Data", "Hora (Local)"], inplace = True)
print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{abr25}\n")

mai25["data"] = pd.to_datetime(mai25["Data"] + " " + mai25["Hora (Local)"].astype(str).str.zfill(4).str.slice_replace(2, 2, ":"), format="%d/%m/%Y %H:%M")
mai25.set_index("data", inplace = True)
mai25.drop(columns = ["Data", "Hora (Local)"], inplace = True)
print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{mai25}\n")

jun25["data"] = pd.to_datetime(jun25["Data"] + " " + jun25["Hora (Local)"].astype(str).str.zfill(4).str.slice_replace(2, 2, ":"), format="%d/%m/%Y %H:%M")
jun25.set_index("data", inplace = True)
jun25.drop(columns = ["Data", "Hora (Local)"], inplace = True)
print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{jun25}\n")

sys.exit()


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
