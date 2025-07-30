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

##### DEFININDO FUNÇÕES ##########################################################
def abrir_arquivo(entrada):
	saida = pd.read_csv(f"{caminho_dados}{entrada}", sep = ";")
	saida["data"] = pd.to_datetime(saida["Data"] + " " + saida["Hora (Local)"].astype(str).str.zfill(4).str.slice_replace(2, 2, ":"), format="%d/%m/%Y %H:%M")
	saida.set_index("data", inplace = True)
	saida.drop(columns = ["Data", "Hora (Local)"], inplace = True)
	print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{saida}\n")
	return saida

def renomear_colunas(entrada):
	print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{entrada.columns}\n")
	colunas_renomear = {"Temp. Med. (C)":"tmed", "Temp. Max. (C)":"tmax", "Temp. Min. (C)":"tmin",
			"Umi. Med. (%)":"urar", "Umi. Max. (%)":"urmax", "Umi. Min. (%)":"urmin",
			"Pto Orvalho Med. (C)":"tomed", "Pto Orvalho Max. (C)":"tomax", "Pto Orvalho Min. (C)":"tomin",
			"Pressao Tend. (hPa)":"pmed", "Pressao Max. (hPa)":"pmax", "Pressao Min. (hPa)":"pmin",
			"Vel. Vento (m/s)":"ventovel", "Dir. Vento (graus)":"ventodir",
			"Raj. Vento (m/s)":"rajada", "Radiacao (KJ/m²)":"rad","Chuva (mm)":"prec"}
	saida = entrada.rename(columns = colunas_renomear)
	print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{saida}\n")
	print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{saida.columns}\n")
	return saida

def diario_semanal(entrada):
	print(f"\n{green}EBC IFSC FLORIPA (horário):\n{reset}{entrada}\n")
	fator_agregacao = {"rad":"mean", "pmax":"max", "pmin":"min", "pmed":"mean",
					"tmax":"max", "tmin":"min", "tmed":"mean",
					"tomax":"max", "tomin":"min", "tomed":"mean",
					"prec":"sum", "urar":"mean", "urmax":"max", "urmin":"min",
					"ventodir":"mean", "rajada":"max", "ventovel":"mean"}
	diario = entrada.resample("D").agg(fator_agregacao)
	print(f"\n{green}EBC IFSC FLORIPA (diário):\n{reset}{diario}\n")
	semanal = entrada.resample("W").agg(fator_agregacao)
	print(f"\n{green}EBC IFSC FLORIPA (semanal):\n{reset}{semanal}\n")
	return diario, semanal


##### ABRINDO ARQUIVOS ###########################################################
jan25 = abrir_arquivo(jan25)
fev25 = abrir_arquivo(fev25)
mar25 = abrir_arquivo(mar25)
abr25 = abrir_arquivo(abr25)
mai25 = abrir_arquivo(mai25)
jun25 = abrir_arquivo(jun25)

### PRÉ-PROCESSAMENTO ############################################################
jan25 = renomear_colunas(jan25)
fev25 = renomear_colunas(fev25)
mar25 = renomear_colunas(mar25)
abr25 = renomear_colunas(abr25)
mai25 = renomear_colunas(mai25)
jun25 = renomear_colunas(jun25)
jan25_diario, jan25_semanal = diario_semanal(jan25)
fev25_diario, fev25_semanal = diario_semanal(fev25)
mar25_diario, mar25_semanal = diario_semanal(mar25)
abr25_diario, abr25_semanal = diario_semanal(abr25)
mai25_diario, mai25_semanal = diario_semanal(mai25)
jun25_diario, jun25_semanal = diario_semanal(jun25)

sys.exit()
sys.exit()


### Tratar NaN (NECESSÁRIO?)
inmet.dropna(inplace = True) # REVER ESSE PASSO  (48192 X 20) >> (45202 X 20)
inmet["data"] = pd.to_datetime(inmet["dia"] + " " + inmet["hora"].astype(str).str.zfill(4).str.slice_replace(2, 2, ":"))
inmet.set_index("data", inplace=True)
inmet.drop(["dia", "hora"], axis=1, inplace=True)
colunas = inmet.columns
for col in colunas:
	inmet[col] = inmet[col].astype(str).str.replace(",", ".").astype(float)
print(f"\n{green}INMET BD-MEP:\n{reset}{inmet.columns}\n")
print(f"\n{green}INMET BD-MEP:\n{reset}{inmet}\n")

print(f"\n{green}INMET BD-MEP:\n{reset}{inmet}\n")
print(f"\n{green}INMET BD-MEP (diário):\n{reset}{inmet_diario}\n")
print(f"\n{green}INMET BD-MEP (semanal):\n{reset}{inmet_semanal}\n")
inmet.to_csv(f"{caminho_dados}inmet_horario.csv", index = False)
print(f"\n{green}SALVANDO INMET BD-MEP (horário):\n{reset}{inmet}\n")
inmet_diario.to_csv(f"{caminho_dados}inmet_diario.csv", index = False)
print(f"\n{green}SALVANDO INMET BD-MEP (diário):\n{reset}{inmet_diario}\n")
inmet_semanal.to_csv(f"{caminho_dados}inmet_semanal.csv", index = False)
print(f"\n{green}SALVANDO INMET BD-MEP (semanal):\n{reset}{inmet_semanal}\n")
