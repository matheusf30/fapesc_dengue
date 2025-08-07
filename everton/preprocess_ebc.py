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
#2022
jan22 = "IFLORI114-2022-01-01-2022-01-31.csv"
fev22 = "IFLORI114-2022-02-01-2022-02-28.csv"
mar22 = "IFLORI114-2022-03-01-2022-03-31.csv"
abr22 = "IFLORI114-2022-04-01-2022-04-30.csv"
mai22 = "IFLORI114-2022-05-01-2022-05-31.csv"
jun22 = "IFLORI114-2022-06-01-2022-06-30.csv"
jul22 = "IFLORI114-2022-07-01-2022-07-31.csv"
ago22 = "IFLORI114-2022-08-01-2022-08-31.csv"
set22 = "IFLORI114-2022-09-01-2022-09-30.csv"
out22 = "IFLORI114-2022-10-01-2022-10-31.csv"
nov22 = "IFLORI114-2022-11-01-2022-11-30.csv"
dez22 = "IFLORI114-2022-12-01-2022-12-31.csv"
#2023
jan23 = "IFLORI114-2023-01-01-2023-01-31.csv"
fev23 = "IFLORI114-2023-02-01-2023-02-28.csv"
mar23 = "IFLORI114-2023-03-01-2023-03-31.csv"
abr23 = "IFLORI114-2023-04-01-2023-04-30.csv"
mai23 = "IFLORI114-2023-05-01-2023-05-31.csv"
jun23 = "IFLORI114-2023-06-01-2023-06-30.csv"
jul23 = "IFLORI114-2023-07-01-2023-07-31.csv"
ago23 = "IFLORI114-2023-08-01-2023-08-31.csv"
set23 = "IFLORI114-2023-09-01-2023-09-30.csv"
out23 = "IFLORI114-2023-10-01-2023-10-31.csv"
nov23 = "IFLORI114-2023-11-01-2023-11-30.csv"
dez23 = "IFLORI114-2023-12-01-2023-12-31.csv"
#2024
jan24 = "IFLORI114-2024-01-01-2024-01-31.csv"
fev24 = "IFLORI114-2024-02-01-2024-02-29.csv"
mar24 = "IFLORI114-2024-03-01-2024-03-31.csv"
abr24 = "IFLORI114-2024-04-01-2024-04-30.csv"
mai24 = "IFLORI114-2024-05-01-2024-05-31.csv"
jun24 = "IFLORI114-2024-06-01-2024-06-30.csv"
jul24 = "IFLORI114-2024-07-01-2024-07-31.csv"
ago24 = "IFLORI114-2024-08-01-2024-08-31.csv"
set24 = "IFLORI114-2024-09-01-2024-09-30.csv"
out24 = "IFLORI114-2024-10-01-2024-10-31.csv"
nov24 = "IFLORI114-2024-11-01-2024-11-30.csv"
dez24 = "IFLORI114-2024-12-01-2024-12-31.csv"
#2025
jan25 = "IFLORI114-2025-01-01-2025-01-31.csv"
fev25 = "IFLORI114-2025-02-01-2025-02-28.csv"
mar25 = "IFLORI114-2025-03-01-2025-03-31.csv"
abr25 = "IFLORI114-2025-04-01-2025-04-30.csv"
mai25 = "IFLORI114-2025-05-01-2025-05-31.csv"
jun25 = "IFLORI114-2025-06-01-2025-06-30.csv"

##### DEFININDO FUNÇÕES ##########################################################
def abrir_arquivo(entrada):
	"""
	Função para abrir arquivos de Estações de Baixo Custo (EBC).
	Dados provenientes do projeto 'Meteorologia na Palma da Mão' (IFSC).
	Entrada: Arquivo (.csv).
	Saída: DataFrame
	"""
	saida = pd.read_csv(f"{caminho_dados}{entrada}", sep = ";")
	saida["data"] = pd.to_datetime(saida["Data"] + " " + saida["Hora (Local)"].astype(str).str.zfill(4).str.slice_replace(2, 2, ":"), format="%d/%m/%Y %H:%M")
	saida.set_index("data", inplace = True)
	saida.drop(columns = ["Data", "Hora (Local)"], inplace = True)
	print(f"\n{green}ABRINDO ARQUIVO\n\nEBC IFSC FLORIPA:\n{reset}{saida}\n")
	saida = renomear_colunas(saida)
	info_adicionais(saida)
	return saida

def info_adicionais(entrada):
	"""
	Função para descrever algumas informações.
	Entrada: Arquivo (DataFrame/Series) a ser analisado.
	Saída: Visualização de Informações (Tipos de Variáveis, Colunas, Estatística Básica...) 
	"""
	print(f"{red}={reset}"*80)
	print(f"\n{cyan}ESTAÇÃO DE BAIXO CUSTO (IFSC) METEOROLOGIA NA PALMA DA MÃO{reset}\n")
	print(f"\n{green}INFO BÁSICA:\n{reset}{entrada.info()}\n")
	print(f"\n{green}ESTATÍSTICA BÁSICA:\n{reset}{entrada.describe()}\n")
	print(f"\n{green}COLUNAS PRESENTES:\n{reset}{entrada.columns}\n")
	print(f"{red}={reset}"*80)

def concatenar_arquivos(m1 = None, m2 = None, m3 = None, m4 = None, m5 = None, m6 = None,
						m7 = None, m8 = None, m9 = None, m10 = None, m11 = None, m12 = None):
	"""
	Função para concatenar arquivos (DataFrames).
	Entrada: DataFrames (até 12 meses podem ser concatenados em arquivo anual)
	Saída: DataFrame (concatenação dos arquivos disponíveis) 
	"""
	lista_arquivos = [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12]
	saida = pd.concat(lista_arquivos)
	print(f"\n{green}ARQUIVO CONCATENADO:\n{reset}{saida}\n")
	info_adicionais(saida)
	return saida

def renomear_colunas(entrada):
	"""
	Função para renomear colunas do DataFrames conforme dicionário colunas_renomear.
	Entrada: DataFrame
	Saída: DataFrame (Renomeação das colunas de acordo com dicionário) 
	"""
	print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{entrada.columns}\n")
	colunas_renomear = {"Temp. Med. (C)":"tmed", "Temp. Max. (C)":"tmax", "Temp. Min. (C)":"tmin",
			"Umi. Med. (%)":"urar", "Umi. Max. (%)":"urmax", "Umi. Min. (%)":"urmin",
			"Pto Orvalho Med. (C)":"tomed", "Pto Orvalho Max. (C)":"tomax", "Pto Orvalho Min. (C)":"tomin",
			"Pressao Tend. (hPa)":"pmed", "Pressao Max. (hPa)":"pmax", "Pressao Min. (hPa)":"pmin",
			"Vel. Vento (m/s)":"ventovel", "Dir. Vento (graus)":"ventodir",
			"Raj. Vento (m/s)":"rajada", "Radiacao (KJ/m²)":"rad","Chuva (mm)":"prec"}
	colunas = entrada.columns
	info_adicionais(entrada)
	for c in colunas:
		#entrada[c] = entrada[c].astype(str).str.replace(".","").astype(float)
		entrada[c] = pd.to_numeric(entrada[c].astype(str).str.replace(",", "."), errors = "coerce")
		saida = entrada.rename(columns = colunas_renomear)
	info_adicionais(saida)
	print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{saida}\n")
	print(f"\n{green}EBC IFSC FLORIPA:\n{reset}{saida.columns}\n")
	return saida

def diario_semanal(entrada):
	"""
	Função para reorganizar/reagrupar o tempo ('data') do DataFrame.
	Reagrupa em duas frequências: diária e semanal.
	Entrada: DataFrame (possivelmente em frequência horária)
	Saída: DataFrame_diario, DataFrame_semanal (data reagrupada) 
	"""
	print(f"\n{green}EBC IFSC FLORIPA (horário):\n{reset}{entrada}\n")
	fator_agregacao = {"rad":"mean", "pmax":"max", "pmin":"min", "pmed":"mean",
					"tmax":"max", "tmin":"min", "tmed":"mean",
					"tomax":"max", "tomin":"min", "tomed":"mean",
					"prec":"sum", "urar":"mean", "urmax":"max", "urmin":"min",
					"ventodir":"mean", "rajada":"max", "ventovel":"mean"}
	diario = entrada.resample("D").agg(fator_agregacao)
	info_adicionais(diario)
	print(f"\n{green}EBC IFSC FLORIPA (diário):\n{reset}{diario}\n")
	diario.to_csv(f"{caminho_dados}ebc_floripa_diario.csv", index = False)
	print(f"\n{green}SALVANDO EBC IFSC FLORIPA (diário):\n{reset}{caminho_dados}ebc_floripa_diario.csv\n")
	semanal = entrada.resample("W").agg(fator_agregacao)
	info_adicionais(semanal)
	print(f"\n{green}EBC IFSC FLORIPA (semanal):\n{reset}{semanal}\n")
	semanal.to_csv(f"{caminho_dados}ebc_floripa_semanal.csv", index = False)
	print(f"\n{green}SALVANDO EBC IFSC FLORIPA (semanal):\n{reset}{caminho_dados}ebc_floripa_semanal.csv")
	return diario, semanal

##### ABRINDO ARQUIVOS ###########################################################
#2022
jan22 = abrir_arquivo(jan22)
fev22 = abrir_arquivo(fev22)
mar22 = abrir_arquivo(mar22)
abr22 = abrir_arquivo(abr22)
mai22 = abrir_arquivo(mai22)
jun22 = abrir_arquivo(jun22)
jul22 = abrir_arquivo(jul22)
ago22 = abrir_arquivo(ago22)
set22 = abrir_arquivo(set22)
out22 = abrir_arquivo(out22)
nov22 = abrir_arquivo(nov22)
dez22 = abrir_arquivo(dez22)
#2023
jan23 = abrir_arquivo(jan23)
fev23 = abrir_arquivo(fev23)
mar23 = abrir_arquivo(mar23)
abr23 = abrir_arquivo(abr23)
mai23 = abrir_arquivo(mai23)
jun23 = abrir_arquivo(jun23)
jul23 = abrir_arquivo(jul23)
ago23 = abrir_arquivo(ago23)
set23 = abrir_arquivo(set23)
out23 = abrir_arquivo(out23)
nov23 = abrir_arquivo(nov23)
dez23 = abrir_arquivo(dez23)
#2024
jan24 = abrir_arquivo(jan24)
fev24 = abrir_arquivo(fev24)
mar24 = abrir_arquivo(mar24)
abr24 = abrir_arquivo(abr24)
mai24 = abrir_arquivo(mai24)
jun24 = abrir_arquivo(jun24)
jul24 = abrir_arquivo(jul24)
ago24 = abrir_arquivo(ago24)
set24 = abrir_arquivo(set24)
out24 = abrir_arquivo(out24)
nov24 = abrir_arquivo(nov24)
dez24 = abrir_arquivo(dez24)
#2025
jan25 = abrir_arquivo(jan25)
fev25 = abrir_arquivo(fev25)
mar25 = abrir_arquivo(mar25)
abr25 = abrir_arquivo(abr25)
mai25 = abrir_arquivo(mai25)
jun25 = abrir_arquivo(jun25)

### PRÉ-PROCESSAMENTO ############################################################
ebc22 = concatenar_arquivos(jan22, fev22, mar22, abr22, mai22, jun22,
							jul22, ago22, set22, out22, nov22, dez22)
ebc23 = concatenar_arquivos(jan23, fev23, mar23, abr23, mai23, jun23,
							jul23, ago23, set23, out23, nov23, dez23)
ebc24 = concatenar_arquivos(jan24, fev24, mar24, abr24, mai24, jun24,
							jul24, ago24, set24, out24, nov24, dez24)
ebc25 = concatenar_arquivos(jan25, fev25, mar25, abr25, mai25, jun25)
ebc_floripa = concatenar_arquivos(ebc22, ebc23, ebc24, ebc25)
ebc_floripa.to_csv(f"{caminho_dados}ebc_floripa_horario.csv", index = False)
print(f"\n{green}SALVANDO EBC IFSC FLORIPA (horário):\n{reset}{caminho_dados}ebc_floripa_horario.csv\n")

ebc_floripa_diario, ebc_floripa_semanal = diario_semanal(ebc_floripa)

sys.exit()


### Tratar NaN (NECESSÁRIO?)
ebc_floripa.dropna(inplace = True) # REVER ESSE PASSO  (48192 X 20) >> (45202 X 20)

