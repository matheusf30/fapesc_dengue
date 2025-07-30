###################################################
## Roteiro adaptado para estruturar dados        ##
## Dados: Regional de Saúde (.pdf), Macrorreg.,  ##
##        Censo IBGE 2022 e Regional D.TK5 (.csv)##
## Demanda: FAPESC edital nº 37/2024             ##
## Adaptado por: Matheus Ferreira de Souza       ##
##               e Domênica Tcacenco             ##
## Data: 29/07/2025                              ##
###################################################

##### Bibliotecas correlatas ####################################################
import pandas as pd
import geopandas as gpd
import os, sys, unicodedata
from unidecode import unidecode

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
resultado = "censo_sc_regional.csv"
caminho_resultado = f"/home/sifapsc/scripts/matheus/fapesc_dengue/matheus/{resultado}"
censo = "censo_sc_xy.csv"
caminho_shape = "/home/sifapsc/scripts/matheus/dados_dengue/shapefiles/"
municipios = "SC_Municipios_2022.shp"

##### ABRINDO ARQUIVOS ###########################################################
regional = pd.read_csv(caminho_arquivo)
censo = pd.read_csv(censo)
municipios = gpd.read_file(f"{caminho_shape}{municipios}")

### PRÉ-PROCESSAMENTO ############################################################
print(f"\n{green}ARQUIVO:\n{reset}{regional}\n")
regional = regional.melt(var_name = "regional", value_name = "municipio")
regional = regional.dropna(subset = ["municipio"])
regional["regional"] = regional["regional"].str.strip()
regional["municipio"] = regional["municipio"].str.strip()
regional = regional.sort_values(by = ["regional", "municipio"]).reset_index(drop = True)
print(f"\n{green}ARQUIVO:\n{reset}{regional}\n")
print(f"\n{green}REGIONAIS:\n{reset}{regional['regional'].unique()}\n")
print(f"\n{green}MUNICÍPIOS:\n{reset}{regional['municipio'].unique()}\n")
censo["Municipio"] = censo["Municipio"].str.upper()
censo["tratado"] = censo["Municipio"].apply(unidecode)
print(f"\n{green}CENSO IBGE 2022:\n{reset}{censo}\n")
print(f"\n{green}municipios\n{reset}{municipios}\n")
mun_dict = censo[["Municipio", "tratado"]].set_index("Municipio")
mun_dict = mun_dict.to_dict()
print(f"\n{green}MUNICÍPIOS (dict):\n{reset}{mun_dict}\n")
censo = censo.merge(regional, left_on = "tratado", right_on = "municipio", how = "inner")
censo.drop(columns = ["tratado", "municipio"], inplace = True)
macro = {"XANXERÊ":"GRANDE OESTE",
		"OESTE":"GRANDE OESTE",
		"EXTREMO OESTE":"GRANDE OESTE",
		"ALTO URUGUAI CATARINENSE":"MEIO OESTE",
		"ALTO VALE DO RIO DO PEIXE":"MEIO OESTE",
		"MEIO OESTE":"MEIO OESTE",
		"PLANALTO NORTE":"PLANALTO NORTE",
		"NORDESTE":"NORDESTE",
		"VALE DO ITAPOCU":"NORDESTE",
		"ALTO VALE DO ITAJAÍ":"VALE DO ITAJAÍ",
		"MÉDIO VALE DO ITAJAÍ":"VALE DO ITAJAÍ",
		"FOZ DO RIO ITAJAÍ":"FOZ DO RIO ITAJAÍ",
		"GRANDE FLORIANÓPOLIS":"GRANDE FLORIANÓPOLIS",
		"SERRA CATARINENSE":"SERRA CATARINENSE",
		"CARBONÍFERA":"SUL",
		"EXTREMO SUL CATARINENSE":"SUL",
		"LAGUNA":"SUL"}
censo["macro"] = censo["regional"].map(macro)
censo = censo[["Municipio", "Populacao", "macro", "regional", "lat", "lon"]]
censo = censo.sort_values(by = ["macro", "regional", "Municipio"]).reset_index(drop = True)
print(f"\n{green}CENSO IBGE 2022:\n{reset}{censo}\n")
print(f"\n{green}CENSO IBGE 2022:\n{reset}{censo.columns}\n")
"""
lista_municipios_pdf = list(regional['municipio'].unique())
lista_municipios_censo = list(censo['Municipio'].unique())
lista_municipios_pdf = sorted(lista_municipios_pdf)
lista_municipios_censo = sorted(lista_municipios_censo)
print(f"\n{green}MUNICÍPIOS (pdf):\n{reset}{lista_municipios_pdf}\n")
print(f"\n{green}MUNICÍPIOS (censo):\n{reset}{lista_municipios_censo}\n")
"""
#sys.exit()
censo.to_csv(caminho_resultado)
print(f"\n{green}SALVANDO:\n{reset}{caminho_resultado}\n")
