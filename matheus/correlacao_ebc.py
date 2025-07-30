###################################################
## Roteiro adaptado para correlacionar dados     ##
## entomo-epidemiológicos (Aedes sp. e dengue)   ##
## Dados: Focos (Dive), Casos Prováveis (Dive) e ##
##        meteorológicos (EBC_IFSC e INMET_BDMEP)##
## Demanda: FAPESC edital nº 37/2024             ##
## Adaptado por: Matheus Ferreira de Souza       ##
##               e Everton Weber Galliani        ##
## Data: 29/07/2025                              ##
###################################################

##### Bibliotecas correlatas ####################################################
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
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
caminho_dados = "/home/sifapsc/scripts/matheus/fapesc_dengue/everton/"
caminho_dive = "/home/sifapsc/scripts/matheus/dados_dengue/"
caminho_resultado = f"/home/sifapsc/scripts/matheus/fapesc_dengue/matheus/resultado/"
focos = "focos_pivot.csv"
casos = "casos_dive_pivot_total.csv"
# ARQ.METEO

##### ABRINDO ARQUIVOS ###########################################################
casos = pd.read_csv(f"{caminho_dive}{casos}")
focos = pd.read_csv(f"{caminho_dive}{focos}")


### PRÉ-PROCESSAMENTO ############################################################
casos = casos[["Semana", "FLORIANÓPOLIS"]].rename(columns = {"FLORIANÓPOLIS":"casos"})
focos = focos[["Semana", "FLORIANÓPOLIS"]].rename(columns = {"FLORIANÓPOLIS":"focos"})
print(f"\n{green}CASOS:\n{reset}{casos}\n")
print(f"\n{green}FOCOS:\n{reset}{focos}\n")

### MONTAGEM DE DATASET ##########################################################
_RETROAGIR = 4
dataset = casos.merge(focos, on = "Semana", how = "inner")
dataset[["casos", "focos"]] = dataset[["casos", "focos"]].astype(int)
print(f"\n{green}DATASET:\n{reset}{dataset}\n")
for r in range(1, _RETROAGIR + 1):
	dataset[f"casos_r{r}"] = dataset["casos"].shift(-r)
	dataset[f"focos_r{r}"] = dataset["focos"].shift(-r)
dataset.dropna(inplace = True)
print(f"\n{green}DATASET:\n{reset}{dataset}\n")
dataset.columns.name = "FLORIANÓPOLIS"
dataset.set_index("Semana", inplace = True)

### CORRELAÇÕES ##################################################################
lista_metodos = ["pearson", "spearman", "kendall"]
for _METODO in lista_metodos:
	correlacao_dataset = dataset.corr(method = f"{_METODO}")
	print("="*80)
	print(f"Método de {_METODO.title()} \n", correlacao_dataset)
	print("="*80)
	fig, ax = plt.subplots(figsize = (10, 6), layout = "constrained", frameon = False)
	filtro = np.triu(np.ones_like(correlacao_dataset, dtype = bool), k = 1)
	sns.heatmap(correlacao_dataset, annot = True, cmap = "Spectral",
				vmin = -1, vmax = 1, linewidth = 0.5, mask = filtro)
	ax.set_yticklabels(ax.get_yticklabels(), rotation = "horizontal")
	fig.suptitle(f"MATRIZ DE CORRELAÇÃO* entre \n FOCOS E CASOS EM FLORIANÓPOLIS \n*(Método de {_METODO.title()}; retroagindo {_RETROAGIR} semanas)", weight = "bold", size = "medium") #durante {_ANO};
	#plt.savefig(f'{caminho_correlacao}correlacao_casos_{__CIDADE}_.pdf', format = "pdf", dpi = 1200,  bbox_inches = "tight", pad_inches = 0.0)
	plt.show()


sys.exit()
censo.to_csv(caminho_resultado)
print(f"\n{green}SALVANDO:\n{reset}{caminho_resultado}\n")
