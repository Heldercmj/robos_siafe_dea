import pandas as pd

# Carregar o CSV
df = pd.read_csv('PROCESSOS RODADOS GERAL.csv')

# Salvar como XLSX
df.to_excel('DEA 2023 - PROCESSOS RODADOS GERAL.xlsx', index=False)
