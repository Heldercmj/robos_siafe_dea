import pandas as pd

# Carregar o CSV
df = pd.read_csv('T3_202405081037.csv')

# Salvar como XLSX
df.to_excel('Listagem de processos a serem rodados - DEA 2023.xlsx', index=False)
