from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from dotenv import dotenv_values
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from datetime import datetime
from selenium.common.exceptions import NoSuchElementException
# from IPython.display import display

# import pandas as pd
import time
import cx_Oracle
import logging
import os
import traceback
import json
import requests

env_path = os.path.join(os.path.dirname(__file__), ".env")

# Configurando arquivo de log
logging.basicConfig(filename='error_robo_empenho_dea.log', level=logging.ERROR, 
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')

# Obter a data atual
data_corrente = datetime.now()
# Formatar a data no estilo DD/MM/AAAA
data_atual = data_corrente.strftime('%d/%m/%Y')
# Imprimir a data formatada
print(data_atual)

# Configurações de ambiente utilizando o arquivo .env
config = dotenv_values(".env")

# Dados para configurações de homologação
config_homologacao = {
    "url_siafe": config["URL_SIAFE_HOMOLOGACAO"],
    "url_api_siafe": config["URL_API_SIAFE_HOMOLOGACAO"],
    "usuario_siafe": config["USUARIO_SIAFE_HOMOLOGACAO"],
    "senha_siafe": config["SENHA_SIAFE_HOMOLOGACAO"],
    "db_user": config["DB_USER"],
    "db_password": config["DB_PASSWORD"],
    "db_dsn": config["DB_DSN"],
    "url_pagina_inicial": config["URL_SIAFE_HOMOLOGACAO_INICIAL"],
    "url_pagina_consulta_empenho": config["URL_SIAFE_HOMOLOGACAO_CONSULTA_EMPENHO"],
}

# Dados para configurações de produção
config_producao = {
    "url_siafe": config["URL_SIAFE_PRODUCAO"],
    "url_api_siafe": config["URL_API_SIAFE_PRODUCAO"],
    "usuario_siafe": config["USUARIO_SIAFE_PRODUCAO"],
    "senha_siafe": config["SENHA_SIAFE_PRODUCAO"],
    "db_user": config["DB_USER_PRODUCAO"],
    "db_password": config["DB_PASSWORD_PRODUCAO"],
    "db_dsn": config["DB_DSN_PRODUCAO"],
    "url_pagina_inicial": config["URL_SIAFE_PRODUCAO_INICIAL"],
    "url_pagina_consulta_empenho": config["URL_SIAFE_PRODUCAO_CONSULTA_EMPENHO"],
}

# Configurações de ambiente
def configuracao_de_ambiente(ambiente):
    if ambiente == "homologacao":
        return config_homologacao
    elif ambiente == "producao":
        return config_producao
    else:
        raise ValueError("Ambiente inválido. Escolha entre 'homologacao' e 'producao'.")

# Setar ambiente

ambiente_escolhido = configuracao_de_ambiente("homologacao")

print(f"Dados do arquivo .env: {env_path}: ")
print(f'dns: {ambiente_escolhido["db_dsn"]}')
print(f'usuario: {ambiente_escolhido["db_user"]}')
print(f'senha: {ambiente_escolhido["db_password"]}')


#Dados NRs válidas 2022 - HDOC
# dados_nr = [
#     {"id": 133945, "codigo": "2024NR001158", "PA": 20848, "MACROR": 3},
# ]

# Dados das NRs válidas 2022
# dados_nr = [
#     {"id": 118961, "codigo": "2024NR000027", "PA": 21023, "MACROR": 3},
#     {"id": 118962, "codigo": "2024NR000028", "PA": 21023, "MACROR": 8},
#     {"id": 118963, "codigo": "2024NR000029", "PA": 21023, "MACROR": 7},
#     {"id": 118964, "codigo": "2024NR000030", "PA": 21023, "MACROR": 10},
#     {"id": 118965, "codigo": "2024NR000031", "PA": 21023, "MACROR": 14},
#     {"id": 118966, "codigo": "2024NR000032", "PA": 21023, "MACROR": 9},
#     {"id": 118968, "codigo": "2024NR000033", "PA": 21069, "MACROR": 13},
#     {"id": 118969, "codigo": "2024NR000034", "PA": 21069, "MACROR": 3},
#     {"id": 133945, "codigo": "2024NR000036", "PA": 20848, "MACROR": 3},
#     {"id": 133946, "codigo": "2024NR000037", "PA": 20848, "MACROR": 2},
# ]

## Dados das NRs Válidas 2023
dados_nr = [
    {"id": 168685, "codigo": "2024NR000122", "PA": 21109, "MACROR": 3},
    {"id": 168688, "codigo": "2024NR000123", "PA": 21108, "MACROR": 3},
    {"id": 168691, "codigo": "2024NR000124", "PA": 21107, "MACROR": 1},
    {"id": 168692, "codigo": "2024NR000125", "PA": 21107, "MACROR": 3},
    {"id": 168729, "codigo": "2024NR000126", "PA": 21069, "MACROR": 5},
    {"id": 168730, "codigo": "2024NR000127", "PA": 21069, "MACROR": 1},
    {"id": 168731, "codigo": "2024NR000128", "PA": 21069, "MACROR": 3},
    {"id": 168732, "codigo": "2024NR000129", "PA": 21069, "MACROR": 8},
    {"id": 168739, "codigo": "2024NR000130", "PA": 21024, "MACROR": 1},
    {"id": 168740, "codigo": "2024NR000131", "PA": 21024, "MACROR": 9},
    {"id": 168741, "codigo": "2024NR000132", "PA": 21024, "MACROR": 3},
    {"id": 168742, "codigo": "2024NR000133", "PA": 21024, "MACROR": 13},
    {"id": 168743, "codigo": "2024NR000134", "PA": 21024, "MACROR": 10},
    {"id": 168757, "codigo": "2024NR000135", "PA": 21023, "MACROR": 1},
    {"id": 168758, "codigo": "2024NR000136", "PA": 21023, "MACROR": 3},
    {"id": 168759, "codigo": "2024NR000137", "PA": 21023, "MACROR": 7},
    {"id": 168760, "codigo": "2024NR000138", "PA": 21023, "MACROR": 9},
    {"id": 168761, "codigo": "2024NR000139", "PA": 21023, "MACROR": 8},
    {"id": 168762, "codigo": "2024NR000140", "PA": 21023, "MACROR": 2},
    {"id": 168763, "codigo": "2024NR000141", "PA": 21023, "MACROR": 4},
    {"id": 168764, "codigo": "2024NR000142", "PA": 21023, "MACROR": 14},
    {"id": 168765, "codigo": "2024NR000143", "PA": 21023, "MACROR": 6},
    {"id": 168766, "codigo": "2024NR000144", "PA": 21023, "MACROR": 12},
    {"id": 168767, "codigo": "2024NR000145", "PA": 21023, "MACROR": 5},
    {"id": 168772, "codigo": "2024NR000146", "PA": 20867, "MACROR": 1},
    {"id": 168773, "codigo": "2024NR000147", "PA": 20867, "MACROR": 3},
    {"id": 168774, "codigo": "2024NR000148", "PA": 20867, "MACROR": 2},
    {"id": 168775, "codigo": "2024NR000149", "PA": 20867, "MACROR": 8},
    {"id": 168776, "codigo": "2024NR000150", "PA": 20867, "MACROR": 4},
    {"id": 168777, "codigo": "2024NR000151", "PA": 20867, "MACROR": 7},
    {"id": 168778, "codigo": "2024NR000152", "PA": 20867, "MACROR": 14},
    {"id": 168898, "codigo": "2024NR000153", "PA": 20848, "MACROR": 9},
    {"id": 168899, "codigo": "2024NR000154", "PA": 20848, "MACROR": 2},
    {"id": 168900, "codigo": "2024NR000155", "PA": 20848, "MACROR": 3},
    {"id": 168901, "codigo": "2024NR000156", "PA": 20848, "MACROR": 12},
    {"id": 168902, "codigo": "2024NR000157", "PA": 20848, "MACROR": 1},
]
# NR HEMOCE DEA 2023
# dados_nr = [
#     {"id": 168685, "codigo": "2024NR000353", "PA": 20848, "MACROR": 3}
# ]



## ÁREA DE DADOS E FUNÇÕES DE CONEXÃO COM O BANCO DE DADOS

def conexao_bd_oracle(ambiente_escolhido, query, params=None, is_update=False):
    connection = None
    cursor = None
    try:
        connection = cx_Oracle.connect(
            ambiente_escolhido["db_user"],
            ambiente_escolhido["db_password"],
            ambiente_escolhido["db_dsn"],
            encoding="UTF-8",
        )
        cursor = connection.cursor()

        # Defina a zona horária (se necessário)

        query_zone = """ALTER SESSION SET TIME_ZONE = '-3:0'"""
        cursor.execute(query_zone)

        # Executar a consulta ou a operação de atualização com parâmetros, se houver

        if params:
            if is_update:
                cursor.execute(query, params)
                connection.commit()  # Se é uma operação de atualização, faça commit
            else:
                cursor.execute(query, params)
        else:
            if is_update:
                cursor.execute(query)
                connection.commit()  # Se é uma operação de atualização, faça commit
            else:
                cursor.execute(query)
        # Obter todas as linhas resultantes, apenas se for uma consulta
        rows = cursor.fetchall() if not is_update else None
    except cx_Oracle.DatabaseError as e:
        logging.error("Erro Oracle: %s", e)
        rows = None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    return rows if not is_update else rows is not None

# QUERY PARA SELECIONAR OS DEAS REALIZADOS

query_credor_generico = """
SELECT 
    r.NOTA_RESERVA,
    r.SALDO,
    CASE r.PA
        WHEN '20848' THEN 'ASSISTÊNCIA MÉDICA HOSPITALAR'
        WHEN '21075' THEN 'MANUTENÇÃO DAS AÇÕES FINALÍSTICAS DO FASSEC'
        WHEN '20867' THEN 'ASSISTÊNCIA MÉDICA EM CONSULTA'
        WHEN '21023' THEN 'REALIZAÇÃO DE EXAMES'
        WHEN '21024' THEN 'ASSISTÊNCIA EM ODONTOLOGIA'
        WHEN '21069' THEN 'ASSISTÊNCIA EM FISIOTERAPIA'
        WHEN '21108' THEN 'ASSISTÊNCIA EM FONOAUDIOLOGIA'
        WHEN '21107' THEN 'ASSISTÊNCIA EM PSICOLOGIA'
        WHEN '21109' THEN 'ASSISTÊNCIA AS PESSOAS PORTADORAS DE NECESSIDADES ESPECIAIS'
        WHEN '00031' THEN 'REPASSE FINANCEIRO PARA O PLANO DE CUSTEIO DO ISSEC'
        ELSE 'TRATAMENTO DESCONHECIDO'
    END AS ACAO
FROM ASSIST.TB_REGISTRO_PROCESSO_SALDO_RESERVA r
LEFT JOIN ASSIST.TB_CREDOR_GENERICO c
    ON c.NOTA_RESERVA = r.NOTA_RESERVA
WHERE c.NOTA_RESERVA IS NULL
AND r.NOTA_RESERVA IN ('2024NR000136')
"""

# DADOS NECESSÁRIOS: NR, VALOR, AÇÃO(TIPO TRATAMENTO), 

# Formatação de dados do BD para serem usados no SIAFE
def formatar_dados(NOTA_RESERVA, SALDO, ACAO):
    nr = str(NOTA_RESERVA)
    # Formatar valor substituindo ponto por vírgula
    if isinstance(SALDO, str):
        # Se for uma string, substituir o ponto por uma vírgula
        valor = SALDO.replace('.', ',')
    else:
        # Se não for uma string, converter para float e formatar
        valor = f"{float(SALDO):.2f}".replace('.', ',')
    # Formatar a ação para que fique em maiúsculas
    tipo_tratamento = ACAO.upper()

    # Retornar os dados formatados como uma tupla
    return nr, valor, tipo_tratamento

resultados = conexao_bd_oracle(ambiente_escolhido, query_credor_generico)


def verificar_cg_para_empenhar(resultados):
    if resultados:
        # Extrair apenas os números de processo
        credor_generico = [resultado[0] for resultado in resultados]
        quantidade = len(credor_generico)
        return {"mensagem": f"Há {quantidade} credores genericos a serem empenhados.", "quantidade": quantidade, "credor_generico": credor_generico}
    else:
        return {"mensagem": "Não há credores genericos a serem empenhados.", "quantidade": 0, "credor_generico": []}
    
print(verificar_cg_para_empenhar(resultados))

# ÁREA UPDATE NO BANCO DE DADOS

def atualizar_tabela_credor_generico(resultados):
    """
    Atualiza a tabela TB_CREDOR_GENERICO com as informações de NOTA_RESERVA e VALOR.
    Os dados são obtidos diretamente da variável `resultados` e formatados com `formatar_dados`.

    Args:
        resultados (list): Resultado do SELECT com os dados (NOTA_RESERVA, SALDO, ACAO).
    
    Returns:
        dict: Resultado do processamento com status e mensagens.
    """
    try:
        if not resultados:
            return {"status": "sucesso", "mensagem": "Nenhuma atualização necessária. Nenhum dado a processar."}

        # Query de inserção/atualização
        query_update = """
        MERGE INTO ASSIST.TB_CREDOR_GENERICO c
        USING (SELECT :nota_reserva AS NOTA_RESERVA, :valor AS VALOR FROM DUAL) src
        ON (c.NOTA_RESERVA = src.NOTA_RESERVA)
        WHEN MATCHED THEN
            UPDATE SET VALOR = src.VALOR
        WHEN NOT MATCHED THEN
            INSERT (NOTA_RESERVA, VALOR)
            VALUES (src.NOTA_RESERVA, src.VALOR)
        """
        
        # Iterar pelos resultados e processar cada registro
        for row in resultados:
            try:
                # Formatar dados usando a função `formatar_dados`
                nr, valor, _ = formatar_dados(row[0], row[1], row[2])  
                
                # Executar a query para atualizar/inserir no banco
                conexao_bd_oracle(
                    ambiente_escolhido, 
                    query_update, 
                    params={"nota_reserva": nr, "valor": valor}, 
                    is_update=True
                )
            except Exception as e:
                logging.error(f"Erro ao processar NOTA_RESERVA {row[0]}: {e}")
                continue
        
        # Retorno final após o processamento de todos os dados
        return {"status": "sucesso", "mensagem": "Tabela TB_CREDOR_GENERICO atualizada com sucesso."}
    
    except Exception as e:
        # Log de erros gerais
        logging.error(f"Erro geral ao atualizar TB_CREDOR_GENERICO: {e}")
        return {"status": "erro", "mensagem": f"Erro: {e}"}


## ÁREA DE TELAS E AÇÕES

servico = Service(ChromeDriverManager().install())

navegador = webdriver.Chrome(service = servico)
navegador.get(ambiente_escolhido['url_siafe'])
navegador.maximize_window()

def tempo_espera(tempo):
    time.sleep(tempo)
    
def espera_campo(campo):
    WebDriverWait(navegador, 10).until(EC.visibility_of_element_located((By.XPATH, campo)))
    
def validar_campo_preenchido(xpath):
    tentativas = 0
    while tentativas < 3:
        try:
            # Tenta encontrar o elemento
            elemento = navegador.find_element_by_xpath(xpath)
            # Verifica se o campo foi preenchido
            if elemento.get_attribute('value'):
                return True
            else:
                tentativas += 1
                time.sleep(1)  # Espera um segundo antes de tentar novamente
        except NoSuchElementException:
            tentativas += 1
            time.sleep(1)  # Espera um segundo antes de tentar novamente
    return False  # Retorna False se o campo não for preenchido após 3 tentativas

def reinciar_navegador(navegador, ambiente_escolhido, servico):
    navegador.quit()
    time.sleep(5)
    navegador = webdriver.Chrome(service = servico)
    navegador.get(ambiente_escolhido['url_siafe'])
    navegador.maximize_window()
    tela_login(ambiente_escolhido, navegador)


def tela_login(ambiente_escolhido):
    for i in range(3):
        try:
            tempo_espera(2)
            # Campo login
            campo_login = navegador.find_element(By.XPATH, '//*[@id="loginBox:itxUsuario::content"]')
            campo_login.send_keys(Keys.CONTROL + 'a')
            campo_login.send_keys(ambiente_escolhido['usuario_siafe'])
            tempo_espera(1)
            # Campo senha
            campo_senha = navegador.find_element(By.XPATH, '//*[@id="loginBox:itxSenhaAtual::content"]')
            campo_senha.send_keys(Keys.CONTROL + 'a')
            campo_senha.send_keys(ambiente_escolhido['senha_siafe'])
            tempo_espera(1)
            # Botão confirmar
            botao_confirmar = navegador.find_element(By.XPATH, '//*[@id="loginBox:btnConfirmar"]/span')
            botao_confirmar.click()
            tempo_espera(2)
            # Acessar a página direto para livrar as mensagens que sempre mudam no Siafe
            navegador.get(ambiente_escolhido['url_pagina_inicial'])
            return True
        except Exception as e:
            logging.error(f'Tentattiva {i + 1} - Erro ao tentar logar no sistema: {e}')
            logging.error(traceback.format_exc())
            if i == 2:
                reinciar_navegador(ambiente_escolhido)
                return False

def selecionar_menu_empenho():
    for i in range(3):
        try:
            tempo_espera(1)
            # Selecionar UG Fassec
            select_ug = navegador.find_element(By.XPATH, '//*[@id="pt1:selUg::content"]')
            opcao_ug = Select(select_ug)
            opcao_ug.select_by_value('2')
            tempo_espera(1)
            # Botão execução
            botao_execucao = navegador.find_element(By.XPATH, '//*[@id="pt1:pt_np4:1:pt_cni6::disclosureAnchor"]')
            botao_execucao.click()
            tempo_espera(1)
            # Botão execução orçamentária
            botao_execucao_orcamentaria = navegador.find_element(By.XPATH, '//*[@id="pt1:pt_np3:0:pt_cni4::disclosureAnchor"]')
            botao_execucao_orcamentaria.click()
            tempo_espera(1)
            # Botão nota de empenho
            botao_nota_empenho = navegador.find_element(By.XPATH, '//*[@id="pt1:pt_np2:4:pt_cni3"]')
            botao_nota_empenho.click()
            tempo_espera(2)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Erro ao tentar selecionar o menu empenho: {e}')
            logging.error(traceback.format_exc())
            if i == 2:
                reinciar_navegador(ambiente_escolhido)
                return False

def selecionar_empenho_cg():
    for i in range(3):
        try:
            tempo_espera(2)
            # btn Inserir Empenho
            botao_inserir_empenho = navegador.find_element(By.XPATH, '//*[@id="pt1:tblDocumento:btnInsert"]/a/span')
            botao_inserir_empenho.click()
            tempo_espera(2)
            # Input radio Credor Genérico
            input_radio_cg = navegador.find_element(By.XPATH, '//*[@id="tplSip:radTipoCredor:_2"]')
            input_radio_cg.click()
            # campo Código do Credor Genérico
            campo_cg = navegador.find_element(By.XPATH, '//*[@id="tplSip:lovPJ:itxLovDec::content"]')
            campo_cg.send_keys(Keys.CONTROL + 'a')
            tempo_espera(3)
            campo_cg.send_keys('CG0000029')
            tempo_espera(0.5)
            campo_cg.send_keys(Keys.TAB)
            tempo_espera(2)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Erro ao tentar selecionar o código de credor generico: {e}')
            logging.error(traceback.format_exc())
            if i == 2:
                reinciar_navegador(ambiente_escolhido)
                return False

def aba_classificacao(nr):
    for i in range (3):
        try:
            tempo_espera(2)
            # Click na aba Classificação
            botao_aba_classificacao = navegador.find_element(By.XPATH, '//*[@id="tplSip:slcClassificacao::disAcr"]')
            botao_aba_classificacao.click()
            tempo_espera(1)
            # Select Tipo Reconhecimento de Passivo
            select_tipo_reconhecimento_de_passivo = navegador.find_element(By.XPATH, '//*[@id="tplSip:slcTipoRecPassivo::content"]')
            opcao_tipo_reconhecimento_de_passivo = Select(select_tipo_reconhecimento_de_passivo)
            # Select 0 = Passivo a ser reconhecido
            opcao_tipo_reconhecimento_de_passivo.select_by_value('0')
            select_tipo_reconhecimento_de_passivo.send_keys(Keys.TAB)
            tempo_espera(2)
            # Campo NR
            campo_nr = navegador.find_element(By.XPATH, '//*[@id="tplSip:lovNotaReserva:itxLovDec::content"]')
            campo_nr.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_nr.send_keys(nr)
            tempo_espera(1)
            campo_nr.send_keys(Keys.TAB)
            tempo_espera(2)
            # Select Efeito do Documento
            select_efeito_do_documento = navegador.find_element(By.XPATH, '//*[@id="tplSip:pnlClassificacao_chc_595::content"]')
            opcao_efeito_do_documento = Select(select_efeito_do_documento)
            opcao_efeito_do_documento.select_by_value('0')
            select_efeito_do_documento.send_keys(Keys.TAB)
            tempo_espera(1)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Erro ao tentar executar a função aba_classificacao: {e}')
            logging.error(traceback.format_exc())
            if i == 2:
                reinciar_navegador(ambiente_escolhido)
                return False

def aba_detalhamento():
    for i in range(3):
        try:
            tempo_espera(2)
            # Click na aba Detalhamento
            botao_aba_detalhamento = navegador.find_element(By.XPATH, '//*[@id="tplSip:slcDetalhamento::disAcr"]')
            botao_aba_detalhamento.click()
            tempo_espera(1)
            # Escolher Modalidade do Empenho
            botao_modalidade_empenho = navegador.find_element(By.XPATH, '//*[@id="tplSip:radTipoModalidadeEmpenho:_1"]')
            botao_modalidade_empenho.click()
            tempo_espera(0.5)
            botao_modalidade_empenho.send_keys(Keys.TAB)
            tempo_espera(1)
            # Select Modalidade de Licitação
            select_modalidade_licitacao = navegador.find_element(By.XPATH, '//*[@id="tplSip:cbxTipoLicitacao::content"]')
            opcao_modalidade_licitacao = Select(select_modalidade_licitacao)
            ##### 4 = Dispensa de Licitação -> HEMOCE
            ##### 5 = Inexigibilidade de Licitação
            opcao_modalidade_licitacao.select_by_value('5')
            tempo_espera(0.5)
            select_modalidade_licitacao.send_keys(Keys.TAB)
            tempo_espera(1)
            # Select Lei
            select_lei = navegador.find_element(By.XPATH, '//*[@id="tplSip:cbxEmbasamentoLegal::content"]')
            opcao_lei = Select(select_lei)
            opcao_lei.select_by_value('0')
            tempo_espera(1)
            select_lei.send_keys(Keys.TAB)
            tempo_espera(0.5)
            select_lei.send_keys(Keys.TAB)
            tempo_espera(0.5)
            # Select Origem de Material
            select_origem_material = navegador.find_element(By.XPATH, '//*[@id="tplSip:cbxOrigemMaterial::content"]')
            opcao_origem_material = Select(select_origem_material)
            opcao_origem_material.select_by_value('0')
            tempo_espera(1)
            select_origem_material.send_keys(Keys.TAB)
            tempo_espera(1)
            # Campo UF
            campo_uf = navegador.find_element(By.XPATH, '//*[@id="tplSip:lovUf:itxLovDec::content"]')
            campo_uf.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_uf.send_keys('CE')
            tempo_espera(0.5)
            campo_uf.send_keys(Keys.TAB)
            tempo_espera(0.5)
            # Campo Municipio
            # print(f'Nome da cidade: {nome_cidade}')
            campo_municipio = navegador.find_element(By.XPATH, '//*[@id="tplSip:lovMunicipio:itxLovDec::content"]')
            campo_municipio.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_municipio.send_keys('Ceará')
            tempo_espera(0.5)
            campo_municipio.send_keys(Keys.TAB)
            tempo_espera(2)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Falha ao tentar executar a função aba_detalhamento {e}')
            logging.error(traceback.format_exc())  # imprime a exceção completa
            if i == 2:
                reinciar_navegador(ambiente_escolhido)
                return False
        
def aba_itens(valor):
    for i in range(3):
        try:
            tempo_espera(2)
            # Click no menu Itens
            botao_menu_itens = navegador.find_element(By.XPATH, '//*[@id="tplSip:sdiItens::disAcr"]')
            botao_menu_itens.click()
            tempo_espera(2)
            # Click no conteúdo Sub-item da Despesa
            conteudo_sub_item_despesa = navegador.find_element(By.XPATH, '//*[@id="tplSip:painelItens:tabItens:tabViewerDec::db"]/table/tbody/tr/td[2]')
            conteudo_sub_item_despesa.click()
            tempo_espera(2)
            # Click no botão alterar
            botao_alterar = navegador.find_element(By.XPATH, '//*[@id="tplSip:painelItens:tabItens:btnEdit"]/a/span')
            botao_alterar.click()
            tempo_espera(3)
            # Campo Valor do Item e insira o valor do CREDOR GENÉRICO
            print(f'Valor CG: {valor}') # ALTERAR! valor_nr
            campo_valor_item = navegador.find_element(By.XPATH, '//*[@id="tplSip:painelItens:tabItens:itxValorItem::content"]')
            campo_valor_item.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_valor_item.send_keys(valor)
            tempo_espera(1)
            # Click no botão Confirmar
            botao_confirmar = navegador.find_element(By.XPATH, '//*[@id="tplSip:painelItens:tabItens:pnlItemWindow::yes"]')
            botao_confirmar.click()
            tempo_espera(2)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Falha ao tentar executar a função aba_itens {e}')
            logging.error(traceback.format_exc())  # imprime a exceção completa
            if i == 2:
                reinciar_navegador(ambiente_escolhido)
                return False
        
def aba_produtos(tipo_tratamento, valor):
    for i in range(3):
        try:
            tempo_espera(1)
            # Click na aba Produtos
            botao_aba_produtos = navegador.find_element(By.XPATH, '//*[@id="tplSip:slcProdutos::disAcr"]')
            botao_aba_produtos.click()
            tempo_espera(1)
            # Click no botão inserir
            botao_inserir = navegador.find_element(By.XPATH, '//*[@id="tplSip:tblProdutosEmpenho:btnInsert"]/a/span')
            botao_inserir.click()
            tempo_espera(1)
            # Campo Produto
            campo_produto = navegador.find_element(By.XPATH, '//*[@id="tplSip:itxNomeProdutoGenerico::content"]')
            campo_produto.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_produto.send_keys(tipo_tratamento) # ALTERAR! AÇÃO
            tempo_espera(1)
            # Campo Descrição
            campo_descricao = navegador.find_element(By.XPATH, '//*[@id="tplSip:itxDescricaoProdutoGenerico::content"]')
            campo_descricao.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_descricao.send_keys(f'REFERENTE AO PAGAMENTO DE {tipo_tratamento} RELATIVO A 2024') # ALTERAR! AÇÃO
            tempo_espera(1)
            #Campo Unidade de Fornecimento
            campo_unidade_fornecimento = navegador.find_element(By.XPATH, '//*[@id="tplSip:itxUnidadeFornecimentoProdutoGenerico::content"]')
            campo_unidade_fornecimento.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_unidade_fornecimento.send_keys('UND')
            tempo_espera(0.5)
            # Campo Quantidade
            campo_quantidade = navegador.find_element(By.XPATH, '//*[@id="tplSip:itxQtdPrev::content"]')
            campo_quantidade.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_quantidade.send_keys('1')
            tempo_espera(0.5)
            campo_quantidade.send_keys(Keys.TAB)
            tempo_espera(1)
            # Campo Preço Unitário
            campo_preco_unitario = navegador.find_element(By.XPATH, '//*[@id="tplSip:itxPrecoUnitario::content"]')
            campo_preco_unitario.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_preco_unitario.send_keys(valor) # ALTERAR! VALOR_CG
            tempo_espera(0.5)
            # Click no botão Confirmar
            botao_confirmar = navegador.find_element(By.XPATH, '//*[@id="tplSip:btnConfirmar"]/span')
            botao_confirmar.click()
            tempo_espera(2)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Falha ao tentar executar a função aba_produtos {e}')
            logging.error(traceback.format_exc())  # imprime a exceção completa
            if i == 2:
                reinciar_navegador(ambiente_escolhido)
                return False
    
def aba_processo():
    for i in range(3):
        try:
            tempo_espera(1)
            # Click na aba Processo
            botao_aba_processo = navegador.find_element(By.XPATH, '//*[@id="tplSip:slcProcesso::disAcr"]')
            botao_aba_processo.click()
            tempo_espera(1)
            # Campo Número do Processo
            campo_numero_processo = navegador.find_element(By.XPATH, '//*[@id="tplSip:painelProcesso:itxProcesso::content"]')
            campo_numero_processo.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_numero_processo.send_keys('000000')
            tempo_espera(2)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Falha ao tentar executar a função aba_processo {e}')
            logging.error(traceback.format_exc())  # imprime a exceção completa
            if i == 2:
                reinciar_navegador(ambiente_escolhido)
                return False

def aba_observacao(tipo_tratamento):
    for i in range(3):
        try:
            tempo_espera(1)
            # Click na aba Observação
            botao_aba_observacao = navegador.find_element(By.XPATH, '//*[@id="tplSip:slcObservacao::disAcr"]')
            botao_aba_observacao.click()
            tempo_espera(2)
            # Campo Observação
            print(f'REFERENTE AO PAGAMENTO DE {tipo_tratamento} RELATIVO A 2024')
            campo_observacao = navegador.find_element(By.XPATH, '//*[@id="tplSip:painelObservacao:itxObservacao::content"]')
            campo_observacao.send_keys(f'REFERENTE AO PAGAMENTO DE {tipo_tratamento} RELATIVO A 2024') # ALTERAR! AÇÃO
            tempo_espera(2)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Falha ao tentar executar a função aba_observacao {e}')
            logging.error(traceback.format_exc())  # imprime a exceção completa
            if i == 2:
                reinciar_navegador(ambiente_escolhido)
                return False

def salvar_rascunho_empenho_cg(ambiente_escolhido):
    for i in range(3):
        try:
            tempo_espera(1)
            # Click no botão Salvar Rascunho
            botao_salvar_rascunho = navegador.find_element(By.XPATH, '//*[@id="tplSip:btnConfirmar"]/span')
            botao_salvar_rascunho.click()
            tempo_espera(2)
            # Voltar a página de consulta de empenhos
            navegador.get(ambiente_escolhido['url_pagina_consulta_empenho'])
            tempo_espera(1)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Erro ao tentar contabilizar o empenho: {e}')
            logging.error(traceback.format_exc())  # imprime a exceção completa
            if i == 2:
                reinciar_navegador(ambiente_escolhido)
                return False

# #######################
# ## Módulo Processamento
# #######################

def inserir_empenho_cg(valor, tipo_tratamento, nr):
    try:
        if not selecionar_empenho_cg():
            return False
        tempo_espera(2)
        if not aba_classificacao(nr):
            return False
        tempo_espera(2)
        if not aba_detalhamento():
            return False
        tempo_espera(2)
        if not aba_itens(valor):
            return False
        tempo_espera(2)
        if not aba_produtos(tipo_tratamento, valor):
            return False
        tempo_espera(2)
        if not aba_processo():
            return False
        tempo_espera(2)
        if not aba_observacao(tipo_tratamento):
            return False
        tempo_espera(2)
        if not salvar_rascunho_empenho_cg(ambiente_escolhido):
            return False
        tempo_espera(2)
        return True
    except Exception as e:
        logging.error(f'Erro ao tentar inserir o Empenho para a NR {nr}: {e}')
        logging.error(traceback.format_exc())
        return False

tela_login(ambiente_escolhido)
tempo_espera(2)
selecionar_menu_empenho()
tempo_espera(2)

def processar_e_atualizar_empenho(credor_generico_falhados):
    credor_generico_falhados_novos = []
    for nr_falhada in credor_generico_falhados:
        tentativas = 0
        while tentativas < 2:
            try:
                # Obtenha os valores necessários com base no nr_falhada
                nr, valor, tipo_tratamento = obter_dados(nr_falhada)

                # Tentar inserir Empenho
                if inserir_empenho_cg(valor, tipo_tratamento, nr):
                    # Se a inserção for bem-sucedida, então atualizar TT_SPU
                    atualizar_tabela_credor_generico(resultados)
                    break  # Sai do loop se a operação for bem-sucedida
            except Exception as e:
                logging.error(f'Erro ao reprocessar o Empenho para a NR {nr_falhada}: {e}')
                tentativas += 1
        if tentativas == 2 and nr_falhada not in credor_generico_falhados_novos:
            credor_generico_falhados_novos.append(nr_falhada)
            logging.error(f'NR {nr_falhada} falhou após 3 tentativas')
    # Adiciona os credor_generico que falharam de volta à lista original
    credor_generico_falhados.extend(credor_generico_falhados_novos)
    logging.error(f'Relacao de credor_generico falhados: {credor_generico_falhados}')
    armazenar_credor_generico_falhados(credor_generico_falhados)
    
def obter_dados(nr_falhada):
    # Dados do processo falhado
    nr = nr_falhada.nr
    valor = nr_falhada.valor
    tipo_tratamento = nr_falhada.tipo_tratamento

    return nr, valor, tipo_tratamento

def armazenar_credor_generico_falhados(credor_generico_falhados):
    with open('credor_generico_falhados.json', 'w') as f:
        json.dump(credor_generico_falhados, f)

credor_generico_falhados = []

if resultados:
    # Obtenha a lista de credor_generico pendentes a partir da função verificar_cg_para_empenhar
    info_credor_generico = verificar_cg_para_empenhar(resultados)
    credor_generico_pendentes = info_credor_generico["credor_generico"]

    for resultado in resultados:
        tentativas = 0
        sucesso = False
        while not sucesso and tentativas < 5:
            try:
                # Formatar os dados para o processamento
                nr, valor, tipo_tratamento = formatar_dados(*resultado)
                print(f'NR: {nr}, Valor Empenho: {valor}, Tipo de tratamento: {tipo_tratamento}')

                # Verificar se o número do processo está na lista de credor_generico pendentes
                if nr in credor_generico_pendentes:
                    # Tentar inserir o empenho
                    if not inserir_empenho_cg(valor, tipo_tratamento, nr):
                        # Se falhar, adicionar na lista de falhados
                        credor_generico_falhados.append(nr)
                    else:
                        # Se for bem-sucedido, atualizar a tabela TB_CREDOR_GENERICO
                        atualizar_tabela_credor_generico(resultados)  # Chama a nova função de atualização
                        sucesso = True  # Marca o sucesso
            except Exception as e:
                # Aumenta a tentativa e loga o erro
                tentativas += 1
                logging.error(f'Erro ao processar o resultado {resultado}: {e}')
                time.sleep(3)  # Aguardar antes de tentar novamente

    # Verifique se há credor_generico falhados e os processe novamente, se necessário
    if credor_generico_falhados:
        # Processar e atualizar os empenhos falhados
        processar_e_atualizar_empenho(credor_generico_falhados)

input('Pressiona enter para sair...')
navegador.quit()