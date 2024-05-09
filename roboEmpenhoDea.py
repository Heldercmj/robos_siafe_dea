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

ambiente_escolhido = configuracao_de_ambiente("producao")

print(f"Dados do arquivo .env: {env_path}: ")
print(f'dns: {ambiente_escolhido["db_dsn"]}')
print(f'usuario: {ambiente_escolhido["db_user"]}')
print(f'senha: {ambiente_escolhido["db_password"]}')

## DADOS DA API DO SIAFE
# obter token da API do SIAFE

# def obter_token_api_siafe(ambiente_escolhido):
#     try:
#         url = ambiente_escolhido["url_api_siafe"] + "/auth"
#         user_data = {
#             "usuario": ambiente_escolhido["usuario_siafe"],
#             "senha": ambiente_escolhido["senha_siafe"],
#         }
#         response = requests.post(url=url, json=user_data)
#         print(response)

#         if 200 <= response.status_code <= 299:
#             # sucesso
#             print('Status Code', response.status_code)
#             response_data = response.json()
#             token = response_data['token']
#             token = {"Authorization": token}
#             print('token', token)
#             return token
#         else:
#             # erros
#             print('Status Code', response.status_code)
#             print('Reason', response.reason)
#             print('Texto', response.text)
#             return None
#     except requests.exceptions.RequestException as e:
#         print(f"Erro ao obter token: {e}")
#         return None

# def obter_dados_api_siafe(ambiente_escolhido, token):
#     try:
#         headers = {
#             'Authorization': 'Bearer ' + token["Authorization"],
#             'Content-Type': 'application/json'
#         }
        
#         url = ambiente_escolhido["url_api_siafe"]

#         url_auth = url + 'nota-reserva/2024/460801'

#         response = requests.get(url_auth, headers=headers, verify=False)
#         response.raise_for_status()
#         data = response.json()

#         for item in data:
#             if item['codNatureza'] == '339092' and item['codFonte'] == '759' and item['codigo'].startswith('2024'):
#                 print(item)
#         return data
#     except requests.exceptions.RequestException as e:
#         print(f"Erro ao obter dados: {e}")
#         return None

# token = obter_token_api_siafe(ambiente_escolhido)
# if token is not None:
#     obter_dados_api_siafe(ambiente_escolhido, token)


## Dados das NRs válidas 2022
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

query_dea_realizados = """
WITH t0 AS (
    SELECT 
        nu_ordem,
        dt_producao,
        cd_prestador,
        cd_tipo_tratamento, 
        CASE fn_pa(cd_tipo_tratamento)
            WHEN 21972 then 20848  
            WHEN 22169 then 21075   
            WHEN 22728 then 20867    
            WHEN 21975 then 21023
            WHEN 22164 then 21024  
            WHEN 22165 then 21069   
            WHEN 22166 then 21108    
            WHEN 22167 then 21107    
            WHEN 22168 then 21109             
            ELSE fn_pa(cd_tipo_tratamento) 
        END pa,
        s.W_QTD1,
        s.W_TIPOINT2
    FROM tb_producao_servico p
    LEFT JOIN tt_spu s ON s.w_num_protocolo = p.nu_ordem
    WHERE dt_producao >= to_date('01/01/2023','dd/mm/yyyy') 
        and dt_producao <= to_date('31/12/2023','dd/mm/yyyy') 
        AND fl_status = 3 
        AND s.W_QTD1 = 0 
        AND s.W_TIPOINT2 IS NOT NULL
        AND EXISTS(
            SELECT 1 
            FROM tt_spu_mov m 
            WHERE m.num_prot = nu_ordem 
                AND m.loc_para = '470000087'  
                AND m.motivo = 7 
                AND m.cd_registro = (SELECT max(i.cd_registro) FROM tt_spu_mov i WHERE i.num_prot = m.num_prot)
        )
),
t1 AS  (
    SELECT t0.*,p.vl_liquido 
    FROM t0 
    LEFT JOIN tb_pagamento_servico p ON p.nu_ordem = t0.nu_ordem
),
t2 AS (
    SELECT t1.*,w.fl_prestador,w.fn_macroregiao,w.nu_cgc_cpf,w.fn_cidade_endereco cidade 
    FROM t1 
    LEFT JOIN vw_prestador w ON w.cd_pessoa = t1.cd_prestador
),
t3 AS (
    SELECT t2.*,nr_tipo_tratamento 
    FROM t2 
    LEFT JOIN tb_tipo_tratamento t ON t.cd_tipo_tratamento = t2.cd_tipo_tratamento 
    ORDER BY 1
)           
SELECT 
    nu_ordem,
    dt_producao,
    cast(pa as int) PA,
    to_char(vl_liquido),
    case fl_prestador when 1 then 'F' else 'J' end TPESSOA,
    cast(fn_macroregiao as int) MACROR,
    to_char(nu_cgc_cpf),
    cidade,
    nr_tipo_tratamento,
    W_QTD1,
    W_TIPOINT2
FROM t3
WHERE nu_ordem > 0 
AND nu_cgc_cpf NOT IN(7954571011491)
ORDER BY W_TIPOINT2 DESC
FETCH FIRST 200 ROWS ONLY
"""

# Formatação de dados do BD para serem usados no SIAFE
def formatar_dados(nu_ordem, dt_producao, PA, vl_liquido, TPESSOA, MACROR, nu_cgc_cpf, cidade, nr_tipo_tratamento, W_QTD1 ,W_TIPOINT2):
    numero_processo = nu_ordem
    # Formatar data para o formato desejado (MM/YYYY)
    data_producao = f"{dt_producao.month:02d}/{dt_producao.year}"

    pa = PA

    # Formatar valor substituindo ponto por vírgula
    if isinstance(vl_liquido, str):
        # Se for uma string, substituir o ponto por uma vírgula
        valor_dea = vl_liquido.replace('.', ',')
    else:
        # Se não for uma string, converter para float e formatar
        valor_dea = f"{float(vl_liquido):.2f}".replace('.', ',')

    tipo_pessoa = TPESSOA
    macroregiao = str(MACROR)

    # Formatar CNPJ/CPF
    cnpj_cpf_para_formatar = str(nu_cgc_cpf)
    cnpj_cpf = ''.join(filter(str.isdigit, cnpj_cpf_para_formatar))

    # Verificar se é CPF ou CNPJ
    if tipo_pessoa == 'F':
        # Garantir que o CPF tenha 11 caracteres, preenchendo com zeros à esquerda se necessário
        cnpj_cpf = cnpj_cpf.zfill(11)
    else:
        # Garantir que o CNPJ tenha 14 caracteres, preenchendo com zeros à esquerda se necessário
        cnpj_cpf = cnpj_cpf.zfill(14)

    nome_cidade = cidade
    if nome_cidade == "TAUA":
        nome_cidade = "TAUÁ"
    if nome_cidade == "QUIXADA":
        nome_cidade = "QUIXADÁ"
    if nome_cidade == "CRATEUS":
        nome_cidade = "CRATEÚS"
    if nome_cidade == "ICO":
        nome_cidade = "ICÓ"
    if nome_cidade == "MARACANAU":
        nome_cidade = "MARACANAÚ"
    if nome_cidade == "ACARAU":
        nome_cidade = "ACARAÚ"
    if nome_cidade == "BATURITE":
        nome_cidade = "BATURITÉ"
    if nome_cidade == "TIANGUA":
        nome_cidade = "TIANGUÁ"
    if nome_cidade == "EUSEBIO":
        nome_cidade = "EUSÉBIO"
    if nome_cidade == "INDEPENDENCIA":
        nome_cidade = "INDEPENDÊNCIA"
    if nome_cidade == "REDENCAO":
        nome_cidade = "REDENÇÃO"
    if nome_cidade == "IPUEIRAS":
        nome_cidade = "IPUEIRAS"
    if nome_cidade == "CANINDE":
        nome_cidade = "CANINDÉ"
    if nome_cidade == "SAO BENEDITO":
        nome_cidade = "SÃO BENEDITO"

    tipo_tratamento = nr_tipo_tratamento
    flag = W_QTD1
    codigo_dea = W_TIPOINT2

    # Retornar os dados formatados como uma tupla
    return numero_processo, data_producao, pa, valor_dea, tipo_pessoa, macroregiao, cnpj_cpf, nome_cidade, tipo_tratamento, flag, codigo_dea

resultados = conexao_bd_oracle(ambiente_escolhido, query_dea_realizados)

# ÁREA QUERY UPDATE

def verificar_deas_para_empenhar(resultados):
    if resultados:
        # Extrair apenas os números de processo
        processos = [resultado[0] for resultado in resultados]
        quantidade = len(processos)
        return {"mensagem": f"Há {quantidade} processos a serem empenhados.", "quantidade": quantidade, "processos": processos}
    else:
        return {"mensagem": "Não há processos a serem empenhados.", "quantidade": 0, "processos": []}
    
print(verificar_deas_para_empenhar(resultados))

def obter_codigo_nr(pa, macroregiao):
    print(f"pa = {pa}, macroregiao = {macroregiao}")
    pa = int(pa)
    macroregiao = int(macroregiao)
    for nr in dados_nr:
        if int(nr['PA']) == pa and int(nr['MACROR']) == macroregiao:
            return nr['codigo']
    return None

def verificar_processo_realizado(numero_processo):
    try:
        query_verificar = """
        SELECT W_QTD1
        FROM TT_SPU
        WHERE W_NUM_PROTOCOLO = :numero_processo
        """
        resultado = conexao_bd_oracle(ambiente_escolhido, query_verificar, params={"numero_processo": numero_processo})
        if resultado is not None and resultado[0][0] == 1:
            return True
        else:
            return False
    except Exception as e:
        print(f"Ocorreu um erro ao verificar o processo: {e}")
        return False

def atualizar_tt_spu(numero_processo):
    # Primeiro, verifique se o valor na coluna W_QTD1 é 0
    if verificar_processo_realizado(numero_processo):
        logging.info(f'O processo {numero_processo} ja foi realizado e será sustituído!')
        
    try:
        query_atualizar = """
        UPDATE TT_SPU
        SET W_QTD1 = 1
        WHERE W_NUM_PROTOCOLO = :numero_processo
        """
        # Parâmetros das consultas
        params = (int(numero_processo),)

        # Verificar se numero_processo é None
        if numero_processo is None:
            logging.error("Numero_processo chegando como None")
        else:
            # Chamando a função de conexão para executar os updates
            resultado_update = conexao_bd_oracle(ambiente_escolhido, query_atualizar, params, is_update=True)
            if resultado_update is not None:
                logging.info(f'Updates realizados com sucesso para o numero_processo {numero_processo}')
            else:
                logging.error(f'Nenhum registro atualizado para o numero_processo {numero_processo}')
    except Exception as e:
        logging.error(f'Erro ao tentar atualizar TT_SPU: {e}')
        logging.error("Detalhes da excecao:", exc_info=True)

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
                return False

def selecionar_empenho_dea(cnpj_cpf):
    for i in range(3):
        try:
            tempo_espera(2)
            # btn Inserir Empenho
            botao_inserir_empenho = navegador.find_element(By.XPATH, '//*[@id="pt1:tblDocumento:btnInsert"]/a/span')
            botao_inserir_empenho.click()
            tempo_espera(2)
            # campo CNPJ
            campo_cnpj = navegador.find_element(By.XPATH, '//*[@id="tplSip:lovPJ:itxLovDec::content"]')
            campo_cnpj.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_cnpj.send_keys(cnpj_cpf)
            print(f'CNPJ: {cnpj_cpf}')
            tempo_espera(0.5)
            campo_cnpj.send_keys(Keys.TAB)
            tempo_espera(2)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Erro ao tentar selecionar o empenho DEA: {e}')
            logging.error(traceback.format_exc())
            if i == 2:
                return False

def aba_classificacao(codigo_nr):
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
            opcao_tipo_reconhecimento_de_passivo.select_by_value('1')
            select_tipo_reconhecimento_de_passivo.send_keys(Keys.TAB)
            tempo_espera(2)
            # Campo NR
            campo_nr = navegador.find_element(By.XPATH, '//*[@id="tplSip:lovNotaReserva:itxLovDec::content"]')
            campo_nr.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_nr.send_keys(codigo_nr)
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
                return False

def aba_detalhamento(nome_cidade):
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
            print(f'Nome da cidade: {nome_cidade}')
            campo_municipio = navegador.find_element(By.XPATH, '//*[@id="tplSip:lovMunicipio:itxLovDec::content"]')
            campo_municipio.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_municipio.send_keys(nome_cidade)
            tempo_espera(0.5)
            campo_municipio.send_keys(Keys.TAB)
            tempo_espera(2)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Falha ao tentar executar a função aba_detalhamento {e}')
            logging.error(traceback.format_exc())  # imprime a exceção completa
            if i == 2:
                return False
        
def aba_itens(codigo_dea, valor_dea):
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
            print(f'Código DEA: {codigo_dea}')
            # Click no select DEA e selecione o código DEA
            select_dea = navegador.find_element(By.XPATH, '//*[@id="tplSip:painelItens:tabItens:pnlClassificacaoItem_chc_216::content"]')
            # Espera até que o elemento select esteja visível
            select_dea = WebDriverWait(navegador, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="tplSip:painelItens:tabItens:pnlClassificacaoItem_chc_216::content"]')))
            opcao_dea = Select(select_dea)
            opcao_dea.select_by_visible_text(str(codigo_dea))
            tempo_espera(2)
            # Campo Valor do Item e insira o valor do DEA
            print(f'Valor DEA: {valor_dea}')
            campo_valor_item = navegador.find_element(By.XPATH, '//*[@id="tplSip:painelItens:tabItens:itxValorItem::content"]')
            campo_valor_item.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_valor_item.send_keys(valor_dea)
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
                return False
        
def aba_produtos(data_producao, tipo_tratamento, nome_cidade, valor_dea):
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
            campo_produto.send_keys(tipo_tratamento)
            tempo_espera(1)
            # Campo Descrição
            campo_descricao = navegador.find_element(By.XPATH, '//*[@id="tplSip:itxDescricaoProdutoGenerico::content"]')
            campo_descricao.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_descricao.send_keys(f'REFERENTE AO PAGAMENTO DE {tipo_tratamento} RELATIVO A {data_producao} - {nome_cidade}/CE')
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
            campo_preco_unitario.send_keys(valor_dea)
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
                return False
    
def aba_processo(numero_processo):
    for i in range(3):
        try:
            tempo_espera(1)
            # Click na aba Processo
            botao_aba_processo = navegador.find_element(By.XPATH, '//*[@id="tplSip:slcProcesso::disAcr"]')
            botao_aba_processo.click()
            tempo_espera(1)
            # Campo Número do Processo
            print(f'Número do processo: {numero_processo}')
            campo_numero_processo = navegador.find_element(By.XPATH, '//*[@id="tplSip:painelProcesso:itxProcesso::content"]')
            campo_numero_processo.send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            campo_numero_processo.send_keys(numero_processo)
            tempo_espera(2)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Falha ao tentar executar a função aba_processo {e}')
            logging.error(traceback.format_exc())  # imprime a exceção completa
            if i == 2:
                return False

def aba_observacao(tipo_tratamento, data_producao, nome_cidade):
    for i in range(3):
        try:
            tempo_espera(1)
            # Click na aba Observação
            botao_aba_observacao = navegador.find_element(By.XPATH, '//*[@id="tplSip:slcObservacao::disAcr"]')
            botao_aba_observacao.click()
            tempo_espera(2)
            # Campo Observação
            print(f'REFERENTE AO PAGAMENTO DE {tipo_tratamento} RELATIVO A {data_producao} - {nome_cidade}/CE')
            campo_observacao = navegador.find_element(By.XPATH, '//*[@id="tplSip:painelObservacao:itxObservacao::content"]')
            campo_observacao.send_keys(f'REFERENTE AO PAGAMENTO DE {tipo_tratamento} RELATIVO A {data_producao} - {nome_cidade}/CE')
            tempo_espera(2)
            return True
        except Exception as e:
            logging.error(f'Tentativa {i + 1} - Falha ao tentar executar a função aba_observacao {e}')
            logging.error(traceback.format_exc())  # imprime a exceção completa
            if i == 2:
                return False

def salvar_rascunho_empenho_dea(ambiente_escolhido):
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
                return False

# #######################
# ## Módulo Processamento
# #######################

def inserir_empenho_dea(numero_processo, data_producao, valor_dea, cnpj_cpf, nome_cidade, tipo_tratamento, codigo_dea, codigo_nr):
    try:
        if not selecionar_empenho_dea(cnpj_cpf):
            return False
        tempo_espera(2)
        if not aba_classificacao(codigo_nr):
            return False
        tempo_espera(2)
        if not aba_detalhamento(nome_cidade):
            return False
        tempo_espera(2)
        if not aba_itens(codigo_dea, valor_dea):
            return False
        tempo_espera(2)
        if not aba_produtos(data_producao, tipo_tratamento, nome_cidade, valor_dea):
            return False
        tempo_espera(2)
        if not aba_processo(numero_processo):
            return False
        tempo_espera(2)
        if not aba_observacao(tipo_tratamento, data_producao, nome_cidade):
            return False
        tempo_espera(2)
        if not salvar_rascunho_empenho_dea(ambiente_escolhido):
            return False
        tempo_espera(2)
        return True
    except Exception as e:
        logging.error(f'Erro ao tentar inserir o Empenho para o processo {numero_processo}: {e}')
        logging.error(traceback.format_exc())
        return False

tela_login(ambiente_escolhido)
tempo_espera(2)
selecionar_menu_empenho()
tempo_espera(2)

def processar_e_atualizar_empenho(processos_falhados):
    processos_falhados_novos = []
    for processo_falhado in processos_falhados:
        tentativas = 0
        while tentativas < 2:
            try:
                # Obtenha os valores necessários com base no processo_falhado
                numero_processo, data_producao, valor_dea, cnpj_cpf, nome_cidade, tipo_tratamento, codigo_dea = obter_dados(processo_falhado)

                # Tentar inserir Empenho
                if inserir_empenho_dea(numero_processo, data_producao, valor_dea, cnpj_cpf, nome_cidade, tipo_tratamento, codigo_dea):
                    # Se a inserção for bem-sucedida, então atualizar TT_SPU
                    atualizar_tt_spu(numero_processo)
                    break  # Sai do loop se a operação for bem-sucedida
            except Exception as e:
                logging.error(f'Erro ao reprocessar o Empenho para o processo {processo_falhado}: {e}')
                tentativas += 1
        if tentativas == 2 and processo_falhado not in processos_falhados_novos:
            processos_falhados_novos.append(processo_falhado)
            logging.error(f'Processo {processo_falhado} falhou após 3 tentativas')
    # Adiciona os processos que falharam de volta à lista original
    processos_falhados.extend(processos_falhados_novos)
    logging.error(f'Relacao de processos falhados: {processos_falhados}')
    armazenar_processos_falhados(processos_falhados)
    
def obter_dados(processo_falhado):
    # Dados do processo falhado
    numero_processo = processo_falhado.numero_processo
    data_producao = processo_falhado.data_producao
    valor_dea = processo_falhado.valor_dea
    cnpj_cpf = processo_falhado.cnpj_cpf
    nome_cidade = processo_falhado.nome_cidade
    tipo_tratamento = processo_falhado.tipo_tratamento
    codigo_dea = processo_falhado.codigo_dea

    return numero_processo, data_producao, valor_dea, cnpj_cpf, nome_cidade, tipo_tratamento, codigo_dea

def armazenar_processos_falhados(processos_falhados):
    with open('processos_falhados.json', 'w') as f:
        json.dump(processos_falhados, f)

processos_falhados = []

if resultados:
    # Obtenha a lista de processos pendentes a partir da função verificar_deas_para_empenhar
    info_processos = verificar_deas_para_empenhar(resultados)
    processos_pendentes = info_processos["processos"]

    for resultado in resultados:
        tentativas = 0
        sucesso = False
        while not sucesso and tentativas < 5:
            try:
                numero_processo, data_producao, pa, valor_dea, tipo_pessoa, macroregiao, cnpj_cpf, nome_cidade, tipo_tratamento, flag, codigo_dea = formatar_dados(*resultado)
                codigo_nr = obter_codigo_nr(pa, macroregiao)
                print(f'Número do processo: {numero_processo}, Data de produção: {data_producao}, Valor DEA: {valor_dea}, CNPJ/CPF: {cnpj_cpf}, Nome da cidade: {nome_cidade}, Tipo de tratamento: {tipo_tratamento}, flag: {flag}, Código DEA: {codigo_dea}, Código NR: {codigo_nr}, PA: {pa}, Macroregião: {macroregiao}')

                # Verifique se o número do processo está na lista de processos pendentes
                if numero_processo in processos_pendentes:
                    if not inserir_empenho_dea(numero_processo, data_producao, valor_dea, cnpj_cpf, nome_cidade, tipo_tratamento, codigo_dea, codigo_nr):
                        processos_falhados.append(numero_processo)
                    else:
                        atualizar_tt_spu(numero_processo)
                        sucesso = True
            except Exception as e:
                tentativas += 1
                logging.error(f'Erro ao processar o resultado {resultado}: {e}')
                time.sleep(3)

    # Verifique se há processos falhados e os processe novamente, se necessário
    if processos_falhados:
        processar_e_atualizar_empenho(processos_falhados)


input('Pressiona enter para sair...')
navegador.quit()