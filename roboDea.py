from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from dotenv import dotenv_values
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from datetime import datetime

# from IPython.display import display

# import pandas as pd

import time
import cx_Oracle
import logging
import os
import json

env_path = os.path.join(os.path.dirname(__file__), ".env")

# Configurando arquivo de log

logging.basicConfig(
    filename="error.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

# Obter a data atual
data_corrente = datetime.now()
# Formatar a data no estilo DD/MM/AAAA
data_atual = data_corrente.strftime("%d/%m/%Y")
# Imprimir a data formatada
print(data_atual)

# Configurações de ambiente utilizando o arquivo .env
config = dotenv_values(".env")

# Dados para configurações de homologação
config_homologacao = {
    "url_siafe": config["URL_SIAFE_HOMOLOGACAO"],
    "usuario_siafe": config["USUARIO_SIAFE_HOMOLOGACAO"],
    "senha_siafe": config["SENHA_SIAFE_HOMOLOGACAO"],
    "db_user": config["DB_USER"],
    "db_password": config["DB_PASSWORD"],
    "db_dsn": config["DB_DSN"],
}

# Dados para configurações de produção
config_producao = {
    "url_siafe": config["URL_SIAFE_PRODUCAO"],
    "usuario_siafe": config["USUARIO_SIAFE_PRODUCAO"],
    "senha_siafe": config["SENHA_SIAFE_PRODUCAO"],
    "db_user": config["DB_USER_PRODUCAO"],
    "db_password": config["DB_PASSWORD_PRODUCAO"],
    "db_dsn": config["DB_DSN_PRODUCAO"],
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


# Query BD
query_selecionar_processos = """
SELECT NU_ORDEM, DT_PRODUCAO, NU_CGC_CPF , VL_PAGAMENTO FROM TB_PAGAMENTO_SERVICO tps
WHERE 
	NU_ORDEM IN(
		2306184890
    )
"""

resultados = conexao_bd_oracle(ambiente_escolhido, query_selecionar_processos)

def verificar_processo_realizado(processo):
    # Verificar o valor da flag
    query_verificacao = """
    SELECT W_NUM_PROTOCOLO
    FROM TT_SPU
    WHERE W_QTD1 IS NOT NULL AND W_NUM_PROTOCOLO = :1
    """

    # Parâmetros da consulta
    params = (int(processo),)

    # Executar a consulta
    resultado_verificacao = conexao_bd_oracle(ambiente_escolhido, query_verificacao, params)

    # Verificar se o resultado da consulta não está vazio
    if resultado_verificacao:
        # Se o resultado da consulta não estiver vazio, o processo já foi realizado
        return resultado_verificacao[0][0]
    else:
        return False

# Quando o processo for finalizado com sucesso fazer o update na TT_SPU na coluna W_QTD1, referente ao processo executado.
def atualizar_tt_spu(processo, codigo_dea):
    # Verificar se o processo já foi realizado
    if verificar_processo_realizado(processo):
        logging.info(f'O processo {processo} ja foi realizado e será sustituído pelo novo código DEA {codigo_dea}')

    try:
        # Consulta SQL para realizar o update em W_QTD1
        query_update_flag_dea_realizado = """
        UPDATE TT_SPU
        SET W_QTD1 = 0
        WHERE W_NUM_PROTOCOLO = :1
        """

        # Consulta SQL para realizar o update em W_TIPOINT2
        query_update_codigo_dea = """
        UPDATE TT_SPU
        SET W_TIPOINT2 = :1
        WHERE W_NUM_PROTOCOLO = :2
        """

        # Parâmetros das consultas
        params1 = (int(processo),)

        # Verificar se codigo_dea ou processo é None
        if codigo_dea is None or processo is None:
            logging.error("codigo_dea ou processo chegando como None")
        else:
            params2 = (codigo_dea, int(processo),)

            # Chamando a função de conexão para executar os updates
            resultado_update1 = conexao_bd_oracle(ambiente_escolhido, query_update_flag_dea_realizado, params1, is_update=True)
            resultado_update2 = conexao_bd_oracle(ambiente_escolhido, query_update_codigo_dea, params2, is_update=True)

            if resultado_update1 is not None and resultado_update2 is not None:
                logging.info(f'Updates realizados com sucesso para o processo {processo}')
            else:
                logging.error(f'Nenhum registro atualizado para o processo {processo}')
    except Exception as e:
        logging.error(f'Erro ao tentar atualizar TT_SPU: {e}')
        logging.error("Detalhes da excecao:", exc_info=True)

# Tratamento dos dados
def formatar_processo(NU_ORDEM, DT_PRODUCAO, NU_CGC_CPF, VL_PAGAMENTO):
    # Formatar data para o formato desejado (MM/YYYY)
    data_formatada = f"{DT_PRODUCAO.month:02d}/{DT_PRODUCAO.year}"

    # Formatar valor substituindo ponto por vírgula
    valor_formatado = f"{VL_PAGAMENTO:.2f}".replace('.', ',')

    # Converter NU_CGC_CPF para string e, em seguida, formatar para conter apenas números
    cnpj_cpf = str(NU_CGC_CPF)
    cnpj_formatado = ''.join(filter(str.isdigit, cnpj_cpf))

    # Garantir que o CNPJ tenha 14 caracteres, preenchendo com zeros à esquerda se necessário
    cnpj_formatado = cnpj_formatado.zfill(14)

    # Retornar os dados formatados como uma tupla
    return NU_ORDEM, data_formatada, cnpj_formatado, valor_formatado

## ÁREA DE TELAS E AÇÕES

servico = Service(ChromeDriverManager().install())
navegador = webdriver.Chrome(service = servico)
navegador.get(ambiente_escolhido['url_siafe'])
navegador.maximize_window()

def tempo_espera(tempo):
    time.sleep(tempo)

def elemento_existe(navegador, xpath):
    try:
        navegador.find_element(by=By.XPATH, value=xpath)
        return True
    except NoSuchElementException:
        return False

def tela_login(navegador, ambiente_escolhido):
    try:
        # Elementos tela Login
        usuario_login = navegador.find_element(by=By.XPATH, value='//*[@id="loginBox:itxUsuario::content"]')
        senha_login = navegador.find_element(by=By.XPATH, value='//*[@id="loginBox:itxSenhaAtual::content"]')
        botao_ok = navegador.find_element(by=By.XPATH, value='//*[@id="loginBox:btnConfirmar"]')
        # Ações tela Login
        usuario_login.send_keys(ambiente_escolhido['usuario_siafe'])
        tempo_espera(2)
        senha_login.send_keys(ambiente_escolhido['senha_siafe'])
        botao_ok.click()
        tempo_espera(2)
        # Click botão ok na mensagem Exercício 2024 se houver
        if elemento_existe(navegador, '//*[@id="pt1:warnMessageDec:frmExec:btnNewWarnMessageOK"]/span'):
            navegador.find_element(by=By.XPATH, value='//*[@id="pt1:warnMessageDec:frmExec:btnNewWarnMessageOK"]/span').click()
        tempo_espera(2)
    except Exception as e:
        print(f"Erro ao tentar fazer login: {e}")

def selecionar_dea():
    try:
        #selecionar UG Fassec
        select_ug = navegador.find_element(by=By.XPATH, value='//*[@id="pt1:selUg::content"]')
        opcao_ug = Select(select_ug)
        opcao_ug.select_by_value('2')  
        tempo_espera(0.5)
        #btn execução
        navegador.find_element(by=By.XPATH, value='//*[@id="pt1:pt_np4:1:pt_cni6::disclosureAnchor"]').click()
        tempo_espera(0.5)
        #btn contabilidade
        navegador.find_element(by=By.XPATH, value='//*[@id="pt1:pt_np3:2:pt_cni4::disclosureAnchor"]').click()
        tempo_espera(0.5)
        # btn Despesa Exercício Anterior
        navegador.find_element(by=By.XPATH, value='//*[@id="pt1:pt_np2:1:pt_cni3"]').click()
        tempo_espera(3)
    except Exception as e:
        logging.error(f'Erro ao tentar acessar o menu execucao: {e}')
        
def identificacao(processo, data_processo, data_atual, cnpj_cpf):
    for tentativa in range(3):  # Tenta até 3 vezes
        try:
            # campo UG
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:lovUnidadeGestora:itxLovDec::content"]').send_keys('460801')
            tempo_espera(0.5)
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:lovUnidadeGestora:itxLovDec::content"]').send_keys(Keys.TAB)
            tempo_espera(0.5)
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:lovUnidadeGestora:itxLovDec::content"]').send_keys(Keys.TAB)
            tempo_espera(1)
            # campo Numero termo
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxNumeroTermo::content"]').send_keys(Keys.CONTROL + 'a')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxNumeroTermo::content"]').send_keys(processo)
            print(f'Processo: {processo}')
            tempo_espera(2)
            # campo competência(Data processo)
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxCompetencia::content"]').send_keys(Keys.CONTROL + 'a')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxCompetencia::content"]').send_keys(data_processo)
            print(f'Data_processo: {data_processo}')
            tempo_espera(2)
            # campo data registro
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxDataRegistro::content"]').send_keys(Keys.CONTROL + 'a')
            tempo_espera(1)
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxDataRegistro::content"]').send_keys(data_atual)
            print(f'Data atual: {data_atual}')
            tempo_espera(1)
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxDataRegistro::content"]').send_keys(Keys.TAB)
            tempo_espera(1)
            # campo data publicação(Não preencher!)
            # campo CNPJ
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:lovPJ:itxLovDec::content"]').send_keys(Keys.CONTROL + 'a')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:lovPJ:itxLovDec::content"]').send_keys(cnpj_cpf)
            print(f'CNPJ: {cnpj_cpf}')
            tempo_espera(0.5)
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:lovPJ:itxLovDec::content"]').send_keys(Keys.TAB)
            tempo_espera(3)
            break  # Sai do loop se tudo correr bem
        except Exception as e:
            logging.error(f'Tentativa {tentativa + 1} falhou na etapa de identificacao da despesa: {e}')
            if tentativa == 2:  # Se foi a última tentativa, lança a exceção
                raise
        
def aba_detalhamento(valor_processo):
    for tentativa in range(3):  # Tenta até 3 vezes
        try:
            # Clicar em botão Detalhamento
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:slcClassificacao::disAcr"]').click()
            tempo_espera(2)
            # Select Natureza
            select_natureza = navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_33::content"]')
            opcao_natureza = Select(select_natureza)
            opcao_natureza.select_by_value('178')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_33::content"]').send_keys(Keys.TAB)
            tempo_espera(0.5)
            # Select Tipo Patrimonial
            select_tipo_patrimonial = navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_116::content"]')
            opcao_tipo_patrimonial = Select(select_tipo_patrimonial)
            opcao_tipo_patrimonial.select_by_value('0')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_116::content"]').send_keys(Keys.TAB)
            tempo_espera(0.5)
            # Select Item Patrimonial
            select_item_patrimonial = navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_109::content"]')
            opcao_item_patrimonial = Select(select_item_patrimonial)
            opcao_item_patrimonial.select_by_value('46')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_109::content"]').send_keys(Keys.TAB)
            tempo_espera(0.5)
            # Select Identificador Exercício Fonte
            select_identificador_exercício_fonte = navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_23::content"]')
            opcao_identificador_exercício_fonte = Select(select_identificador_exercício_fonte)
            opcao_identificador_exercício_fonte.select_by_value('0')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_23::content"]')
            tempo_espera(0.5)
            # Select Fonte
            select_fonte = navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_28::content"]')
            opcao_fonte = Select(select_fonte)
            opcao_fonte.select_by_value('54')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_28::content"]').send_keys(Keys.TAB)
            tempo_espera(0.5)
            # Select Tipo Detalhamento de Fonte
            select_tipo_detalhamento_de_fonte = navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_186::content"]')
            opcao_tipo_detalhamento_de_fonte = Select(select_tipo_detalhamento_de_fonte)
            opcao_tipo_detalhamento_de_fonte.select_by_value('1')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_186::content"]').send_keys(Keys.TAB)
            tempo_espera(0.5)
            #Select Detalhamento Fonte
            select_detalhamento_fonte = navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_159::content"]')
            opcao_detalhamento_fonte = Select(select_detalhamento_fonte)
            opcao_detalhamento_fonte.select_by_value('4')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:pnlClassificacao_chc_159::content"]').send_keys(Keys.TAB)
            tempo_espera(2)
            # Campo de Valor
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxValor::content"]').send_keys(Keys.CONTROL + 'a')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxValor::content"]').send_keys(valor_processo)
            print(f'Valor: {valor_processo}')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxValor::content"]').send_keys(Keys.TAB)
            tempo_espera(4)
            break  # Sai do loop se tudo correr bem
        except Exception as e:
            logging.error(f'Tentativa {tentativa + 1} falhou na Aba Detalhamento: {e}')
            identificacao(processo, data_processo, data_atual, cnpj_cpf)
            if tentativa == 2:  # Se foi a última tentativa, lança a exceção
                raise

def aba_observacao():
    for tentativa in range(3):  # Tenta até 3 vezes
        try:
            # Botão Observação
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:slcObservacao::disAcr"]').click()
            tempo_espera(2)
            # Campo descrição
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxObservacao::content"]').send_keys(Keys.CONTROL + 'a')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:itxObservacao::content"]').send_keys('Reconhecimento de DEA para empenho no exercício de 2024.')
            tempo_espera(0.5)
            # Botão Resgistrar e Reconhecer
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:btnRegistrarReconhecerDEA"]/span').click()
            tempo_espera(0.5)
            # No Pop-up select Operação Patrimonial
            select_operacao_patrimonial = navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:cbxOperacaoPatrimonial::content"]')
            opcao_operacao_patrimonial = Select(select_operacao_patrimonial)
            opcao_operacao_patrimonial.select_by_value('0')
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:cbxOperacaoPatrimonial::content"]').send_keys(Keys.TAB)
            tempo_espera(2)
            # Botão confirmar
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:popConfirmCancelaSim"]/span').click()
            tempo_espera(2)
            # Botão Ok
            navegador.find_element(by=By.XPATH, value='//*[@id="docPrincipal::msgDlg::cancel"]').click()
            tempo_espera(2)
            #botão Salvar e Sair
            navegador.find_element(by=By.XPATH, value='//*[@id="tplSip:pnlContent:btnSalvarSairDec"]/span').click()
            tempo_espera(3)
            break  # Sai do loop se tudo correr bem
        except Exception as e:
            logging.error(f'Tentativa {tentativa + 1} Erro na Aba Observacao: {e}')
            identificacao(processo, data_processo, data_atual, cnpj_cpf)
            if tentativa == 2:  # Se foi a última tentativa, lança a exceção
                raise

def inserir_dea(processo, data_processo, data_atual, cnpj_cpf, valor_processo):
    try:        
        # btn Inserir DEA
        navegador.find_element(by=By.XPATH, value='//*[@id="pagTemplate:tblEntidadeDec:btnInsert"]/a/span').click()
        tempo_espera(2)
        # Identificação DEA
        identificacao(processo, data_processo, data_atual, cnpj_cpf)
        # Aba Detalhamento
        aba_detalhamento(valor_processo)
        # Aba Observação
        aba_observacao()
        return True
    except Exception as e:
        logging.error(f'Erro ao tentar inserir DEA: {e}')
        return False
        
################################
## ÁREA EXECUÇÃO E PROCESSAMENTO
################################

tela_login(navegador, ambiente_escolhido)

# Escolher a opção de DEA no MENU Execução
selecionar_dea()

processos_falhados = []

def processar_e_atualizar(processos_falhados):
    processos_falhados_novos = []
    for processo_falhado in processos_falhados:
        tentativas = 0
        while tentativas < 2:
            try:
                processo, data_processo, valor_dea, cnpj_cpf, nome_cidade, tipo_tratamento, codigo_dea = obter_dados(processo_falhado)
                if inserir_dea(processo, data_processo, valor_dea, cnpj_cpf, nome_cidade, tipo_tratamento, codigo_dea):
                    atualizar_tt_spu(processo, codigo_dea)
                    break
            except Exception as e:
                logging.error(f'Erro ao reprocessar o DEA para o processo {processo}: {e}', exc_info=True)
                tentativas += 1

        if tentativas == 2:
            processos_falhados_novos.append(processo_falhado)
            logging.error(f'Processo {processo} falhou após 2 tentativas')

    processos_falhados.extend(processos_falhados_novos)
    if processos_falhados:
        try:
            with open('error.log', 'a') as f:
                for processo_falhado in processos_falhados:
                    f.write(json.dumps(processo_falhado.__dict__) + '\n')
        except Exception as e:
            logging.error(f'Erro ao armazenar processos falhados no arquivo de log: {e}', exc_info=True)

def obter_dados(processo_falhado):
    # Dados processo falhado
    processo = processo_falhado.processo
    data_processo = processo_falhado.data_processo
    valor_dea = processo_falhado.valor_dea
    cnpj_cpf = processo_falhado.cnpj_cpf
    nome_cidade = processo_falhado.nome_cidade
    tipo_tratamento = processo_falhado.tipo_tratamento
    codigo_dea = processo_falhado.codigo_dea

    return processo, data_processo, valor_dea, cnpj_cpf, nome_cidade, tipo_tratamento, codigo_dea

# Função para pegar o código DEA
def capturar_codigo_dea():
    try:
        # capturar o código DEA
        elemento_codigo_dea = navegador.find_element(by=By.XPATH, value='//*[@id="pagTemplate:tblEntidadeDec:tabViewerDec::db"]/table/tbody/tr[1]/td[1]/span')
        codigo_dea = elemento_codigo_dea.text
        print(f'Código DEA: {codigo_dea}')
        tempo_espera(2)
        return codigo_dea
    except Exception as e:
        logging.error(f'Erro ao tentar pegar o codigo DEA: {e}')

# Processar cada processo individualmente e imprimir os dados formatados
if resultados:
    for resultado in resultados:
        processo, data_processo, cnpj_cpf, valor_processo = formatar_processo(*resultado)
        print(processo, data_processo, cnpj_cpf, valor_processo)
        try:
            # Inserir novo DEA
            if inserir_dea(processo, data_processo, data_atual, cnpj_cpf, valor_processo):
                # Se a inserção for bem-sucedida, então atualizar TT_SPU
                codigo_dea = capturar_codigo_dea()
                atualizar_tt_spu(processo, codigo_dea)
                print(f'Processo {processo} processado com sucesso. Codigo DEA: {codigo_dea}')
        except Exception as e:
            logging.error(f'Erro ao processar o DEA para o processo {processo}: {e}')
            processos_falhados.append(processo)
        
processar_e_atualizar(processos_falhados)


input('Pressiona enter para sair...')
navegador.quit()

