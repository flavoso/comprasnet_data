import datetime
import sqlite3
import time
from collections import namedtuple
from concurrent import futures
from os import remove

import pandas as pd
from tqdm import tqdm

from componentes import Uasg, Pregao, Item
from config import ARQ_UASGS, SUBST, SQLITE_DB, DIR_CSV, MAX_WORKERS_ITENS, \
    MAX_WORKERS_PREGOES

df_uasgs = pd.read_csv(ARQ_UASGS, index_col='codUASG')

Tabela = namedtuple('Tabela', 'dados nome')


def to_csv(dfs, co_uasg):
    """
    Salva os dataframes correspondentes às tabelas da UASG como um conjunto de
    arquivos CSV.
    :param dfs: Dicionário contendo os dataframes a serem salvos.
    :param co_uasg: Integer (código da UASG)
    :return: None
    """
    for df in dfs.values():
        nome_arq = DIR_CSV + co_uasg + '_' + df.tabela + '.csv'
        df.dados.to_csv(nome_arq, index=False)

    return


def to_sqlite():
    """
    Corotina para salvar um dataframe na base sqlite ('SQLITE_DB').
    :param dfs: Dicionário contendo os dataframes a serem salvos.
    """

    base = sqlite3.connect(SQLITE_DB)
    cur = base.cursor()
    temp = sqlite3.connect('temp.db')

    try:
        n = None
        while True:
            tab = yield n
            try:
                tab.dados.to_sql(tab.nome, temp, if_exists='replace',
                                 index=False)
                cur.execute("ATTACH 'temp.db' as db1")
                sql = f"INSERT INTO {tab.nome} SELECT * FROM db1.{tab.nome}"
                cur.execute(sql)
                base.commit()
                cur.execute("DETACH db1")
            except sqlite3.Error as e:
                n = 0
                print('\nErro ao salvar a tabela')
                print(e)
            else:
                n = 1
    finally:
        temp.close()
        base.close()
        remove('temp.db')


def download_item(obj_item):
    """
    Efetua download dos elementos de um item: propostas e
    adjudicações, os quais são armazenados em dataframes correspondentes.
    :param obj_item: objeto (Série pandas com os dados do item)
    :return: tupla elementos Tabela (namedtuple), correspondentes aos
    dataframes das propostas e adjudicaçõo.
    """
    item = Item(obj_item)
    if item.num_partes != 0:
        df_propostas = item.partes()  # Download da tabela de propostas do item
        tupla_adjudic = item.adjudicacao()  # Download da adjudicação do item

        # Ajustando nome das colunas de df_propostas
        cols_orig = df_propostas.columns
        df_propostas.columns = [SUBST[c] for c in cols_orig]

        tab_proposta = Tabela(df_propostas, 'BASE_PREGAO_PROPOSTA')

        return tab_proposta, tupla_adjudic
    return None


def download_pregao(obj_pregao):
    """
    Efetua download dos elementos de um pregão: itens, propostas e
    adjudicações, os quais são armazenados em dataframes correspondentes.
    :param obj_pregao: objeto (Série pandas com os dados do pregão)
    :param contador: dicionário com informações para controle do número de
    pregões baixados
    :return: tupla de dataframes (df_itens, df_propostas, df_adjudicações).
    """
    pregao = Pregao(obj_pregao)
    num_itens = pregao.num_partes
    df_itens = pregao.partes()  # Download da tabela de itens
    lista_tab_propostas = []     # Lista das propostas de todos os itens
    lista_tup_adjudics = []      # Lista das adjudicações de todos os itens

    # Download concorrente dos itens do pregão
    workers = min(MAX_WORKERS_ITENS, num_itens)
    with futures.ThreadPoolExecutor(workers) as executor:
        a_baixar = {}
        for item in pregao:
            futuro = executor.submit(download_item, item)
            a_baixar[futuro] = item
        feito_iter = futures.as_completed(a_baixar)
        for p in feito_iter:
            lista_tab_propostas.append(p.result()[0])
            lista_tup_adjudics.append(p.result()[1])

    # Ajustando nomes das colunas de df_itens
    cols_orig = df_itens.columns
    df_itens.columns = [SUBST[c] for c in cols_orig]

    # Namedtuple que será enviadas à corotina to_sqlite()
    tab_itens = Tabela(df_itens, 'BASE_PREGAO_ITEM')

    return tab_itens, lista_tab_propostas, lista_tup_adjudics


def download_uasg(cod_uasg):
    """
    Efetua o download de todos os elementos do ComprasNet da uasg
    indicada.
    :param cod_uasg: integer (código UASG)
    :return:
    """

    uasg = Uasg(cod_uasg)
    nome = df_uasgs.loc[cod_uasg]['nomeUASG']
    msg = f'{nome} ({cod_uasg}): {uasg.num_partes} pregão(ões)'
    print('\n')
    print('-' * 50)
    print(msg)

    num_pregoes = uasg.num_partes

    if num_pregoes != 0:
        tabelas = []
        lista_adjudics = []
        df_pregoes = uasg.partes()  # Download do dataframe dos pregões da UASG

        # Renomeando as colunas do dataframe dos pregões
        cols_orig = df_pregoes.columns
        df_pregoes.columns = [SUBST[c] for c in cols_orig]

        # Criando namedtuple que será enviada à corotina to_sqlite()
        tab_pregoes = Tabela(df_pregoes, 'BASE_PREGAO')
        tabelas.append(tab_pregoes)

        # Download concorrente dos pregões
        workers = min(MAX_WORKERS_PREGOES, num_pregoes)
        with futures.ThreadPoolExecutor(workers) as executor:
            a_baixar = {}
            print('Processando pregões')
            for pregao in uasg:
                futuro = executor.submit(download_pregao, pregao)
                a_baixar[futuro] = pregao
            feito_iter = futures.as_completed(a_baixar)
            feito_iter = tqdm(feito_iter, total=num_pregoes)
            for p in feito_iter:
                tab_itens, lista_tab_propostas, lista_tup_adjudics = p.result()
                tabelas.append(tab_itens)
                tabelas.extend(lista_tab_propostas)
                lista_adjudics.extend(lista_tup_adjudics)

        # Geração do dataframe das adjudicações de todos os itens
        # O método item.adjudicacao() retorna None quando a adjudicação não
        # ocorreu. Por isso, é necessário expurgar os None da 'lista_adjudic'
        # antes de converter para DataFrame.

        df_adjudicacoes = pd.DataFrame([i for i in lista_adjudics if i])

        # Acrescentando as adjudicações à relação das tabelas que serão
        # enviadas ao banco de dados

        tab_adjudicacoes = Tabela(df_adjudicacoes, 'BASE_PREGAO_ADJUDICACAO')
        tabelas.append(tab_adjudicacoes)

        return tabelas
    return num_pregoes


def relata(co_uasg, nome_arq, n, err):
    """
    Gera o arquivo log.txt, relatando o progresso dos downloads e salvamentos.
    :param co_uasg: inteiro, código da UASG
    :return: None
    """
    with open(nome_arq, 'a') as arq:
        arq.write(f'UASG {co_uasg}: {n} tabelas salvas\n')
        if err:
            arq.write(f'{err}\n')


def salva(tabelas):
    """
    Função que aciona a corotina de salvamento das tabelas na base de dados.
    :param tabelas: lista de namedtuples do tipo Tabela.
    :return: None
    """
    print(f'Salvando as tabelas na base de dados')
    grava = to_sqlite()
    next(grava)  # Preparando ('priming') a corotina
    n = 0
    msg = None
    # Dataframes vazios não precisam ser salvos
    tabs = [t for t in tabelas if len(t.dados) != 0]
    for i, tab in enumerate(tabs, 1):
        print(f"\rSalvando tabela {i} de {len(tabs)}", end='')
        try:
            n = n + grava.send(tab)  # O método 'send' envia dados à corotina
        except sqlite3.Error as err:
            msg = err
    grava.close()
    print(f'\n{n} tabelas salvas')
    return n, msg


def download_todas(lista):
    """
    Efetua o download de todas as UASGs listadas.
    :param lista_uasgs: série pandas contendo relação dos códigos de UASGs
    :return:
    """
    nome_arq = datetime.datetime.now().strftime('%Y-%m-%d_%Hh%Mm%Ss') + '.txt'
    for co_uasg in lista:
        tabelas = download_uasg(co_uasg)
        n = 0
        msg = None
        if tabelas:
            n, msg = salva(tabelas)
        relata(co_uasg, nome_arq, n, msg)

    return len(lista)


def duracao(segundos):
    """
    Transforma um intervalo em segundos para uma string hh:mm:ss
    :param segundos:
    :return:
    """
    hs = int(segundos // 3600)
    mins = int((segundos % 3600) // 60)
    segs = int((segundos % 3600) % 60)
    return '{:0>2}h:{:0>2}m:{:0>2}s'.format(hs, mins, segs)


def main(df):
    t0 = time.time()
    lista = df.index
    # lista = [926266]
    count = download_todas(lista)
    segundos = time.time() - t0
    msg = '\n{} UASGs baixadas em {}.'
    print(msg.format(count, duracao(segundos)))


if __name__ == '__main__':
    main(df_uasgs)
