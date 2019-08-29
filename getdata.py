import sqlite3
import time
from collections import namedtuple
from concurrent import futures
from os import remove

import pandas as pd
from tqdm import tqdm

from componentes import Uasg, Pregao, Item
from config import ARQ_UASGS, SUBST, SQLITE_DB, DIR_CSV

# Máximo de threads na execução concorrente. Um número excessivo poderá
# caracterizar ataque DOS contra o http://compras.dados.gov.br/
MAX_WORKERS = 100
LISTA_UASGS = pd.read_csv(ARQ_UASGS)

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


def to_sqlite2(dfs, uasg):
    """
    Salva os dataframes da uasg em base sqlite.
    :param dfs: Dicionário contendo os dataframes a serem salvos.
    :return: None
    """

    nome = str(uasg) + '.sqlite'
    base = sqlite3.connect(nome)

    for df in dfs.values():
        df.dados.to_sql(df.nome_tabela, base, if_exists='replace', index=False)

    base.close()

    return


def to_sqlite():
    """
    Corrotina para salvar um dataframe na base sqlite ('SQLITE_DB').
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


def to_sqlite3(dfs):
    """
    Salva os dataframes em base sqlite, de modo incremental.
    :param dfs: Dicionário contendo os dataframes a serem salvos.
    :return: None
    """

    main = sqlite3.connect(SQLITE_DB)
    cur = main.cursor()
    temp = sqlite3.connect('temp.db')

    for df in dfs.values():
        df.dados.to_sql(df.nome_tabela, temp, if_exists='replace', index=False)

    cur.execute("ATTACH 'temp.db' as db1")

    for df in dfs.values():
        sql = f"INSERT INTO {df.nome_tabela} SELECT * FROM db1.{df.nome_tabela}"
        cur.execute(sql)

    main.commit()
    cur.execute("DETACH db1")

    temp.close()
    main.close()

    return


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
    df_itens = pregao.partes()  # Download da tabela de itens
    lista_df_propostas = []
    lista_adjudic = []

    for obj_item in pregao:
        item = Item(obj_item)
        lista_df_propostas.append(item.partes())  # Download de propostas
        lista_adjudic.append(item.adjudicacao())  # Download da adjudicação

    # Geração do dataframe das propostas

    df_propostas = pd.concat(lista_df_propostas, ignore_index=True, sort=False)

    # Geração do dataframe das adjudicações
    # O método item.adjudicacao() retorna None quando a adjudicação não
    # ocorreu. Por isso, é necessário expurgar os None da 'lista_adjudic'
    # antes de converter para DataFrame.

    df_adjudicacoes = pd.DataFrame([i for i in lista_adjudic if i])

    # Ajustando nomes das colunas dos dataframes ver arquivo config.py)
    for df in df_itens, df_propostas:
        cols_orig = df.columns
        df.columns = [SUBST[c] for c in cols_orig]

    # Geração das namedtuples que serão enviadas à corrotina to_sqlite()
    tab_itens = Tabela(df_itens, 'BASE_PREGAO_ITEM')
    tab_propostas = Tabela(df_propostas, 'BASE_PREGAO_PROPOSTA')
    tab_adjudicacoes = Tabela(df_adjudicacoes, 'BASE_PREGAO_ADJUDICACAO')

    return tab_itens, tab_propostas, tab_adjudicacoes


def download_uasg(cod_uasg):
    """
    Efetua o download de todos os elementos do ComprasNet da uasg
    indicada.
    :param cod_uasg: integer (código UASG)
    :return:
    """

    uasg = Uasg(cod_uasg)
    nome = 'SECRETARIA DE ESTADO DE CIÊNCIA TECNOLOGIA E INOVAÇÃO'
    # nome = LISTA_UASGS.loc[
    #     LISTA_UASGS['codUASG'] == cod_uasg]['nomeUASG'].iloc[0]
    msg = f'{nome} ({cod_uasg}): {uasg.num_partes} pregão(ões)'
    print('\n')
    print('-' * 50)
    print(msg)

    num_pregoes = uasg.num_partes

    if num_pregoes != 0:
        tabelas = []
        df_pregoes = uasg.partes()  # Download do dataframe dos pregões da UASG

        # Renomeando as colunas do dataframe dos pregões
        cols_orig = df_pregoes.columns
        df_pregoes.columns = [SUBST[c] for c in cols_orig]

        # Criando namedtuple que será enviada à corrotina to_sqlite()
        tab_pregoes = Tabela(df_pregoes, 'BASE_PREGAO')
        tabelas.append(tab_pregoes)

        # Download concorrente dos pregões
        workers = min(MAX_WORKERS, num_pregoes)
        with futures.ThreadPoolExecutor(workers) as executor:
            a_baixar = {}
            print('Processando pregões')
            for pregao in uasg:
                futuro = executor.submit(download_pregao, pregao)
                a_baixar[futuro] = pregao
            feito_iter = futures.as_completed(a_baixar)
            feito_iter = tqdm(feito_iter, total=num_pregoes)
            # n = 0
            for p in feito_iter:
                # n += 1
                # print(f'\rProcessados {n} de {num_pregoes}', end='')
                tabs = p.result()
                for t in tabs:
                    tabelas.append(t)

        # Salvando no sqlite

        print(f'Salvando as tabelas na base de dados')
        grava = to_sqlite()
        next(grava)  # "Priming" a corrotina
        n = 0
        # Dataframes vazios não precisam ser salvos
        tabelas = [t for t in tabelas if len(t.dados) != 0]
        for i, tab in enumerate(tabelas, 1):
            print(f"\rSalvando tabela {i} de {len(tabelas)}", end='')
            n = n + grava.send(tab)
        grava.close()
        print(f'\n{n} tabelas salvas')

        return tabelas
    return num_pregoes


def download_todas(lista):
    """
    Efetua o download de todas as Uasgs listadas.
    :param lista_uasgs:
    :return:
    """

    for co_uasg in lista:
        download_uasg(co_uasg)

    return len(lista)


def duracao(segundos):
    """
    Transforma um intervalo em segundos para uma string hh:mm:ss
    :param segundos:
    :return:
    """
    hs = segundos // 3600
    mins = (segundos % 3600) // 60
    segs = (segundos % 3600) % 60
    return '{:0>2}h:{:0>2}m:{:0>2}s'.format(hs, mins, segs)


def main(df):
    t0 = time.time()
    lista = df['codUASG']
    # lista = [926266]
    count = download_todas(lista)
    segundos = time.time() - t0
    msg = '\n{} UASGs baixadas em {}.'
    print(msg.format(count, duracao(segundos)))


if __name__ == '__main__':
    main(LISTA_UASGS)
