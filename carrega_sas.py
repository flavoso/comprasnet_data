from config import SQLITE_DB, SAS_LIBREF
import saspy
import sqlite3
import pandas as pd
import time

con = sqlite3.connect(SQLITE_DB)

tabelas = {
    'pregao': 'BASE_PREGAO',
    'item': 'BASE_PREGAO_ITEM',
    'proposta': 'BASE_PREGAO_PROPOSTA',
    'adjudicacao': 'BASE_PREGAO_ADJUDICACAO'
}


def de_sqlite():
    """
    Lê os dados da base sqlite para dataframes.
    :param tabelas: dicionário contendo os nomes das tabelas.
    :return dfs: dicionário contendo os dataframes correspondentes a cada
    tabela.
    """
    dfs = {}
    print(f"Lendo dados da base '{SQLITE_DB}'... ", end='')
    for k, v in tabelas.items():
        dfs[k] = pd.read_sql(f'SELECT * FROM {v}', con, parse_dates=True)
    print('OK.')
    con.close()
    return dfs


def para_sas(dfs):
    """
    Carrega os dataframes pandas para a library do SAS.
    :param dfs: dicionário contendo os dataframes a serem carregados
    :return: none
    """
    sas = saspy.SASsession()
    for k, v in dfs.items():
        print(f'Carregando tabela {tabelas[k]}... ', end='')
        sas.df2sd(df=dfs[k], table=tabelas[k], libref=SAS_LIBREF)
        print('OK.')
    sas.disconnect()
    sas.endsas()
    return


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


def main():
    t0 = time.time()
    para_sas(de_sqlite())
    segundos = time.time() - t0
    tempo = duracao(segundos)
    print(f'\nDados carregados em {tempo}.')


if __name__ == '__main__':
    main()
