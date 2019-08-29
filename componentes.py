#!/usr/bin/env python
# coding: utf-8

import re
from abc import ABC, abstractmethod
from collections import namedtuple
from io import StringIO

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError
from config import URL_BASE, PROXIES, COLUNAS


def request(uri, params):
    """
    Retorna resposta de um request.
    :param uri: string
    :param params: dict
    :return: object fornecido por requests.get
    """
    endereco = URL_BASE + ''.join(uri)
    try:
        resp = requests.get(endereco, params, proxies=PROXIES,
                            allow_redirects=True)
    except HTTPError:
        return False
    else:
        return resp


def csv2df(uri, params):
    """
    Converte o conteúdo do CSV disponível em certa url para um dataframe pandas.
    :param url: string
    :param params: dicionário contendo os parâmetros da url
    :return: dataframe pandas com o conteúdo do arquivo CSV.
    """

    resp = request(uri, params)
    resp_bytes = resp.content             # Conteúdo do CSV, em bytes
    resp_str = resp_bytes.decode('utf8')  # Conteúdo do CSV, em texto
    csv = StringIO(resp_str)             # Para que o pandas possa ler da string
    try:
        df = pd.read_csv(csv)
    except pd.io.common.EmptyDataError:   # CSV vazio...
        df = pd.DataFrame()               # ... dataframe também vazio.
    return df


def limpa_cpf_cnpj(cpfcnpj):
    """
    Exclui marcas de pontuação do CPF/CNPJ.
    :param cpfcnpj:
    :return: string
    """
    try:
        limpo = re.sub(r'[./-]', '', cpfcnpj)
    except TypeError:  # Contempla o caso em que cpfcnpj = np.nan
        limpo = np.nan

    return limpo


def limpa_cifra(cifra):
    """
    Transforma uma cifra em float. Funciona inclusive para números escritos
    no padrão da língua inglesa (P. ex. R$ 2,500.00). OBS: não funcionará
    para casos em que não haja separador decimal, tal como 3.000. Neste caso,
    será retornado o float 3.0. Mas esse caso tende a ser raro.
    :param cifra: string
    :return: float
    """

    cifra = re.sub(r'R\$\s*', '', str(cifra))
    try:
        sep_decimal = [x for x in cifra if x in ',.'][-1]
    except IndexError:
        sep_decimal = '.'
    if sep_decimal == ',':
        cifra = cifra.replace('.', '').replace(',', '.')
    else:
        cifra = cifra.replace(',', '')
    try:
        limpo = float(cifra)
    except ValueError:
        limpo = np.nan

    return limpo


def prepara_latin1(entrada):
    """
    Substitui caracteres utf-8 que não podem ser codificados em latin_1 por
    similares, para evitar problemas na conversão efetuada pelo SAS.
    :param entrada: string
    :return: string
    """
    saida = entrada
    if isinstance(entrada, str):
        saida = re.sub('[\u201d\u201c]', '"', entrada)
        saida = re.sub('[\u2019\u2018]', '\'', saida)
        saida = re.sub('[\u2013\u2014]', '--', saida)
        saida = re.sub('[\u2122]', 'TM', saida)
        saida = re.sub('[\u2022]', '*', saida)
        saida = re.sub('[\u02c6]', '^', saida)
        saida = saida.encode('latin1', errors='replace').decode('latin1')
    return saida


def sanitiza_df(df):
    """
    Converte valores monetários de texto para float e reconhece datas.
    Prepara o dataframe para importação pelo SAS (latin_1).
    :param df: Dataframe pandas
    :return: Dataframe sanitizado
    """
    colunas = df.columns
    colunas_data = [d for d in colunas if 'Data' in d]
    colunas_cifra = [c for c in colunas if 'Valor' in c]
    colunas_cnpj = [x for x in colunas if 'cnpj' in x.lower()]

    output = df.copy()
    for col in colunas_data:
        output[col] = pd.to_datetime(output[col])

    for col in colunas_cifra:
        output[col] = output[col].apply(limpa_cifra)

    for col in colunas_cnpj:
        output[col] = output[col].apply(limpa_cpf_cnpj)

    output = output.applymap(prepara_latin1)

    return output


class Componente(ABC):
    """Classe abstrata, da qual herdarão as classes Uasg, Pregao e Item."""

    @property
    def parte_de(self):
        """Retorna o código do componente de que este é parte."""
        try:
            output = self.dados[-1]
        except TypeError:
            output = 'GDF'
        return output

    @abstractmethod
    def partes(self):
        """Retorna dataframe correspondente ao CSV que lista as partes do
        componente."""
        pass

    def __getitem__(self, index):
        try:
            output = self.partes().iloc[index]
        except AttributeError:
            output = 'Instância da classe {} não possui partes.'.format(
                self.__class__.__name__)
        return output

    def __len__(self):
        try:
            tam = len(self.partes())
        except pd.io.common.EmptyDataError:
            tam = 0
        return tam


class Uasg(Componente):
    """Representa uma UASG, no ComprasNet. As "partes" desta classe são os
    pregões."""

    dados = None
    uri = '/pregoes/v1/pregoes'
    colunas = COLUNAS.pregoes.keys()

    def __init__(self, id):
        self._id = str(id)
        self._params = {'co_uasg': str(self._id)}

    @property
    def id(self):
        return self._id

    @property
    def num_partes(self):
        """Retorna o número de pregões da UASG informado no site."""
        resp = request(self.uri, self._params)
        if resp:
            soup = BeautifulSoup(resp.text, 'html.parser')
            return int(
                soup.find_all(class_='num-resultados')[0].text.split(' ')[-1])
        return 0

    def _offsets(self):
        """Retorna a lista dos offsets a serem utilizados como parâmetro para
        download dos CSVs"""

        return [i * 500 for i in range(self.num_partes // 500 + 1)]

    def partes(self):
        """Retorna dataframe correspondente ao CSV dos pregões da UASG."""
        output = pd.DataFrame(columns=self.colunas)
        if self.num_partes:
            for offset in self._offsets():
                self._params['offset'] = offset
                df = csv2df(self.uri + '.csv', self._params)
                output = output.append(df)
            output['id_uasg'] = self.id
            pattern_id_pregao = re.compile(r'/(\d+)/itens$')
            extrai_id_pregao = lambda x: pattern_id_pregao.findall(x)[0]
            output['id_pregao'] = output['Itens do pregão > uri'].apply(
                extrai_id_pregao)

        return sanitiza_df(output)

    def __repr__(self):
        return f'UASG {self._id}'


class Pregao(Componente):
    """Representa um pregão, no ComprasNet. As "partes" do pregão são os
    itens."""

    uri = '/pregoes/doc/pregao/'
    colunas = COLUNAS.itens.keys()

    def __init__(self, dados_pregao):
        """A classe é instanciada a partir dos dados do pregão, retornados por
        um objeto Uasg."""
        self.dados = dados_pregao
        self._params = {}

    @property
    def id(self):
        pattern = re.compile(r'\d+')
        return pattern.findall(self.dados['Itens do pregão > uri'])[0]

    @property
    def num_partes(self):
        """Retorna o número de itens do pregão."""
        end = self.uri + self.id + '/itens'
        resp = request(end, self._params)
        if resp:
            soup = BeautifulSoup(resp.text, 'html.parser')
            return int(
                soup.find_all(class_='num-resultados')[0].text.split(' ')[-1])
        return 0

    def _offsets(self):
        """Retorna a lista dos offsets a serem utilizados como parâmetro para
        download dos CSVs."""

        return [i * 500 for i in range(self.num_partes // 500 + 1)]

    def partes(self):
        """Retorna dataframe correspondente ao CSV dos itens do pregão."""
        output = pd.DataFrame(columns=self.colunas)
        end = self.uri + self.id + '/itens.csv'
        for offset in self._offsets():
            self._params['offset'] = offset
            df = csv2df(end, self._params)
            output = output.append(df, sort=False)
        output['id_pregao'] = self.id
        pattern_id_item = re.compile(r'item=(\d+)')
        extrai_id = lambda x: pattern_id_item.findall(x)[0]
        output['id_item'] = output['Eventos do Item da licitação > uri'].apply(
            extrai_id
        )

        # Agora, é necessário instanciar cada item, para verificar se foi
        # adjudicado.
        output['adjudicado'] = [Item(x).adjudicado() for i, x in
                                output.iterrows()]

        return sanitiza_df(output)

    def __repr__(self):
        return f'Pregão {self.id}'


class Item(Componente):
    """Representa um item de um pregão. As partes componentes deste item são as
    propostas."""

    uri = '/pregoes/v1/proposta_item_pregao'
    colunas = COLUNAS.propostas.keys()

    def __init__(self, dados_item):
        """A classe é instanciada a partir dos dados do item, retornados por um
        objeto Pregao."""
        self.dados = dados_item

    @property
    def id(self):
        pattern = re.compile(r'item=(\d+)')
        return pattern.findall(
            self.dados['Propostas do Item da licitação > uri']
        )[0]

    def co_uasg(self):
        pattern = re.compile(r'co_uasg=(\d+)')
        return pattern.findall(self.dados['Termos do pregão > uri'])[0]

    def co_pregao(self):
        pattern = re.compile(r'co_pregao=(\d+)')
        return pattern.findall(
            self.dados['Propostas do Item da licitação > uri']
        )[0]

    def nu_pregao(self):
        pattern = re.compile(r'nu_pregao=(\d+)')
        return pattern.findall(self.dados[-3])[0]

    @property
    def num_partes(self):
        """Retorna o número de propostas apresentadas para este item."""

        params = {'item': self.id, 'co_pregao': self.co_pregao()}
        resp = request(self.uri + '.html', params)
        if resp:
            soup = BeautifulSoup(resp.text, 'html.parser')
            return int(
                soup.find_all(class_='num-resultados')[0].text.split(' ')[-1])
        return 0

    def _offsets(self):
        """Retorna a lista dos offsets a serem utilizados como parâmetro para
        ownload dos CSVs."""

        return [i * 500 for i in range(self.num_partes // 500 + 1)]

    def eventos(self):
        """Retorna dataframe correspondente ao CSV da lista de eventos deste
        item."""
        colunas = COLUNAS.eventos.keys()
        output = pd.DataFrame(columns=colunas)
        uri = '/pregoes/v1/evento_item_pregao'
        params = {'item': self.id}
        df_eventos = csv2df(uri + '.csv', params)
        if len(df_eventos):
            output = output.append(df_eventos)
            output = sanitiza_df(output)
        return output

    def adjudicado(self):
        eventos = self.eventos()['Descrição do evento'].values
        return 'Adjudicado' in tuple(eventos)

    def partes(self):
        """Retorna dataframe correspondente ao CSV das propostas para esse
        item."""

        output = pd.DataFrame(columns=self.colunas)
        if self.num_partes:
            params = {'item': self.id, 'co_pregao': self.co_pregao()}
            for offset in self._offsets():
                params['offset'] = offset
                df = csv2df(self.uri + '.csv', params)
                output = output.append(df)
            output['id_item'] = self.id
        return sanitiza_df(output)

    def adjudicacao(self):
        """Retorna dados da proposta vencedora, se houver adjudicação.
        Essa informação é extraída dos eventos de cada item, os quais nem
        sempre são preenchidos corretamente."""

        Vencedor = namedtuple('Vencedor', COLUNAS.adjudicacao)

        if self.adjudicado():
            df = self.eventos()
            obs = df.loc[df['Descrição do evento'] == 'Adjudicado']
            obs = obs.iloc[0]['Observação']
            nome_pattern = re.compile(r'Fornecedor:\s*(.*?),')
            try:
                nome = nome_pattern.findall(obs)[0]
            except IndexError:
                nome = np.nan
            cnpj_pattern = re.compile(r'CNPJ/CPF:\s*(.*?),')
            try:
                cnpj = cnpj_pattern.findall(obs)[0]
            except IndexError:
                cnpj = np.nan
            cnpj = limpa_cpf_cnpj(cnpj)

            # A regex abaixo vai encontrar a primeira cifra que constar do
            # campo "Observação". Caso haja mais de uma cifra, o valor
            # retornado pode não ser o da adjudicação.
            valor_pattern = re.compile(r'R\$\s*([\d\.,]+\d)')
            try:
                valor = valor_pattern.findall(obs)[0]
            except IndexError:
                valor = np.nan
            valor = limpa_cifra(valor)
            data_adj = df.loc[df['Descrição do evento'] == 'Adjudicado']
            data_adj = data_adj.iloc[0]['Data e hora do evento']
            data_adj = data_adj.to_pydatetime()
            return Vencedor(self.id, obs, nome, cnpj, valor, data_adj)
        return None

    def __repr__(self):
        return f'Item {self.id} (Pregão {self.parte_de})'


# class Proposta(Componente):
#     """Representa uma proposta apresentada no pregão."""
#
#     def __init__(self, dados):
#         """A classe é instanciada a partir da proposta, retornados por um objeto
#         Item."""
#         self.dados = dados
#
#     @property
#     def id(self):
#         pattern = re.compile(r'co_proposta=(\d+)')
#         return pattern.findall(self.dados[-2])[0]
#
#     def partes(self):
#         return ()
#
#     def __repr__(self):
#         return f'Proposta de {self.dados["Número cpf/cnpj fornecedor"]}'