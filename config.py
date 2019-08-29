# Variáveis e parâmetros utilizados nos demais módulos

from collections import namedtuple

URL_BASE = 'http://compras.dados.gov.br'
PROXIES = {'http': '10.9.16.1:80', 'https': '10.9.16.1:80'}
ARQ_UASGS = 'listaUASGs.csv'
SAS_LIBREF = 'LIB_MATR'
SQLITE_DB = 'DadosComprasNet.sqlite'
DIR_CSV = 'csv/'
# Máximo de threads na execução concorrente. Um número excessivo poderá
# caracterizar ataque DOS contra o http://compras.dados.gov.br/
MAX_WORKERS_PREGOES = 10
MAX_WORKERS_ITENS = 10

colunas = namedtuple('colunas', 'pregoes itens propostas eventos adjudicacao')

COLS_DF_PREGAO_ORIG = {
    'Numero do Pregao': 'numeroPregao',
    'Número portaria': 'numeroPortaria',
    'Data portaria': 'dataPortaria',
    'Código processo': 'numeroProcesso',
    'Tipo do pregão': 'tipoPregao',
    'Tipo de compra': 'tipoCompra',
    'Objeto do pregão': 'objetoPregao',
    'UASG': 'uasg',
    'Situação do pregão': 'situacaoPregao',
    'Data de Abertura do Edital': 'dataAberturaEdital',
    'Data de início da proposta': 'dataInicioProposta',
    'Data do fim da proposta': 'dataFimProposta',
    'Resultados do pregão > uri': 'resultadosPregao',
    'Declarações do pregão > uri': 'declaracoesPregao',
    'Termos do pregão > uri': 'termosPregao',
    'Orgão do pregão > uri': 'orgaosPregao',
    'Itens do pregão > uri': 'itensPregao',
}

COLS_DF_PREGAO = {
    **COLS_DF_PREGAO_ORIG,
    **{'id_uasg': 'idUasg', 'id_pregao': 'idPregao'}
}

COLS_DF_ITEM_ORIG = {
    'Descrição do item': 'descricaoItem',
    'Quantidade do item': 'quantidadeItem',
    'Valor estimado do item': 'valorEstimadoItem',
    'Descrição detalhada do Item': 'descricaoDetalhadaItem',
    'Tratamento diferenciado': 'tratamentoDiferenciado',
    'Decreto 7174': 'decreto7174',
    'Margem preferencial': 'margemPreferencial',
    'Unidade de fornecimento': 'unidadeFornecimento',
    'Situação do item': 'situacaoItem',
    'Fornecedor vencedor': 'fornecedorVencedor',
    'Valor melhor lance': 'valorMelhorLance',
    'Valor negociado do item': 'valorNegociadoItem',
    'Propostas do Item da licitação > uri': 'propostasItem',
    'Termos do pregão > uri': 'termosPregao',
    'Eventos do Item da licitação > uri': 'eventosItem',
}

COLS_DF_ITEM = {
    **COLS_DF_ITEM_ORIG,
    **{'id_pregao': 'idPregao', 'id_item': 'idItem', 'adjudicado': 'adjudicado'}
}

COLS_DF_PROPOSTA_ORIG = {
    'Descrição do Item': 'descricaoItem',
    'Quantidade de itens': 'quantidadeItens',
    'Valor estimado do item': 'valorEstimadoItem',
    'Descrição complementar do item': 'descricaoComplementarItem',
    'Tratamento diferenciado': 'tratamentoDiferenciado',
    'Decreto 7174': 'decreto7174',
    'Margem preferencial': 'margemPreferencial',
    'Unidade de fornecimento': 'unidadeFornecimento',
    'Situação do item': 'situacaoItem',
    'Fornecedor vencedor': 'fornecedorVencedor',
    'Valor menor lance': 'valorMenorLance',
    'Número cpf/cnpj fornecedor': 'cpfCnpjFornecedor',
    'Fornecedor proposta': 'fornecedorProposta',
    'Marca do item': 'marcaItem',
    'Descrição fabricante do item': 'descricaoFabricanteItem',
    'Descrição detalhada do item': 'descricaoDetalhadaItem',
    'Porte da empresa': 'porteEmpresa',
    'Declaração ME/EPP/COOP': 'declaracaoMeEppCoop',
    'Quantidade itens da proposta': 'quantidadeItensProposta',
    'Valor unitário': 'valorUnitario',
    'Valor global': 'valorGlobal',
    'Desconto': 'desconto',
    'Valor com Desconto': 'valorComDesconto',
    'Data do registro': 'dataRegistro',
    'Data das declarações': 'dataDeclaracoes',
    'Declaração superveniente': 'declaracaoSuperveniente',
    'Declaração infantil': 'declaracaoInfantil',
    'Declaração independente': 'declaracaoIndependente',
    'Descrição declaração ciência': 'descricaoDeclaracaoCiencia',
    'Descrição motivo cancelamento': 'descricaoMotivoCancelamento',
    'Valor classificado': 'valorClassificado',
    'Valor negociado': 'valorNegociado',
    'Observações': 'observacoes',
    'Anexos da proposta > uri': 'anexosProposta',
}

COLS_DF_PROPOSTA = {
    **COLS_DF_PROPOSTA_ORIG,
    **{'id_item': 'idItem'}
}

# O DataFrame foi criado por mim. Logo, já contém os nomes adequados para as
# colunas. Por isso, não precisa ser um dicionário.
COLS_DF_ADJUDICACAO = (
    'idItem',
    'observacao',
    'nomeFornecedor',
    'cpfCnpjFornecedor',
    'valorAdjudicado',
    'dataAdjudicacao',
)

COLS_DF_EVENTO = {
    'Número do item': 'numeroItem',
    'Código do evento': 'codigoEvento',
    'Descrição do evento': 'descricaoEvento',
    'Observação': 'observacao',
    'Data e hora do evento': 'dataHoraEvento'
}

COLUNAS = colunas(
    COLS_DF_PREGAO_ORIG,
    COLS_DF_ITEM_ORIG,
    COLS_DF_PROPOSTA_ORIG,
    COLS_DF_EVENTO,
    COLS_DF_ADJUDICACAO
)

SUBST = {**COLS_DF_PREGAO, **COLS_DF_ITEM, **COLS_DF_PROPOSTA}