# -*- coding: utf-8 -*-
"""
Script para redistribuição de estoque baseado em clusters e vendas médias mensais.
Criado em: 26 de Março de 2025
Autor: lucas
"""

import pandas as pd
import psycopg2
from datetime import date
from dateutil.relativedelta import relativedelta

# Definição do caminho das bases
PATH = r'M:\Planejamento\bases_macro\BASES_ACCESS'

# Obtendo a data atual e calculando data de referência (há 3 meses)
hoje = date.today()
data_query = (hoje - relativedelta(months=3)).strftime('%Y-%m-%d')

# Consulta SQL para obter os dados de produção
QUERY_FILB = f'''
    WITH cte_producao AS (
        SELECT id_ipr AS Material, filial AS Centro, SUM(qtde) AS qtde
        FROM pricing_dados.producao
        INNER JOIN public.mara ON id_ipr = sku
        WHERE mara.segmento IN (104160301, 104160302)
        AND data > '{data_query}'
        AND tipo = 1
        GROUP BY id_ipr, filial
    )
    SELECT * FROM cte_producao ORDER BY Material, Centro;
'''

# Conexão com o banco de dados
conn = psycopg2.connect(
    host="qqmtz1561.qq",
    port="5432",
    database="pricing_dados",
    user="155682",
    password="155682"
)

# Carregar dados da consulta SQL para um DataFrame
prod_3m = pd.read_sql(QUERY_FILB, con=conn)
conn.close()

# Calcular a venda média mensal e renomear colunas
prod_3m['Venda_Media_Mensal'] = (prod_3m['qtde'] / 3).round(2)
prod_3m.drop(columns=['qtde'], inplace=True)

# Carregar mapeamento de materiais
map_mat_nova = pd.read_excel(PATH + '\\Map_Material.xlsx')
df = prod_3m.merge(map_mat_nova, on='Material', how='left')

# Carregar dados de cluster
cl_fil = pd.read_csv(PATH + '\\cluster.csv', sep=';', decimal=',', encoding='latin-1')
cl_fil.rename(columns={'FILIAL': 'Centro', 'Cluster_modificado': 'Fil_Cluster'}, inplace=True)
df = df.merge(cl_fil, on=['Centro', 'COD_Segmento'], how='left')
df['Fil_Cluster'].fillna(-10, inplace=True)

# Carregar estoque da ZTMM051
ztmm = pd.read_csv(PATH + '\\ZTMM051.csv', sep=';', decimal=',', encoding='latin-1',
                   usecols=['Material', 'Centro', 'Quantidade', 'Em_Pedido'])
ztmm.rename(columns={'Quantidade': 'Qtd_est_Filial', 'Em_Pedido': 'Qtd_est_em_pedido'}, inplace=True)
df = df.merge(ztmm, on=['Centro', 'Material'], how='left')

# Selecionar colunas finais para análise
df = df[['Centro', 'Material', 'Venda_Media_Mensal', 'COD_Segmento', 'SEGMENTO',
         'Fil_Cluster', 'Qtd_est_Filial', 'Qtd_est_em_pedido']]

df.fillna(0, inplace=True)  # Substituir valores NaN por 0

# Filtrar Cluster 1 e calcular necessidade de estoque
meses_para_outubro = 7  # De março até outubro

df_cluster1 = df[df['Fil_Cluster'] == 1].copy()
df_cluster1['Necessidade_estoque'] = ((df_cluster1['Venda_Media_Mensal'] * meses_para_outubro)
                                       - df_cluster1['Qtd_est_Filial']).clip(lower=0).round(0)

# Estoque disponível para redistribuição
df_outros_clusters = df[df['Fil_Cluster'] != 1]
estoque_disponivel = df_outros_clusters.groupby('Material')[['Qtd_est_Filial', 'Qtd_est_em_pedido']].sum()
estoque_disponivel['Estoque_Total_Disponivel'] = estoque_disponivel.sum(axis=1)

# Juntar estoque disponível ao Cluster 1
df_cluster1 = df_cluster1.merge(estoque_disponivel[['Estoque_Total_Disponivel']], on='Material', how='left')
df_cluster1.fillna(0, inplace=True)

# Ordenar por maior venda mensal

df_cluster1.sort_values(by=['Material', 'Venda_Media_Mensal'], ascending=[True, False], inplace=True)

def distribuir_estoque(grupo):
    estoque_restante = grupo['Estoque_Total_Disponivel'].iloc[0]
    quantidades = []
    estoque_atualizado = []
    for necessidade in grupo['Necessidade_estoque']:
        qtd_enviar = min(necessidade, estoque_restante)
        quantidades.append(qtd_enviar)
        estoque_restante -= qtd_enviar
        estoque_atualizado.append(max(estoque_restante, 0))
    grupo['Qtd_a_Enviar'] = quantidades
    grupo['Estoque_Apos_Envio'] = estoque_atualizado
    return grupo

# Aplicar a distribuição de estoque
df_cluster1 = df_cluster1.groupby('Material', group_keys=False).apply(distribuir_estoque)

# Selecionar colunas finais para exportação
resultado = df_cluster1[['Centro', 'Fil_Cluster', 'Material', 'Venda_Media_Mensal', 'COD_Segmento', 'SEGMENTO',
                         'Qtd_est_Filial', 'Necessidade_estoque', 'Qtd_a_Enviar', 'Estoque_Total_Disponivel',
                         'Estoque_Apos_Envio']]

# Salvar resultado em CSV
output_path = r'M:\Planejamento\Lucas\Redistribuição\Redistribuicao.csv'
resultado.to_csv(output_path, index=False, sep=';', decimal=',', encoding='latin')

print("Redistribuição de estoque concluída e salva em:", output_path)
