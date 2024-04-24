# ETL Microdados do INEP = Educação Superior
# Autor: Murilo
# Data: 14/08/2023

# Conectar na base do DW_INEP
import mysql.connector
import pandas as pd
# Dictionary com a config do banco para conexão
config = {
    'user': 'root',
    'password': '123030',
    'host': '127.0.0.1',
    'port': '3306',
    'database': 'dw_inep'
}
try:
   conn = mysql.connector.connect(**config)
   cursor = conn.cursor()
   
   planilhas  = pd.read_csv('C:/Users/muril/OneDrive/Área de Trabalho/Dados/Microdados do Censo da Educação Superior 2020/dados/MICRODADOS_CADASTRO_CURSOS_2020.CSV'
                            ,sep = ';'
                            , encoding = 'iso-8859-1'
                            , low_memory = False)
   planilhas = planilhas.fillna('')
   
   ## criando a inserção de planilhas relacionadas ao curso de 2020
   
   planilhas_cursos = pd.DataFrame(planilhas['NO_CURSO'].unique(), columns = ['CURSO'])
   for i,r in planilhas_cursos.iterrows():
       
       insert_statement = f"INSERT INTO dim_curso (tf_curso, curso) values({i}, '{r['CURSO']}')"
       print(insert_statement)
       cursor.execute(insert_statement)
       conn.commit()
       
   ## Criando a inserção de planilhas relacionadas ao Ano de 2020
    
   planilhas_ano = pd.DataFrame(planilhas['NU_ANO_CENSO'].unique(), columns = ['ANO'])
   for i,r in planilhas_ano.iterrows():
        
        insert_statement = f"INSERT INTO  dim_ano (tf_ano, ano) values ({i}, '{r['ANO']}')"
        print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()
        
   ## criando a inserção de planilhas relacionadas ao IES 2020
   
   planilhas_ies = pd.read_csv('C:/Users/muril/OneDrive/Área de Trabalho/Dados/Microdados do Censo da Educação Superior 2020/dados/MICRODADOS_CADASTRO_IES_2020.CSV'
                               , sep = ';'
                               , encoding = 'iso-8859-1'
                               , low_memory = False)
   planilhas_ies = planilhas_ies[['CO_IES','NO_IES']]
   
   planilhas_ies_cursos = pd.DataFrame(planilhas['CO_IES'].unique(), columns = ['IES'])
   for i, r in planilhas_ies_cursos.iterrows():
       
       ## Determina o nome da IES
       planilhas_ies_filtrados  = planilhas_ies[planilhas_ies['CO_IES'] == r['IES']]
       filtra_planilha = planilhas_ies_filtrados['NO_IES'].iloc[0].replace("'","")
       insert_statement = f"INSERT INTO dim_ies (tf_ies, ies) values ({i}, '{filtra_planilha}')"
       print(insert_statement)
       cursor.execute(insert_statement)
       conn.commit()
       
       
   ## criando a inserção de planilhas relacionada ao Fact matriuculas 2020 
   
   for i, r in planilhas.iterrows():
       
       modalidade = 'Presencial' if r['TP_MODALIDADE_ENSINO'] == 1 else 'EAD'
       planilhas_ies_filtrados = planilhas_ies[planilhas_ies['CO_IES'] == r['CO_IES']]
       no_ies = planilhas_ies_filtrados['NO_IES'].iloc[0].replace("'","")
       municipio = str(r['NO_MUNICIPIO']).replace("'","")
       
       subquery_matriculas = f"(SELECT {r['QT_INSCRITO_TOTAL']}) as matriculas"
       subquery_tf_ano = f"(SELECT tf_ano from dim_ano where ano = {r['NU_ANO_CENSO']})  as tf_ano"
       subquery_tf_modalidade = f"(SELECT  tf_modalidade from dim_modalidade where modalidade = '{modalidade}') as tf_modalidade"
       subquery_tf_municipio = f"(SELECT tf_municipio from dim_municipio where municipio = '{municipio}') as tf_municipio" \
           if r['NO_MUNICIPIO'] else f"(SELECT Null from dim_municipio where municipio = '{r['NO_MUNICIPIO']}') as tf_municipio"
       subquery_tf_uf = f"(SELECT tf_uf from dim_uf where uf = '{r['NO_UF']}') as tf_uf" \
           if r['NO_UF'] else f"(SELECT Null from dim_uf where uf = '{r['NO_UF']}') AS tf_uf"
       subquery_tf_ies = f"(SELECT Null from dim_ies where ies = '{no_ies}') as tf_ies"
       subquery_tf_curso = f"(SELECT tf_curso from dim_curso where curso = '{r['NO_CURSO']}') as tf_curso"
       
       insert_statement = f""" INSERT INTO  facts_matriculas (matricula, tf_curso, tf_ano, tf_modalidade, tf_municipio, tf_uf, tf_ies)
                               SELECT DISTINCT * FROM
                               {subquery_matriculas}, {subquery_tf_curso},{subquery_tf_ano},{subquery_tf_modalidade},
                               {subquery_tf_municipio},{subquery_tf_uf},{subquery_tf_ies}
                            """
        
       print(insert_statement)
       cursor.execute(insert_statement)
       conn.commit()

except Exception as e:
    print(e)
