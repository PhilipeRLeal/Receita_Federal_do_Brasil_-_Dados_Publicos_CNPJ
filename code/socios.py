# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 13:37:56 2025

@author: lealp
"""

import pandas as pd
import os
from time import time
from data_download import DataHolder
from databaseContext import DatabaseContext
from filtros_de_securitizacao import filtrar_dados_de_securitizacao

def processarSocios(dataHolder: DataHolder, 
                    databaseContext: DatabaseContext,
                    output_directory_of_extracted_files: str):
    
    
    socios_insert_start = time()
    print("""
    ######################
    ## Arquivos de SOCIOS:
    ######################
    """)


    for e in dataHolder.arquivos_socios:
        print('Trabalhando no arquivo: '+e+' [...]')

        dtypes = {'cnpj_basico': str, 
                  'identificador_socio': int, 
                  'nome_socio_razao_social': str, 
                  'cpf_cnpj_socio': str, 
                  'qualificacao_socio': int, 
                  'data_entrada_sociedade': int, 
                  'pais': int,
                  'representante_legal': str, 
                  'nome_do_representante': str, 
                  'qualificacao_representante_legal': int, 
                  'faixa_etaria': int}
        
        extracted_file_path = os.path.join(output_directory_of_extracted_files, e)
        socios = pd.read_csv(filepath_or_buffer=extracted_file_path,
                              sep=';',
                              names=dtypes.keys(),
                              header=None,
                              dtype=dtypes,
                              encoding='latin-1',
                              index_col=False
        )


        # Filtrando empresas do tipo Securitizadora
        socios = filtrar_dados_de_securitizacao(socios, 
                                                dataHolder.securitizadoras['cnpj_basico'])
        
        # Gravar dados no banco:
        # socios
        databaseContext.to_sql(socios, name='socios', if_exists='append', index=False)
        print('Arquivo ' + e + ' inserido com sucesso no banco de dados!')

    del socios
    print('Arquivos de socios finalizados!')
    socios_insert_end = time()
    socios_Tempo_insert = round((socios_insert_end - socios_insert_start))
    print('Tempo de execução do processo de sócios (em segundos): ' + str(socios_Tempo_insert))