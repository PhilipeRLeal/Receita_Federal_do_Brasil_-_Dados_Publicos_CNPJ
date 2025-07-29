# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 14:27:38 2025

@author: lealp
"""

import pandas as pd
import os
from time import time
from data_download import DataHolder
from databaseContext import DatabaseContext
from filtros_de_securitizacao import filtrar_dados_de_securitizacao

def processarEmpresas(dataHolder: DataHolder, 
                      databaseContext: DatabaseContext,
                      output_directory_of_extracted_files: str):
    
    empresa_insert_start = time()
    print("""
    #######################
    ## Arquivos de EMPRESA:
    #######################
    """)


    for e in dataHolder.arquivos_empresa:
        print('Trabalhando no arquivo: '+ e +' [...]')
        
        empresa_dtypes = {'cnpj_basico': str, 
                          'razao_social': str, 
                          'natureza_juridica': int, 
                          'qualificacao_responsavel': int, 
                          'capital_social': str, 
                          'porte_empresa': float, 
                          'ente_federativo_responsavel': str}
        
        extracted_file_path = os.path.join(output_directory_of_extracted_files, 
                                           e)

        empresa = pd.read_csv(filepath_or_buffer=extracted_file_path,
                              sep=';',
                              names=empresa_dtypes.keys(),
                              header=None,
                              dtype=empresa_dtypes,
                              encoding='latin-1',
                              index_col=False
        )

        
        # Filtrando empresas do tipo Securitizadora
        empresa = filtrar_dados_de_securitizacao(empresa, 
                                                 dataHolder.securitizadoras['cnpj_basico'])

        # Replace "," por "."
        empresa['capital_social'] = empresa['capital_social'].apply(lambda x: x.replace(',','.'))
        empresa['capital_social'] = empresa['capital_social'].astype(float)

        # Gravar dados no banco:
        # Empresa
        databaseContext.to_sql(empresa, name='empresa', if_exists='append', index=False)
        print('Arquivo ' + e + ' inserido com sucesso no banco de dados!')


    print('Arquivos de empresa finalizados!')
    empresa_insert_end = time()
    empresa_Tempo_insert = round((empresa_insert_end - empresa_insert_start))
    print('Tempo de execução do processo de empresa (em segundos): ' + str(empresa_Tempo_insert))
