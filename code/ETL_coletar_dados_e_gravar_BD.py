import os
import pandas as pd
from databaseContext import DatabaseContext
from time import time
from data_download import (download, 
                           unzipFiles, 
                           output_directory_of_extracted_files, 
                           segregarDadosPorTabelaDoBD)

from estabelecimentos import processarEstabelecimentos
from simples import processarSimples
from socios import processarSocios
from empresas import processarEmpresas
from cnae import processarCNAEs
from moti import processarMOTI
from municipio import processarMunicipios
from natju import processarNATJU
from pais import processarPaises
from qualificacaoDeSocio import processarQualificacoesDeSocios

#%% main CONFIG

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)

#%%
# Extracting files:
    
insert_start = time()
    
Files: list[str] = download()

unzipFiles(Files)

dataHolder = segregarDadosPorTabelaDoBD(output_directory_of_extracted_files)

#%%
# Conectar no banco de dados:
# Dados da conexão com o BD
user=os.getenv('DB_USER')
passw=os.getenv('DB_PASSWORD')
host=os.getenv('DB_HOST')
port=os.getenv('DB_PORT')
database=os.getenv('DB_NAME')


databaseContext = DatabaseContext(user, passw, host, port, database)
databaseContext.sanitizarBD()

#%% 
# processando securitizadoras

print('Tem %i arquivos de estabelecimento!' % len(dataHolder.arquivos_estabelecimento))


securitizadoras = processarEstabelecimentos(dataHolder.arquivos_estabelecimento, 
                                            output_directory_of_extracted_files,
                                            databaseContext)

dataHolder.securitizadoras = securitizadoras

#%%
# # Arquivos de empresa:

processarEmpresas(dataHolder, 
                  databaseContext,
                  output_directory_of_extracted_files)

#%%
# Arquivos de socios:
    
processarSocios(dataHolder,
                databaseContext,
                output_directory_of_extracted_files)
    

#%%
# Arquivos de simples:

processarSimples(dataHolder, 
                 databaseContext,
                 output_directory_of_extracted_files)

#%%
# Arquivos de cnae:
processarCNAEs(dataHolder, 
               databaseContext,
               output_directory_of_extracted_files)

#%%
# Arquivos de moti:
processarMOTI(dataHolder, 
               databaseContext,
               output_directory_of_extracted_files)
    
#%%
# Arquivos de munic:

processarMunicipios(dataHolder, 
                    databaseContext,
                    output_directory_of_extracted_files)    

#%%
# Arquivos de natju:
    
processarNATJU(dataHolder, 
               databaseContext,
               output_directory_of_extracted_files)    


#%%
# Arquivos de pais:
    
processarPaises(dataHolder, 
                databaseContext,
                output_directory_of_extracted_files)   
    
#%%
# Arquivos de qualificação de sócios:

    
processarQualificacoesDeSocios(dataHolder, 
                               databaseContext,
                               output_directory_of_extracted_files)  
    
#%%
insert_end = time()
Tempo_insert = round((insert_end - insert_start))
del insert_start, insert_end

print("""
#############################################
## Processo de carga dos arquivos finalizado!
#############################################
""")

print('Tempo total de execução do processo de carga (em segundos): ' + str(Tempo_insert)) # Tempo de execução do processo (em segundos): 17.770 (4hrs e 57 min)

# ###############################
# Tamanho dos arquivos:
# empresa = 45.811.638
# estabelecimento = 48.421.619
# socios = 20.426.417
# simples = 27.893.923
# ###############################

#%% Fechando conexão com o BD

    
def criarIndicesNasTabelasdoBd():
    index_start = time()
    print("""
    #######################################
    ## Criar índices na base de dados [...]
    #######################################
    """)
    databaseContext.cur.execute("""
    create index if not exists empresa_cnpj on empresa(cnpj_basico);
    commit;
    create index if not exists estabelecimento_cnpj on estabelecimento(cnpj_basico);
    commit;
    create index if not exists socios_cnpj on socios(cnpj_basico);
    commit;
    create index if not exists simples_cnpj on simples(cnpj_basico);
    commit;
    """)
    databaseContext.conn.commit()
    print("""
    ############################################################
    ## Índices criados nas tabelas, para a coluna `cnpj_basico`:
       - empresa
       - estabelecimento
       - socios
       - simples
    ############################################################
    """)
    
    index_end = time()
    index_time = round(index_end - index_start)
    print('Tempo para criar os índices (em segundos): ' + str(index_time))


criarIndicesNasTabelasdoBd()
databaseContext.conn.close()
databaseContext.engine.close()

