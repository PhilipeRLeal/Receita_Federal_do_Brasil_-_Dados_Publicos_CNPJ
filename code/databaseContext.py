# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 12:19:27 2025

@author: lealp
"""
from sqlalchemy import create_engine
import psycopg2
import sys
import pandas as pd

class DatabaseContext:
    def __init__(self, user:str, passw: str, host:str, port: str, database:str):
            
        self.engine = create_engine('postgresql://'+user+':'+passw+'@'+host+':'+port+'/'+database)
        self.conn = psycopg2.connect('dbname='+database+' '+'user='+user+' '+'host='+host+' '+'port='+port+' '+'password='+passw)
        self.cur = self.conn.cursor()
        
        
    def sanitizarBD(self):
        self.cur.execute('DROP TABLE IF EXISTS "estabelecimento";')
        
        self.cur.execute('DROP TABLE IF EXISTS "empresa";')
        
        # Drop table antes do insert
        self.cur.execute('DROP TABLE IF EXISTS "socios";')
        
        self.cur.execute('DROP TABLE IF EXISTS "simples";')
        
        self.cur.execute('DROP TABLE IF EXISTS "cnae";')
        
        self.cur.execute('DROP TABLE IF EXISTS "moti";')
        
        self.cur.execute('DROP TABLE IF EXISTS "munic";')
        
        # Drop table antes do insert
        self.cur.execute('DROP TABLE IF EXISTS "natju";')
        
        self.cur.execute('DROP TABLE IF EXISTS "pais";')
        
        self.cur.execute('DROP TABLE IF EXISTS "quals";')
        
        self.conn.commit()
        
    def __exit__(self):
        self.close()
        
    def close(self):
        self.conn.close()
        self.engine.dispose()

        
    def to_sql(self, 
               dataframe: pd.DataFrame, 
               chunksize = (2**15), 
               **kwargs):
        """
        Quebra em pedacos a tarefa de inserir registros no banco

        Parameters
        ----------
        dataframe : pd.DataFrame
            Arquivo que será escrito no BD.
            
        chunksize : TYPE, optional
            DESCRIPTION: define o tamanho que o arquivo será quebrado
            para respectiva escrita no BD
            Default: 2^15 (dois elevado à 15ª)
            
        **kwargs : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        
        if (not dataframe.empty):
            
            total = len(dataframe)//chunksize
            total += 1
            name = kwargs.get('name')
        
            def chunker(df):
                return (df[i:i + chunksize] for i in range(0, len(df), chunksize))
        
            for i, df in enumerate(chunker(dataframe)):
                df.to_sql(con=self.engine, **kwargs)
                index = (i+1)
                progress = f'Escrevendo dados de: {name}. Processo: {index} de {total}.'
                sys.stdout.write(f'\r{progress}')
            sys.stdout.write('\n')
        else:
            print("Arquivo vazio. Nenhum dado para ser gravado no BD")
            
    def __del__(self):
        self.close()
    