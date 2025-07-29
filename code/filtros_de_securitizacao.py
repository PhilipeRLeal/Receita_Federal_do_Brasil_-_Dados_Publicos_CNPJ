# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 12:38:33 2025

@author: lealp
"""
import pandas as pd
from dask import dataframe as dd

def filtrar_dados_de_securitizacao(df: pd.DataFrame, 
                                   securitizadoras: pd.Series) -> pd.DataFrame:
    """
    Filtrando empresas do tipo Securitizadora

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe que será filtrado. Este dataframe deverá ser aquele que contém dados das empresas.
    
    securitizadoras : pd.Series
        Dataframe de referência listando os CNPJs das empresas do tipo Securitizadora.
            E.g., securitizadoras["cnpj_basico"]

    Returns
    -------
    df : TYPE
        O dataframe de entrada que foi filtrado.

    """
    
    if (isinstance(df, pd.DataFrame)):
        mask = [securitizadoras.str.contains(cnpj) for cnpj in df['cnpj_basico']]
        
        df = df.loc[mask]
        
    elif (isinstance(df, dd.DataFrame)):
        merged = df.merge(dd.DataFrame(securitizadoras), how="inner", on='cnpj_basico')
        
        df = merged
        
    else:
        pass
    
    return df