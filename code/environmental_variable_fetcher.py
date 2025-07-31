# -*- coding: utf-8 -*-
"""
Created on Wed Jul 30 10:15:57 2025

@author: lealp
"""
import os
import pathlib
from dotenv import load_dotenv

def loadEnvironmentalVariables():
    current_path = os.path.join(pathlib.Path().resolve(),"code").replace("\\", "/")
    dotenv_path = os.path.join(current_path, '.env')
    load_dotenv(dotenv_path=dotenv_path)

