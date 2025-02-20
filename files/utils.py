import pandas as pd
import numpy as np

def Subtracao(op1,op2):
    resultado = [0] * len(op1)
    pos = 0
    for i, j in zip(op1,op2):
        if not np.isnan(i) and not np.isnan(j):
            resultado[pos] = i - j
            pos += 1
        elif not np.isnan(i) and np.isnan(j):
            resultado[pos] = i
            pos += 1
        elif np.isnan(i) and not np.isnan(j):
            resultado[pos] = -j
            pos += 1
        elif np.isnan(i) and np.isnan(j):
            resultado[pos] = np.nan
            pos += 1
    return resultado


def SomaElem(vetor):
    resultado = [np.nan] * len(vetor)
    pos = 0
    for listas in vetor:
        valor_pos = np.nan
        valor = 0
        for elemento in listas:
            if (not np.isnan(elemento)):
                valor += elemento
                valor_pos = valor
        resultado[pos] = valor_pos
        pos += 1
    return resultado


def Merge(df):
    df.columns = df.columns.droplevel(1)
    merged_data = {}
    for usina in df.columns.levels[0]:  # Iterando pelas usinas
        actual_agg = [np.nan] * len(df) 
        actual_con = [np.nan] * len(df)
            
        if ("Actual Aggregated" in df[usina]):
            if (type(df[usina]["Actual Aggregated"].values[0]) == np.ndarray):
                actual_agg = SomaElem(df[usina]["Actual Aggregated"].values)
            else:
                actual_agg = df[usina]["Actual Aggregated"].values

        if ("Actual Consumption" in df[usina]):
            if (type(df[usina]["Actual Consumption"].values[0]) == np.ndarray):
                actual_con = SomaElem(df[usina]["Actual Consumption"].values)
            else:
                actual_con = df[usina]["Actual Consumption"].values

        merged_data[usina] = Subtracao(actual_agg, actual_con)

    merged_df = pd.DataFrame(merged_data, index=df.index)
    return merged_df