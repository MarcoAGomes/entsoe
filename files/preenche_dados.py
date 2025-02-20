import pandas as pd

def preenche_dados(df):
    df = df.reindex(pd.date_range(start=df.index.min(), end=df.index.max(), freq='h'))
    return df

def preenche_nulos(df):
    """
    Preenche valores nulos no DataFrame usando o valor mais próximo da mesma hora,
    baseado em intervalos de semanas (7, 14, 21 dias, etc.).
    """
    # Criar uma cópia do DataFrame para evitar alterar o original
    df_preenchido = df.copy()

    # Iterar pelas colunas (usinas)
    for coluna in df.columns:
        for i in range(len(df)):
            if pd.isna(df_preenchido[coluna].iloc[i]):
                # Data e hora atuais
                data_hora_atual = df.index[i]
                
                # Procurar por intervalos crescentes de semanas
                valor_anterior = None
                valor_posterior = None
                valor_preenchido = None
                semanas = 1
                while valor_preenchido is None and semanas <= 4:  # Limite de busca
                    # Tentar n semanas antes
                    data_hora_anterior = data_hora_atual - pd.Timedelta(weeks=semanas)
                    if data_hora_anterior in df.index:
                        aux = df_preenchido.at[data_hora_anterior, coluna]
                        if not pd.isna(aux):
                            valor_anterior = aux
                            

                    # Tentar n semanas depois
                    data_hora_posterior = data_hora_atual + pd.Timedelta(weeks=semanas)
                    if data_hora_posterior in df.index:
                        aux = df_preenchido.at[data_hora_posterior, coluna]
                        if not pd.isna(aux):
                            valor_posterior = aux

                    if ((pd.notna(valor_anterior) and valor_anterior != 0) and (pd.notna(valor_posterior) and valor_posterior != 0)):
                        valor_preenchido = (valor_anterior + valor_posterior)/2
                        break
                    else:
                        # Aumentar intervalo de semanas
                        semanas += 1

                # Preencher o valor se encontrado
                if (not pd.isna(valor_preenchido)):                    
                    df_preenchido.at[data_hora_atual, coluna] = valor_preenchido
                else:
                    df_preenchido.at[data_hora_atual, coluna] = 0
    
    df_preenchido.index = df_preenchido.index.tz_convert('Europe/Brussels')
    return df_preenchido