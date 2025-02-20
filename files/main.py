from datetime import datetime
from dotenv import load_dotenv
import os
import sys
from entsoe import EntsoePandasClient
import entsoe.exceptions
import pandas
from warnings import warn
from join import join_raw, get_header_count
import utils
import preenche_dados
from entsoe import mappings
import glob

class Paises:
    def __init__(self,nome,opcao,ano,passo):
        self.nome = nome
        self.opcao = opcao
        self.ano = ano
        self.passo = int(passo)

lista_paises = []

file = open("paises.txt","r")
for line in file:
    #formato do arquivo: CodigoPais;opcao1,opcao2;anoInicial-anoFinal;diasPorRequest
    line = line.rstrip('\n').split(";")
    lista_paises.append(Paises(line[0],line[1],line[2],line[3]))
file.close()


# Timezone
default_tz = 'Europe/Brussels'

HOUR_AGGREGATION_BY_MEAN = True # agrega em media
SAVE_PARTIAL = True
SAVE_ALL = True
FILL_ALL = True
MAX_RETRIES = 10


load_dotenv()
client = EntsoePandasClient(api_key=os.getenv('API_KEY'))

def ts_to_str(ts):
    return ts.strftime("%Y-%m-%dT%H-%M-%S")

def convert_tz(df: pandas.DataFrame | pandas.Series, tz: str = default_tz) -> pandas.DataFrame | pandas.Series:
    if isinstance(df.index, (pandas.DatetimeIndex, pandas.PeriodIndex)):
        return df.tz_convert(tz=tz)
    warn("pandas.Index doesn't support tz_convert")
    return df

def date_range(start, end, freq):
    diff = pandas.Timedelta(days=freq)
    current_date = start
    while current_date < end:
        yield current_date
        current_date = current_date + diff
    yield end

def get_file_name(start_date, end_date,COUNTRY, opt, border):
    dt1 = ts_to_str(start_date)
    dt2 = ts_to_str(end_date)
    if (border == ""):
        return f"{COUNTRY}_{opt}_{dt1}_{dt2}.csv" # nome dos arquivos salvos
    return f"{COUNTRY}_{opt}_{border}_{dt1}_{dt2}.csv"

def get_code(nome_pais, opt, ano_inicial, ano_final, border = None):
    if (border == ""):
        return(f"Country code is: {nome_pais}, option is: {opt}, from {ano_inicial} to {ano_final}.")
    return(f"Country code is: {nome_pais}, option is: {opt}, border: {border}, from {ano_inicial} to {ano_final}.")



for COUNTRY in lista_paises:
    for OPT in COUNTRY.opcao.split(","):
        borders = None
        if (OPT == "crossborder_flows"):
            borders = mappings.NEIGHBOURS[COUNTRY.nome]
        for border in (borders if borders is not None else [""]):
            start_year, end_year = COUNTRY.ano.split("-")
            start_year = int(start_year)
            end_year = int(end_year)
            
            print(get_code(COUNTRY.nome,OPT,start_year,end_year,border))
            
            for year in range(start_year, end_year + 1):
                start_date = pandas.Timestamp(f"{year}0101", tz=default_tz)
                end_date = pandas.Timestamp(f"{year + 1}0101", tz=default_tz)

                print(f"Total Period from {start_date} to {end_date}")
                date_chunks = list(date_range(start_date, end_date, COUNTRY.passo))
                total_chunks = len(date_chunks)
                every_day_or_days = f"{COUNTRY.passo} days" if COUNTRY.passo > 1 else "day"

                print(f"Number of requests for the period: {total_chunks-1} (every {every_day_or_days})")

                full_df = None
                current_start_date = date_chunks[0]
                for ichunk in range(1, len(date_chunks)):
                    current_end_date = date_chunks[ichunk]
                    dt1 = ts_to_str(current_start_date)
                    dt2 = ts_to_str(current_end_date)

                    tries = 0
                    good = False
                    if (border == ""):
                        while tries < MAX_RETRIES:
                            try:
                                print(f"Request from {COUNTRY.nome}, opt: {OPT}, from {dt1} to {dt2} ({ichunk}/{total_chunks-1}, {ichunk/(total_chunks-1)*100:.2f}%)... ", end="")
                                metodo = getattr(client, f"query_{OPT}")
                                df = convert_tz(metodo(COUNTRY.nome, start=current_start_date, end=current_end_date)) # query do ENTSOE
                                print("OK")
                                good = True
                                break
                            except:
                                print(f" Retrying ({tries+1}/{MAX_RETRIES})...", end="")
                                tries += 1
                    else:
                        #exportacao
                        while tries < MAX_RETRIES:
                            try:
                                print(f"Request from {COUNTRY.nome} from {COUNTRY.nome} to {border} from {dt1} to {dt2} ({ichunk}/{total_chunks-1}, {ichunk/(total_chunks-1)*100:.2f}%)... ", end="")
                                metodo = getattr(client, f"query_{OPT}")
                                df_export = convert_tz(metodo(COUNTRY.nome, border, start=current_start_date, end=current_end_date, per_hour = True)) # query do ENTSOE
                                df_export.name = f"{COUNTRY.nome}->{border}"
                                print("OK")
                                good = True
                                break
                            except:
                                print(f" Retrying ({tries+1}/{MAX_RETRIES})...", end="")
                                tries += 1

                        #importacao
                        tries = 0
                        good = False
                        while tries < MAX_RETRIES:
                            try:
                                print(f"Request from {COUNTRY.nome} from {border} to {COUNTRY.nome} from {dt1} to {dt2} ({ichunk}/{total_chunks-1}, {ichunk/(total_chunks-1)*100:.2f}%)... ", end="")
                                metodo = getattr(client, f"query_{OPT}")
                                df_import = convert_tz(metodo(border, COUNTRY.nome, start=current_start_date, end=current_end_date, per_hour = True)) # query do ENTSOE
                                df_import.name = f"{border}->{COUNTRY.nome}"
                                print("OK")
                                good = True
                                break
                            except:
                                print(f" Retrying ({tries+1}/{MAX_RETRIES})...", end="")
                                tries += 1

                    if not good:
                        print()
                        print("Failed retrieving data.")
                        continue

                    if (border != ""):
                        df = pandas.concat([df_export,df_import],axis = 1)


                    if HOUR_AGGREGATION_BY_MEAN:
                        df = df.resample("h").mean()

                    if full_df is None:
                        full_df = df
                    else:
                        full_df = pandas.concat([full_df, df])

                    if SAVE_PARTIAL:
                        partial_dir = (f"data/{COUNTRY.nome}/{OPT}/{year}" if border == "" else f"data/{COUNTRY.nome}/{OPT}/{COUNTRY.nome}_to_{border}/{year}")
                        os.makedirs(partial_dir, exist_ok=True)
                        full_df.to_csv(os.path.join(partial_dir, "partial_" + get_file_name(start_date, current_end_date,COUNTRY.nome, OPT, border)))

                    current_start_date = current_end_date

                year_dir = (f"data/{COUNTRY.nome}/{OPT}/{year}" if border == "" else f"data/{COUNTRY.nome}/{OPT}/{COUNTRY.nome}_to_{border}/{year}")
                os.makedirs(year_dir, exist_ok=True)
                filename = get_file_name(start_date, end_date,COUNTRY.nome, OPT, border)
                print(f"Finished year {year}, saving data to {filename}")
                if full_df is not None:
                    full_df.to_csv(os.path.join(year_dir, filename))
                print("Done.")

            if(SAVE_ALL):
                df_filled = join_raw(COUNTRY.nome,OPT,border)
                if(FILL_ALL):
                    df_filled = preenche_dados.preenche_dados(df_filled)
                    if (OPT == "generation_per_plant"):
                        df_filled = utils.Merge(df_filled)
                    df_filled = preenche_dados.preenche_nulos(df_filled)
                    
                    if (border != ""):
                        df_filled.to_csv(f"data/{COUNTRY.nome}/{OPT}/{COUNTRY.nome}_to_{border}/{COUNTRY.nome}_to_{border}_all_{OPT}_{start_year}-{end_year}_filled.csv")
                        if (border == borders[-1]):
                            files = glob.glob(f"./**/{COUNTRY.nome}_to_*_all_{OPT}_2*_filled.csv",recursive=True)
                            df_filled = None
                            for file in files:
                                df_aux = pandas.read_csv(file,header = list(range(get_header_count(file))), index_col = 0 )
                                if df_filled is None:
                                    df_filled = df_aux
                                else:
                                    df_filled = pandas.concat([df_filled,df_aux], axis = 1)
                            df_filled.to_csv(f"data/{COUNTRY.nome}/{OPT}/{COUNTRY.nome}_all_{OPT}_{start_year}-{end_year}_filled.csv")
                    else:
                        df_filled.to_csv(f"data/{COUNTRY.nome}/{OPT}/{COUNTRY.nome}_all_{OPT}_{start_year}-{end_year}_filled.csv")


