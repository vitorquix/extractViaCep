import requests
import pandas as pd
import logging
import time
import csv
import random
from datetime import datetime

# Log
LOG_FILENAME = "{}-error_viacep.log".format(datetime.today().strftime("%Y-%m-%d"))
logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
logger = logging.getLogger()

# Time randômico entre 1 e 3 segundos e contador de chamadas
def temporizador(counter=[0]):
    counter[0] += 1
    seg = random.randint(1, 3)
    print('Requisição: {} - Aguardar: {} segundos'.format(counter[0], seg))
    time.sleep(seg)
    return

# Consultar a API viaCep
def consultarAPI(cep):
    temporizador()
    url = 'https://viacep.com.br/ws/{}/json/'.format(cep)
    try:
        endereco = requests.request('GET', url)
        if endereco.status_code == requests.codes.ok:
            return endereco.json()
        else:
            return 'Erro no retorno da API - Status Code:', endereco.status_code
        endereco.close()
    except requests.exceptions.RequestException as e:
        logger.info(e)
        raise SystemExit(e)

# Read origem
baseCEPs = pd.read_csv("cep.csv")

# Tratar CPFs com 7 digitos
baseCEPs['CEP'] = baseCEPs.CEP.apply(lambda cep: '0' + str(cep) if len(str(cep)) == 7 else cep)

# Cria destino csv
baseViaCep = pd.DataFrame(columns=['CEP', 'Viacep'])
baseViaCep.to_csv('baseViaCep.csv', sep=';', encoding="utf-8", index=False)

# Request na API
with open('baseViaCep.csv', 'a', newline='') as csv_file:
    writer = csv.writer(csv_file, delimiter=';')
    for index, row in baseCEPs.iterrows():
        writer.writerow([row['CEP'], consultarAPI(baseCEPs.at[index, 'CEP'])])
csv_file.close()
