import requests
import sys
import json
from datetime import datetime
import time
from random import *
from elasticsearch import Elasticsearch
import numpy as np

MIN_REFRESH_INTERVAL = 3
MAX_REFRESH_INTERVAL = 10

FATOR_FREQ_MIN = [0.05,0.01,0.005,0,0,0.3,0.50,0.8,0.9,0.9,0.80,0.70,0.8,0.80,0.7,0.7,0.60,0.85,0.9,0.9,0.9,0.7,0.60,0.4]
FATOR_FREQ_MAX = [0.1 ,0.05,0.01 ,0,0,0.4,0.70,0.9,1.0,1.0,0.99,0.85,0.9,0.95,0.8,0.8,0.75,0.95,1.0,1.0,1.0,0.8,0.75,0.6]

EMPRESAS = ['CARRIS','MOB','VIA LESTE','MAIS','VIVA SUL']

def cria_carro(linhas,idcarro,hora):
    #seleciona uma linha de forma aleatoria
    linha_selecionada = int(round( uniform(0,len(linhas)-1)))
    
    print('criando novo carro na linha {} id {}'.format(linhas[linha_selecionada]['nome'],idcarro))
    # carro é a informação da linha + enriquecimento
    carro = linhas[linha_selecionada].copy()
    
    carro['ts'] = time.time()
    carro['datahora'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))    
    carro['idcarro'] = idcarro
    carro['empresa'] = EMPRESAS[int(round(uniform(0,len(EMPRESAS)-1)))]
    carro['acessivel'] = uniform(0,1) < 0.3
    #minutos
    carro['tempo_viagem'] = round(uniform(0,120))    
    
    carro['total_passageiros_meia'] = int(round(FATOR_FREQ_MAX[hora-1] * 0.3 * 50 * hora))
    carro['total_passageiros_isento'] = int(round(FATOR_FREQ_MAX[hora-1] * 0.1 * 50 * hora))
    carro['total_passageiros_integral'] = int(round(FATOR_FREQ_MAX[hora-1] * 0.6 * 50 * hora))
    carro['total_passageiros'] = carro['total_passageiros_meia'] + \
                                 carro['total_passageiros_isento'] + \
                                 carro['total_passageiros_integral']      
    
    carro['total_passageiros_hora_meia'] = int(round(FATOR_FREQ_MAX[hora-1] * 0.3 * 50))
    carro['total_passageiros_hora_isento'] = int(round(FATOR_FREQ_MAX[hora-1] * 0.1 * 50))
    carro['total_passageiros_hora_integral'] = int(round(FATOR_FREQ_MAX[hora-1] * 0.6 * 50))
    carro['total_passageiros_hora'] = carro['total_passageiros_hora_meia'] + \
                                      carro['total_passageiros_hora_isento'] + \
                                      carro['total_passageiros_hora_integral']
            
    carro['lat'] = round(uniform(MNLAT,MXLAT),6) 
    carro['lng'] = round(uniform(MNLON,MXLON),6)   
    
    return carro

def executa_gerador(elastic):
    while(True):
        
        SLEEP_INTERVAL = int(round(uniform(MIN_REFRESH_INTERVAL,MAX_REFRESH_INTERVAL)))

        hora = int(time.strftime("%H", time.localtime(time.time())))
        
        #for carro in carros:
        carro = carros[int(round(uniform(0,len(carros)-1)))]    
            
        carro['ts'] = time.time()
        carro['datahora'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))    
        #minutos
        carro['tempo_viagem'] += SLEEP_INTERVAL  
        novos_passageiros = int(round(uniform(1,SLEEP_INTERVAL)))

        inc_meia = int(round(FATOR_FREQ_MAX[hora-1] * 0.3 * novos_passageiros))
        inc_isento = int(round(FATOR_FREQ_MAX[hora-1] * 0.1 * novos_passageiros))
        inc_integral = int(round(FATOR_FREQ_MAX[hora-1] * 0.6 * novos_passageiros))

        carro['total_passageiros_hora_meia'] += inc_meia
        carro['total_passageiros_hora_isento'] += inc_isento
        carro['total_passageiros_hora_integral'] += inc_integral
        carro['total_passageiros_hora'] = carro['total_passageiros_hora_meia'] + \
                                          carro['total_passageiros_hora_isento'] + \
                                          carro['total_passageiros_hora_integral']

        carro['total_passageiros_meia'] += inc_meia
        carro['total_passageiros_isento'] += inc_isento
        carro['total_passageiros_integral'] += inc_integral
        carro['total_passageiros'] = carro['total_passageiros_meia'] + \
                                     carro['total_passageiros_isento'] + \
                                     carro['total_passageiros_integral']      


        carro['lat'] = round(uniform(MNLAT,MXLAT),6) 
        carro['lng'] = round(uniform(MNLON,MXLON),6)  

        time.sleep(SLEEP_INTERVAL)
        print(carro)
        
        elastic.index(index="paradas_index",
             doc_type="json",
             body=carro,
             ignore=400)

        #cria/destroi carros aleatoriamente
        if(uniform(0,1) < 0.1):
            if(uniform(0,1) <= 0.5):
                print('removendo carro...')
                carros.remove(carros[int(round(uniform(0,len(carros)-1)))])
            else:
                idcarro = (np.max(idcarros) + 1).item()
                novo_carro = cria_carro(linhas,idcarro,hora)
                carros.append(novo_carro)
             
#Busca informações sobre linhas via API
resp = requests.get('http://www.poatransporte.com.br/php/facades/process.php?a=nc&p=%&t=o')

if resp.status_code != 200:
    # This means something went wrong.
    raise ApiError('GET /tasks/ {}'.format(resp.status_code))

linhas = resp.json()

tbl_horaria_frequencia = []
for i in range(0,24):
    tbl_horaria_frequencia.append({'min': int(round(len(linhas) * FATOR_FREQ_MIN[i])), \
                                   'max': int(round(len(linhas) * FATOR_FREQ_MAX[i])) })
			 
## inicializa os carros para as linhas de forma randomica, com base na tabela de frequencia
now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
hora = int(time.strftime("%H", time.localtime(time.time())))
print("data {}".format(now))

idcarros = np.random.permutation(tbl_horaria_frequencia[hora-1]['max'])

min_carros_hora = tbl_horaria_frequencia[hora-1]['min']
max_carros_hora = tbl_horaria_frequencia[hora-1]['max']
total_carros_hora = int(round(uniform(min_carros_hora,max_carros_hora)))
total_carros_hora = 5 #temporario para teste

#canto inferior esquerdo
MXLAT = -30.14296222668432 
MXLON = -51.87917968750003
#canto superior direito
MNLAT = -29.79200328961529
MNLON = -50.56082031250003

print("Inicializando informações sobre carros...")
carros = []
for i in range(0,total_carros_hora):
    idcarro = idcarros[i].item() #para converter para um tipo nativo do python     
    carro = cria_carro(linhas,idcarro,hora)
    carros.append(carro)

	
if __name__ == "__main__":
	es = Elasticsearch([{'host' : '127.0.0.1', 'port':9200}])
	executa_gerador(es)
