import sys
import json
from datetime import datetime
from elasticsearch import Elasticsearch

def cria_indice(es):	
	es.indices.create(index='paradas_index',ignore=400)
	print('indice paradas_index criado com sucesso')

def importa_paradas(es):
	with open('paradas.json') as json_file:  
		data = json.load(json_file)
		
	for document in data:
		es.index(index="paradas_index",
             doc_type="json",
             body=document,
             ignore=400)
			 
	print('paradas importadas pelo elastic')
			 
if __name__ == "__main__":
	elastic = Elasticsearch([{'host' : '127.0.0.1', 'port':9200}])
	cria_indice(elastic)
	importa_paradas(elastic)