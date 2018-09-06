import sys
import json
from datetime import datetime
from elasticsearch import Elasticsearch

paradas =  {
      "properties": {
        "bairro": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "codigo": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "latitude": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "longitude": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },          
        "localizacao": {
          "type": "geo_point"           
        },
        "linhas": {
          "properties": {
            "codigoLinha": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "idLinha": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "nomeLinha": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            }
          }
        },
        "terminal": {
          "type": "boolean"          
        },
		"n_linhas" : {
		  "type": "long"
		}
      }
    }

carros =  {
      "properties": {
        "acessivel": {
          "type": "boolean"
        },
        "codigo": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "datahora": {
          "type": "date"
        },
        "empresa": {
          "type": "keyword"
        },
        "id": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "idcarro": {
          "type": "long"
        },
        "localizacao": {
          "type": "geo_point"           
        },  
        "nome": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        # "tempo_viagem": {
          # "type": "float"
        # },
        "total_passageiros": {
          "type": "long"
        },
        # "total_passageiros_hora": {
          # "type": "long"
        # },
        # "total_passageiros_hora_integral": {
          # "type": "long"
        # },
        # "total_passageiros_hora_isento": {
          # "type": "long"
        # },
        # "total_passageiros_hora_meia": {
          # "type": "long"
        # },
        "total_passageiros_integral": {
          "type": "long"
        },
        "total_passageiros_isento": {
          "type": "long"
        },
        "total_passageiros_meia": {
          "type": "long"
        },
        "ts": {
          "type": "date"
        }
      }
    }

def cria_indice(es):    
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {'paradas': paradas}
        }
    es.indices.create(index='datapoa_paradas',body=settings,ignore=400)
    
    print('indice datapoa_paradas criado com sucesso')
    
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {'carros': carros}
        }
    es.indices.create(index='datapoa_carros',body=settings,ignore=400)
    
    print('indice datapoa_carros criado com sucesso')

def importa_paradas(es):
    with open('paradas.json') as json_file:  
        data = json.load(json_file)
        
    for document in data:
        document['localizacao'] = [float(document['longitude']),float(document['latitude'])]
        document['terminal'] = document['terminal'] == 'S'
        document['n_linhas'] = len(document['linhas'])
		
        es.index(index="datapoa_paradas",
             doc_type="paradas",
             body=document,
             ignore=400)

    print('paradas importadas pelo elastic')

if __name__ == "__main__":
    elastic = Elasticsearch([{'host' : '127.0.0.1', 'port':9200}])
    cria_indice(elastic)
    importa_paradas(elastic)