
import json
import os
import myGeminiClient as gc


# https://cloud.google.com/vertex-ai/generative-ai/docs/learn/model-versions
#llmmodel="models/gemini-2.0-flash-001"
# llmmodel="models/gemini-2.5-pro"

# file_path = os.getcwd() + "/key.json"
# with open(file_path, 'r') as f:
#    jkey = json.load(f)


secret_id = "poc_ai_001" # The name you gave the secret
sk = json.loads( access_secret_version(project_id,secret_id).replace("\n", "").strip() )

gemini_api_key = sk["gemini_api_key"]
os.environ["GOOGLE_API_KEY"] = gemini_api_key
project_id= sk["project_id"]
location= sk["region"]
llmmodel= sk["model"]

bucket_name="fdld-poc2"
object_name="ai1/rawdocs/Fidelidade_Auto_Liber3G_CG058_AU052_mar2024.pdf"
logbbp = True

doc,pgs = gc.getGCP_Doc(bucket_name,object_name)
cacheInfo = "Este documento é sobre regras e clausulas referentes a seguro automovel " + object_name
descDoc = f"""
É necessário guardar todo o documento pdf numa base de dados vectorizada. Para isso é preciso partir o documento em n blocos para serem criados embeddings
Os blocos devem estar sempre alinhados com clausulas e uma clausula não deve ser partida por estar em paginas diferentes nem deve haver varias clausulas por bloco
Quando o texto não disser respeito a clausulas ou for uma tabela deve estar separado em blocos lógicos de assuntos
"""
descDoc += """
Todo o texto deve estar presente nos blocos partidos dentro do limite de páginas indicado.
Retorna um objeto json com o formato {"totalDeBlocos":<int>,"items":<list>}
a lista de itens é feita por items com o seguinte formato:
{"BlocoID":<int>,"BlocoMetadata":<string>,"BlocoContent":<string>,"BlocoType":<string>,"docPage_ini":<int>,"docPage_end":<int>,"ClausulaOuTopico":<string>}"}
os blocos são apenas uma divisão por isso o texto deve ser retornado na totalidade em cada blocoContent. 
a chunkMetadata é para ser usada como metadados em queries sobre vectores, o chunkType deve indicar se é texto, código, tabela ou imagem. 
docPage_ini e docPage_end tem a pagina inicial e final do bloco do documento de onde o bloco foi retirado
ClausulaOuTopico deve indicar o capitulo, a clausula e o nome da tabela quando houver
"""



gg = gc.myGeminiClient(project_id,gemini_api_key,location,llmmodel)
# cn = gg.createChunksPDFDoc_LoadDoc(doc,cacheInfo)
# fr = gg.createChunksPDFDoc_GetTextChunks(cn,1,10,descDoc)
# jl = gg.createChunksPDFDoc_CreateJSON(fr)
# save_JsonFile(resp_json[1],"chunks.json")
cache_name,resp_text = gg.createChunksPDFDoc(doc,pgs,cacheInfo,descDoc)
j_list = gg.createChunksPDFDoc_CreateJSON_Final(resp_text)
dataset_id = 'rag_dataset'
table_id = 'testFomCode'
ee = gc.createBQTableWithList_Batch(j_list,project_id,dataset_id,table_id)





ee = []
for i in resp_json:
    errors = client.insert_rows_json(full_table_id, i['items'])
    ee.append(errors)
    
    
    



question="Usa as tabelas para calcular, tendo seguro faz 3 anos, qual o agravamento se tiver tido 2 sinistros nos ultimos 5 anos mas nenhum nos ultimos 2 anos?"



