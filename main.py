
import json
import os
import myGeminiClient


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

doc,pgs = getGCP_Doc(bucket_name,object_name)
cacheInfo = "Este documento é sobre regras e clausulas referentes a seguro automovel " + object_name
descDoc = "Este documento está dividido em capitulos e em clausulas que estão interligadas entre si, os blocos e os metadados devem refletir esta divisao para ajudar nas queries sobre vectores. Todo o texto tem que ser retornado"
descDoc = ""

gg = myGeminiClient(project_id,gemini_api_key,location,llmmodel)
# cn = gg.createChunksPDFDoc_LoadDoc(doc,cacheInfo)
# fr = gg.createChunksPDFDoc_GetTextChunks(cn,1,10,descDoc)
# jl = gg.createChunksPDFDoc_CreateJSON(fr)
# save_JsonFile(resp_json[1],"chunks.json")
cache_name,resp_text = gg.createChunksPDFDoc(doc,pgs,cacheInfo,descDoc)
j_list = gg.createChunksPDFDoc_CreateJSON_Final(resp_text)
dataset_id = 'rag_dataset'
table_id = 'testFomCode'
ee = createBQTableWithList_Batch(j_list,project_id,dataset_id,table_id)





ee = []
for i in resp_json:
    errors = client.insert_rows_json(full_table_id, i['items'])
    ee.append(errors)
    
    
    





