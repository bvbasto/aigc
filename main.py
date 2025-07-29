from google import genai
from google.genai import types
from google.genai.types import HttpOptions
from google.cloud import storage
from google.cloud import bigquery
import io
import httpx
import sys
import os
from datetime import date,datetime
import json
import re
import fitz  # PyMuPDF

#  Apps https://console.cloud.google.com/gen-app-builder/engines?inv=1&invt=Ab35yQ&project=bs-fdld-ai

def bbp(o):
    global logbbp
    if not logbbp:
        return
    s1 = ""
    if type(o)==type([]):
        for io in o:
            s1 = s1 + str(io) + "\n"
    else:
        s1=str(o)
    d1 = datetime.now()
    if len(s1) > 200:
        s1 = "\n" + s1
    s1 = str(d1) + " " + s1
    print(s1)
    return s1

def save_JsonFile(j,filename="j.json"):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(j, f, ensure_ascii=False, indent=4)


def getGCP_Doc(bucket_name,object_name):    
    d1 = datetime.now()
    bbp("get document from gcs")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob_content = blob.download_as_bytes()
    doc_io = io.BytesIO(blob_content)
    d2 = datetime.now()
    bbp(str((d2-d1).total_seconds()) + "s")
    doc = fitz.open(stream=doc_io, filetype="pdf")
    page_count = doc.page_count
    doc.close()
    return doc_io,page_count

def createBQTableWithList (listOfListOfItems,project_id,dataset_id,table_id):
    client = bigquery.Client()
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    
    schema = [
        bigquery.SchemaField("BlocoID", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("BlocoMetadata", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("BlocoContent", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("BlocoType", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("docPage_ini", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("docPage_end", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("ClausulaOuTopico", "STRING", mode="NULLABLE"),
    ]
    print(f"Creating table {full_table_id} if it doesn't exist...")
    table = bigquery.Table(full_table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"Table {table.project}.{table.dataset_id}.{table.table_id} is ready.")

    ee = []
    for i in listOfListOfItems:
        errors = client.insert_rows_json(full_table_id, i['items'])
        ee.append(errors)
    return ee

class myGeminiClient:
    __gemini_api_key = None
    __llmodel = llmmodel
    client = None
    __project_id = None
    __location = None
    onCache = {}
    logTokens = [] 
    totalTokens = 0
    lastListOfChunks = []
    lastFullString = ""
    __bbplog = True

    def __init__(self,project_id,gemini_api_key,location="europe-west1", llmmodel="models/gemini-2.5-pro",bbplog=True):
        self.__gemini_api_key = gemini_api_key
        self.__llmodel = llmmodel
        self.__project_id = project_id
        self.__location = location
        self.client = genai.Client(project=self.__project_id, location=self.__location)
        self.__bbplog = bbplog
        onCache = {}

    def gemini_client(self):
        return self._client

    def doLogTokens(self,oper,delta,tokens):
        o = {"dt":datetime.now(),"oper":oper,"delta":delta,"tokens":tokens}
        self.logTokens.append(o)
        self.totalTokens = self.totalTokens + tokens
        
    def bbp(self,o):
        if not self.__bbplog:
            return
        s1 = ""
        if type(o)==type([]):
            for io in o:
                s1 = s1 + str(io) + "\n"
        else:
            s1=str(o)
        d1 = datetime.now()
        if len(s1) > 200:
            s1 = "\n" + s1
        s1 = str(d1) + " " + s1
        print(s1)
        return s1

    def createChunksPDFDoc_LoadDoc(self,doc,cache_info):
        doc = self.client.files.upload(file=doc,config=dict(mime_type='application/pdf'))
        d1 = datetime.now()
        self.bbp("Caching Doc " + doc.name)
        cache = self.client.caches.create(model=self.__llmodel,config=types.CreateCachedContentConfig(system_instruction=cache_info,contents=[doc],))
        self.onCache[cache.name]={"doc":doc,"cache":cache}
        d2 = datetime.now()
        ts = (d2-d1).total_seconds()
        tks = cache.usage_metadata.total_token_count
        # self.totalTokens += tks
        self.doLogTokens("CreateCachedDoc",ts,tks)
        self.bbp("Cached Doc " + str(ts) + "s tokens: " + str(tks))
        return cache.name

    def createChunksPDFDoc_GetTextChunks(self,cache_name,pg_from,pg_to,chunking_request_to_add=""):
        printat = 100
        d1 = datetime.now()
        self.bbp("Starting to create chunks ")
        resps = []
    
        system_instruction = f"""
        É necessário guardar todo o documento pdf numa base de dados vectorizada. Para isso é preciso partir o documento em n blocos para serem criados embeddings
        Os blocos devem estar sempre alinhados com clausulas e uma clausula não deve ser partida por estar em paginas diferentes nem deve haver varias clausulas por bloco
        Quando o texto não disser respeito a clausulas ou for uma tabela deve estar separado em blocos lógicos de assuntos
        """
        system_instruction += f"Faz essa partição de todo o texto do documento exclusivamente entre a página {str(pg_from)} e a pagina {str(pg_to)} inclusive. Apenas dados ente estas duas páginas devem ser retornados\n"
        system_instruction += """
        Todo o texto deve estar presente nos blocos partidos dentro do limite de páginas indicado.
        Retorna um objeto json com o formato {"totalDeBlocos":<int>,"items":<list>}
        a lista de itens é feita por items com o seguinte formato:
        {"BlocoID":<int>,"BlocoMetadata":<string>,"BlocoContent":<string>,"BlocoType":<string>,"docPage_ini":<int>,"docPage_end":<int>,"ClausulaOuTopico":<string>}"}
        os blocos são apenas uma divisão por isso o texto deve ser retornado na totalidade em cada blocoContent. 
        a chunkMetadata é para ser usada como metadados em queries sobre vectores, o chunkType deve indicar se é texto, código, tabela ou imagem. 
        docPage_ini e docPage_end tem a pagina inicial e final do bloco do documento de onde o bloco foi retirado
        ClausulaOuTopico deve indicar o capitulo, a clausula e o nome da tabela quando houver
        """
        system_instruction += chunking_request_to_add

        d1a = datetime.now()
        ii = 1 
        for chunk in self.client.models.generate_content_stream(model=self.__llmodel,contents=system_instruction,config=types.GenerateContentConfig(cached_content=cache_name)):
            resps.append(chunk.text)
            if ii % printat == 0 or ii==1:                
                d2 = datetime.now()
                ts = (d2-d1a).total_seconds()
                tks = chunk.usage_metadata.total_token_count
                msg = "TextChunk number " + str(ii) + "; tokens: " + str(tks) + " time " + str(ts) + "s"
                self.bbp(msg)
            #if ii % printat <= 4 :
            if ii < 4 :
                self.bbp("\t" + chunk.text)
            ii = ii + 1
        
        d2 = datetime.now()
        ts = (d2-d1a).total_seconds()
        
        tks = chunk.usage_metadata.total_token_count
        self.doLogTokens(msg,ts,tks)
        fullresp = ""
        for r in resps:
            if type(r) != type("dummy"):
                self.bbp(("x" * 50) + " Erro, retornado objecto não string :" + str(type(r))) 
            else:
                fullresp = fullresp + r
        ts = (d2-d1).total_seconds()
        self.bbp("create chunks #" + str(len(resps)) + " tokens: " + str(tks) + " time " + str(ts) + "s" + " strLength: " + str(len(fullresp)))
        return fullresp

    def createChunksPDFDoc_CreateJSON(self,fullresp):
        d1 = datetime.now()
        self.bbp("Starting crete final json ")
        if "```json" in fullresp:
            cleanresp = fullresp.replace("```json", "").replace("```", "").strip()
        else:
            cleanresp = fullresp
        finalj = json.loads(cleanresp)
        self.lastListOfChunks = finalj["items"]
        self.lastFullString = cleanresp
        d2 = datetime.now()
        ts = (d2-d1).total_seconds()
        self.bbp("create chunks #" + str(len(finalj["items"]))  + " time " + str(ts) + "s")
        return finalj

    def createChunksPDFDoc(self,doc,total_pages,cache_info,chunking_request_to_add):
        pg_control = 0
        pg_batch = 10
        d1 = datetime.now()
        self.bbp("Process start")
        cache_name = self.createChunksPDFDoc_LoadDoc(doc,cache_info)
        j_list = []
        while pg_control < total_pages:
            pg_from = 0 if pg_control == 0 else pg_control - 1
            pg_to= total_pages if pg_control + pg_batch + 1 > total_pages else pg_control + pg_batch + 1
            self.bbp("Process pages from " + str(pg_from) + " to " + str(pg_to) + " of " + str(total_pages))
            fullresp = self.createChunksPDFDoc_GetTextChunks(cache_name,pg_from,pg_to,chunking_request_to_add)
            list_json = self.createChunksPDFDoc_CreateJSON(fullresp)
            j_list.append(list_json)
            pg_control += pg_batch
        d2 = datetime.now()
        ts = (d2-d1).total_seconds()
        self.bbp("Process end " + str(ts) + "s")
        return cache_name,j_list
    


# https://cloud.google.com/vertex-ai/generative-ai/docs/learn/model-versions
#llmmodel="models/gemini-2.0-flash-001"
llmmodel="models/gemini-2.5-pro"

file_path = os.getcwd() + "/key.json"

with open(file_path, 'r') as f:
    jkey = json.load(f)

gemini_api_key = jkey["gemini_api_key"]
os.environ["GOOGLE_API_KEY"] = gemini_api_key

project_id= jkey["project_id"]
location= jkey["region"]
llmmodel= jkey["model"]


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
cache_name,resp_json = gg.createChunksPDFDoc(doc,pgs,cacheInfo,descDoc)
dataset_id = 'rag_dataset'
table_id = 'testFomCode'
ee = createBQTableWithList(resp_json,project_id,dataset_id,table_id)


resp_json[1]




ee = []
for i in resp_json:
    errors = client.insert_rows_json(full_table_id, i['items'])
    ee.append(errors)