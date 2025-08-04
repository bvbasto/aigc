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
from google.cloud import secretmanager

from google.cloud import aiplatform
from langchain_core.documents import Document
from langchain_google_vertexai import VertexAIEmbeddings, ChatVertexAI
from langchain_community.vectorstores import BigQueryVectorSearch
from langchain.chains import RetrievalQA


#  Apps https://console.cloud.google.com/gen-app-builder/engines?inv=1&invt=Ab35yQ&project=bs-fdld-ai

def bbp(o,logbbp=True):
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

def createBQTableWithList (listOfListOfItems,project_id,dataset_id,table_id,schema):
    client = bigquery.Client()
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    

    print(f"Creating table {full_table_id} if it doesn't exist...")
    table = bigquery.Table(full_table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"Table {table.project}.{table.dataset_id}.{table.table_id} is ready.")

    ee = []
    for i in listOfListOfItems:
        errors = client.insert_rows_json(full_table_id, i['items'])
        ee.append(errors)
    return ee


def createBQTableWithList_Batch(listOfListOfItems,project_id,dataset_id,table_id,schema):
    client = bigquery.Client()
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Create the table if it doesn't exist
    print(f"Ensuring table {full_table_id} exists...")
    table = bigquery.Table(full_table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)

    ee = []
    # Configure the batch load job
    for i in listOfListOfItems:
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        print(f"Starting batch load job to insert {len(listOfListOfItems)} rows...")
        load_job = client.load_table_from_json(
            i['items'], full_table_id, job_config=job_config
        )
        errors =  load_job.result()
        ee.append(errors)
    return ee


def access_secret_version(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    payload = response.payload.data.decode("UTF-8")
    return payload


class myGeminiClient:
    __gemini_api_key = None
    __llmodel = None
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
        return cache

    def createChunksPDFDoc_GetTextChunks(self,cache,pg_from,pg_to,chunking_request_to_add=""):
        #print(cache)
        cache_name= cache.name
        printat = 100
        d1 = datetime.now()
        self.bbp("Starting to create chunks ")
        resps = []
    
        system_instruction = f"Faz essa partição de todo o texto do documento exclusivamente entre a página {str(pg_from)} e a pagina {str(pg_to)} inclusive. Apenas dados ente estas duas páginas devem ser retornados\n"
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
            if ii < 3 :
                self.bbp("\t\t\t" + chunk.text)
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
    
    
    def createChunksPDFDoc_CreateJSON_Final(self,t_list):
        j_list = []
        for t in t_list:
            list_json = self.createChunksPDFDoc_CreateJSON(t)
            j_list.append(list_json)
        return j_list
    
    #
    def createChunksPDFDoc(self,doc,total_pages,cache_info,chunking_request_to_add):
        pg_control = 0
        pg_batch = 10
        d1 = datetime.now()
        self.bbp("Process start")
        cache = self.createChunksPDFDoc_LoadDoc(doc,cache_info)
        t_list = []
        while pg_control < total_pages:
            pg_from = 0 if pg_control == 0 else pg_control - 1
            pg_to= total_pages if pg_control + pg_batch + 1 > total_pages else pg_control + pg_batch + 1
            self.bbp("Process pages from " + str(pg_from) + " to " + str(pg_to) + " of " + str(total_pages))
            fullresp = self.createChunksPDFDoc_GetTextChunks(cache,pg_from,pg_to,chunking_request_to_add)
            #list_json = self.createChunksPDFDoc_CreateJSON(fullresp)
            t_list.append(fullresp)
            pg_control += pg_batch
        d2 = datetime.now()
        ts = (d2-d1).total_seconds()
        self.bbp("Process end " + str(ts) + "s")
        return cache,t_list
    
    __vembedding_model= None
    __llm = None
    __vectorstore = None
    __k_docs_to_work= None

    def rag_config(self, dataset_id,table_id,region,embeddings_model="text-multilingual-embedding-002",content_field="content",text_embedding_field="embedding",doc_id_field="id",metadata_field="metadata",k_docs_to_work=20):
        self.__k_docs_to_work=k_docs_to_work
        self.__vembedding_model = VertexAIEmbeddings(model_name=embeddings_model, project=self.__project_id )
        self.__llm = ChatVertexAI(model_name=self.__llmodel, project=self.__project_id)
        self.__vectorstore = BigQueryVectorSearch(
            project_id=self.__project_id,
            dataset_name=dataset_id,
            table_name=table_id,
            location=region,
            embedding=self.__vembedding_model,
            content_field=content_field,       # Column in BQ table containing the text
            text_embedding_field=text_embedding_field, # Column in BQ table containing the vector
            doc_id_field=doc_id_field,             # Optional: Unique ID column
            metadata_field=metadata_field      # Optional: JSON metadata column
            )
        
    def rag_question(self, question):
        d1 = datetime.now()
        self.bbp("Starting question")
        retrieved_docs = self.__vectorstore.similarity_search(query=question, k=self.__k_docs_to_work)
        self.bbp(f"Retrieved {len(retrieved_docs)} relevant documents from BigQuery.")
        
        # Step 3: Combine retrieved documents with the question and ask the LLM
        qa_chain = RetrievalQA.from_chain_type(
            llm=self.__llm,
            chain_type="stuff",
            retriever=self.__vectorstore.as_retriever(search_kwargs={"k": self.__k_docs_to_work}),
            return_source_documents=True
        )
        result = qa_chain({"query": question})
        llm_answer = result["result"]
        source_documents = result["source_documents"]
        d2 = datetime.now()
        ts = (d2-d1).total_seconds()
        self.bbp("Process end " + str(ts) + "s")
        return llm_answer,source_documents
        


    def resp_using_cache(self,question,cache,llm_model=None):
        if llm_model != None:
            llm_model = self.__llmodel
        d1 = datetime.now()
        self.bbp("Starting question")
        response = self.client.models.generate_content(
                    model=llm_model,
                    contents=question,
                    config=types.GenerateContentConfig(
                    cached_content=cache.name
                    ))
        d2 = datetime.now()
        ts= (d2-d1)
        self.bbp("Process end " + str(ts) + "s")
        return response.text



project_id= "bs-fdld-ai"

secret_id = "poc_ai_001" # The name you gave the secret
sk = json.loads( access_secret_version(project_id,secret_id).replace("\n", "").strip() )

gemini_api_key = sk["gemini_api_key"]
os.environ["GOOGLE_API_KEY"] = gemini_api_key
project_id= sk["project_id"]
location= sk["region"]
llmmodel= sk["model"]

bucket_name="fdld-poc2"

logbbp = True



gg = myGeminiClient(project_id,gemini_api_key,location,llmmodel)



descDoc = """
Todo o texto deve estar presente nos blocos partidos dentro do limite de páginas indicado.
Retorna um objeto json com o formato {"totalDeBlocos":<int>,"items":<list>}
a lista de itens é feita por items com o seguinte formato:
{"DocName":<string>,"BlocoID":<int>,"BlocoMetadata":<string>,"BlocoContent":<string>,"BlocoType":<string>,"docPage_ini":<int>,"docPage_end":<int>,"ClausulaOuTopico":<string>}"}
os blocos são apenas uma divisão por isso o texto deve ser retornado na totalidade em cada blocoContent. 
a chunkMetadata é para ser usada como metadados em queries sobre vectores, o chunkType deve indicar se é texto, código, tabela ou imagem. 
docPage_ini e docPage_end tem a pagina inicial e final do bloco do documento de onde o bloco foi retirado
ClausulaOuTopico deve indicar o capitulo, a clausula e o nome da tabela quando houver, DocName, é o nome do documento.
O retorno deve ser somente o json indicado acime e mais nenhum outro texto.
"""

##### creating chunks

object_nameL5="ai1/rawdocs/Fidelidade_Auto_Liber3G_CG058_AU052_mar2024.pdf"
docL5,pgsL5 = getGCP_Doc(bucket_name,object_nameL5)
cacheInfoL5 = "Este documento é sobre regras e clausulas referentes a seguro automovel " + object_nameL5
descDocL5 = f"""
É necessário guardar todo o documento pdf numa base de dados vectorizada. Para isso é preciso partir o documento em n blocos para serem criados embeddings
Os blocos devem estar sempre alinhados com clausulas e uma clausula não deve ser partida por estar em paginas diferentes nem deve haver varias clausulas por bloco
Quando o texto não disser respeito a clausulas ou for uma tabela deve estar separado em blocos lógicos de assuntos
"""
descDocL5 += descDoc
cache_name,resp_text = gg.createChunksPDFDoc(docL5,pgsL5,cacheInfoL5,descDocL5)

object_nameSS="ai1/rawdocs/CG 28 - Multicare - Fidelidade_DL_Final_Hifenizada.pdf"
docSS,pgsSS = getGCP_Doc(bucket_name,object_nameSS)
cacheInfoSS = "Este documento é sobre regras e clausulas referentes a um seguro de saude " + object_nameSS
descDocSS = f"""
É necessário guardar todo o documento pdf numa base de dados vectorizada. Para isso é preciso partir o documento em n blocos para serem criados embeddings
Os blocos devem estar sempre alinhados com clausulas e uma clausula não deve ser partida por estar em paginas diferentes nem deve haver varias clausulas por bloco.
As clausulas estão definidas no indice da pagina 2.
Quando o texto não disser respeito a clausulas ou for uma tabela deve estar separado em blocos lógicos de assuntos
"""
descDocSS += descDoc
cache_name,resp_text = gg.createChunksPDFDoc(docSS,pgsSS,cacheInfoSS,descDocSS)


# cn = gg.createChunksPDFDoc_LoadDoc(doc,cacheInfo)
# fr = gg.createChunksPDFDoc_GetTextChunks(cn,1,10,descDoc)
# jl = gg.createChunksPDFDoc_CreateJSON(fr)
# save_JsonFile(resp_json[1],"chunks.json")

j_list = gg.createChunksPDFDoc_CreateJSON_Final(resp_text)

save_JsonFile(j_list,"chunks.json")

dataset_id = 'rag_dataset_eu'
table_id = 'testFomCode'
schema = [
        bigquery.SchemaField("DocName", "STRING", mode="NULLABLE"),  
        bigquery.SchemaField("BlocoID", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("BlocoMetadata", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("BlocoContent", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("BlocoType", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("docPage_ini", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("docPage_end", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("ClausulaOuTopico", "STRING", mode="NULLABLE"),
    ]
ee = createBQTableWithList_Batch(j_list,project_id,dataset_id,table_id,schema)




##### rag


table_id_embeddings = 'testFomCode_embeddings_materialized'
embeddings_model="text-multilingual-embedding-002"   # text-embedding-005
region_embeddings = "eu"
dataset_id = 'rag_dataset_eu'

gg.rag_config(dataset_id,table_id_embeddings,region_embeddings)
question="Determina a classe para o seguro automovel, tendo 3 anos de seguro, qual o agravamento se tiver tido 2 sinistros nos ultimos 5 anos mas nenhum nos ultimos 2 anos?"
question="Como pode ser resolvido o contrato de seguro de saude?"
llm_answer,source_documents = gg.rag_question(question)
llm_answer

#Determina a classe para o seguro automovel, tendo 3 anos de seguro, qual o agravamento se tiver tido 2 sinistros nos ultimos 5 anos mas nenhum nos ultimos 2 anos?



#### using only cache

gg = myGeminiClient(project_id,gemini_api_key,location,llmmodel)

doc,pgs = getGCP_Doc(bucket_name,object_nameL5)
cacheInfo = "Este documento é sobre regras e clausulas referentes a seguro automovel " + object_nameL5
cn = gg.createChunksPDFDoc_LoadDoc(doc,cacheInfo)
question="Determina a classe para o seguro automovel, tendo 3 anos de seguro, qual o agravamento se tiver tido 2 sinistros nos ultimos 5 anos mas nenhum nos ultimos 2 anos?"
resp = gg.resp_using_cache(question,cn)
resp


'''