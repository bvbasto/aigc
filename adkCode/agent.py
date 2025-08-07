# https://google.github.io/adk-docs/tools/built-in-tools/#code-execution
# https://cloud.google.com/vertex-ai/generative-ai/docs/agent-development-kit/quickstart
# https://www.youtube.com/watch?v=bPtKnDIVEsg

# agent.py
from google.adk.agents import Agent
from google.adk.tools import VertexAiSearchTool


PROJECT_ID = "bs-fdld-ai" # Replace with your project ID
DATASTORE_ID = "projects/bs-fdld-ai/locations/eu/collections/default_collection/dataStores/bqplus1_1753909283715"

root_agent = Agent(
    name="vertex_search_agent",
    model="gemini-2.5-flash",
    instruction="""
    You are an expert assistant that uses a specialized knowledge base to answer questions.
    When a user asks a question, use the Vertex AI Search tool to find relevant information.
    Use the retrieved documents to formulate a concise and accurate answer. Retrive 20 documents.
    Always provide the source citations for your answers.
    If the search results do not contain a definitive answer, state that you cannot find the relevant information.
    Answere in the same language as the user.
    """,
    description="Enterprise document search assistant with Vertex AI Search capabilities",
    tools=[VertexAiSearchTool(data_store_id=DATASTORE_ID)]
)




