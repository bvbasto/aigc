# https://bgiri-gcloud.medium.com/step-by-step-guide-to-deploy-adk-agents-to-agent-engine-2acf73df1df0
import vertexai
from vertexai import agent_engines
from dotenv import load_dotenv
import os

from agent import root_agent

load_dotenv()

vertexai.init(
    project=os.getenv("GOOGLE_CLOUD_PROJECT"),
    location=os.getenv("GOOGLE_CLOUD_LOCATION"),
    staging_bucket=os.getenv("GOOGLE_CLOUD_STAGING_BUCKET"),
)

remote_app = agent_engines.create(
    display_name=os.getenv("APP_NAME", "Agent App"),
    agent_engine=root_agent,
    requirements=["google-cloud-aiplatform[adk,agent-engines,llama-index]==1.95.1",
        "google-adk==1.4.2",
        "pydantic-settings",
        "cloudpickle==3.0"] 
)
