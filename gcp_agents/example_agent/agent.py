from google.adk.agents import LlmAgent
from google.adk.tools import agent_tool
from google.adk.tools.google_search_tool import GoogleSearchTool
from google.adk.tools.url_context_tool import url_context

subagente_mail_google_search_agent = LlmAgent(
    name="Subagente_Mail_google_search_agent",
    model="gemini-2.5-flash",
    description=("Agent specialized in performing Google searches."),
    sub_agents=[],
    instruction="Use the GoogleSearchTool to find information on the web.",
    tools=[GoogleSearchTool()],
)
subagente_mail_url_context_agent = LlmAgent(
    name="Subagente_Mail_url_context_agent",
    model="gemini-2.5-flash",
    description=("Agent specialized in fetching content from URLs."),
    sub_agents=[],
    instruction="Use the UrlContextTool to retrieve content from provided URLs.",
    tools=[url_context],
)
subagente_mail = LlmAgent(
    name="subagente_mail",
    model="gemini-2.5-flash",
    description=(""),
    sub_agents=[],
    instruction=(
        "Crei delle mail nello stile di Shakespeare sulla base delle richieste "
        "del cliente"
    ),
    tools=[
        agent_tool.AgentTool(agent=subagente_mail_google_search_agent),
        agent_tool.AgentTool(agent=subagente_mail_url_context_agent),
    ],
)
subagente_traduzione_google_search_agent = LlmAgent(
    name="Subagente_traduzione_google_search_agent",
    model="gemini-2.5-flash",
    description=("Agent specialized in performing Google searches."),
    sub_agents=[],
    instruction="Use the GoogleSearchTool to find information on the web.",
    tools=[GoogleSearchTool()],
)
subagente_traduzione_url_context_agent = LlmAgent(
    name="Subagente_traduzione_url_context_agent",
    model="gemini-2.5-flash",
    description=("Agent specialized in fetching content from URLs."),
    sub_agents=[],
    instruction="Use the UrlContextTool to retrieve content from provided URLs.",
    tools=[url_context],
)
subagente_traduzione = LlmAgent(
    name="subagente_traduzione",
    model="gemini-2.5-flash",
    description=(""),
    sub_agents=[],
    instruction="Traduci tutto quello che chiede l'utente in spagnolo",
    tools=[
        agent_tool.AgentTool(agent=subagente_traduzione_google_search_agent),
        agent_tool.AgentTool(agent=subagente_traduzione_url_context_agent),
    ],
)
il_mio_agente_google_search_agent = LlmAgent(
    name="Il_mio_agente_google_search_agent",
    model="gemini-2.5-flash",
    description=("Agent specialized in performing Google searches."),
    sub_agents=[],
    instruction="Use the GoogleSearchTool to find information on the web.",
    tools=[GoogleSearchTool()],
)
il_mio_agente_url_context_agent = LlmAgent(
    name="Il_mio_agente_url_context_agent",
    model="gemini-2.5-flash",
    description=("Agent specialized in fetching content from URLs."),
    sub_agents=[],
    instruction="Use the UrlContextTool to retrieve content from provided URLs.",
    tools=[url_context],
)
root_agent = LlmAgent(
    name="Il_mio_agente",
    model="gemini-2.5-flash",
    description=(
        "Riceve le informazioni dall'utente e decide se far partire il subagent per "
        "traduzione o quello per composizione mail"
    ),
    sub_agents=[subagente_mail, subagente_traduzione],
    instruction=(
        "In base allle informazioni fornite dall'utente scegli se utilizzare "
        "il subagent di traduzione o quello per creare le mail.\n"
        "Se la richiesta dell'utente esce da queste richieste fermati e prosegui "
        "solo quando ottieni un prompt valido."
    ),
    tools=[
        agent_tool.AgentTool(agent=il_mio_agente_google_search_agent),
        agent_tool.AgentTool(agent=il_mio_agente_url_context_agent),
    ],
)
