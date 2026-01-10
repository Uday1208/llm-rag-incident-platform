"""
LangChain-based Resolution Agent.

Alternative implementation using LangChain's AgentExecutor
for comparison with the custom ReAct implementation.
"""

import os
import logging
from typing import List, Dict, Any, Optional

from .executor import ToolExecutor

log = logging.getLogger("reasoning-agent.langchain_agent")

# Check if LangChain is available
try:
    from langchain_core.tools import StructuredTool
    from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
    from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
    from langchain.agents import AgentExecutor, create_react_agent
    from langchain.agents.format_scratchpad import format_to_openai_function_messages
    from langchain.agents.output_parsers import ReActSingleInputOutputParser
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    log.warning("LangChain not installed. Use: pip install langchain langchain-core langchain-openai")

# LLM providers
try:
    from langchain_openai import AzureChatOpenAI, ChatOpenAI
    LANGCHAIN_OPENAI_AVAILABLE = True
except ImportError:
    LANGCHAIN_OPENAI_AVAILABLE = False

try:
    from langchain_community.chat_models import ChatOllama
    LANGCHAIN_OLLAMA_AVAILABLE = True
except ImportError:
    LANGCHAIN_OLLAMA_AVAILABLE = False


# =============================================================================
# Configuration
# =============================================================================

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5:7b")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://local-llm:11434")
AOAI_ENDPOINT = os.getenv("AOAI_ENDPOINT", "")
AOAI_KEY = os.getenv("AOAI_KEY", "")
AOAI_DEPLOYMENT = os.getenv("AOAI_CHAT_DEPLOYMENT", "")
AOAI_API_VERSION = os.getenv("AOAI_API_VERSION", "2024-02-01")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")


# =============================================================================
# System Prompt
# =============================================================================

LANGCHAIN_SYSTEM_PROMPT = """You are an expert SRE resolution agent. Your job is to help operators resolve incidents quickly.

You have access to the following tools:
{tools}

Follow this process:
1. THINK: Analyze what you know and what you need
2. ACT: Use tools to gather information  
3. OBSERVE: Process tool results
4. REPEAT until you have enough context
5. RESPOND: Provide resolution with specific steps

Use the following format:
Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Always cite your sources. If evidence is weak, say so clearly.

Begin!

Question: {input}
{agent_scratchpad}"""


# =============================================================================
# LangChain Tool Wrappers
# =============================================================================

def create_langchain_tools(executor: ToolExecutor) -> List["StructuredTool"]:
    """Create LangChain StructuredTool wrappers around our executor."""
    if not LANGCHAIN_AVAILABLE:
        return []
    
    tools = []
    
    # search_incidents tool
    async def search_incidents(query: str, top_k: int = 5, service_filter: str = None) -> str:
        result = await executor.execute("search_incidents", {
            "query": query,
            "top_k": top_k,
            "service_filter": service_filter,
        })
        return _format_result(result)
    
    tools.append(StructuredTool.from_function(
        func=search_incidents,
        coroutine=search_incidents,
        name="search_incidents",
        description="Search past incidents by semantic similarity. Returns similar incidents with severity and content.",
    ))
    
    # search_resolutions tool
    async def search_resolutions(query: str, top_k: int = 3) -> str:
        result = await executor.execute("search_resolutions", {
            "query": query,
            "top_k": top_k,
        })
        return _format_result(result)
    
    tools.append(StructuredTool.from_function(
        func=search_resolutions,
        coroutine=search_resolutions,
        name="search_resolutions",
        description="Find past resolutions for incidents. Use to find how similar issues were resolved.",
    ))
    
    # analyze_trace tool
    async def analyze_trace(incident_id: str = None, trace_id: str = None) -> str:
        result = await executor.execute("analyze_trace", {
            "incident_id": incident_id,
            "trace_id": trace_id,
        })
        return _format_result(result)
    
    tools.append(StructuredTool.from_function(
        func=analyze_trace,
        coroutine=analyze_trace,
        name="analyze_trace",
        description="Get detailed analysis of a specific incident trace.",
    ))
    
    # get_service_health tool
    async def get_service_health(service_name: str) -> str:
        result = await executor.execute("get_service_health", {
            "service_name": service_name,
        })
        return _format_result(result)
    
    tools.append(StructuredTool.from_function(
        func=get_service_health,
        coroutine=get_service_health,
        name="get_service_health",
        description="Get current health status and recent incident count for a service.",
    ))
    
    return tools


def _format_result(result: Dict[str, Any]) -> str:
    """Format tool result as readable string for LLM."""
    import json
    if "error" in result:
        return f"Error: {result['error']}"
    return json.dumps(result, indent=2, default=str)[:2000]


# =============================================================================
# LLM Factory
# =============================================================================

def get_langchain_llm():
    """Get LangChain LLM based on configuration."""
    if LLM_PROVIDER == "azure" and LANGCHAIN_OPENAI_AVAILABLE:
        if not AOAI_ENDPOINT or not AOAI_KEY:
            raise ValueError("Azure OpenAI requires AOAI_ENDPOINT and AOAI_KEY")
        return AzureChatOpenAI(
            azure_endpoint=AOAI_ENDPOINT,
            api_key=AOAI_KEY,
            api_version=AOAI_API_VERSION,
            deployment_name=AOAI_DEPLOYMENT or LLM_MODEL,
            temperature=0.3,
        )
    
    elif LLM_PROVIDER == "openai" and LANGCHAIN_OPENAI_AVAILABLE:
        if not OPENAI_API_KEY:
            raise ValueError("OpenAI requires OPENAI_API_KEY")
        return ChatOpenAI(
            api_key=OPENAI_API_KEY,
            model=LLM_MODEL,
            temperature=0.3,
        )
    
    elif LANGCHAIN_OLLAMA_AVAILABLE:
        return ChatOllama(
            base_url=OLLAMA_BASE_URL,
            model=LLM_MODEL,
            temperature=0.3,
        )
    
    else:
        raise RuntimeError("No LangChain LLM provider available. Install: pip install langchain-openai or langchain-community")


# =============================================================================
# LangChain Resolution Agent
# =============================================================================

class LangChainResolutionAgent:
    """
    LangChain-based resolution agent.
    
    Uses LangChain's AgentExecutor with ReAct prompting pattern.
    """
    
    def __init__(self, tool_executor: Optional[ToolExecutor] = None, max_iterations: int = 5):
        if not LANGCHAIN_AVAILABLE:
            raise ImportError("LangChain not installed. Use: pip install langchain langchain-core")
        
        self.executor = tool_executor or ToolExecutor()
        self.max_iterations = max_iterations
        
        # Setup LLM
        self.llm = get_langchain_llm()
        
        # Setup tools
        self.tools = create_langchain_tools(self.executor)
        
        # Create prompt
        self.prompt = ChatPromptTemplate.from_template(LANGCHAIN_SYSTEM_PROMPT)
        
        # Create agent
        self.agent = create_react_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=self.prompt,
        )
        
        # Create executor
        self.agent_executor = AgentExecutor(
            agent=self.agent,
            tools=self.tools,
            verbose=True,
            max_iterations=max_iterations,
            handle_parsing_errors=True,
        )
    
    async def resolve(self, query: str, session_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Resolve an incident using LangChain agent.
        
        Args:
            query: User's incident description
            session_id: Optional session ID
            
        Returns:
            Resolution result
        """
        import uuid
        from datetime import datetime
        
        session_id = session_id or str(uuid.uuid4())[:8]
        start_time = datetime.utcnow()
        
        try:
            result = await self.agent_executor.ainvoke({"input": query})
            
            return {
                "id": session_id,
                "user_query": query,
                "status": "resolved",
                "result": result.get("output", ""),
                "iterations": len(result.get("intermediate_steps", [])),
                "cited_incidents": [],  # LangChain doesn't track these automatically
                "cited_resolutions": [],
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                "agent_type": "langchain",
            }
            
        except Exception as e:
            log.error(f"LangChain agent failed: {e}")
            return {
                "id": session_id,
                "user_query": query,
                "status": "failed",
                "result": f"Resolution failed: {str(e)}",
                "iterations": 0,
                "cited_incidents": [],
                "cited_resolutions": [],
                "agent_type": "langchain",
            }


def is_langchain_available() -> bool:
    """Check if LangChain is available."""
    return LANGCHAIN_AVAILABLE
