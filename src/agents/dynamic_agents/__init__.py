"""Sub-agents: child agents running as facets (upstream ``dynamic-agents/``)."""

from .api import DynamicAgents
from .errors import SubAgentAbortedError
from .paths import build_agent_path, build_agent_url
from .stubs import AgentStub
from .types import AgentPathStep, AgentRoute, SubAgentInfo

__all__ = (
    "AgentPathStep",
    "AgentRoute",
    "AgentStub",
    "DynamicAgents",
    "SubAgentAbortedError",
    "SubAgentInfo",
    "build_agent_path",
    "build_agent_url",
)
