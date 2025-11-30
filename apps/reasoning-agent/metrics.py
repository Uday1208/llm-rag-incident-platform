"""Process-level Prom counters for RAG quality signals."""
from prometheus_client import Counter

# Count how many RAG retrievals we attempted from the agent
rag_retrieval_queries_total = Counter(
    "rag_retrieval_queries_total",
    "Number of /v1/reason requests that performed a RAG retrieval"
)

# Sum of docs we actually attached as context across all queries
rag_retrieved_docs_total = Counter(
    "rag_retrieved_docs_total",
    "Total number of retrieved docs used as context"
)

# Count queries where a resolution doc was among the chosen contexts
rag_resolution_hits_total = Counter(
    "rag_resolution_hits_total",
    "Number of queries where a resolution document was included in the context"
)
