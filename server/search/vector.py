import time
from clients import aerospike_client, vector_client
from nlp_embed import get_embedding
from aerospike import exception as ex
from utils import EmbedTask
from config import Config

async def vector_search(q: str, count: int):
    """
    Perform a vector search on a query and return the results.

    This function first attempts to retrieve a cached embedding for the query
    from Aerospike. If the embedding is not found in the cache, it generates
    the embedding using the `get_embedding` function and caches the result
    in Aerospike. Then, it uses the `vector_client` to perform a vector search
    using the query's embedding in a vector index within Aerospike. The search
    results are returned along with the time taken for the search.

    Args:
        q (str): The query string for which the vector search is performed.
        count (int): The maximum number of search results to return.

    Returns:
        tuple: A tuple containing:
            - results (list): A list of dictionaries containing the search results, 
              where each dictionary has the 'id' of the search result.
            - elapsed_time (float): The time taken for the search in milliseconds.

    Raises:
        aerospike.exception.RecordNotFound: If the query cache record is not found in Aerospike.
    """

    start = time.time()
    key = ("query-cache", "vectors_vertex", q)
    try:
        (_, _, bins) = aerospike_client.get(key)
        embedding = bins["embedding"]
    except ex.RecordNotFound:
        embedding = get_embedding(q, EmbedTask.QUERY)
        aerospike_client.put(key, {"embedding": embedding})

    vector_results = vector_client.vector_search(
        namespace=Config.NAMESPACE,
        index_name=Config.VECTOR_INDEX,
        query=embedding,
        limit=count
    )

    results = []
    for result in vector_results:
        if result.distance < .4:
            results.append({"id": result.key.key})
    
    return (results, (time.time() - start) * 1000)
   