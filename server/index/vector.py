from aerospike_vector_search import types, AdminClient, Client
from nlp_embed import MODEL_DIM
from config import Config

# Creates the vector index
# Returns if it already exists
def create_vector_index(vector_admin: AdminClient, logger):   
    logger.info("Checking for vector index")

    for idx in vector_admin.index_list():
        if (
            idx["id"]["namespace"] == Config.NAMESPACE
            and idx["id"]["name"] == Config.VECTOR_INDEX
        ):
            logger.info("Index already exists")
            return
        
    logger.info("Creating vector index")
    vector_admin.index_create(
        namespace=Config.NAMESPACE,
        name=Config.VECTOR_INDEX,
        sets=Config.VECTOR_SET,
        vector_field=Config.VECTOR_FIELD,
        dimensions=MODEL_DIM,
        vector_distance_metric=types.VectorDistanceMetric.COSINE,
    )    
    logger.info("Index created")

def update_vector_index(vector_client: Client, url: str, embeddings: list[list[float]]):
    for idx, embedding in enumerate(embeddings):
        vector_client.upsert(
            namespace=Config.NAMESPACE, 
            set_name=Config.VECTOR_SET,
            key=f"{url}___{str(idx)}", 
            record_data={Config.VECTOR_FIELD: embedding}
        )
