import aerospike
from aerospike_vector_search import Client
from aerospike_helpers.batch.records import BatchRecords, Write
from aerospike_helpers.operations import operations as ops, map_operations as map_ops
from config import Config

policy = {
    'key': aerospike.POLICY_KEY_SEND
}

# Remove inactive documents from the keyword index
def clean_keywords(aerospike_client: aerospike.Client, keyword_keys: list[str]):
    bg_query = aerospike_client.query(Config.NAMESPACE, Config.KEYWORD_SET)
    query_ops = [map_ops.map_remove_by_key_list(bin_name=Config.KEYWORD_BIN, key_list=keyword_keys, return_type=aerospike.MAP_RETURN_NONE)]
    bg_query.add_ops(query_ops)
    bg_query.execute_background()

# Remove inactive documents from the document set
def clean_documents(aerospike_client: aerospike.Client, document_keys: list[str]):
    batch_keys = []
    for key in document_keys:
        batch_keys.append((Config.NAMESPACE, Config.DOCUMENT_SET, key))
    
    aerospike_client.batch_remove(batch_keys)

# Query the doc_meta set to identify inactive documents and remove them from the vector and keyword indexes
def remove_from_index(aerospike_client: aerospike.Client, vector_client: Client, logger):
    del_urls = []
    def update_urls(record):
        key, _, bins = record
        if bins.get("active") == 1:
            aerospike_client.put(key, {"active": 0})
        else:
            del_urls.append((key[2], bins.get("chunks")))
            aerospike_client.remove(key)

    query = aerospike_client.query(Config.NAMESPACE, "doc_meta")
    query.foreach(update_urls)

    if len(del_urls) > 0:
        logger.info(f"The following documents will be removed:")
        document_keys = []
        for url, chunks in del_urls:
            for i in range(chunks):
                key = f"{url}___{str(i)}"
                logger.info(key)
                document_keys.append(key)
                vector_client.delete(namespace=Config.NAMESPACE, set_name=Config.VECTOR_SET, key=key)

        if len(document_keys) > 0:
            clean_keywords(aerospike_client, document_keys)
            clean_documents(aerospike_client, document_keys)

# Remove excess chunks from the vector and keyword indexes after processing the document
def cleanup_chunks(aerospike_client: aerospike.Client, vector_client: Client, url: str, current_chunks: int, new_chunks: int):
    document_keys = []
    for i in range(new_chunks, current_chunks):
        key = f"{url}___{str(i)}"
        document_keys.append(key)
        vector_client.delete(namespace=Config.NAMESPACE, set_name=Config.VECTOR_SET, key=key)
    
    if len(document_keys) > 0:
        clean_keywords(aerospike_client, document_keys)
        clean_documents(aerospike_client, document_keys)

# Sync the doc_meta set with the current indexed documents
def sync_meta(aerospike_client: aerospike.Client):
    print("Getting documents and generating dictionary")
    documents = {}
    query = aerospike_client.query(Config.NAMESPACE, Config.DOCUMENT_SET)

    def create_dict(record):
        key, _, _ = record
        url = key[2].split("___")[0]
        
        if documents.get(url):
            documents[url] += 1
        else:
            documents[url] = 1

    query.foreach(create_dict, options={"nobins": True})

    print("Creating batch for update")
    batch = BatchRecords()

    for url, chunks in documents.items():
        batch.batch_records.append(Write(
            (Config.NAMESPACE, "doc_meta", url), 
            [
                ops.write("chunks", chunks),
                ops.write("active", 0)
            ], 
            policy=policy
        ))

    print("Updating records")
    aerospike_client.batch_write(batch)
    print("Update complete")

# Sync the keyword index with the current indexed documents
def sync_keyword(aerospike_client: aerospike.Client):
    query = aerospike_client.query(Config.NAMESPACE, Config.KEYWORD_SET)

    def update_index(record):
        key, _, bins = record
        documents = bins.get(Config.KEYWORD_BIN)
        if documents:
            for doc in documents:
                _, meta = aerospike_client.exists((Config.NAMESPACE, Config.DOCUMENT_SET, doc))
                if meta == None:
                    print(f"Removing {doc} from keyword {key[2]} in index")
                    aerospike_client.operate(key, [map_ops.map_remove_by_key(Config.KEYWORD_BIN, doc, aerospike.MAP_RETURN_NONE)])
                    print("Removed from index")
        else:
            print(f"No documents, removing keyword {key[2]} from index")
            aerospike_client.remove(key)
    
    print("Getting keywords and cleaning index")
    query.foreach(update_index)

    aerospike_client.close()
    print("Cleaning complete")