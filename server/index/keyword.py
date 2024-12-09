import aerospike
from aerospike import Client
from aerospike_helpers.batch.records import BatchRecords, Write
from aerospike_helpers.operations import map_operations as map_ops
from config import Config
from collections import defaultdict

write_policy = {
    'key': aerospike.POLICY_KEY_SEND,  # Store the key along with the record
}

# Update the inverted index in Aerospike
def update_keyword_index(aerospike_client: Client, url: str, docs: list[list[str]], title_tokens: list[str], desc_tokens: list[str]):
    inverted_index_map = defaultdict(lambda: defaultdict(lambda: {"positions": [], "frequency": 0, "title_tokens": [], "desc_tokens": [], "num_tokens": 0}))
    
    for idx, tokens in enumerate(docs):
        chunk_key = f"{url}___{str(idx)}"
        num_tokens = len(docs[idx])

        for position, token in enumerate(tokens):
            inverted_index_map[token][chunk_key]["positions"].append(position)
            inverted_index_map[token][chunk_key]["frequency"] += 1

            if len(inverted_index_map[token][chunk_key]["title_tokens"]) == 0:
                inverted_index_map[token][chunk_key]["title_tokens"] = title_tokens

            if len(inverted_index_map[token][chunk_key]["desc_tokens"]) == 0:
                inverted_index_map[token][chunk_key]["desc_tokens"] = desc_tokens

            if inverted_index_map[token][chunk_key]['num_tokens'] == 0:
                inverted_index_map[token][chunk_key]['num_tokens'] = num_tokens

    batch = BatchRecords()
    for term, doc_info in inverted_index_map.items():
        key = (Config.NAMESPACE, Config.KEYWORD_SET, term)
        batch_ops = []

        for chunk_id, info in doc_info.items():
            batch_ops.append(
                map_ops.map_put(
                    Config.KEYWORD_BIN,  
                    chunk_id, 
                    {  
                        "positions": info["positions"],
                        "frequency": info["frequency"],
                        "title_tokens": info["title_tokens"],
                        "desc_tokens": info["desc_tokens"],
                        "num_tokens": info["num_tokens"]
                    }
                )
            )
        batch.batch_records.append(Write(key, batch_ops, policy=write_policy))
        
    aerospike_client.batch_write(batch)