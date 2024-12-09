import gc
import hashlib
import aerospike
from aerospike_helpers import expressions as exp
from aerospike_vector_search import Client
from aerospike_helpers.operations import operations as ops, expression_operations as exp_ops
from aerospike_helpers.batch.records import BatchRecords, Write

from llama_index.core.node_parser import SentenceSplitter
from llama_index.core import Document

from scraper.run_scraper import Scraper
from nlp_spacy import get_tokens
from nlp_embed import get_embedding
from index.clean import cleanup_chunks
from index.vector import update_vector_index
from index.keyword import update_keyword_index
from utils import md, EmbedTask, get_category
from config import Config

base_splitter = SentenceSplitter(chunk_size=512, chunk_overlap=32)
write_policy = {
    'key': aerospike.POLICY_KEY_SEND,  # Store the key along with the record
}

def get_totals(aerospike_client: aerospike.Client):
    query = aerospike_client.query(Config.NAMESPACE, Config.DOCUMENT_SET)
    total_docs = 0
    total_tokens = 0

    def tally_records(record):
        nonlocal total_docs
        nonlocal total_tokens
        
        _, _, bins = record
        total_docs += 1
        total_tokens += bins["num_tokens"]

    query.foreach(tally_records)
    aerospike_client.put((Config.NAMESPACE, "totals", "total"), {"docs": total_docs, "tokens": total_tokens})

def parse_document(document, aerospike_client: aerospike.Client, logger):
    url = document["meta"]["url"]
    title = document["meta"]["title"]
    desc = document["meta"]["desc"]
    doc = "\n".join([md(html) for html in document["doc"]])

    doc_key = (Config.NAMESPACE, "doc_meta", url)
    doc_hash = hashlib.sha256(f"{url}\n{title}\n{desc}\n{doc}".encode("utf-8")).hexdigest()

    expr = exp.Eq(exp.StrBin("doc_hash"), doc_hash).compile()
    operations = [
        exp_ops.expression_read("unchanged", expr, aerospike.EXP_READ_EVAL_NO_FAIL),
        ops.write("doc_hash", doc_hash),
        ops.write("active", 1),
        ops.read("chunks")
    ]
    
    (_, _, bins) = aerospike_client.operate(doc_key, operations, policy=write_policy)
    unchanged = bins.get("unchanged")
    chunks = bins.get("chunks") or 0

    if not unchanged:
        logger.info(f"New or changed document, adding \"{url}\" to index.")
        return (url, title, desc, doc, chunks)
    else:
        return None

# Add Document to Vector and Keyword index 
def chunk_and_index_document(aerospike_client: aerospike.Client, vector_client: Client, document: dict, logger):
    results = parse_document(document, aerospike_client, logger)
    if results == None:
        return
    
    (url, title, desc, doc, chunks) = results

    cat = get_category(url)
    # Get document chunks
    nodes = base_splitter.get_nodes_from_documents([Document(text=doc)])
    batch = BatchRecords()

    # Get title and description tokens
    doc_tokens = get_tokens([title, desc] + [node.get_content() for node in nodes])
    title_tokens = doc_tokens[0]
    desc_tokens = doc_tokens[1]
    chunk_tokens = doc_tokens[2:]

    update_keyword_index(aerospike_client, url, docs=chunk_tokens, title_tokens=title_tokens, desc_tokens=desc_tokens)

    chunk_count = 0
    embeddings = []
    for idx, node in enumerate(nodes):
        chunk_count += 1
        key = f"{url}___{str(idx)}"
        content = node.get_content()
        num_tokens = len(chunk_tokens[idx])
        batch_ops = [
            ops.write("title", title),
            ops.write("url", url),
            ops.write("desc", desc),
            ops.write("content", content),
            ops.write("cat", cat),
            ops.write("num_tokens", num_tokens)
        ]

        batch.batch_records.append(Write((Config.NAMESPACE, Config.DOCUMENT_SET, key), batch_ops, policy=write_policy))
        text = f"TITLE: {title}, DESCRIPTION: {desc}, CONTENT: {content}"
        embeddings.append(get_embedding(text, EmbedTask.DOCUMENT))

    # embeddings = get_embedding(chunks, EmbedTask.DOCUMENT)
    update_vector_index(vector_client, url, embeddings)
        
    batch.batch_records.append(
        Write((Config.NAMESPACE, "doc_meta", url), [ops.write("chunks", chunk_count)])
    )
    
    aerospike_client.batch_write(batch)
    if chunks > chunk_count:
        cleanup_chunks(aerospike_client, vector_client, url, chunks, chunk_count)

    del document, nodes, batch, embeddings, chunks
    gc.collect()        

if __name__=="__main__": 
    docs_scraper = Scraper()
    docs_scraper.run_spider()