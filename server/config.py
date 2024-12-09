import os

class Config(object):
    AEROSPIKE_HOST = os.getenv("AEROSPIKE_HOST") or "localhost"
    AEROSPIKE_PORT = os.getenv("AEROSPIKE_PORT") or 3000
    NAMESPACE = os.getenv("NAMESPACE") or "search"
    KEYWORD_SET = os.getenv("KEYWORD_SET") or "keywords"
    KEYWORD_BIN = os.getenv("KEYWORD_BIN") or "term_data"
    DOCUMENT_SET = os.getenv("DOCUMENT_SET") or "documents"

    VECTOR_HOST = os.getenv("VECTOR_HOST") or "localhost"
    VECTOR_PORT = os.getenv("VECTOR_PORT") or 5000
    VECTOR_INDEX = os.getenv("VECTOR_INDEX") or "vector_idx"
    VECTOR_SET = os.getenv("VECTOR_SET") or "vectors"
    VECTOR_FIELD = os.getenv("VECTOR_FIELD") or "vector"
