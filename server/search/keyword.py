import math
import time
from clients import aerospike_client
from nlp_spacy import get_tokens
from config import Config

# BM25 parameters
k1 = 1.5
b = 0.75

def bm25(tf: int, doc_len: int, avg_doc_len: int, idf: float):
    """
    Calculate the BM25 score for a term within a document.

    BM25 is a ranking function used in information retrieval. It calculates the relevance
    of a document to a query based on term frequency (tf), document length (doc_len), 
    average document length (avg_doc_len), and inverse document frequency (idf).

    Args:
        tf (int): Term frequency, the number of times the term appears in the document.
        doc_len (int): The total number of tokens in the document.
        avg_doc_len (float): The average number of tokens across all documents.
        idf (float): Inverse document frequency, indicating how rare the term is across documents.

    Returns:
        float: The BM25 score for the term in the document.
    """

    numerator = tf * (k1 + 1)
    denominator = tf + k1 * (1 - b + b * (doc_len / avg_doc_len))
    return idf * (numerator / denominator)

def rank_ids(results: dict, total_docs: int, total_tokens: int):
    """
    Rank documents based on BM25 and proximity scoring.

    This function ranks the documents using a combination of BM25 scores, proximity 
    scoring of keywords in content, title, and description, and weighting of title and 
    description token frequencies. It computes the final score for each document and returns 
    the top-ranked documents.

    Args:
        results (dict): A dictionary of results where keys are keywords and values are dictionaries 
                        of documents containing term data such as frequency, positions, title tokens, 
                        description tokens, and number of tokens.
        total_docs (int): The total number of documents in the corpus.
        total_tokens (int): The total number of tokens across all documents.

    Returns:
        list: A list of top-ranked documents (up to 200), sorted by their computed scores. 
              Each document is represented by a dictionary containing its 'id'.
    """

    ranked_docs = {}

    for docs in results.values():
        for doc in docs:
            ranked_docs[doc] = {
                "doc_score": 0.0,
                "id": doc,
                "title_tokens": docs[doc]["title_tokens"],
                "desc_tokens": docs[doc]["desc_tokens"],
                "num_tokens": docs[doc]["num_tokens"]
            }

    avg_doc_len = total_tokens / total_docs

    # Loop through each keyword and document to compute BM25 and proximity scores
    for keyword in results:
        num_docs = len(results[keyword])
        if num_docs < 1:
            break
        idf = math.log((total_docs - num_docs + 0.5) / (num_docs + 0.5) + 1)

        for doc_id in results[keyword]:
            content_tf = float(results[keyword][doc_id]['frequency'])
            doc_len = ranked_docs[doc_id]["num_tokens"]

            # Calculate BM25 score for content
            content_score = bm25(content_tf, doc_len, avg_doc_len, idf)
            
            # Calculate title and description token frequencies
            title_tokens = ranked_docs[doc_id]["title_tokens"]
            desc_tokens = ranked_docs[doc_id]["desc_tokens"]
            title_score = title_tokens.count(keyword) * (10 / (len(title_tokens) or 1))
            desc_score = desc_tokens.count(keyword) * (5 / (len(desc_tokens) or 1))

            # Base score: BM25 + title and description token scores
            base_score = content_score + title_score + desc_score

            # Proximity scoring for keyword proximity in content, title, and description
            content_keyword_positions = results[keyword][doc_id]['positions']
            proximity_score_content = 0
            proximity_score_title = 0
            proximity_score_desc = 0

            # Check proximity of the keyword to other keywords in the same document
            for other_keyword in results:
                if other_keyword != keyword and doc_id in results[other_keyword]:
                    other_keyword_positions = results[other_keyword][doc_id]['positions']

                    # Content proximity scoring
                    if content_keyword_positions and other_keyword_positions:
                        min_content_distance = min(abs(pos1 - pos2) for pos1 in content_keyword_positions for pos2 in other_keyword_positions)
                        if min_content_distance > 0:
                            proximity_score_content += 1 / min_content_distance  

                    # Title proximity scoring
                    title_keyword_positions = [i for i, token in enumerate(title_tokens) if token == keyword]
                    title_other_keyword_positions = [i for i, token in enumerate(title_tokens) if token == other_keyword]
                    if title_keyword_positions and title_other_keyword_positions:
                        min_title_distance = min(abs(pos1 - pos2) for pos1 in title_keyword_positions for pos2 in title_other_keyword_positions)
                        if min_title_distance > 0:
                            proximity_score_title += 1 / min_title_distance  

                    # Description proximity scoring
                    desc_keyword_positions = [i for i, token in enumerate(desc_tokens) if token == keyword]
                    desc_other_keyword_positions = [i for i, token in enumerate(desc_tokens) if token == other_keyword]
                    if desc_keyword_positions and desc_other_keyword_positions:
                        min_desc_distance = min(abs(pos1 - pos2) for pos1 in desc_keyword_positions for pos2 in desc_other_keyword_positions)
                        if min_desc_distance > 0:
                            proximity_score_desc += 1 / min_desc_distance  

            # Combine proximity scores with appropriate weights
            final_proximity_score = (proximity_score_content * 1) + (proximity_score_title * 10) + (proximity_score_desc * 5)

            # Add proximity score to the base score
            ranked_docs[doc_id]["doc_score"] += base_score + final_proximity_score

    # Sort and return top-ranked documents
    if len(ranked_docs) > 0:
        sorted_docs = sorted(ranked_docs.values(), key=lambda doc: doc["doc_score"], reverse=True)[:200]
        for doc in sorted_docs:
            for key in ["doc_score", "desc_tokens", "title_tokens", "num_tokens"]:
                del doc[key]
        return sorted_docs
    else:
        return []

async def keyword_search(q: str):
    """
    Perform a keyword search using BM25 and proximity scoring.

    This function tokenizes the query string, fetches keyword data from Aerospike, 
    computes BM25 scores, and ranks documents based on keyword frequency, proximity 
    of keywords, and relevance in titles and descriptions. It ensures that all query 
    terms are present in the final ranked documents.

    Args:
        q (str): The query string to search for.

    Returns:
        tuple: A tuple containing:
            - final_results (list): A list of top-ranked documents based on the query. 
              Each document is represented by a dictionary with the document 'id'.
            - time_taken (float): The time taken to perform the search in milliseconds.
    """

    start = time.time()
    query = get_tokens([q])[0]
    (_,_,bins) = aerospike_client.get(("search", "totals", "total"))
    total_docs = bins["docs"]
    total_tokens = bins["tokens"]

    bin_name = "term_data"
    
    keyword_keys = []
    results = {}
    for keyword in query:
        keyword_keys.append((Config.NAMESPACE, Config.KEYWORD_SET, keyword)) 
    
    records = aerospike_client.batch_read(keyword_keys, [bin_name])
    
    for idx, batch_record in enumerate(records.batch_records):
        if batch_record.result == 0:
            if batch_record.record:
                (key, _, bins) = batch_record.record
                (_, _, kywrd, _) = key
                results[kywrd] = bins[bin_name]
            else:
                query.pop(idx)
        else:
            return ([], 0) 
    
    if len(query) > 0:
        if len(query) > 1:
            # Ensure documents contain all query terms
            common_doc_ids = set(results[query[0]].keys())
            for keyword in query:
                common_doc_ids.intersection_update(results[keyword].keys())
            for keyword, doc_ids in results.items():
                for doc_id in list(doc_ids.keys()): 
                    if doc_id not in common_doc_ids:
                        results[keyword].pop(doc_id) 

        final_results = rank_ids(results, total_docs, total_tokens) 
        time_taken = time.time() - start

        return (final_results, time_taken * 1000)
        
    return ([], 0)