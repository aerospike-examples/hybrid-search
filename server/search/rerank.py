def calculate_score(results, reranked_docs, k):
    for rank, result in enumerate(results):
        id = result['id']
        if (id not in reranked_docs):
            reranked_docs[id] = { 
                'score': 0,
                'info': results[rank]
            }
        reranked_docs[id]['score'] += 1 / (rank + 1 + k)
    return reranked_docs

# Perform Reciprocal Rank Fusion
def rrf(vector_results, text_results, k=60):
    reranked_docs = {}
    reranked_docs = calculate_score(vector_results, reranked_docs, k)
    reranked_docs = calculate_score(text_results, reranked_docs, k)
    return [details['info'] for _, details in sorted(reranked_docs.items(), key=lambda x: x[1]['score'], reverse=True)]