import time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from search.vector import vector_search
from search.keyword import keyword_search
from search.rerank import rrf
from config import Config
from clients import aerospike_client
from utils import get_category
import math

app = FastAPI(
    title="Aerospike Search",
    openapi_url=None, 
    docs_url=None,
    redoc_url=None,
    swagger_ui_oauth2_redirect_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/rest/v1/search/")
async def search(q: str, count: int = 5, search_type: str = "hybrid", page: int = 0, pageSize: int = 10, filters: str = ""):
    start = time.time()
    vector_results = []
    keyword_results = []
    search_results = []
    time_taken = {}
    
    filters_list = filters.split(",") or []

    if search_type == "hybrid" or search_type == "vector": 
        (vector_results, v_time) = await vector_search(q, 100)

    if search_type == "hybrid" or search_type == "keyword": 
        (keyword_results, k_time) = await keyword_search(q)

    if search_type == "hybrid":
        search_results = rrf(vector_results, keyword_results)

        time_taken = {
            "vector": v_time,
            "keyword": k_time, 
        }
    else:
        search_results = vector_results or keyword_results
 
    results = {}
    categories = set()
    for result in search_results:
        url = result["id"].split("___")[0]
        key = url.split("?client=")
        client = None
        if len(key) > 1:
            client = key[1]
        cat = get_category(key[0])
        categories.add(cat)
        if results.get(key[0]):
            if client != None:
                results[key[0]]["clients"].add(client)
        else:
            results[key[0]] = {"cat": cat, "key": (Config.NAMESPACE, Config.DOCUMENT_SET, result["id"])}
            if client != None:
                results[key[0]]["clients"] = {client}
    
    filtered_results = list(results.values())
    if len(filters) > 0:
        filtered_results = list(filter(lambda item: item["cat"] in filters_list, filtered_results))

    page_values = filtered_results[(page * pageSize): ((page + 1) * pageSize)]
    results_keys = [result["key"] for result in page_values]
    
    final_results = []
    batch_records = aerospike_client.batch_read(results_keys, ["title", "desc", "url", "cat"])
    for batch_record in batch_records.batch_records:
        if batch_record.result == 0:
            (_,_, bins) = batch_record.record
            key = bins.get("url").split("?client=")
            bins["url"] = key[0]
            clients = results[key[0]].get("clients")
            if clients:
                bins["clients"] = list(clients)
            final_results.append(bins)
    
    time_taken["total"] = (time.time() - start) * 1000
    count = len(filtered_results)

    return {
        "time": time_taken,
        "count": count,
        "categories": list(categories),
        "nPages": math.ceil(count/pageSize),
        "page": page,
        "results": final_results
    }