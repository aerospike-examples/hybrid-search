from clients import vector_client, vector_admin, aerospike_client
from load import chunk_and_index_document, get_totals
from index.clean import remove_from_index
from index.vector import create_vector_index
from tqdm import tqdm

class DocsPipeline:
    def open_spider(self, spider):
        self.progress = None
        self.vector_client = vector_client
        self.aerospike_client = aerospike_client
        create_vector_index(vector_admin, logger=spider.logger)
        vector_admin.close()

    def close_spider(self, spider):
        remove_from_index(self.aerospike_client, self.vector_client, logger=spider.logger)
        get_totals(self.aerospike_client)
        self.progress.close()
        self.vector_client.close()
        self.aerospike_client.close()
        spider.logger.info("Crawling complete, content and embeddings loaded.")

    def process_item(self, item, spider):
        if self.progress is None:
            self.progress = tqdm(desc="Crawling site and generating embeddings...", total=spider.page_total)
        self.progress.update(1)
        if item.get("generated_idx") is None:
            chunk_and_index_document(self.aerospike_client, self.vector_client, item, logger=spider.logger)
        return
