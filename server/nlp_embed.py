from utils import EmbedTask
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("nomic-ai/nomic-embed-text-v1.5", trust_remote_code=True)
MODEL_DIM = 768

def get_embedding(sentence: str, task: EmbedTask):
    embeddings = model.encode([f"{task}: {sentence}"])
    return embeddings[0].tolist()
