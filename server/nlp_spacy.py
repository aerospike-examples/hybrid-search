import spacy

nlp = spacy.load("en_core_web_sm")

def get_tokens(texts: list[str]):
    docs = []
    for doc in nlp.pipe(texts):
        tokens = []
        for token in doc:
            if (token.is_alpha and not token.is_stop):
                tokens.append(token.lemma_.lower())
        docs.append(tokens)    
    return docs