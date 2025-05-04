import os
import time
import logging
from pymongo import MongoClient
from datetime import datetime
import stanza
import re

# ----------------- Налаштування -----------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "kspd")
COLLECTION_NAME = "files"
LOGS_COLLECTION_NAME = "logs"

STOP_WORDS = set(['z:', 'kspd', 'нгву', 'а', 'по', 'в', '_'])

# ----------------- Лематизатор -----------------
print("📦 Завантаження моделі Stanza...")
stanza.download('uk')  # потрібно лише один раз
nlp = stanza.Pipeline(lang='uk', processors='tokenize,mwt,pos,lemma')

# ----------------- Логування -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("normalizer")

# ----------------- Функції -----------------
def clean_text(path: str) -> list[str]:
    return [
        word for word in re.split(r"[\\_\.\(\)\[\]\{\}\|,;\s]+", path.lower())
        if word and word not in STOP_WORDS
    ]

def lemmatize_words(words: list[str]) -> str:
    text = ' '.join(words)
    doc = nlp(text)
    lemmas = {
        word.lemma for sentence in doc.sentences for word in sentence.words
        if len(word.lemma) >= 3
    }
    return ' '.join(sorted(lemmas))

def normalize_documents(db):
    logger.info("🔍 Початок нормалізації...")
    collection = db[COLLECTION_NAME]
    cursor = collection.find({
        "path": {"$exists": True},
        "normalizedText": {"$exists": False}
    })

    processed = 0
    for doc in cursor:
        cleaned_words = clean_text(doc['path'])
        normalized = lemmatize_words(cleaned_words)

        logger.info("P: %s", doc['path'])
        logger.info("N: %s", normalized)

        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"normalizedText": normalized}}
        )
        processed += 1

    logger.info("✅ Опрацьовано документів: %d", processed)

def log_execution(db, start_time, end_time, status):
    logs_collection = db[LOGS_COLLECTION_NAME]
    logs_collection.insert_one({
        "type": "normalizer",
        "text": "Нормалізація тексту для пошуку",
        "startTime": datetime.fromtimestamp(start_time).isoformat(),
        "endTime": datetime.fromtimestamp(end_time).isoformat(),
        "status": status
    })

# ----------------- Основний процес -----------------
def main():
    start_time = time.time()
    status = "success"

    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]

        normalize_documents(db)

    except Exception as e:
        logger.error("❌ Error: %s", e)
        status = "error"
    finally:
        end_time = time.time()
        try:
            log_execution(db, start_time, end_time, status)
        except Exception as log_err:
            logger.error("❌ Logging error: %s", log_err)

        duration = int(end_time - start_time)
        logger.info("⏳ Час виконання: %dh %dm %ds",
                    duration // 3600, (duration % 3600) // 60, duration % 60)

if __name__ == "__main__":
    main()
