import os
import time
import logging
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import re
import pymorphy3

# ----------------- Налаштування -----------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "kspd")
COLLECTION_NAME = "files"
LOGS_COLLECTION_NAME = "logs"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
LIMIT = int(os.getenv("LIMIT", 0))

STOP_WORDS = set(['z:', 'kspd', 'нгву', 'а', 'по', 'в', '_'])

# ----------------- Лематизатор -----------------
morph = pymorphy3.MorphAnalyzer(lang='uk')

# ----------------- Логування -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("normalizer")

# ----------------- Функції -----------------
def clean_text(path: str) -> list[str]:
    return [
        word for word in re.split(r"[\\_\.\(\)\[\]\{\}\|,;\s]+", path.lower())
        if word and word not in STOP_WORDS
    ]

def to_nominative_singular(word: str) -> str:
    parsed = morph.parse(word)
    for p in parsed:
        if 'NOUN' in p.tag and 'nomn' in p.tag and 'sing' in p.tag:
            inflected = p.inflect({'nomn', 'sing'})
            if inflected:
                return inflected.word
    return parsed[0].normal_form

def lemmatize_and_normalize(text: str) -> list[str]:
    words = re.findall(r'\b\w+\b', text.lower())
    normalized_set = {
        to_nominative_singular(w)
        for w in words
        if len(w) >= 2 and w not in STOP_WORDS
    }
    return sorted(normalized_set)  # список лем

def normalize_documents(db):
    logger.info("🔍 Початок нормалізації...")
    collection = db[COLLECTION_NAME]

    cursor = collection.find(
        {"path": {"$exists": True}, "lemmas": {"$exists": False}},
        no_cursor_timeout=True,
        batch_size=BATCH_SIZE
    )
    if LIMIT:
        cursor = cursor.limit(LIMIT)

    ops = []
    processed = 0

    for doc in cursor:
        cleaned_words = clean_text(doc['path'])
        lemmas = lemmatize_and_normalize(' '.join(cleaned_words))

        ops.append(UpdateOne(
            {"_id": doc["_id"]},
            {"$set": {"lemmas": lemmas}}
        ))
        processed += 1

        if len(ops) >= BATCH_SIZE:
            collection.bulk_write(ops, ordered=False)
            logger.info("📄 Оброблено документів: %d", processed)
            ops.clear()

    if ops:
        collection.bulk_write(ops, ordered=False)
        logger.info("📄 Завершальні документи: %d", processed)

    logger.info("✅ Всього опрацьовано документів: %d", processed)

def log_execution(db, start_time, end_time, status):
    try:
        logs_collection = db[LOGS_COLLECTION_NAME]
        logs_collection.insert_one({
            "type": "normalizer",
            "text": "Нормалізація тексту для пошуку (lemmas)",
            "startTime": datetime.fromtimestamp(start_time).isoformat(),
            "endTime": datetime.fromtimestamp(end_time).isoformat(),
            "status": status
        })
    except Exception as log_err:
        logger.error("❌ Logging error: %s", log_err)

# ----------------- Основний процес -----------------
def main():
    start_time = time.time()
    status = "success"
    db = None

    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        normalize_documents(db)
    except Exception as e:
        logger.error("❌ Error: %s", e)
        status = "error"
    finally:
        end_time = time.time()
        if db is not None:
            log_execution(db, start_time, end_time, status)

        duration = int(end_time - start_time)
        logger.info("⏳ Час виконання: %dh %dm %ds",
                    duration // 3600, (duration % 3600) // 60, duration % 60)

if __name__ == "__main__":
    main()