import {Db} from "mongodb";
import {runWithLogging} from "../utils/runWithLogging";
import {FILES_COLLECTION_NAME, TEMP_COLLECTION_NAME} from "../constants";

async function markAsNotChecked(db: Db) {
    try {
        const filesCollection = db.collection(FILES_COLLECTION_NAME);
        const tempCollection = db.collection(TEMP_COLLECTION_NAME);

        console.log(`🌾 Mark as not checked`);

        const result = await filesCollection.updateMany({}, {
            $set: {
                isChecked: false
            }
        });

        await tempCollection.updateMany({}, {
            $set: {
                isChecked: true
            }
        });

        console.log(`✅ Updated ${result.modifiedCount} documents.`);
    } catch (error) {
        console.error("❌ Error reset fields:", error);
    }
}

async function compareTables(db: Db) {
    try {
        const filesCollection = db.collection(FILES_COLLECTION_NAME);
        const tempCollection = db.collection(TEMP_COLLECTION_NAME);

        console.log(`🌾 Find the same (stream mode)`);

        // 1️⃣ Оновлення isChecked (без distinct)
        const bulkOps = [];
        const tempCursor = tempCollection.find({}, {projection: {id: 1}});

        for await (const doc of tempCursor) {
            bulkOps.push({
                updateOne: {
                    filter: {id: doc.id},
                    update: {$set: {isChecked: true}}
                }
            });

            if (bulkOps.length >= 10000) {  // Виконуємо запит батчами
                await filesCollection.bulkWrite(bulkOps);
                bulkOps.length = 0;  // Очистка буфера
            }
        }

        if (bulkOps.length > 0) {
            await filesCollection.bulkWrite(bulkOps);
        }

        console.log(`✅ Updated documents.`);
        console.log(`🌾 Add new (stream mode)`);

        // 2️⃣ Додавання нових записів (без distinct)
        const existingIds = new Set();
        const filesCursor = filesCollection.find({}, {projection: {id: 1}});

        for await (const file of filesCursor) {
            existingIds.add(file.id);
        }

        const newBulkOps = [];
        let insertedCount = 0; // 👈 Лічильник нових документів
        const tempCursorNew = tempCollection.find({}, {projection: {_id: 0}});

        for await (const tempDoc of tempCursorNew) {
            if (!existingIds.has(tempDoc.id)) {
                newBulkOps.push({insertOne: {document: tempDoc}});
                insertedCount++; // 👈 Збільшення лічильника
            }

            if (newBulkOps.length >= 10000) {
                await filesCollection.bulkWrite(newBulkOps);
                newBulkOps.length = 0;
            }
        }

        if (newBulkOps.length > 0) {
            await filesCollection.bulkWrite(newBulkOps);
        }

        console.log(`✅ Added ${insertedCount} new documents.`);
    } catch (error) {
        console.error("❌ Error setting files extension:", error);
    }
}

async function clearNotChecked(db: Db) {
    try {
        const collection = db.collection(FILES_COLLECTION_NAME);

        console.log(`🧽 Clear not checked`);

        const result = await collection.deleteMany({isChecked: false});

        console.log(`✅ Deleted ${result.deletedCount} documents.`);
    } catch (error) {
        console.error("❌ Error reset fields:", error);
    }
}

runWithLogging({
    script: {
        name: "🐣 COMPARER",
        index: 2,
        version: "1.0",
        text: "Оновлення даних"
    },
    run: async (db) => {
        await markAsNotChecked(db);
        await compareTables(db);
        await clearNotChecked(db);
    },
});
