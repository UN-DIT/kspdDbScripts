import {configDotenv} from 'dotenv';
import {Db} from "mongodb";
import dbInit from "../dbInit";
import {sendMessageToTeams} from "./teamsSender";


configDotenv();

const {APP_VERSION} = process.env;
const SCRIPT_NAME = "👯 COMPARER"
const FILES_COLLECTION_NAME = "files";
const TEMP_COLLECTION_NAME = "tmp";
const LOGS_COLLECTION_NAME = "logs";

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
        const tempCursorNew = tempCollection.find({}, {projection: {_id: 0}});

        for await (const tempDoc of tempCursorNew) {
            if (!existingIds.has(tempDoc.id)) {
                newBulkOps.push({insertOne: {document: tempDoc}});
            }

            if (newBulkOps.length >= 10000) {
                await filesCollection.bulkWrite(newBulkOps);
                newBulkOps.length = 0;
            }
        }

        if (newBulkOps.length > 0) {
            await filesCollection.bulkWrite(newBulkOps);
        }
        console.log(`✅ Added new documents.`);

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

const main = async () => {
    console.log(`${SCRIPT_NAME} v.${APP_VERSION}`)

    const startTime = Date.now(); // Початковий час
    const [connect, disconnect] = await dbInit()
    let status = "success"

    try {
        const db = await connect();

        if (!db) {
            return
        }

        await markAsNotChecked(db);
        await compareTables(db);
        await clearNotChecked(db);

    } catch (error) {
        status = "error";
        console.error("❌ Error:", error);
    }

    const endTime = Date.now(); // Час після завершення операції
    const durationMs = endTime - startTime; // Загальний час у мілісекундах

    try {
        const db = await connect();

        if (!db) {
            return
        }

        const logsCollection = db.collection(LOGS_COLLECTION_NAME);
        await logsCollection.insertOne({
            type: "compare",
            text: "Оновлення даних",
            startTime: new Date(startTime).toISOString(),
            endTime: new Date(endTime).toISOString(),
            status
        });
        await sendMessageToTeams(`Оновлення даних - ${status}`);
    } catch (error) {
        console.error("❌ Error:", error);
    } finally {
        await disconnect()
        console.log("🔌 Disconnected from MongoDB");
    }

    // Розрахунок годин, хвилин, секунд
    const hours = Math.floor(durationMs / 3600000);
    const minutes = Math.floor((durationMs % 3600000) / 60000);
    const seconds = Math.floor((durationMs % 60000) / 1000);

    console.log(`⏳ Execution time: ${hours}h ${minutes}m ${seconds}s`);
}

// Run the script
main()