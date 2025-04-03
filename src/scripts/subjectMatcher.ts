import { configDotenv } from 'dotenv';
import { Db } from "mongodb";
import dbInit from "../dbInit";


configDotenv();

const { APP_VERSION } = process.env;
const SCRIPT_NAME = "⛩️ STRUCTUREMATCHER"
const STRUCTURE_COLLECTION_NAME = "data_structure";
const FILES_COLLECTION_NAME = "files";
const LOGS_COLLECTION_NAME = "logs";

async function getSubjects(db: Db) {
    try {
        const collection = db.collection(STRUCTURE_COLLECTION_NAME);
        const result = await collection.find({ fileMask: { $ne: "" } }).toArray();

        console.log(`👀 found ${result.length} not empty subjects.`);

        return result;
    } catch (error) {
        console.error("❌ Error finding subjects:", error);
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

        const collection = db.collection(FILES_COLLECTION_NAME);
        const subjects = await getSubjects(db) || [];

        for (const subject of subjects) {
            console.log(`🗼 Searching for ${subject.path}...`)

            const result = await collection.updateMany(
                { path: { $regex: subject.fileMask, $options: "i" } },
                { $push: { subjects: subject.id } }
            );

            console.log(`✅ Updated ${result.modifiedCount} documents`);
        }
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
            type: "subjectMatcher",
            text: "Маркування категорій",
            startTime: new Date(startTime).toISOString(),
            endTime: new Date(endTime).toISOString(),
            status
        });
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