import { configDotenv } from 'dotenv';
import { Db } from "mongodb";
import dbInit from "../dbInit";


configDotenv();

const { APP_VERSION } = process.env;
const SCRIPT_NAME = "🦖 RESET"
const COLLECTION_NAME = "files";

async function resetIsEmptyField(db: Db) {
    try {
        const collection = db.collection(COLLECTION_NAME);

        console.log(`🌾 Reset isEmpty field`);
        const result = await collection.updateMany({}, { $set: { isEmpty: false } });
        console.log(`✅ Updated ${result.modifiedCount} documents.`);
    } catch (error) {
        console.error("❌ Error updating isEmpty field:", error);
    }
}

async function resetSubjectsField(db: Db) {
    try {
        const collection = db.collection(COLLECTION_NAME);

        console.log(`🌾 Reset subjects field`);
        const result = await collection.updateMany({}, { $set: { subjects: [] } });
        console.log(`✅ Updated ${result.modifiedCount} documents.`);
    } catch (error) {
        console.error("❌ Error updating subjects field:", error);
    }
}

const main = async () => {
    console.log(`${SCRIPT_NAME} v.${APP_VERSION}`)

    const startTime = Date.now(); // Початковий час
    const [connect, disconnect] = await dbInit()

    try {
        const db = await connect();

        if (!db) {
            return
        }

        await resetIsEmptyField(db);
        await resetSubjectsField(db);
    } catch (error) {
        console.error("❌ Error:", error);
    } finally {
        await disconnect()
        console.log("🔌 Disconnected from MongoDB");
    }

    const endTime = Date.now(); // Час після завершення операції
    const durationMs = endTime - startTime; // Загальний час у мілісекундах

    // Розрахунок годин, хвилин, секунд
    const hours = Math.floor(durationMs / 3600000);
    const minutes = Math.floor((durationMs % 3600000) / 60000);
    const seconds = Math.floor((durationMs % 60000) / 1000);

    console.log(`⏳ Execution time: ${hours}h ${minutes}m ${seconds}s`);
}

// Run the script
main()