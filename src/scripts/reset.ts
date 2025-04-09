import {configDotenv} from 'dotenv';
import {Db} from "mongodb";
import dbInit from "../dbInit";
import {sendMessageToTeams} from "./teamsSender";


configDotenv();

const {APP_VERSION} = process.env;
const SCRIPT_NAME = "🦖 RESET"
const COLLECTION_NAME = "files";
const LOGS_COLLECTION_NAME = "logs";

async function resetIsEmptyField(db: Db) {
    try {
        const collection = db.collection(COLLECTION_NAME);

        console.log(`🌾 Reset fields`);
        const result = await collection.updateMany({}, {
            $set: {
                isEmpty: false,
                subjects: [],
                isWarning: false,
                filesExt: [],
                isChecked: true
            }
        });
        console.log(`✅ Updated ${result.modifiedCount} documents.`);
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

        await resetIsEmptyField(db);
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
            type: "reset",
            text: "Скидання пошукової інформації",
            startTime: new Date(startTime).toISOString(),
            endTime: new Date(endTime).toISOString(),
            status
        });
        await sendMessageToTeams(`Скидання пошукової інформації - ${status}`);
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