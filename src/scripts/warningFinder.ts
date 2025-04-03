import { configDotenv } from 'dotenv';
import { Db } from "mongodb";
import dbInit from "../dbInit";
import {sendMessageToTeams} from "./teamsSender";


configDotenv();

const { APP_VERSION } = process.env;
const SCRIPT_NAME = "⚠️ WARNINGFINDER"
const COLLECTION_NAME = "files";
const LOGS_COLLECTION_NAME = "logs";

async function setExtension(db: Db) {
    try {
        const collection = db.collection(COLLECTION_NAME);

        console.log(`🗂️ Set files extension`);
        const result = await collection.updateMany(
            { path: { $exists: true }, type: "file" },
            [
                {
                    $set: {
                        ext: {
                            $toLower: {
                                $arrayElemAt: [{ $split: ["$path", "."] }, -1]
                            }
                        }
                    }
                },
                {
                    $set: {
                        ext: {
                            $cond: {
                                if: { $lte: [{ $strLenCP: "$ext" }, 10] }, // Перевіряємо довжину ext
                                then: "$ext",
                                else: "$$REMOVE" // Видаляємо поле, якщо довжина більше 10
                            }
                        }
                    }
                }
            ]
        );
        console.log(`✅ Updated ${result.modifiedCount} documents.`);
    } catch (error) {
        console.error("❌ Error setting files extension:", error);
    }
}

async function markWarningByExt(db: Db) {
    try {
        const collection = db.collection(COLLECTION_NAME);

        console.log(`📝 Mark possibly warning files by extension (lnk, crdownload)`);
        const result = await collection.updateMany(
            { ext: { $in: ["lnk", "crdownload"] } },
            { $set: { isWarning: true } }
        );
        console.log(`✅ Updated ${result.modifiedCount} documents.`);
    } catch (error) {
        console.error("❌ Error marking possibly warning fields:", error);
    }
}

async function markWarningByFileName(db: Db) {
    try {
        const collection = db.collection(COLLECTION_NAME);

        console.log(`📝 Mark possibly warning files by filename (thumbs.db)`);
        const result = await collection.updateMany(
            { name: { $regex: /^thumbs\.db$/i } }, // Регулярний вираз для нечутливого до регістру пошуку
            { $set: { isWarning: true } }
        );
        console.log(`✅ Updated ${result.modifiedCount} documents.`);
    } catch (error) {
        console.error("❌ Error marking possibly warning fields:", error);
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

        await setExtension(db);
        await markWarningByExt(db);
        await markWarningByFileName(db);
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
            type: "warningFinder",
            text: "Пошук сміття",
            startTime: new Date(startTime).toISOString(),
            endTime: new Date(endTime).toISOString(),
            status
        });
        await sendMessageToTeams(`Пошук сміття - ${status}`);
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