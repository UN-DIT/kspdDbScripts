import { configDotenv } from 'dotenv';
import { Db } from "mongodb";
import dbInit from "../dbInit";
import {sendMessageToTeams} from "./teamsSender";
import axios from "axios";


configDotenv();

const { APP_VERSION } = process.env;
const SCRIPT_NAME = "🈲 NORMALIZER"
const COLLECTION_NAME = "files";
const LOGS_COLLECTION_NAME = "logs";
const normalizerUrl = 'http://localhost:5001/lemmatize';

const stopWords = new Set(['z:', 'kspd', 'нгву', 'а', 'по', 'в', '_']);

const cleanText = (path: string): string[] => {
    return path
        .toLowerCase()
        .split(/[\\_\.\(\)\[\]\{\},;\s]+/)  // розбиває за всіма переліченими роздільниками
        .filter(word => word.length > 0 && !stopWords.has(word));
};

const normalizeText = async (words: string[]) => {
    try {
        const response = await axios.post(normalizerUrl, {
            text: words.join(' ')
        });
        return response.data.lemmatized || '';
    } catch (error) {
        console.error('Error normalizing:', error);
        return '';
    }
}

async function normalize(db: Db) {
    console.log("Normalizing database...");

    try {
        const collection = db.collection(COLLECTION_NAME);
        const cursor = collection.find({
            path: { $exists: true },
            normalizedText: { $exists: false }
        });

        for await (const doc of cursor) {
            const cleanedWords = cleanText(doc.path);
            const normText = await normalizeText(cleanedWords);

            console.log("P: ", doc.path);
            console.log("N: ", normText)

            await collection.updateOne(
                { _id: doc._id },
                { $set: { normalizedText: normText } }
            );
        }
    } catch (error) {
        console.error("❌ Error normalizing text", error);
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

        await normalize(db);
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
            type: "normalizer",
            text: "Нормалізація тексту для пошуку",
            startTime: new Date(startTime).toISOString(),
            endTime: new Date(endTime).toISOString(),
            status
        });
        await sendMessageToTeams(`Нормалізація тексту для пошуку - ${status}`);
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