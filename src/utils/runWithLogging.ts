import { configDotenv } from 'dotenv';
import {Db, ObjectId} from "mongodb";
import dbInit from "../db/dbInit";
import {createInitialLog, finalizeLog} from "./logToMongo";
import {sendMessageToTeams} from "./teamsSender";
import {logExecutionTime} from "./logExecutionTime";

configDotenv();

const { APP_VERSION, TOTAL_SCRIPTS_COUNT } = process.env;

interface ScriptConfig {
    script: {
        index: number;
        name: string,
        version: string,
        text: string
    };
    run: (db: Db) => Promise<void>;
}

export async function runWithLogging(config: ScriptConfig) {
    const { script, run } = config;

    console.log();
    console.log(`${script.index}/${TOTAL_SCRIPTS_COUNT} ${script.name} v.${APP_VERSION}`);

    const startTime = Date.now();
    let endTime = Date.now();

    const [connect, disconnect] = await dbInit();
    let status: "success" | "error" = "success";
    let logId: ObjectId | null = null;

    try {
        const db = await connect();
        if (!db) return;

        // 🔹 Створюємо запис зі статусом "progress"
        logId = await createInitialLog(db, `${script.index}/${TOTAL_SCRIPTS_COUNT} ${script.name}`, script.text, startTime);

        await run(db);
        endTime = Date.now();

        // 🔹 Оновлюємо запис — встановлюємо endTime та статус
        await finalizeLog(db, logId, endTime, status);

        await sendMessageToTeams(`${script.index}/${TOTAL_SCRIPTS_COUNT} ${script.name} | ${script.text} | ${status}`);
    } catch (err) {
        console.error("❌ Error:", err);
        status = "error";
        endTime = Date.now();

        const db = await connect();

        if (logId && db) {
            await finalizeLog(db, logId, endTime, status);
        }
    } finally {
        await disconnect();
        console.log("🔌 Disconnected from MongoDB");
    }

    logExecutionTime(startTime, endTime);
}