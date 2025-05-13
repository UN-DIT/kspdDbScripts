import { configDotenv } from 'dotenv';
import { Db } from "mongodb";
import dbInit from "../dbInit";
import { sendMessageToTeams } from "./teamsSender";

configDotenv();

const { APP_VERSION } = process.env;
const SCRIPT_NAME = "🌳 ANCESTORIDSUPDATER";
const COLLECTION_NAME = "files";
const LOGS_COLLECTION_NAME = "logs";

async function getMaxDepth(db: Db): Promise<number> {
    const collection = db.collection(COLLECTION_NAME);
    const result = await collection.find({}, { projection: { depth: 1 } }).sort({ depth: -1 }).limit(1).toArray();
    return result[0]?.depth ?? 0;
}

async function updateAncestorIds(db: Db, depth: number) {
    const collection = db.collection(COLLECTION_NAME);

    console.log(`🔄 Processing depth ${depth}...`);
    const parents = await collection
        .find({ depth })
        .project({ id: 1, ancestorIds: 1 })
        .toArray();

    const parentMap = new Map<string, string[]>();
    parents.forEach(p => {
        parentMap.set(p.id, p.ancestorIds ?? []);
    });

    const children = await collection
        .find({ depth: depth + 1 })
        .project({ id: 1, parentId: 1 })
        .toArray();

    const bulkOps = children.map(child => {
        const parentAncestors = parentMap.get(child.parentId) ?? [];
        return {
            updateOne: {
                filter: { id: child.id },
                update: { $set: { ancestorIds: [...parentAncestors, child.parentId] } }
            }
        };
    });

    if (bulkOps.length > 0) {
        const result = await collection.bulkWrite(bulkOps);
        console.log(`✅ Updated ${result.modifiedCount} records at depth ${depth + 1}`);
    } else {
        console.log(`⚠️ No children found at depth ${depth + 1}`);
    }
}

const main = async () => {
    console.log(`${SCRIPT_NAME} v.${APP_VERSION}`);
    const startTime = Date.now();
    const [connect, disconnect] = await dbInit();
    let status = "success";

    try {
        const db = await connect();
        if (!db) return;

        const maxDepth = await getMaxDepth(db);
        for (let depth = 0; depth <= maxDepth; depth++) {
            await updateAncestorIds(db, depth);
        }
    } catch (err) {
        console.error("❌ Error:", err);
        status = "error";
    }

    const endTime = Date.now();
    const durationMs = endTime - startTime;
    const logEntry = {
        type: "ancestorUpdater",
        text: "Оновлення ancestorIds",
        startTime: new Date(startTime).toISOString(),
        endTime: new Date(endTime).toISOString(),
        status
    };

    try {
        const db = await connect();
        if (!db) return;

        await db.collection(LOGS_COLLECTION_NAME).insertOne(logEntry);
        await sendMessageToTeams(`Оновлення ancestorIds завершено: ${status}`);
    } catch (err) {
        console.error("❌ Error logging or sending message:", err);
    } finally {
        await disconnect();
        console.log("🔌 Disconnected from MongoDB");
    }

    const h = Math.floor(durationMs / 3600000);
    const m = Math.floor((durationMs % 3600000) / 60000);
    const s = Math.floor((durationMs % 60000) / 1000);
    console.log(`⏳ Execution time: ${h}h ${m}m ${s}s`);
};

main();
