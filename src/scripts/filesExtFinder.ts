import { configDotenv } from 'dotenv';
import { Db } from "mongodb";
import dbInit from "../dbInit"

configDotenv();

const { APP_VERSION } = process.env;
const SCRIPT_NAME = "🕵️‍♀️ FILESEXTFINDER"
const COLLECTION_NAME = "files";
const LOGS_COLLECTION_NAME = "logs";

async function getMaxDepth(db: Db) {
    try {
        const collection = db.collection(COLLECTION_NAME);

        const maxDepthDoc = await collection
            .find({}, { projection: { depth: 1, _id: 0 } })
            .sort({ depth: -1 })
            .limit(1)
            .toArray();

        if (maxDepthDoc.length > 0) {
            console.log("🤿 Max depth:", maxDepthDoc[0].depth);
            return maxDepthDoc[0].depth;
        } else {
            console.log("0️⃣ No documents found");
            return null;
        }
    } catch (error) {
        console.error("❌ Error fetching max depth:", error);
    }
}

async function updateParentExtensions(db: Db, depth = 0) {
    try {
        const collection = db.collection("files");

        console.log(`🔎 Processing depth ${depth}...`);

        // 1. Знайти всі файли та папки глибини `depth + 1`
        const children = await collection
            .aggregate([
                {
                    $match: { depth: depth + 1 }
                },
                {
                    $group: {
                        _id: "$parentId",
                        fileExts: {
                            $addToSet: { $cond: [{ $eq: ["$type", "file"] }, "$ext", "$$REMOVE"] }
                        },
                        folderExts: {
                            $addToSet: { $cond: [{ $eq: ["$type", "folder"] }, "$filesExt", "$$REMOVE"] }
                        }
                    }
                },
                {
                    $project: {
                        _id: 1,
                        filesExt: { $setUnion: ["$fileExts", { $reduce: { input: "$folderExts", initialValue: [], in: { $setUnion: ["$$value", "$$this"] } } }] }
                    }
                }
            ])
            .toArray();

        if (children.length === 0) {
            console.log("✅ No updates needed.");
            return;
        }

        console.log(`📂 Updating ${children.length} parent folders with extensions...`);

        // 2. Масове оновлення батьківських папок
        const bulkOps = children.map((doc) => ({
            updateOne: {
                filter: { id: doc._id },
                update: { $set: { filesExt: doc.filesExt } }
            }
        }));

        if (bulkOps.length > 0) {
            await collection.bulkWrite(bulkOps);
            console.log(`✅ Updated ${bulkOps.length} folders.`);
        }
    } catch (error) {
        console.error("❌ Error:", error);
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

        //await setExtension(db);

        let maxDepth = await getMaxDepth(db);

        for (let depth = maxDepth; depth >= 0; depth--) {
            console.log('🤿 Depth: ', depth)
            await updateParentExtensions(db, depth);
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
            type: "filesExtFinder",
            text: "Пошук розширень файлів",
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