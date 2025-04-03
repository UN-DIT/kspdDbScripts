import { configDotenv } from 'dotenv';
import { Db } from "mongodb";
import dbInit from "../dbInit";
import {sendMessageToTeams} from "./teamsSender";


configDotenv();

const { APP_VERSION } = process.env;
const SCRIPT_NAME = "🪫 EMPTYFINDER"
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

async function checkOneDepth(db: Db, depth = 0) {
    try {
        const collection = db.collection("files");

        console.log(`🔎 Checking folders at depth ${depth}...`);

        // Step 1: Find candidate folders
        const folders = await collection
            .find({
                type: "folder",
                depth,
                isEmpty: false
            })
            .project({ _id: 1, id: 1 }) // Fetch only necessary fields
            .toArray();

        if (folders.length === 0) {
            console.log("✅ No folders to check.");
            return;
        }

        console.log(`📁 Found ${folders.length} folders to check`);

        // Step 2: Find if they contain files or non-empty folders
        const folderIds = folders.map((folder) => folder.id);
        const hasChildren = await collection
            .find({
                parentId: { $in: folderIds },
                $or: [{ type: "file" }, { type: "folder", isEmpty: false }]
            })
            .project({ parentId: 1 })
            .toArray();

        // Step 3: Identify empty folders (those without children)
        const nonEmptyFolders = new Set(hasChildren.map((child) => child.parentId));
        const emptyFolders = folders.filter((folder) => !nonEmptyFolders.has(folder.id));

        console.log(`📂 Marking ${emptyFolders.length} folders as empty`);

        // Step 4: Bulk update empty folders
        if (emptyFolders.length > 0) {
            const folderIdsToUpdate = emptyFolders.map((folder) => folder._id);

            await collection.updateMany(
                { _id: { $in: folderIdsToUpdate } },
                { $set: { isEmpty: true } }
            );

            console.log(`✅ Updated ${folderIdsToUpdate.length} folders.`);
        }
    } catch (error) {
        console.error("❌ Error:", error);
    }
}

async function countTotalEmpty(db: Db) {
    try {
        const collection = db.collection("files");
        const emptyCount = await collection.countDocuments({isEmpty: true});
        console.log(`🟰 Total empty folders: ${emptyCount}`);
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

        let maxDepth = await getMaxDepth(db);

        for (let depth = maxDepth; depth >= 0; depth--) {
            console.log('🤿 Depth: ', depth)
            await checkOneDepth(db, depth);
        }

        await countTotalEmpty(db)
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
            type: "emptyFinder",
            text: "Пошук порожніх папок",
            startTime: new Date(startTime).toISOString(),
            endTime: new Date(endTime).toISOString(),
            status
        });
        await sendMessageToTeams(`Пошук порожніх папок - ${status}`);
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