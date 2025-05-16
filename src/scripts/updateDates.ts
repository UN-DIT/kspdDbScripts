import { Db } from "mongodb";
import {runWithLogging} from "../utils/runWithLogging";
import {getMaxDepth} from "../utils/getMaxDepth";

async function updateFolderUpdatedDates(db: Db, depth = 0) {
    const collection = db.collection("files");

    try {
        console.log(`🔍 Step 1: Aggregating max(updated) per parentId...`);

        // 1. Групуємо дітей за parentId, беремо max(updated)
        const childrenUpdated = await collection.aggregate([
            {
                $match: {
                    updated: { $exists: true },
                    parentId: { $exists: true },
                    isEmpty: false,
                    isWarning: false,
                }
            },
            {
                $group: {
                    _id: "$parentId",
                    maxUpdated: { $max: "$updated" }
                }
            }
        ]).toArray();

        console.log(`📊 Found ${childrenUpdated.length} parents with updated children.`);

        // 2. Отримаємо id всіх тек на поточному depth
        const folders = await collection.find({
            type: "folder",
            depth
        }).project({ _id: 1, id: 1 }).toArray();

        const idToFolderMap = new Map(folders.map(f => [f.id, f._id]));

        // 3. Формуємо bulk update тільки для тих, хто є у мапі
        const bulkOps = childrenUpdated
            .filter(entry => idToFolderMap.has(entry._id))
            .map(entry => ({
                updateOne: {
                    filter: { _id: idToFolderMap.get(entry._id) },
                    update: { $set: { updated: entry.maxUpdated } }
                }
            }));

        console.log(`🛠 Preparing to update ${bulkOps.length} folders...`);

        if (bulkOps.length > 0) {
            const res = await collection.bulkWrite(bulkOps);
            console.log(`✅ Updated ${res.modifiedCount} folders.`);
        } else {
            console.log("ℹ️ Nothing to update.");
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

runWithLogging({
    script: {
        name: "️📆 DATEUPDATER",
        index: 6,
        version: "1.0",
        text: "Зміна дат оновлення для батьківських елементів"
    },
    run: async (db) => {
        let maxDepth = await getMaxDepth(db);

        for (let depth = maxDepth; depth >= 0; depth--) {
            console.log('🤿 Depth: ', depth)

            await updateFolderUpdatedDates(db, depth);
        }

        await countTotalEmpty(db)
    },
});
