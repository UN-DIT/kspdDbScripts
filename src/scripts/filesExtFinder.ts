import { Db } from "mongodb";
import {getMaxDepth} from "../utils/getMaxDepth";
import {runWithLogging} from "../utils/runWithLogging";
import {FILES_COLLECTION_NAME} from "../constants";

async function updateParentExtensions(db: Db, depth = 0) {
    try {
        const collection = db.collection(FILES_COLLECTION_NAME);

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

runWithLogging({
    script: {
        name: "️‍📂 FILESEXTFINDER",
        index: 7,
        version: "1.0",
        text: "Пошук розширень файлів"
    },
    run: async (db) => {
        let maxDepth = await getMaxDepth(db);

        for (let depth = maxDepth; depth >= 0; depth--) {
            console.log('🤿 Depth: ', depth)
            await updateParentExtensions(db, depth);
        }
    },
});
