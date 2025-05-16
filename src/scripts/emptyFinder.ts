import { Db } from "mongodb";
import {runWithLogging} from "../utils/runWithLogging";
import {getMaxDepth} from "../utils/getMaxDepth";
import {FILES_COLLECTION_NAME} from "../constants";

async function checkOneDepth(db: Db, depth = 0) {
    try {
        const collection = db.collection(FILES_COLLECTION_NAME);

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
                $or: [{ type: "file" }, { type: "folder", isEmpty: false }],
                isWarning: false,
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

runWithLogging({
    script: {
        name: "🪫 EMPTYFINDER",
        index: 5,
        version: "1.0",
        text: "Пошук порожніх папок"
    },
    run: async (db) => {
        let maxDepth = await getMaxDepth(db);

        for (let depth = maxDepth; depth >= 0; depth--) {
            console.log('🤿 Depth: ', depth)
            await checkOneDepth(db, depth);
        }

        await countTotalEmpty(db)
    },
});
