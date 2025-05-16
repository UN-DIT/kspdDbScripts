import { Db } from "mongodb";
import {FILES_COLLECTION_NAME} from "../constants";
import {runWithLogging} from "../utils/runWithLogging";
import {getMaxDepth} from "../utils/getMaxDepth";

async function updateAncestorIds(db: Db, depth: number) {
    const collection = db.collection(FILES_COLLECTION_NAME);

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

runWithLogging({
    script: {
        name: "🌳 ANCESTORIDSUPDATER",
        index: 9,
        version: "1.0",
        text: "Оновлення ancestorIds"
    },
    run: async (db) => {
        const maxDepth = await getMaxDepth(db);

        for (let depth = 0; depth <= maxDepth; depth++) {
            await updateAncestorIds(db, depth);
        }
    },
});
