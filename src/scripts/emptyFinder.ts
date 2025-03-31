import { configDotenv } from 'dotenv';
import { Db } from "mongodb";
import dbInit from "../dbInit";


configDotenv();

const { APP_VERSION } = process.env;
const SCRIPT_NAME = "🪫 EMPTYFINDER"
const COLLECTION_NAME = "files";

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

        const folders = await collection
            .aggregate([
                {
                    $match: {
                        type: "folder",
                        depth,
                        isEmpty: false,
                    },
                },
                {
                    $graphLookup: {
                        from: COLLECTION_NAME,
                        startWith: "$id",
                        connectFromField: "id",
                        connectToField: "parentId",
                        as: "descendants",
                        maxDepth: 1,
                    },
                },
                {
                    $match: {
                        "descendants": {
                            $not: {
                                $elemMatch: {
                                    $or: [{type: "file"}, {type: "folder", isEmpty: false}],
                                },
                            },
                        },
                    },
                },
            ])
            .toArray();

        console.log(`📁 Found ${folders.length} empty folders`);

        // 3. Update all empty folders
        if (folders.length > 0) {
            const folderIds = folders.map((folder) => folder._id);
            const updateResult = await collection.updateMany(
                {_id: {$in: folderIds}, depth},
                {$set: {isEmpty: true}}
            );
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
        console.error("❌ Error:", error);
    } finally {
        await disconnect()
        console.log("🔌 Disconnected from MongoDB");
    }


    const endTime = Date.now(); // Час після завершення операції
    const durationMs = endTime - startTime; // Загальний час у мілісекундах

    // Розрахунок годин, хвилин, секунд
    const hours = Math.floor(durationMs / 3600000);
    const minutes = Math.floor((durationMs % 3600000) / 60000);
    const seconds = Math.floor((durationMs % 60000) / 1000);

    console.log(`⏳ Execution time: ${hours}h ${minutes}m ${seconds}s`);
}

// Run the script
main()