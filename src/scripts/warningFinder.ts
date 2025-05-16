import { Db } from "mongodb";
import {runWithLogging} from "../utils/runWithLogging";
import {FILES_COLLECTION_NAME} from "../constants";

async function markWarningByExt(db: Db) {
    try {
        const collection = db.collection(FILES_COLLECTION_NAME);

        console.log(`📝 Mark possibly warning files by extension (lnk, crdownload)`);
        const result = await collection.updateMany(
            { ext: { $in: ["lnk", "crdownload"] } },
            { $set: { isWarning: true } }
        );
        console.log(`✅ Updated ${result.modifiedCount} documents.`);
    } catch (error) {
        console.error("❌ Error marking possibly warning fields:", error);
    }
}

async function markWarningByFileName(db: Db) {
    try {
        const collection = db.collection(FILES_COLLECTION_NAME);

        console.log(`📝 Mark possibly warning files by filename (thumbs.db)`);
        const result = await collection.updateMany(
            { name: { $regex: /^thumbs\.db$/i } }, // Регулярний вираз для нечутливого до регістру пошуку
            { $set: { isWarning: true } }
        );
        console.log(`✅ Updated ${result.modifiedCount} documents.`);
    } catch (error) {
        console.error("❌ Error marking possibly warning fields:", error);
    }
}

runWithLogging({
    script: {
        name: "️⚠️ WARNINGFINDER",
        index: 4,
        version: "1.0",
        text: "Пошук сміття"
    },
    run: async (db) => {
        await markWarningByExt(db);
        await markWarningByFileName(db);
    },
});
