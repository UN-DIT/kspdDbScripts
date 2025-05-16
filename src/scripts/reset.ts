import {Db} from "mongodb";
import {runWithLogging} from "../utils/runWithLogging";
import {FILES_COLLECTION_NAME} from "../constants";

async function resetIsEmptyField(db: Db) {
    try {
        const collection = db.collection(FILES_COLLECTION_NAME);

        console.log(`🌾 Reset fields`);
        const result = await collection.updateMany({}, {
            $set: {
                isEmpty: false,
                subjects: [],
                isWarning: false,
                filesExt: [],
                isChecked: true,
                lemmas: [],
                ancestorIds: []
            }
        });
        console.log(`✅ Updated ${result.modifiedCount} documents.`);
    } catch (error) {
        console.error("❌ Error reset fields:", error);
    }
}

runWithLogging({
    script: {
        name: "️‍🦖 RESET",
        index: 3,
        version: "1.0",
        text: "Скидання інформації"
    },
    run: async (db) => {
        await resetIsEmptyField(db);
    },
});
