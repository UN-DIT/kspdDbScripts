import { Db } from "mongodb";
import {runWithLogging} from "../utils/runWithLogging";
import {FILES_COLLECTION_NAME, STRUCTURE_COLLECTION_NAME} from "../constants";

async function getSubjects(db: Db) {
    try {
        const collection = db.collection(STRUCTURE_COLLECTION_NAME);
        const result = await collection.find({ fileMask: { $ne: "" } }).toArray();

        console.log(`👀 found ${result.length} not empty subjects.`);

        return result;
    } catch (error) {
        console.error("❌ Error finding subjects:", error);
    }
}

runWithLogging({
    script: {
        name: "️‍⛩️ SUBJECTMATCHER",
        index: 8,
        version: "1.0",
        text: "Маркування категорій"
    },
    run: async (db) => {
        const collection = db.collection(FILES_COLLECTION_NAME);
        const subjects = await getSubjects(db) || [];

        for (const subject of subjects) {
            console.log(`🗼 Searching for ${subject.path}...`)

            const result = await collection.updateMany(
                { path: { $regex: subject.fileMask, $options: "i" } },
                { $push: { subjects: subject.id } }
            );

            console.log(`✅ Updated ${result.modifiedCount} documents`);
        }
    },
});
