import {Db} from "mongodb";
import {FILES_COLLECTION_NAME} from "../constants";

export const getMaxDepth = async (db: Db): Promise<number> => {
    const collection = db.collection(FILES_COLLECTION_NAME);
    const result = await collection.find({}, { projection: { depth: 1 } }).sort({ depth: -1 }).limit(1).toArray();

    return result[0]?.depth ?? 0;
}