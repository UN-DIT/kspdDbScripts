import {Db, ObjectId} from "mongodb";
import {LOGS_COLLECTION_NAME} from "../constants";

export async function createInitialLog(
    db: Db,
    type: string,
    text: string,
    startTime: number
): Promise<ObjectId> {
    const result = await db.collection(LOGS_COLLECTION_NAME).insertOne({
        type,
        text,
        startTime: new Date(startTime).toISOString(),
        status: "progress"
    });

    return result.insertedId;
}

export async function finalizeLog(
    db: Db,
    logId: ObjectId,
    endTime: number,
    status: "success" | "error"
) {
    await db.collection(LOGS_COLLECTION_NAME).updateOne(
        { _id: logId },
        {
            $set: {
                endTime: new Date(endTime).toISOString(),
                status
            }
        }
    );
}