import { createReadStream } from 'fs';
import { Db } from 'mongodb';
import {PathLike} from "node:fs";
import {runWithLogging} from "../utils/runWithLogging";
import {TEMP_COLLECTION_NAME} from "../constants";
const { parser } = require("stream-json");
const { streamArray } = require('stream-json/streamers/StreamArray');
const { chain } = require('stream-chain');

const { JSON_FILE_PATH } = process.env;

if (!JSON_FILE_PATH) {
    throw new Error("❌ Missing required env variable: JSON_FILE_PATH");
}

const BATCH_SIZE = 1000;

async function clearTmpCollection(db: Db) {
    try {
        const collection = db.collection(TEMP_COLLECTION_NAME);
        console.log(`🧽 Clear collection '${TEMP_COLLECTION_NAME}'`);

        const result = await collection.deleteMany({});
        console.log(`✅ Deleted ${result.deletedCount} documents.`);
    } catch (error) {
        console.error("❌ Error clearing tmp:", error);
        throw error;
    }
}

function normalizeDates(doc: any) {
    if (doc.created instanceof Date) {
        doc.created = doc.created.toISOString();
    } else if (doc.created && typeof doc.created === 'object' && '$date' in doc.created) {
        doc.created = new Date(doc.created.$date).toISOString();
    }

    if (doc.updated instanceof Date) {
        doc.updated = doc.updated.toISOString();
    } else if (doc.updated && typeof doc.updated === 'object' && '$date' in doc.updated) {
        doc.updated = new Date(doc.updated.$date).toISOString();
    }

    return doc;
}

async function insertTmpFromFile(db: Db) {
    const tempCollection = db.collection(TEMP_COLLECTION_NAME);
    const pipeline = chain([
        createReadStream(JSON_FILE_PATH as PathLike),
        parser(),
        streamArray()
    ]);

    let batch: any[] = [];
    let insertedCount = 0;

    console.log(`📥 Streaming and inserting into '${TEMP_COLLECTION_NAME}'...`);

    return new Promise<void>((resolve, reject) => {
        // @ts-ignore
        pipeline.on('data', async ({ value }) => {
            batch.push(normalizeDates(value));
            if (batch.length >= BATCH_SIZE) {
                pipeline.pause();
                try {
                    await tempCollection.insertMany(batch);
                    insertedCount += batch.length;
                    console.log(`🔹 Inserted ${insertedCount} documents...`);
                    batch = [];
                    pipeline.resume();
                } catch (err) {
                    console.error("❌ Error inserting batch:", err);
                    reject(err);
                }
            }
        });

        pipeline.on('end', async () => {
            if (batch.length > 0) {
                try {
                    await tempCollection.insertMany(batch);
                    insertedCount += batch.length;
                    console.log(`🔹 Inserted final ${batch.length} documents.`);
                } catch (err) {
                    console.error("❌ Error inserting final batch:", err);
                    return reject(err);
                }
            }
            console.log(`✅ Total inserted: ${insertedCount}`);
            resolve();
        });

        pipeline.on('error', (err: any) => {
            console.error("❌ Stream error:", err);
            reject(err);
        });
    });
}

runWithLogging({
    script: {
        name: "️‍📦 IMPORTER",
        index: 1,
        version: "1.0",
        text: "Імпорт даних"
    },
    run: async (db) => {
        await clearTmpCollection(db);
        await insertTmpFromFile(db);
    },
});
