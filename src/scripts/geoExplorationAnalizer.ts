import { Db, ObjectId } from "mongodb";
import { runWithLogging } from "../utils/runWithLogging";
import {
    FILES_COLLECTION_NAME,
    WELL_LOGS_COLLECTION_NAME,
    EXPLORATION_COLLECTION_NAME,
    EXPLORATION_INTERVALS_COLLECTION_NAME // перейменували
} from "../constants";
import pLimit from "p-limit";

const limit = pLimit(10);

async function synchronizeExploration(db: Db) {
    const filesCollection = db.collection(FILES_COLLECTION_NAME);
    const wellLogsCollection = db.collection(WELL_LOGS_COLLECTION_NAME);
    const explorationCollection = db.collection(EXPLORATION_COLLECTION_NAME);
    const explorationIntervalsCollection = db.collection(EXPLORATION_INTERVALS_COLLECTION_NAME);

    await explorationCollection.deleteMany({});
    console.log(`🗑️ Колекція ${EXPLORATION_COLLECTION_NAME} очищена.`);

    await explorationIntervalsCollection.deleteMany({});
    console.log(`🗑️ Колекція ${EXPLORATION_INTERVALS_COLLECTION_NAME} очищена.`);

    const wellData = await filesCollection
        .find({ "dbRef.dbType": "well" }, { projection: { id: 1, dbRef: 1 } })
        .toArray();
    const wellIds = wellData.map(well => well.id);
    const logData = await wellLogsCollection.find({}, { projection: { id: 1 } }).toArray();
    const logIds = logData.map(log => log.id);
    const wellMap = new Map(wellData.map(well => [String(well.id), well.dbRef.id]));

    const filter = {
        ancestorIds: { $in: wellIds },
        $and: [
            { path: { $regex: /\\Геофізика\\/, $options: "i" } },
            { $or: logIds.map(id => ({ path: { $regex: id, $options: "i" } })) }
        ]
    };

    const totalDocs = await filesCollection.countDocuments(filter);
    console.log(`📊 Для обробки знайдено ${totalDocs} документ(ів).`);

    const cursor = filesCollection.find(filter);

    let insertedCount = 0;
    while (await cursor.hasNext()) {
        const file = await cursor.next();
        if (!file) continue;

        for (const wellId of file.ancestorIds || []) {
            const wellID_DOB_SKV = wellMap.get(String(wellId));
            if (!wellID_DOB_SKV) continue;

            for (const logId of logIds) {
                if (file.path && file.name.includes(`_${logId}_`)) {
                    console.log(file.name, logId);
                    // Спочатку вставляємо в explorationCollection
                    await explorationCollection.insertOne({
                        wellId: wellID_DOB_SKV,
                        wellLog: logId,
                        fileName: file.name,
                        filePath: file.path,
                        fileId: file.id
                    });

                    // Потім парсимо файл і вставляємо в explorationIntervalsCollection
                    await parseAndInsertLogFile(
                        file.name,
                        wellID_DOB_SKV,
                        logId,
                        file.id,
                        file.path,
                        file.updated,
                        explorationIntervalsCollection
                    );

                    insertedCount++;
                    break;
                }
            }
        }
    }

    console.log(`✅ Синхронізація завершена. Додано записів: ${insertedCount}`);
}

function parseFileName(fileName: string) {
    const regex = /^(\d{4}\.\d{2}\.\d{2})_(.*?)_(.*?)_([A-Z_]+)?_\((\d+)-(\d+)\)_?([A-Z]+)?\.([^.]+)$/i;
    const match = fileName.match(regex);

    if (!match) {
        console.warn(`Неможливо розпарсити рядок: ${fileName}`);
        return [];
    }

    const [
        ,
        date,
        _location,
        _id,
        wellLogRaw = "",
        fromStr,
        toStr,
        zoomStr,
        extRaw
    ] = match;

    const wellLogs = wellLogRaw ? wellLogRaw.split("_").filter(Boolean) : [];
    const from = parseInt(fromStr, 10);
    const to = parseInt(toStr, 10);
    const ext = extRaw.toLowerCase();
    let zoom = 200;
    let zond = "";

    if (zoomStr && !isNaN(Number(zoomStr))) {
        zoom = parseInt(zoomStr, 10);
    } else if (zoomStr) {
        zond = zoomStr;
    }

    let type = "other";
    if (["jpg", "jpeg", "bmp", "png", "tiff", "gif"].includes(ext)) {
        type = "image";
    } else if (ext === "las") {
        type = "las";
    } else if (ext === "idw") {
        type = "idw";
    }

    const logs = wellLogs.length > 0 ? wellLogs : [""];

    return logs.map(wellLog => ({
        date,
        wellLog,
        fileName,
        interval: { from, to },
        type,
        ext,
        zoom,
        zond
    }));
}

async function parseAndInsertLogFile(
    fileName: string,
    wellId: string,
    wellLog: string,
    fileId: string,
    filePath: string,
    updated: string,
    explorationIntervalsCollection: any
) {
    const parsedFiles = parseFileName(fileName);

    for (const parsed of parsedFiles) {
        await explorationIntervalsCollection.insertOne({
            fileId,
            fileName,
            filePath,
            date: parsed.date,
            wellId,
            wellLog: parsed.wellLog,
            interval: parsed.interval,
            type: parsed.type,
            ext: parsed.ext,
            zoom: parsed.zoom,
            zond: parsed.zond,
            updated,
        });
    }
}

runWithLogging({
    script: {
        name: "⛏️ WELL LOG SYNCHRONIZATION",
        index: 12,
        version: "1.3",
        text: "Парсинг інтервалів"
    },
    run: async (db) => {
        await synchronizeExploration(db);
    }
});