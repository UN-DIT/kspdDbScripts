import { Db } from "mongodb";
import { runWithLogging } from "../utils/runWithLogging";
import {
    FIELDS_COLLECTION_NAME,
    FILES_COLLECTION_NAME,
    ORGANIZATIONS_COLLECTION_NAME,
    WELLS_COLLECTION_NAME
} from "../constants";
import pLimit from "p-limit";

const limit = pLimit(10); // Обмеження на 10 паралельних задач

function padStartNumbers(input: string): string {
    return input.replace(/^\d{1,3}/, (match) => match.padStart(3, '0'));
}

async function updateFilesWithOrganizations(db: Db) {
    const files = db.collection(FILES_COLLECTION_NAME);
    const orgs = db.collection(ORGANIZATIONS_COLLECTION_NAME);

    const orgsList = await orgs
        .find({ db_name: { $exists: true }, ID_DOB_SP_NGDU: { $exists: true } })
        .toArray();

    if (orgsList.length === 0) return;

    const bulkOps = orgsList.map(org => ({
        updateOne: {
            filter: {
                depth: 0,
                name: org.db_name
            },
            update: {
                $set: {
                    dbRef: {
                        dbType: "organization",
                        id: org.ID_DOB_SP_NGDU
                    }
                }
            }
        }
    }));

    const result = await files.bulkWrite(bulkOps, { ordered: false });

    console.log(`✅ Organizations updated: ${result.modifiedCount}`);
}

async function updateFilesWithFields(db: Db) {
    const files = db.collection(FILES_COLLECTION_NAME);
    const fields = db.collection(FIELDS_COLLECTION_NAME);

    const orgFiles = await files.find({
        depth: 0,
        "dbRef.dbType": "organization",
        "dbRef.id": { $exists: true }
    }).toArray();

    for (const orgFile of orgFiles) {
        const fieldsForOrg = await fields.find({ ID_DOB_SP_NGDU: orgFile.dbRef.id }).toArray();

        if (fieldsForOrg.length === 0) continue;

        const bulkOps = fieldsForOrg.map(field => ({
            updateOne: {
                filter: {
                    depth: 1,
                    name: field.db_name,
                    ancestorIds: orgFile.id
                },
                update: {
                    $set: {
                        dbRef: {
                            dbType: "field",
                            id_organization: orgFile.dbRef.id,
                            id: field.ID_DOB_SP_MEST
                        }
                    }
                }
            }
        }));

        if (bulkOps.length > 0) {
            const result = await files.bulkWrite(bulkOps, { ordered: false });
            console.log(`✅ Org ${orgFile.dbRef.id}: updated fields: ${result.modifiedCount}`);
        }
    }
}

async function updateFilesWithWells(db: Db) {
    const files = db.collection(FILES_COLLECTION_NAME);
    const wells = db.collection(WELLS_COLLECTION_NAME);

    // Завантажуємо всі свердловини й групуємо їх по ID_DOB_SP_MEST
    const allWells = await wells.find({}).toArray();
    const wellsByFieldId = new Map<string, any[]>();

    for (const well of allWells) {
        const fieldId = well.ID_DOB_SP_MEST;
        if (!fieldId) continue;
        if (!wellsByFieldId.has(fieldId)) {
            wellsByFieldId.set(fieldId, []);
        }
        wellsByFieldId.get(fieldId)!.push(well);
    }

    // Отримуємо всі файли, що відповідають полям
    const fieldFiles = await files.find({
        depth: 1,
        "dbRef.dbType": "field",
        "dbRef.id": { $exists: true }
    }).toArray();

    // Обробляємо кожне поле паралельно з обмеженням
    await Promise.all(fieldFiles.map(fieldFile => limit(async () => {
        const wellsForField = wellsByFieldId.get(fieldFile.dbRef.id);
        if (!wellsForField || wellsForField.length === 0) return;

        const bulkOps = wellsForField.map(well => ({
            updateOne: {
                filter: {
                    depth: 3,
                    name: padStartNumbers(well.db_name),
                    ancestorIds: fieldFile.id
                },
                update: {
                    $set: {
                        dbRef: {
                            dbType: "well",
                            id_organization: fieldFile.dbRef.id_organization,
                            id_field: fieldFile.dbRef.id,
                            id: well.ID_DOB_SKV
                        }
                    }
                }
            }
        }));

        if (bulkOps.length > 0) {
            const result = await files.bulkWrite(bulkOps, { ordered: false });
            console.log(`✅ Field ${fieldFile.dbRef.id}: updated wells: ${result.modifiedCount}`);
        }
    })));
}

runWithLogging({
    script: {
        name: "️‍📀 DBSYNCH",
        index: 11,
        version: "1.0",
        text: "Синхронізація полів з базою даних"
    },
    run: async (db) => {
        await updateFilesWithOrganizations(db);
        await updateFilesWithFields(db);
        await updateFilesWithWells(db);
    }
});