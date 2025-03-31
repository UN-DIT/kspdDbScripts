import { configDotenv } from 'dotenv';
import {Db, MongoClient} from "mongodb";

configDotenv();

const { DB_HOST, DB_PORT, DB_NAME } = process.env;

const uri = `mongodb://${DB_HOST}:${DB_PORT}/?authSource=admin`; // Change if needed

export default async () => {
    const dbClient = new MongoClient(uri);

    const connect = async (): Promise<Db> => {
        await dbClient.connect();
        console.log("✅ Connected to MongoDB");
        return  dbClient.db(DB_NAME);
    }

    const disconnect = async () => {
        await dbClient.close();
    }

    return [connect, disconnect];
};