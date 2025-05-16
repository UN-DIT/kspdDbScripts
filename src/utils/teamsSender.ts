import axios from "axios";
import {configDotenv} from "dotenv";

configDotenv();

const { WEBHOOK_URL } = process.env;

async function sendMessageToTeams(message: string) {
    if (!WEBHOOK_URL || !message) {
        return;
    }

    try {
        const payload = {
            text: message,
        };

        const response = await axios.post(WEBHOOK_URL, payload, {
            headers: { "Content-Type": "application/json" },
        });

        console.log("✅ Message sent to Teams:", response.status);
    } catch (error) {
        console.error("❌ Error sending message to Teams:", error);
    }
}


export { sendMessageToTeams };
