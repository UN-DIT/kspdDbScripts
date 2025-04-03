import axios from "axios";

const webhookUrl = "https://ukrnaftaoffice365.webhook.office.com/webhookb2/41973cce-b249-4c71-8953-3510b3c61b1f@ce629f56-05df-43c6-b979-8b0b7992fc55/IncomingWebhook/f408d71ad471494596bde569c0cdcb0a/5d110652-e806-465e-b4e9-6d673654644a/V2rBUuWd6COt0dyKL6rF3Nnu_XzPZCr3EbsXGOdFJtcYU1"

async function sendMessageToTeams(message: string) {
    try {
        const payload = {
            text: message,
        };

        const response = await axios.post(webhookUrl, payload, {
            headers: { "Content-Type": "application/json" },
        });

        console.log("✅ Message sent:", response.status);
    } catch (error) {
        console.error("❌ Error sending message:", error);
    }
}

export { sendMessageToTeams };

// sendMessageToTeams("Hello, Teams! 🚀");