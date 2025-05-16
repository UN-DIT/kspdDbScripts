export function logExecutionTime(startTime: number, endTime: number) {
    const durationMs = endTime - startTime;
    const h = Math.floor(durationMs / 3600000);
    const m = Math.floor((durationMs % 3600000) / 60000);
    const s = Math.floor((durationMs % 60000) / 1000);

    console.log(`⏳ Execution time: ${h}h ${m}m ${s}s`);
}