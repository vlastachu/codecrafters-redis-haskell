import Redis from 'ioredis';
import dotenv from 'dotenv';


dotenv.config();
const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const opsPerClient = parseInt(process.env.OPS_PER_CLIENT || '100000', 10);
const redis = new Redis(redisUrl);


let checks = 0;
async function poll() {
    try {
        const v = await redis.get('tx_counter');
        const parsed = v === null ? null : Number(v);
        const ok = (v === null) || (Number.isInteger(parsed) && parsed % opsPerClient === 0);
        if (!ok)
            process.send && process.send({ check: ++checks, value: v, ok });
    } catch (err) {
        process.send && process.send({ check: ++checks, error: String(err) });
    }
}


// poll every 500ms
const timer = setInterval(poll, 5);


// do an initial poll
poll();


// cleanup on exit
process.on('SIGINT', async () => {
    clearInterval(timer);
    await redis.quit();
    process.exit(0);
});