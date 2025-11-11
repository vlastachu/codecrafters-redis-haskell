
import Redis from 'ioredis';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname } from 'path';


dotenv.config();


const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const clientId = process.env.CLIENT_ID || '0';
const opsPerClient = parseInt(process.env.OPS_PER_CLIENT || '100000', 10);


const redis = new Redis(redisUrl);


async function run() {
    try {
        // process.send && process.send({ status: 'starting', clientId });


        // Optional small randomized delay to increase race conditions
        const delayMs = Math.floor(Math.random() * 200);
        await new Promise(r => setTimeout(r, delayMs));


        // Use MULTI/EXEC transaction and push many INCR commands
        const multi = redis.multi();
        for (let i = 0; i < opsPerClient; i++) {
            multi.incr('tx_counter');
            // occasionally yield to event loop
            if ((i & 0x3FFF) === 0 && i !== 0) await new Promise(r => setImmediate(r));
        }


        // process.send && process.send({ status: 'exec', clientId });
        const results = await multi.exec();


        // results is an array of [err, value] tuples in redis v4+ it may be array of values or tuples depending
        const succeeded = Array.isArray(results) ? results.length : 0;
        // process.send && process.send({ status: 'done', clientId, ops: succeeded });
    } catch (err) {
        process.send && process.send({ status: 'error', clientId, error: String(err) });
    } finally {
        redis.disconnect();
        process.exit(0);
    }
}


run();