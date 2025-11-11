import { fork } from 'child_process';
import { cpus } from 'os';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import dotenv from 'dotenv';
import Redis from "ioredis";

Redis.Command.setArgumentTransformer('info', () => []); // не посылает аргументы
Redis.Command.setReplyTransformer('info', () => '');    // возвращает пустую строку

dotenv.config();
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);


const clients = parseInt(process.env.CLIENTS || '10', 10);
const opsPerClient = parseInt(process.env.OPS_PER_CLIENT || '100000', 10);
const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';


console.log(`Starting test: clients=${clients}, opsPerClient=${opsPerClient}, redis=${redisUrl}`);


// spawn monitor
const monitor = fork(join(__dirname, 'monitor.js'), [], {
    env: { REDIS_URL: redisUrl, OPS_PER_CLIENT: String(opsPerClient) }
});


monitor.on('message', m => console.log('[monitor]', m));
monitor.on('exit', code => console.log('[monitor] exited', code));


// spawn workers
const workers = [];
for (let i = 0; i < clients; i++) {
    const w = fork(join(__dirname, 'worker.js'), [], {
        env: { REDIS_URL: redisUrl, CLIENT_ID: String(i), OPS_PER_CLIENT: String(opsPerClient) }
    });
    w.on('message', m => console.log(`[worker ${i}]`, m));
    w.on('exit', code => console.log(`[worker ${i}] exited ${code}`));
    workers.push(w);
}


// optional: stop monitor after all workers exit
let exited = 0;
for (const w of workers) {
    w.on('exit', () => {
        exited++;
        if (exited === workers.length) {
            console.log('All workers finished — killing monitor in 3s');
            setTimeout(() => monitor.kill(), 3000);
        }
    });
}

