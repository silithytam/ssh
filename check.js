/*

masscan -p443,2053,2083,2087,2096,8443 -iL <(curl -s https://raw.githubusercontent.com/ipverse/rir-ip/refs/heads/master/country/id/aggregated.json | jq -r '.subnets.ipv4[]') --excludefile <(curl -sSL "https://pastebin.com/raw/JtgbgepA") --rate 80000 2>/dev/null | node proxyipscan.js

*/

const tls = require("node:tls")
const cluster = require("node:cluster")
const os = require("node:os")
const fs = require("node:fs")
const { spawn } = require("node:child_process")
const readline = require("node:readline")

const color = {
    reset: "\x1b[0m",
    gray: (text) => `\x1b[90m${text}\x1b[0m`,
    blue: (text) => `\x1b[34m${text}\x1b[0m`,
    cyan: (text) => `\x1b[36m${text}\x1b[0m`,
    green: (text) => `\x1b[32m${text}\x1b[0m`,
    red: (text) => `\x1b[31m${text}\x1b[0m`,
    yellow: (text) => `\x1b[33m${text}\x1b[0m`,
    magenta: (text) => `\x1b[35m${text}\x1b[0m`,
};

function log(message, type = "info") {
    const timestamp = new Date().toISOString().split("T")[1].split(".")[0]
    const prefix = {
        info: color.blue("[INFO]"),
        success: color.green("[SUCCESS]"),
        error: color.red("[ERROR]"),
        scan: color.magenta("[SCAN]"),
    }
    console.log(`${color.gray(timestamp)} ${prefix[type] || prefix.info} ${message}`)
}
const getProgressPercentage = (current, total) => {
    const percentage = Math.min((current / total) * 100, 100);
    return `${percentage.toFixed(1)}%`;
};
const formatDuration = (milliseconds) => {
    let totalSeconds = Math.floor(milliseconds / 1000);
    let hours = Math.floor(totalSeconds / 3600);
    let minutes = Math.floor((totalSeconds % 3600) / 60);
    let seconds = totalSeconds % 60;

    let formatted = [];
    if (hours > 0) formatted.push(`${hours} hours`);
    if (minutes > 0) formatted.push(`${minutes} minutes`);
    if (seconds > 0 || formatted.length === 0) formatted.push(`${seconds} seconds`);
    return formatted.join(" ");
};

if (cluster.isPrimary) {
    let completedWorkers = 0
    let numCPUs = os.cpus().length || 4
    const startTime = Date.now()
    let totalProxiesFound = 0
    let totalScanned = 0
    let totalChecked = 0
    const jsonOutputFile = `prxip.json`
    const activeProxies = []
    let proxyQueue = []
    let myip = null
    let masscanFinished = false
    let workersStarted = false
    let lastProgressUpdate = 0
    const activeWorkers = new Map();
    const workerActiveJobs = new Map(); 
    const MAX_CONCURRENT_PER_WORKER = 50;
    const seen = new Set()
    if (fs.existsSync(jsonOutputFile)) {
              try {
                const existingProxies = JSON.parse(fs.readFileSync(jsonOutputFile, 'utf8'));
                proxyQueue = existingProxies.map(p => `${p.port}:${p.port}`)
              } catch (e) {}
            }

    function updateProgress() {
        const now = Date.now()
        if (now - lastProgressUpdate > 2000 || lastProgressUpdate === 0) {
            const queueSize = proxyQueue.length
            const checkRate = totalChecked > 0 ? (totalChecked / ((now - startTime) / 1000)).toFixed(1) : '0'
            const successRate = totalChecked > 0 ? ((totalProxiesFound / totalChecked) * 100).toFixed(1) : '0'
            const totalActiveJobs = Array.from(workerActiveJobs.values()).reduce((sum, count) => sum + count, 0)
            const progressPercent = getProgressPercentage(totalChecked, totalScanned);
 
            console.log(`\r${color.gray('[')}${color.cyan('PROGRESS')}${color.gray(']')} Progress: ${color.magenta(progressPercent)} (${color.cyan(`${totalChecked}/${totalScanned}`)}) | Active: ${color.green(totalProxiesFound)} | Rate: ${color.magenta(checkRate + '/s')}`);
            lastProgressUpdate = now
        }
    }

    async function getMyIP() {
        try {
            const response = await fetch("https://speed.cloudflare.com/meta", {
              headers: {
                "Referer": "https://speed.cloudflare.com/"
              }
            })
            if (!response.ok) throw new Error(`Failed to fetch IP: ${response.status}`)
            const data = await response.json()
            return data.clientIp
        } catch (error) {
            throw new Error(`Failed to get IP: ${error.message}`)
        }
    }

    function distributeWork() {
        activeWorkers.forEach((worker, workerId) => {
            const currentJobs = workerActiveJobs.get(workerId) || 0
            const availableSlots = MAX_CONCURRENT_PER_WORKER - currentJobs
            
            if (availableSlots > 0 && proxyQueue.length > 0) {
                const proxiesToSend = Math.min(availableSlots, proxyQueue.length)
                const batch = proxyQueue.splice(0, proxiesToSend)
                
                if (batch.length > 0) {
                    workerActiveJobs.set(workerId, currentJobs + batch.length)
                    worker.send({
                        type: "work",
                        proxies: batch
                    })
                }
            }
        });
    }

    function checkGlobalCompletion() {
        if (masscanFinished && proxyQueue.length === 0) {
            const totalActiveJobs = Array.from(workerActiveJobs.values()).reduce((sum, count) => sum + count, 0)
            
            if (totalActiveJobs === 0) {
                setTimeout(() => {
                    activeWorkers.forEach(worker => {
                        worker.send({ type: "finish" });
                    });
                }, 1000);
            }
        }
    }

    function startWorkers() {
        if (workersStarted) return
        workersStarted = true

        log(`Starting ${numCPUs} worker threads for proxy checking`, "info")
        setInterval(() => fs.writeFileSync(jsonOutputFile, JSON.stringify(activeProxies, null, 2)), 60000);

        for (let i = 0; i < numCPUs; i++) {
            const worker = cluster.fork()
            const workerId = worker.process.pid
            activeWorkers.set(workerId, worker);
            workerActiveJobs.set(workerId, 0);

            worker.send({
                myip
            })

            worker.on("message", (msg) => {
                if (msg.type === "proxyFound") {
                    const key = `${msg.data.proxy}:${msg.data.port}`;
                    if (!seen.has(key)) {
                        seen.add(key);
                        totalProxiesFound++;
                        activeProxies.push(msg.data);
                     //   fs.writeFileSync(jsonOutputFile, JSON.stringify(activeProxies, null, 2))
                    }
                    log(`Found: ${color.cyan(msg.data.proxy)}:${color.cyan(msg.data.port)} | ${color.yellow(msg.data.countryCode || 'UNK')} | ${color.blue((msg.data.org || 'Unknown').substring(0, 30))} | ${color.cyan(msg.data.latency + 'ms')}`, "success")
                    updateProgress()
                } else if (msg.type === "proxyChecked") {
                    totalChecked++
                    const currentJobs = workerActiveJobs.get(workerId) || 0
                    workerActiveJobs.set(workerId, Math.max(0, currentJobs - 1))
                    updateProgress()
                    distributeWork();
                    checkGlobalCompletion();
                } else if (msg.type === "requestWork") {
                    distributeWork();
                }
            })

            worker.on('disconnect', () => {
                log(`Worker ${workerId} disconnected.`, 'info');
                activeWorkers.delete(workerId);
                workerActiveJobs.delete(workerId);
            });
            
            worker.on('exit', (code, signal) => {
                activeWorkers.delete(workerId);
                workerActiveJobs.delete(workerId);
                if (signal) {
                    log(`Worker ${workerId} was killed by signal: ${signal}`, 'error');
                } else if (code !== 0) {
                    log(`Worker ${workerId} exited with error code: ${code}`, 'error');
                }
                completedWorkers++;
                if (completedWorkers === numCPUs) {
                    console.log()
                    fs.writeFileSync(jsonOutputFile, JSON.stringify(activeProxies, null, 2))
                    const duration = Date.now() - startTime
                    log(`Scan completed in ${formatDuration(duration)}`, "success")
                    log(`Total scanned: ${totalScanned}, Checked: ${totalChecked}, Found ${totalProxiesFound} working proxies`, "success")
                    log(`Total unique proxies in database: ${activeProxies.length}`, "success")
                    log(`Results saved to ${jsonOutputFile}`, "success")
                    process.exit(0)
                }
            });
        }
    }

    (async () => {
        try {
            myip = await getMyIP()
            log(`Your IP: ${color.cyan(myip)}`, "info")
            
            if (process.stdin.isTTY) {
                log("masscan -p443,8443 --rate=1000 192.168.1.0/24 | node script.js", "info")
                process.exit(0)
            } else {
                const rl = readline.createInterface({
                    input: process.stdin,
                    crlfDelay: Infinity
                })

                rl.on('line', (line) => {
                    const match = line.match(/Discovered open port (\d+)\/tcp on ([\d.]+)/)
                    if (match) {
                        const [, port, ip] = match
                        const proxy = `${ip}:${port}`
                        proxyQueue.push(proxy)
                        totalScanned++
                        updateProgress()
                        if (!workersStarted) {
                            startWorkers()
                        }
                        distributeWork();
                    }
                })

                rl.on('close', () => {
                    masscanFinished = true
                    console.log()
                    log(`Stdin closed (masscan pipe finished). Total scanned: ${totalScanned}`, "scan")
                    
                    if (!workersStarted) {
                        startWorkers()
                    }
                    distributeWork();
                    checkGlobalCompletion();
                })
            }
        } catch (error) {
            log(`${error.message}`, "error")
            process.exit(1)
        }
    })()

} else {
    let myips
    let isFinished = false

    process.on("message", (msg) => {
        if (msg.myip) {
            myips = msg.myip
            
            process.send({ type: "requestWork" })
        } else if (msg.type === "work") {
            if (!isFinished) {
                msg.proxies.forEach(proxy => {
                    checkProxy(proxy).catch(err => {
                    })
                })
            }
        } else if (msg.type === "finish") {
            isFinished = true
            process.exit(0)
        }
    })

    async function sendRequest(host, port, targetHost, path) {
        return new Promise((resolve, reject) => {
            if (!host || !port) {
                return reject(new Error("Missing host or port"));
            }

            const start = Date.now();
            let timeoutId;

            timeoutId = setTimeout(() => {
                if (socket) {
                    socket.destroy();
                }
                reject(new Error("Request timeout after 5s"));
            }, 5000);

            const socket = tls.connect({
                host,
                port: Number.parseInt(port),
                servername: targetHost
            }, () => {
                const request =
                    `GET ${path} HTTP/1.1\r\n` +
                    `Host: ${targetHost}\r\n` +
                    `User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n` +
                    `Connection: close\r\n\r\n`;
                socket.write(request);
            });

            let responseBody = "";
            socket.on("data", (data) => {
                responseBody += data.toString();
            });

            socket.on("end", () => {
                clearTimeout(timeoutId);
                try {
                    const latency = Date.now() - start;
                    const parts = responseBody.split("\r\n\r\n");
                    const body = parts.length > 1 ? parts.slice(1).join("\r\n\r\n") : "";
                    resolve({ body, latency });
                } catch (error) {
                    reject(new Error("Parse error: " + error.message));
                } finally {
                    socket.destroy();
                }
            });

            socket.on("error", (err) => {
                clearTimeout(timeoutId);
                socket.destroy();
                reject(new Error("Socket error: " + err.message));
            });
        });
    }

    async function checkIP(proxy) {
        const [host, port] = proxy.split(':')

        return new Promise(async (resolve) => {
            try {
                const ipinfo = await sendRequest(host, port, "myip.bexcode.us.to", "/")
                if (!ipinfo || !ipinfo.body) return resolve({ proxyip: false, msg: "No response body or invalid response" })

                let ipingfo
                try {
                    ipingfo = JSON.parse(ipinfo.body)
                } catch (e) {
                    return resolve({ proxyip: false, msg: "Invalid JSON response from proxy target" })
                }

                if (typeof ipingfo.clientIp === 'string' && ipingfo.clientIp.trim() !== '' && ipingfo.clientIp !== myips) {
                    const { clientIp, ...otherInfo } = ipingfo
                    resolve({
                        proxy: host,
                        port: port,
                        proxyip: true,
                        ip: clientIp,
                        latency: ipinfo.latency,
                        ...otherInfo,
                    })
                } else {
                    resolve({
                        proxy: host,
                        port: port,
                        proxyip: false,
                        msg: ipingfo.clientIp === myips ? "Proxy shows own IP" : "Invalid IP response from target"
                    })
                }
            } catch (error) {
                resolve({
                    msg: error.message,
                    proxy: host,
                    port: port,
                    proxyip: false,
                })
            }
        })
    }

    async function checkProxy(proxy) {
        try {
            const data = await checkIP(proxy)

            process.send({ type: "proxyChecked" });
            
            if (data.proxyip) {
                process.send({ type: "proxyFound", data });
            }
        } catch (err) {
            process.send({ type: "proxyChecked" });
        }
    }
}
