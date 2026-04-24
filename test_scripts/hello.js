const os = require('os');
console.log(`Node.js ${process.version}`);
console.log(`Host: ${os.hostname()}`);
console.log(`PID: ${process.pid}`);
console.log('Hello from Node.js worker!');
