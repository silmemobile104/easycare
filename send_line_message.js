require('dotenv').config();
const https = require('https');

async function sendLineMessage(text, targetId) {
    const token = process.env.LINE_CHANNEL_ACCESS_TOKEN;
    const groupId = targetId || process.env.LINE_GROUP_ID;

    if (!token) {
        throw new Error('ไม่พบ LINE_CHANNEL_ACCESS_TOKEN ในไฟล์ .env');
    }
    if (!groupId) {
        throw new Error('ไม่พบ LINE_GROUP_ID ในไฟล์ .env กรุณาระบุ Group ID (ขึ้นต้นด้วย C...)');
    }

    const postData = JSON.stringify({
        to: groupId,
        messages: [
            {
                type: 'text',
                text: text
            }
        ]
    });

    return new Promise((resolve, reject) => {
        const req = https.request({
            hostname: 'api.line.me',
            path: '/v2/bot/message/push',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`,
                'Content-Length': Buffer.byteLength(postData)
            }
        }, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    resolve({ success: true, statusCode: res.statusCode, data });
                } else {
                    reject(new Error(`LINE API Error (${res.statusCode}): ${data}`));
                }
            });
        });

        req.on('error', reject);
        req.write(postData);
        req.end();
    });
}

// ถ้าสั่งรันตรงๆ จาก command line
if (require.main === module) {
    const message = process.argv[2] || 'สวัสดีฉันคือบอทแจ้งเตือนอัตโนมัติ';
    const target = process.argv[3] || process.env.LINE_GROUP_ID;

    console.log(`กำลังส่งข้อความ: "${message}"`);
    console.log(`เป้าหมาย (Group ID): ${target || 'ยังไม่ได้ระบุ'}`);

    sendLineMessage(message, target)
        .then(res => {
            console.log('✅ ส่งข้อความสำเร็จเรียบร้อยแล้ว!');
            console.log(res);
        })
        .catch(err => {
            console.error('❌ ไม่สามารถส่งข้อความได้:', err.message);
        });
}

module.exports = { sendLineMessage };
