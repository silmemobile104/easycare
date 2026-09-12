require('dotenv').config();
const https = require('https');

let isLineNotificationsEnabled = true;

function setLineNotificationsEnabled(enabled) {
    isLineNotificationsEnabled = !!enabled;
    console.log(`⚙️ [LINE Notify] สถานะการแจ้งเตือน LINE: ${isLineNotificationsEnabled ? 'เปิด (ENABLED)' : 'ปิด (PAUSED)'}`);
}

function getLineNotificationsEnabled() {
    return isLineNotificationsEnabled;
}

async function sendLineMessage(message, targetId, options = {}) {
    if (!isLineNotificationsEnabled && !options.force) {
        console.log(`⏸️ [LINE Notify] ข้ามการส่งข้อความ LINE เนื่องจากปิดการแจ้งเตือน LINE ชั่วคราว`);
        return { success: false, skipped: true, message: 'LINE notifications disabled' };
    }

    const rawToken = process.env.LINE_CHANNEL_ACCESS_TOKEN;
    const token = rawToken ? String(rawToken).trim().replace(/^["']+|["']+$/g, '').trim() : '';

    const rawGroupId = targetId || process.env.LINE_GROUP_ID;
    const groupId = rawGroupId ? String(rawGroupId).trim().replace(/^["'(\[]+|["')\]]+$/g, '').trim() : '';

    if (!token) {
        throw new Error('ไม่พบ LINE_CHANNEL_ACCESS_TOKEN ในไฟล์ .env');
    }
    if (!groupId) {
        throw new Error('ไม่พบ LINE_GROUP_ID ในไฟล์ .env กรุณาระบุ Group ID (ขึ้นต้นด้วย C...)');
    }

    let messages = [];
    if (typeof message === 'string') {
        messages = [{ type: 'text', text: message }];
    } else if (Array.isArray(message)) {
        messages = message;
    } else if (typeof message === 'object' && message !== null) {
        messages = [message];
    } 

    const postData = JSON.stringify({
        to: groupId,
        messages: messages
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

const defaultFinanceRates = [
    { tierName: "Package 1", minDeviceValue: 0, maxDeviceValue: 5000, packagePrice: 699, downPayment: 60, financedAmount: 640, installmentPlans: [{ months: 6, monthlyAmount: 160 }, { months: 10, monthlyAmount: 100 }, { months: 12, monthlyAmount: 60 }, { months: 15, monthlyAmount: 60 }, { months: 18, monthlyAmount: 60 }] },
    { tierName: "Package 2", minDeviceValue: 5001, maxDeviceValue: 10000, packagePrice: 899, downPayment: 80, financedAmount: 820, installmentPlans: [{ months: 6, monthlyAmount: 180 }, { months: 10, monthlyAmount: 120 }, { months: 12, monthlyAmount: 80 }, { months: 15, monthlyAmount: 80 }, { months: 18, monthlyAmount: 80 }] },
    { tierName: "Package 3", minDeviceValue: 10001, maxDeviceValue: 15000, packagePrice: 1099, downPayment: 100, financedAmount: 1000, installmentPlans: [{ months: 6, monthlyAmount: 200 }, { months: 10, monthlyAmount: 140 }, { months: 12, monthlyAmount: 100 }, { months: 15, monthlyAmount: 100 }, { months: 18, monthlyAmount: 100 }] },
    { tierName: "Package 4", minDeviceValue: 15001, maxDeviceValue: 20000, packagePrice: 1299, downPayment: 120, financedAmount: 1180, installmentPlans: [{ months: 6, monthlyAmount: 240 }, { months: 10, monthlyAmount: 160 }, { months: 12, monthlyAmount: 120 }, { months: 15, monthlyAmount: 120 }, { months: 18, monthlyAmount: 120 }] },
    { tierName: "Package 5", minDeviceValue: 20001, maxDeviceValue: 25000, packagePrice: 1499, downPayment: 150, financedAmount: 1350, installmentPlans: [{ months: 6, monthlyAmount: 270 }, { months: 10, monthlyAmount: 180 }, { months: 12, monthlyAmount: 150 }, { months: 15, monthlyAmount: 150 }, { months: 18, monthlyAmount: 150 }] },
    { tierName: "Package 6", minDeviceValue: 25001, maxDeviceValue: 30000, packagePrice: 1699, downPayment: 180, financedAmount: 1520, installmentPlans: [{ months: 6, monthlyAmount: 320 }, { months: 10, monthlyAmount: 190 }, { months: 12, monthlyAmount: 180 }, { months: 15, monthlyAmount: 180 }, { months: 18, monthlyAmount: 180 }] },
    { tierName: "Package 7", minDeviceValue: 30001, maxDeviceValue: 35000, packagePrice: 1899, downPayment: 190, financedAmount: 1710, installmentPlans: [{ months: 6, monthlyAmount: 350 }, { months: 10, monthlyAmount: 210 }, { months: 12, monthlyAmount: 190 }, { months: 15, monthlyAmount: 190 }, { months: 18, monthlyAmount: 190 }] },
    { tierName: "Package 8", minDeviceValue: 35001, maxDeviceValue: 40000, packagePrice: 2099, downPayment: 200, financedAmount: 1900, installmentPlans: [{ months: 6, monthlyAmount: 390 }, { months: 10, monthlyAmount: 230 }, { months: 12, monthlyAmount: 200 }, { months: 15, monthlyAmount: 200 }, { months: 18, monthlyAmount: 200 }] },
    { tierName: "Package 9", minDeviceValue: 40001, maxDeviceValue: 45000, packagePrice: 2299, downPayment: 250, financedAmount: 2050, installmentPlans: [{ months: 6, monthlyAmount: 420 }, { months: 10, monthlyAmount: 270 }, { months: 12, monthlyAmount: 250 }, { months: 15, monthlyAmount: 250 }, { months: 18, monthlyAmount: 250 }] },
    { tierName: "Package 10", minDeviceValue: 45001, maxDeviceValue: 50000, packagePrice: 2499, downPayment: 270, financedAmount: 2230, installmentPlans: [{ months: 6, monthlyAmount: 490 }, { months: 10, monthlyAmount: 300 }, { months: 12, monthlyAmount: 270 }, { months: 15, monthlyAmount: 270 }, { months: 18, monthlyAmount: 270 }] }
];

/**
 * คำนวณค่างวดต่อเดือนสำหรับสัญญาผ่อนด้วยไฟแนนซ์ตามตารางเรท InstallmentPlan
 * @param {object} w - Warranty record
 * @returns {number|null} ค่างวดต่อเดือน (บาท)
 */
function getFinanceMonthlyAmount(w) {
    if (!w || !w.financeDetails) return null;
    const months = Number(w.financeDetails.financeMonths || 0);
    if (!months) return null;

    const planName = (w.package?.plan || '').trim();
    const pkgPrice = Number(w.package?.price || 0);
    const devVal = Number(w.device?.deviceValue ?? w.devicePrice ?? 0);

    // 1. ค้นหาจากชื่อแพ็กเกจ (เช่น "Package 2")
    let tier = defaultFinanceRates.find(t => t.tierName.toLowerCase() === planName.toLowerCase());
    // 2. ค้นหาจากราคาแพ็กเกจ (เช่น 899)
    if (!tier && pkgPrice > 0) {
        tier = defaultFinanceRates.find(t => t.packagePrice === pkgPrice);
    }
    // 3. ค้นหาจากช่วงราคาประเมินตัวเครื่อง (minDeviceValue - maxDeviceValue)
    if (!tier && devVal > 0) {
        tier = defaultFinanceRates.find(t => devVal >= t.minDeviceValue && devVal <= t.maxDeviceValue);
    }

    if (tier && Array.isArray(tier.installmentPlans)) {
        const plan = tier.installmentPlans.find(p => p.months === months);
        if (plan && plan.monthlyAmount) {
            return plan.monthlyAmount;
        }
    }

    // กรณีไม่พบในตารางเรท ให้คำนวณจากราคาแพ็กเกจหารจำนวนเดือน
    return pkgPrice > 0 && months > 0 ? Math.round(pkgPrice / months) : null;
}

/**
 * Format warranty details into a structured LINE notification message
 * @param {object} w - Warranty record
 * @param {object} [options] - Optional flags (isResubmit, etc.)
 * @returns {string} Formatted text message
 */
function formatWarrantyPendingMessage(w, options = {}) {
    if (!w) return '🔔 [EasyCare] มีสัญญารออนุมัติ (ไม่พบข้อมูลสัญญา)';

    const isResubmit = options.isResubmit || false;
    const headerTitle = isResubmit
        ? '🔄 [EasyCare] สัญญาแก้ไขส่งมารออนุมัติซ้ำ'
        : '🔔 [EasyCare] แจ้งเตือนสัญญารอการอนุมัติ';

    const createdDate = w.createdAt ? new Date(w.createdAt) : new Date();
    const createdStr = createdDate.toLocaleString('th-TH', {
        timeZone: 'Asia/Bangkok',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    }) + ' น.';

    // Customer info
    const custName = [w.customer?.firstName, w.customer?.lastName].filter(Boolean).join(' ') || '-';
    const custPhone = w.customer?.phone || '-';
    const memberId = w.memberId || w.customer?.id || '-';

    // Device info
    const devType = (w.device?.type || '').trim();
    const devModel = (w.device?.model || '').trim();
    let fullModel = devModel || devType || '-';
    if (devType && devModel && !devModel.toLowerCase().includes(devType.toLowerCase())) {
        fullModel = `${devType} ${devModel}`;
    }

    const devSpecs = [w.device?.color, w.device?.capacity].filter(Boolean).join(' / ') || '-';
    const devCondition = w.device?.deviceCondition === 'Second-hand' ? 'เครื่องมือสอง' : 'เครื่องใหม่ (มือหนึ่ง)';

    // ราคาเครื่องจริง (Retail / Selling price): ใช้ w.devicePrice เป็นหลัก
    const actualDevicePrice = Number(w.devicePrice ?? w.device?.price ?? w.device?.deviceValue ?? 0);
    const actualDevicePriceStr = actualDevicePrice > 0 ? `${actualDevicePrice.toLocaleString()} บาท` : '-';

    const serial = w.device?.serial || '-';
    const imei = w.device?.imei || '-';
    const imgCount = Array.isArray(w.device?.images) ? w.device.images.length : 0;
    const inspectCount = Array.isArray(w.device?.inspectionResult) ? w.device.inspectionResult.length : 0;

    // Package info
    const plan = w.package?.plan || '-';
    const pkgPrice = Number(w.package?.price || 0);
    const pkgPriceStr = `${pkgPrice.toLocaleString()} บาท`;

    // วงเงินคุ้มครองสูงสุด (Coverage Limit): ใช้ราคาประเมิน deviceValue หรือตาม cap ของแพ็กเกจ
    let maxCoverageNum = 0;
    if (typeof w.maxLimit === 'number' && w.maxLimit > 0) {
        maxCoverageNum = w.maxLimit;
    } else {
        const baseCoverage = Number(w.device?.deviceValue ?? w.devicePrice ?? 0);
        const caps = {
            'Package 1': 5000, 'Package 2': 10000, 'Package 3': 15000, 'Package 4': 20000, 'Package 5': 25000,
            'Package 6': 30000, 'Package 7': 35000, 'Package 8': 40000, 'Package 9': 45000, 'Package 10': 50000
        };
        const cap = caps[w.package?.plan] || Infinity;
        maxCoverageNum = Math.floor(Math.min(baseCoverage, cap));
    }
    const maxCoverageStr = maxCoverageNum > 0 ? `${maxCoverageNum.toLocaleString()} บาท` : '-';

    let coveragePeriod = '-';
    if (w.warrantyDates?.start && w.warrantyDates?.end) {
        const s = new Date(w.warrantyDates.start).toLocaleDateString('th-TH', {
            timeZone: 'Asia/Bangkok',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        });
        const e = new Date(w.warrantyDates.end).toLocaleDateString('th-TH', {
            timeZone: 'Asia/Bangkok',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        });
        coveragePeriod = `${s} - ${e}`;
    }

    // Payment method info
    const payMethod = w.payment?.method || '-';
    let payDetailText = '';

    if (payMethod === 'Full Payment') {
        const isPaid = w.payment?.status === 'Paid';
        payDetailText = [
            `• รูปแบบ: ซื้อสด (ชำระเต็มจำนวน)`,
            `• ยอดชำระ: ${pkgPriceStr}`,
            `• สถานะชำระ: ${isPaid ? 'ชำระเงินเรียบร้อยแล้ว' : 'รอชำระเงิน'}`
        ].join('\n');
    } else if (payMethod === 'Installment') {
        const sched = Array.isArray(w.payment?.schedule) ? w.payment.schedule : [];
        const firstInst = sched[0];
        const instAmt = firstInst?.amount || Math.round(pkgPrice / 3);
        const firstPaid = firstInst?.status === 'Paid';
        payDetailText = [
            `• รูปแบบ: แบ่งจ่าย 3 งวด`,
            `• ยอดต่องวด: ${instAmt.toLocaleString()} บาท/งวด`,
            `• สถานะงวดที่ 1: ${firstPaid ? 'ชำระงวดที่ 1 แล้ว' : 'รอชำระงวดที่ 1'}`
        ].join('\n');
    } else if (payMethod === 'finance') {
        const finProvider = w.financeDetails?.provider || '-';
        const finMonths = w.financeDetails?.financeMonths || '-';
        const finDueDay = w.financeDetails?.financeDueDay ? `ทุกวันที่ ${w.financeDetails.financeDueDay} ของเดือน` : '-';
        const finMonthly = getFinanceMonthlyAmount(w);
        const finMonthlyStr = finMonthly ? `${finMonthly.toLocaleString()} บาท/งวด` : '-';
        payDetailText = [
            `• รูปแบบ: ผ่อนด้วยไฟแนนซ์ (${finProvider})`,
            `• ยอดต่องวด: ${finMonthlyStr}`,
            `• ระยะเวลาผ่อน: ${finMonths} เดือน (${finDueDay})`,
            `• เงินดาวน์: 0 บาท (ผ่อนชำระกับไฟแนนซ์)`
        ].join('\n');
    } else {
        payDetailText = `• รูปแบบ: ${payMethod}`;
    }

    // Additional info: Second-hand checklist
    let conditionDetail = devCondition;
    if (w.device?.deviceCondition === 'Second-hand' && inspectCount > 0) {
        conditionDetail += ` (ตรวจสภาพผ่าน ${inspectCount} รายการ)`;
    }

    // Additional Note for resubmissions
    let resubmitNote = '';
    if (isResubmit && w.rejectReason) {
        resubmitNote = `⚠️ หมายเหตุที่เคยให้แก้ไข: ${w.rejectReason}`;
    }

    const lines = [
        headerTitle,
        `━━━━━━━━━━━━━━━━━━━`,
        `📋 ข้อมูลสัญญา`,
        `• เลขที่กรมธรรม์: ${w.policyNumber || '-'}`,
        `• วันที่-เวลาสร้าง: ${createdStr}`,
        `• ร้านค้าที่ทำรายการ: ${w.shopName || '-'}`,
        `• พนักงานทำรายการ: ${w.staffName || '-'}`,
        ``,
        `👤 ข้อมูลลูกค้า`,
        `• ชื่อลูกค้า: ${custName}`,
        `• เบอร์โทรติดต่อ: ${custPhone}`,
        `• รหัสสมาชิก: ${memberId}`,
        ``,
        `📱 ข้อมูลตัวเครื่อง`,
        `• รุ่นเครื่อง: ${fullModel}`,
        `• สเปก: ${devSpecs}`,
        `• สภาพเครื่อง: ${conditionDetail}`,
        `• ราคาตัวเครื่อง: ${actualDevicePriceStr}`,
        `• Serial Number: ${serial}`,
        `• IMEI: ${imei}`,
        imgCount > 0 ? `• รูปภาพแนบ: ${imgCount} รูป` : null,
        ``,
        `🛡️ รายละเอียดแพ็กเกจ`,
        `• แพ็กเกจ: ${plan}`,
        `• ราคาแพ็กเกจ: ${pkgPriceStr}`,
        `• วงเงินคุ้มครองสูงสุด: ${maxCoverageStr}`,
        `• ระยะเวลาคุ้มครอง: ${coveragePeriod}`,
        ``,
        `💳 รูปแบบการชำระเงิน`,
        payDetailText,
        resubmitNote ? `\n${resubmitNote}` : null,
        `━━━━━━━━━━━━━━━━━━━`,
        `👉 กรุณาเข้าสู่ระบบ EasyCare เพื่อตรวจสอบและอนุมัติสัญญา`
    ].filter(line => line !== null);

    return lines.join('\n');
}

/**
 * Send pending approval notification to LINE group
 * @param {object} warranty - Warranty record
 * @param {object} [options] - Options (e.g. isResubmit: true, force: true)
 */
async function notifyWarrantyPending(warranty, options = {}) {
    if (!isLineNotificationsEnabled && !options.force) {
        console.log(`⏸️ [LINE Notify] ข้ามการแจ้งเตือนสัญญา (${warranty?.policyNumber}) เนื่องจากปิดการแจ้งเตือน LINE ชั่วคราว`);
        return { success: false, skipped: true, message: 'LINE notifications disabled' };
    }

    try {
        const messageText = formatWarrantyPendingMessage(warranty, options);
        console.log(`\n🔔 [LINE Notify] Sending pending warranty notification (Policy: ${warranty.policyNumber})...`);
        const result = await sendLineMessage(messageText);
        console.log(`✅ [LINE Notify] Notification sent successfully for Policy: ${warranty.policyNumber}`);
        return result;
    } catch (err) {
        console.error(`⚠️ [LINE Notify] Failed to send warranty pending notification (Policy: ${warranty?.policyNumber}):`, err.message);
        return { success: false, error: err.message };
    }
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

module.exports = {
    sendLineMessage,
    formatWarrantyPendingMessage,
    notifyWarrantyPending,
    getFinanceMonthlyAmount,
    defaultFinanceRates,
    setLineNotificationsEnabled,
    getLineNotificationsEnabled
};
