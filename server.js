require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const http = require('http');
const { Server } = require('socket.io');
const ExcelJS = require('exceljs');

const app = express();
const PORT = process.env.PORT || 3005;

let io;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname)));

// Cloudinary Config
const cloudinary = require('cloudinary').v2;
const { CloudinaryStorage } = require('multer-storage-cloudinary');

cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET
});

const storage = new CloudinaryStorage({
    cloudinary: cloudinary,
    params: {
        folder: 'easycare/claim', // Folder name in Cloudinary
        allowed_formats: ['jpg', 'png', 'jpeg'],
        // transformation: [{ width: 500, height: 500, crop: 'limit' }] // Optional: Resize
    },
});

const claimUpload = multer({ storage: storage });

const genericStorage = new CloudinaryStorage({
    cloudinary: cloudinary,
    params: {
        folder: 'easycare/finance',
        allowed_formats: ['jpg', 'png', 'jpeg', 'pdf']
    },
});

const genericUpload = multer({ storage: genericStorage });

async function expireOverdueInstallments() {
    const now = new Date();
    const overdueCutoff = new Date(Date.now() - (5 * 24 * 60 * 60 * 1000));
    await Warranty.updateMany(
        {
            'payment.method': 'Installment',
            'payment.schedule': {
                $elemMatch: {
                    status: 'Pending',
                    dueDate: { $lt: overdueCutoff }
                }
            },
            'warrantyDates.end': { $gte: now }
        },
        { $set: { 'warrantyDates.end': new Date(now.getTime() - 1000) } }
    );
}

async function getMemberBlacklistReasonsByMemberId(memberId) {
    const cutoff = new Date(Date.now() - (5 * 24 * 60 * 60 * 1000));
    const warranties = await Warranty.find({
        memberId: String(memberId),
        'payment.method': 'Installment',
        'payment.schedule': {
            $elemMatch: {
                status: 'Pending',
                dueDate: { $lt: cutoff }
            }
        }
    })
        .select({ memberId: 1, policyNumber: 1, payment: 1 })
        .lean();

    const reasons = [];
    for (const w of warranties) {
        const schedule = (w && w.payment && Array.isArray(w.payment.schedule)) ? w.payment.schedule : [];
        for (const s of schedule) {
            const due = s && s.dueDate ? new Date(s.dueDate) : null;
            if (!due) continue;
            if (s.status === 'Pending' && due < cutoff) {
                const daysOverdue = Math.floor((Date.now() - due.getTime()) / 86400000);
                reasons.push({
                    type: 'installment_overdue',
                    policyNumber: w.policyNumber || '-',
                    installmentNo: s.installmentNo,
                    dueDate: s.dueDate,
                    daysOverdue
                });
            }
        }
    }

    return reasons;
}

// MongoDB Connection
mongoose.connect(process.env.MONGO_URI)
    .then(() => {
        console.log('Connected to MongoDB Atlas (Cloudinary Enabled)');
        // Drop unique index on memberId if it exists (to allow multi-package per member)
        mongoose.connection.collection('warranties').dropIndex('memberId_1').catch(err => {
            // Ignore error if index doesn't exist
            if (err.code !== 27) console.log('MemberId index already cleaned or not found');
        });
    })
    .catch(err => console.error('MongoDB connection error:', err));

// Mongoose Schema
const WarrantySchema = new mongoose.Schema({
    policyNumber: { type: String, unique: true, index: true },
    memberId: { type: String, index: true },
    shopName: String,
    protectionType: String,
    staffName: String,
    devicePrice: Number,
    installmentsPaid: { type: Number, default: 1 },
    usedCoverage: { type: Number, default: 0 },
    customer: {
        firstName: String,
        lastName: String,
        phone: String,
        dob: Date,
        age: Number,
        address: String
    },
    device: {
        type: { type: String }, // 'type' is a reserved keyword in some contexts, but works in nested objects
        model: String,
        color: String,
        capacity: String,
        serial: String,
        imei: String,
        deviceValue: Number,
        officialWarrantyEnd: Date
    },
    package: {
        plan: String,
        price: Number
    },
    warrantyDates: {
        start: Date,
        end: Date
    },
    payment: {
        method: String,
        status: { type: String, default: 'Pending' },
        paidDate: Date,
        paidCash: Number,
        paidTransfer: Number,
        refId: String,
        schedule: [{
            installmentNo: Number,
            amount: Number,
            dueDate: Date,
            graceDate: Date,
            status: { type: String, default: 'Pending' },
            paidDate: Date,
            paidCash: Number,
            paidTransfer: Number,
            refId: String
        }]
    },
    approvalStatus: {
        type: String,
        enum: ['pending', 'approved', 'rejected'],
        default: 'pending'
    },
    approver: String,
    approvalDate: Date,
    rejectReason: String,
    rejectBy: String,
    rejectDate: Date,
    claimStatus: { type: String, default: 'normal', enum: ['normal', 'pending', 'completed'] }
}, { timestamps: true });

WarrantySchema.virtual('maxLimit').get(function () {
    const basePrice = Number(this.devicePrice ?? this.device?.deviceValue ?? 0);
    return Math.floor(basePrice * 0.70);
});

WarrantySchema.virtual('currentLimit').get(function () {
    const maxLimit = Number(this.maxLimit ?? 0);
    const paid = Number(this.installmentsPaid ?? 1);
    if (paid >= 3) return Math.floor(maxLimit * 1.0);
    if (paid === 2) return Math.floor(maxLimit * 0.30);
    return Math.floor(maxLimit * 0.10);
});

WarrantySchema.virtual('remainingLimit').get(function () {
    const used = Number(this.usedCoverage ?? 0);
    const current = Number(this.currentLimit ?? 0);
    return current - used;
});

WarrantySchema.set('toJSON', { virtuals: true });
WarrantySchema.set('toObject', { virtuals: true });

const Warranty = mongoose.model('Warranty', WarrantySchema);

async function expireWarrantyIfNoRemaining(warrantyId) {
    if (!warrantyId) return;
    const w = await Warranty.findById(warrantyId);
    if (!w) return;
    const remaining = Number(w.remainingLimit ?? 0);
    if (Number.isFinite(remaining) && remaining <= 0) {
        await Warranty.findByIdAndUpdate(w._id, {
            'warrantyDates.end': new Date(),
            claimStatus: 'completed'
        });
    }
}

// Member Schema
const MemberSchema = new mongoose.Schema({
    memberId: { type: String, unique: true, index: true, required: true },
    citizenId: { type: String, unique: true, index: true },
    prefix: { type: String },
    firstName: { type: String, required: true },
    lastName: { type: String, required: true },
    firstNameEn: { type: String },
    lastNameEn: { type: String },
    phone: { type: String, unique: true, index: true, required: true },
    birthdate: { type: Date },
    gender: { type: String },
    address: { type: String },
    idCardAddress: { type: String },
    shippingAddress: { type: String },
    postalCode: { type: String },
    issueDate: { type: Date },
    expiryDate: { type: Date },
    facebook: { type: String },
    facebookLink: { type: String },
    photo: { type: String } // Base64 encoded image string
}, { timestamps: true });

const Member = mongoose.model('Member', MemberSchema);

// Shop Schema
const ShopSchema = new mongoose.Schema({
    shopId: { type: String, unique: true, index: true, required: true },
    shopName: { type: String, required: true },
    location: { type: String }
}, { timestamps: true });
const Shop = mongoose.model('Shop', ShopSchema);

// Staff Schema
const StaffSchema = new mongoose.Schema({
    staffId: { type: String, unique: true },
    staffName: String,
    staffPosition: String,
    username: { type: String, unique: true, index: true },
    password: { type: String, required: true },
    role: { type: String, enum: ['sales', 'approver', 'admin'], default: 'sales' }
}, { timestamps: true });

const Staff = mongoose.model('Staff', StaffSchema);

// Claim Schema
const ClaimSchema = new mongoose.Schema({
    claimId: { type: String, unique: true, index: true },
    warrantyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warranty' },
    policyNumber: String,
    memberId: String,
    claimShopName: String,
    customerName: String,
    customerPhone: String,
    deviceModel: String,
    devicePowerState: { type: String, enum: ['on', 'off'], default: 'on' },
    imei: String,
    serialNumber: String,
    color: String,
    claimDate: { type: Date, default: Date.now },
    symptoms: String,
    images: [String],
    staffName: String,
    returnMethod: { type: String, enum: ['pickup', 'delivery'] },
    pickupBranch: String,
    deliveryAddressType: { type: String, enum: ['card', 'memberShipping', 'new', 'original'] },
    deliveryAddressDetail: String,
    customerSignature: String,
    staffSignature: String,
    managerSignature: String,
    status: { type: String, default: 'รอเคลม', enum: ['รอเคลม', 'รับเครื่องแล้ว', 'รอการตัดสินใจจากลูกค้า', 'ลูกค้าสละสิทธิ์เครื่อง'] },
    totalCost: { type: Number, default: 0 },
    excessCost: { type: Number, default: 0 },
    refundAmount: { type: Number, default: 0 },
    customerDecision: { type: String, default: '' },
    completedReturnMethod: { type: String, enum: ['pickup', 'delivery'] },
    completedReturnBranch: String,
    completedDeliveryAddressType: { type: String, enum: ['card', 'memberShipping', 'new', 'original'] },
    completedDeliveryAddressDetail: String,
    pickupDate: Date,
    updates: [{
        step: Number,
        title: String,
        date: { type: Date, default: Date.now },
        cost: { type: Number, default: 0 },
        centerName: { type: String, default: '' },
        centerLocation: { type: String, default: '' },
        centerPhone: { type: String, default: '' },
        technicianName: { type: String, default: '' },
        technicianPhone: { type: String, default: '' },
        images: [String],
        evidenceImages: [String]
    }],
    deviceCondition: {
        exterior: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        screen: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        assembly: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        appleLogo: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        buttons: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        chargingPort: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        simTray: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        imeiMatch: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        modelMatch: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        screenTouch: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        faceIdTouchId: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        cameras: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        speakerMic: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        connectivity: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        battery: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        warrantyVoid: { status: { type: String, default: '' }, reason: { type: String, default: '' } },
        other: { status: { type: String, default: '' }, reason: { type: String, default: '' } }
    }
}, { timestamps: true });

const Claim = mongoose.model('Claim', ClaimSchema);

// FinanceTransaction Schema
const FinanceTransactionSchema = new mongoose.Schema({
    transactionDate: { type: Date, default: Date.now },
    policyNumber: { type: String, index: true },
    customerName: String,
    actionType: String,
    paymentMethod: String,
    cashReceived: { type: Number, default: 0 },
    transferAmount: { type: Number, default: 0 },
    changeAmount: { type: Number, default: 0 },
    netTotal: { type: Number, default: 0 },
    evidenceUrl: String,
    recordedBy: String
}, { timestamps: true });

const FinanceTransaction = mongoose.model('FinanceTransaction', FinanceTransactionSchema);

// ═══════════════════════════════════════════════════════════════════
// FILTER HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════

function buildExpenseFilterMatch(query) {
    const match = {};
    const { search, startDate, endDate } = query || {};

    if (search) {
        const regex = { $regex: String(search), $options: 'i' };
        match.$or = [
            { claimId: regex },
            { customerName: regex },
            { customerPhone: regex },
            { policyNumber: regex },
            { deviceModel: regex }
        ];
    }

    if (startDate) {
        match.__expenseDate = { ...(match.__expenseDate || {}), $gte: new Date(String(startDate)) };
    }
    if (endDate) {
        match.__expenseDate = { ...(match.__expenseDate || {}), $lte: new Date(String(endDate) + 'T23:59:59.999Z') };
    }

    return match;
}

// Build dynamic $match for Warranty queries from query params
function buildWarrantyFilterMatch(query, baseMatch = {}) {
    const match = { ...baseMatch };
    const { search, startDate, endDate } = query;

    if (search) {
        const regex = { $regex: search, $options: 'i' };
        match.$or = [
            { 'customer.firstName': regex },
            { 'customer.lastName': regex },
            { 'customer.phone': regex },
            { policyNumber: regex },
            { memberId: regex },
            { 'device.imei': regex },
            { 'device.serial': regex }
        ];
    }
    if (startDate) {
        match.createdAt = { ...(match.createdAt || {}), $gte: new Date(startDate) };
    }
    if (endDate) {
        match.createdAt = { ...(match.createdAt || {}), $lte: new Date(endDate + 'T23:59:59.999Z') };
    }
    return match;
}

// Build dynamic $match for Claim queries from query params
function buildClaimFilterMatch(query, baseMatch = {}) {
    const match = { ...baseMatch };
    const { search, startDate, endDate } = query;

    if (search) {
        const regex = { $regex: search, $options: 'i' };
        match.$or = [
            { customerName: regex },
            { customerPhone: regex },
            { claimId: regex },
            { policyNumber: regex },
            { imei: regex },
            { deviceModel: regex }
        ];
    }
    if (startDate) {
        match.claimDate = { ...(match.claimDate || {}), $gte: new Date(startDate) };
    }
    if (endDate) {
        match.claimDate = { ...(match.claimDate || {}), $lte: new Date(endDate + 'T23:59:59.999Z') };
    }
    return match;
}

// API Routes


app.post('/api/public/customer/portal', async (req, res) => {
    try {
        const { idCard, memberId } = req.body || {};
        if (!idCard || !memberId) {
            return res.status(400).json({ success: false, message: 'กรุณาระบุเลขบัตรประชาชนและรหัสสมาชิก' });
        }

        try {
            await expireOverdueInstallments();
        } catch (e) {
            console.error('expireOverdueInstallments failed:', e);
        }

        const member = await Member.findOne({ citizenId: idCard, memberId }).lean();
        if (!member) {
            return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลสมาชิก' });
        }

        const warranties = await Warranty.find({ memberId: member.memberId }).sort({ createdAt: -1 }).lean();
        const warrantyIds = warranties.map(w => w._id);

        const claims = warrantyIds.length
            ? await Claim.find({ warrantyId: { $in: warrantyIds } }).sort({ createdAt: -1 }).lean()
            : [];

        return res.json({ success: true, member, warranties, claims });
    } catch (err) {
        return res.status(500).json({ success: false, message: err.message });
    }
});

app.get('/api/finance/expenses', async (req, res) => {
    try {
        const baseMatch = buildExpenseFilterMatch(req.query);

        const pipeline = [
            {
                $project: {
                    claimId: 1,
                    policyNumber: 1,
                    customerName: 1,
                    customerPhone: 1,
                    deviceModel: 1,
                    claimShopName: 1,
                    claimDate: 1,
                    totalCost: 1,
                    updates: 1
                }
            },
            {
                $facet: {
                    updateExpenses: [
                        { $unwind: { path: '$updates', preserveNullAndEmptyArrays: false } },
                        {
                            $addFields: {
                                __expenseDate: '$updates.date',
                                __expenseAmount: { $ifNull: ['$updates.cost', 0] }
                            }
                        },
                        { $match: { __expenseAmount: { $gt: 0 } } },
                        ...(Object.keys(baseMatch).length > 0 ? [{ $match: baseMatch }] : []),
                        {
                            $project: {
                                _id: 0,
                                expenseDate: '$__expenseDate',
                                claimId: 1,
                                policyNumber: 1,
                                customerName: 1,
                                deviceModel: 1,
                                claimShopName: 1,
                                expenseTitle: { $ifNull: ['$updates.title', 'ค่าใช้จ่าย'] },
                                amount: '$__expenseAmount',
                                centerName: { $ifNull: ['$updates.centerName', ''] }
                            }
                        }
                    ]
                }
            },
            {
                $project: {
                    expenses: { $concatArrays: ['$updateExpenses'] }
                }
            },
            { $unwind: { path: '$expenses', preserveNullAndEmptyArrays: true } },
            { $replaceRoot: { newRoot: '$expenses' } },
            { $sort: { expenseDate: -1 } }
        ];

        const rows = await Claim.aggregate(pipeline);
        res.json(Array.isArray(rows) ? rows : []);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

app.get('/api/finance/expenses/summary', async (req, res) => {
    try {
        const baseMatch = buildExpenseFilterMatch(req.query);

        const pipeline = [
            {
                $project: {
                    claimId: 1,
                    policyNumber: 1,
                    customerName: 1,
                    customerPhone: 1,
                    deviceModel: 1,
                    claimShopName: 1,
                    claimDate: 1,
                    totalCost: 1,
                    updates: 1
                }
            },
            {
                $facet: {
                    updateAgg: [
                        { $unwind: { path: '$updates', preserveNullAndEmptyArrays: false } },
                        {
                            $addFields: {
                                __expenseDate: '$updates.date',
                                __expenseAmount: { $ifNull: ['$updates.cost', 0] }
                            }
                        },
                        { $match: { __expenseAmount: { $gt: 0 } } },
                        ...(Object.keys(baseMatch).length > 0 ? [{ $match: baseMatch }] : []),
                        { $group: { _id: null, totalExpense: { $sum: '$__expenseAmount' } } }
                    ],
                    totalCostAgg: [
                        {
                            $addFields: {
                                __expenseDate: '$claimDate',
                                __expenseAmount: { $ifNull: ['$totalCost', 0] }
                            }
                        },
                        { $match: { __expenseAmount: { $gt: 0 } } },
                        ...(Object.keys(baseMatch).length > 0 ? [{ $match: baseMatch }] : []),
                        { $group: { _id: null, totalExpense: { $sum: '$__expenseAmount' } } }
                    ]
                }
            },
            {
                $project: {
                    totalExpense: {
                        $add: [
                            { $ifNull: [{ $arrayElemAt: ['$updateAgg.totalExpense', 0] }, 0] },
                            { $ifNull: [{ $arrayElemAt: ['$totalCostAgg.totalExpense', 0] }, 0] }
                        ]
                    }
                }
            }
        ];

        const rows = await Claim.aggregate(pipeline);
        const totalExpense = rows && rows[0] ? Number(rows[0].totalExpense || 0) : 0;
        res.json({ totalExpense });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

app.get('/api/finance/expenses/export/excel', async (req, res) => {
    try {
        const { startDate, endDate } = req.query || {};
        const baseMatch = buildExpenseFilterMatch(req.query);

        const pipeline = [
            {
                $project: {
                    claimId: 1,
                    policyNumber: 1,
                    customerName: 1,
                    customerPhone: 1,
                    deviceModel: 1,
                    claimShopName: 1,
                    claimDate: 1,
                    totalCost: 1,
                    updates: 1
                }
            },
            {
                $facet: {
                    updateExpenses: [
                        { $unwind: { path: '$updates', preserveNullAndEmptyArrays: false } },
                        {
                            $addFields: {
                                __expenseDate: '$updates.date',
                                __expenseAmount: { $ifNull: ['$updates.cost', 0] }
                            }
                        },
                        { $match: { __expenseAmount: { $gt: 0 } } },
                        ...(Object.keys(baseMatch).length > 0 ? [{ $match: baseMatch }] : []),
                        {
                            $project: {
                                _id: 0,
                                expenseDate: '$__expenseDate',
                                claimId: 1,
                                policyNumber: 1,
                                customerName: 1,
                                deviceModel: 1,
                                claimShopName: 1,
                                expenseTitle: { $ifNull: ['$updates.title', 'ค่าใช้จ่าย'] },
                                amount: '$__expenseAmount',
                                centerName: { $ifNull: ['$updates.centerName', ''] }
                            }
                        }
                    ]
                }
            },
            {
                $project: {
                    expenses: { $concatArrays: ['$updateExpenses'] }
                }
            },
            { $unwind: { path: '$expenses', preserveNullAndEmptyArrays: true } },
            { $replaceRoot: { newRoot: '$expenses' } },
            { $sort: { expenseDate: -1 } }
        ];

        const rows = await Claim.aggregate(pipeline);

        const workbook = new ExcelJS.Workbook();
        workbook.creator = 'EasyCare';
        workbook.created = new Date();

        const ws = workbook.addWorksheet('Claim Expenses');
        ws.columns = [
            { header: 'วันที่ทำรายการ', key: 'expenseDate', width: 22 },
            { header: 'เลขที่เคลม', key: 'claimId', width: 14 },
            { header: 'เลขกรมธรรม์', key: 'policyNumber', width: 16 },
            { header: 'ลูกค้า', key: 'customerName', width: 22 },
            { header: 'สินค้า', key: 'deviceModel', width: 18 },
            { header: 'ร้านค้า', key: 'claimShopName', width: 18 },
            { header: 'รายการ', key: 'expenseTitle', width: 20 },
            { header: 'สถานที่', key: 'centerName', width: 18 },
            { header: 'จำนวนเงิน', key: 'amount', width: 14 }
        ];
        ws.getRow(1).font = { bold: true };

        for (const r of (Array.isArray(rows) ? rows : [])) {
            ws.addRow({
                expenseDate: r && r.expenseDate ? new Date(r.expenseDate) : null,
                claimId: (r && r.claimId) || '',
                policyNumber: (r && r.policyNumber) || '',
                customerName: (r && r.customerName) || '',
                deviceModel: (r && r.deviceModel) || '',
                claimShopName: (r && r.claimShopName) || '',
                expenseTitle: (r && r.expenseTitle) || '',
                centerName: (r && r.centerName) || '',
                amount: Number((r && r.amount) || 0)
            });
        }

        ws.getColumn('expenseDate').numFmt = 'dd/mm/yyyy hh:mm';
        ws.getColumn('amount').numFmt = '#,##0.00';

        const safeStart = startDate ? String(startDate) : '';
        const safeEnd = endDate ? String(endDate) : '';
        const fileName = `claim_expenses_${safeStart || 'all'}_${safeEnd || 'all'}.xlsx`;

        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
        await workbook.xlsx.write(res);
        res.end();
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

app.get('/api/dashboard/sales/overdue-claims', async (req, res) => {
    try {
        const cutoff = new Date(Date.now() - (5 * 24 * 60 * 60 * 1000));

        const overdue = await Claim.find({
            updatedAt: { $lt: cutoff },
            status: { $nin: ['รับเครื่องแล้ว', 'เสร็จสิ้น', 'ลูกค้ามารับเครื่องแล้ว'] }
        })
            .sort({ updatedAt: 1 })
            .lean();

        res.json({ items: overdue });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

app.get('/api/dashboard/sales/summary', async (req, res) => {
    try {
        const cutoff = new Date(Date.now() - (5 * 24 * 60 * 60 * 1000));
        const now = new Date();
        const overdueInstallmentCutoff = new Date(Date.now() - (5 * 24 * 60 * 60 * 1000));

        const [overdueClaims, pendingApprovals, unpaidPackages, installmentOverdue] = await Promise.all([
            Claim.countDocuments({
                updatedAt: { $lt: cutoff },
                status: { $nin: ['รับเครื่องแล้ว', 'เสร็จสิ้น', 'ลูกค้ามารับเครื่องแล้ว'] }
            }),
            Warranty.countDocuments({ approvalStatus: 'pending' }),
            Warranty.countDocuments({
                'payment.method': 'Full Payment',
                'payment.status': { $ne: 'Paid' }
            }),
            Warranty.countDocuments({
                'payment.method': 'Installment',
                'payment.schedule': {
                    $elemMatch: {
                        status: 'Pending',
                        dueDate: { $lt: overdueInstallmentCutoff }
                    }
                }
            })
        ]);

        return res.json({
            overdueClaims,
            pendingApprovals,
            unpaidPackages,
            installmentOverdue
        });
    } catch (err) {
        return res.status(500).json({ message: err.message });
    }
});

app.get('/api/dashboard/approver/pending-warranties', async (req, res) => {
    try {
        const items = await Warranty.find({ approvalStatus: 'pending' })
            .sort({ createdAt: -1 })
            .lean();

        res.json({ count: items.length, items });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Staff Registration
app.post('/api/register', async (req, res) => {
    try {
        const { staffName, staffPosition, username, password, role } = req.body;
        console.log('Registering staff:', { staffName, staffPosition, username, role });

        // Check if username exists
        const existingStaff = await Staff.findOne({ username });
        if (existingStaff) {
            return res.status(400).json({ success: false, message: 'Username already exists' });
        }

        const staffId = 'STF' + Math.floor(Math.random() * 1000).toString().padStart(3, '0');
        const newStaff = new Staff({ staffId, staffName, staffPosition, username, password, role: role || 'sales' });
        await newStaff.save();

        res.status(201).json({ success: true, user: { staffName, staffId, staffPosition } });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Login (Database-backed)
app.post('/api/login', async (req, res) => {
    try {
        const { username, password } = req.body;

        // Find staff in database
        const staff = await Staff.findOne({ username, password });

        if (staff) {
            res.json({
                success: true,
                user: { staffName: staff.staffName, staffId: staff.staffId, staffPosition: staff.staffPosition, role: staff.role }
            });
        } else {
            // Fallback for admin if no staff exists yet (optional, but keep for convenience as per requirement)
            if (username === 'admin' && password === '1234') {
                return res.json({
                    success: true,
                    user: { staffName: 'Admin', staffId: 'STF000', role: 'admin' }
                });
            }
            res.status(401).json({ success: false, message: 'Invalid credentials' });
        }
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Middleware to check Admin Role (simplified for this context)
const checkAdminRole = (req, res, next) => {
    // In a real app we'd use JWT. Here, since it's a simple app, we can expect the role in headers
    const userRole = req.headers['x-user-role'];
    if (userRole !== 'admin') {
        return res.status(403).json({ success: false, message: 'Forbidden: Admin access required' });
    }
    next();
};

// ═══════════════════════════════════════════════════════════════════
// EXECUTIVE DASHBOARD (Admin Only)
// ═══════════════════════════════════════════════════════════════════

app.get('/api/dashboard/stats', checkAdminRole, async (req, res) => {
    try {
        const { startDate, endDate, staff } = req.query || {};

        const warrantyMatch = {};
        const claimMatch = {};

        if (staff) {
            warrantyMatch.staffName = String(staff);
            claimMatch.staffName = String(staff);
        }

        if (startDate) {
            warrantyMatch.createdAt = { ...(warrantyMatch.createdAt || {}), $gte: new Date(startDate) };
            claimMatch.claimDate = { ...(claimMatch.claimDate || {}), $gte: new Date(startDate) };
        }
        if (endDate) {
            const end = new Date(endDate + 'T23:59:59.999Z');
            warrantyMatch.createdAt = { ...(warrantyMatch.createdAt || {}), $lte: end };
            claimMatch.claimDate = { ...(claimMatch.claimDate || {}), $lte: end };
        }

        const now = new Date();

        const [revenueAgg, claimCostAgg, activeAgg, overdueAgg, packagesAgg, claimStatusAgg, warrantyTrendAgg, claimTrendAgg] = await Promise.all([
            Warranty.aggregate([
                { $match: warrantyMatch },
                {
                    $group: {
                        _id: null,
                        totalRevenue: { $sum: { $ifNull: ['$package.price', 0] } }
                    }
                },
                { $project: { _id: 0, totalRevenue: 1 } }
            ]),
            Claim.aggregate([
                { $match: claimMatch },
                {
                    $group: {
                        _id: null,
                        totalClaimCost: { $sum: { $ifNull: ['$totalCost', 0] } }
                    }
                },
                { $project: { _id: 0, totalClaimCost: 1 } }
            ]),
            Warranty.aggregate([
                {
                    $match: {
                        ...warrantyMatch,
                        'warrantyDates.end': { $gte: now }
                    }
                },
                { $count: 'activeWarranties' }
            ]),
            Claim.aggregate([
                {
                    $match: {
                        ...claimMatch,
                        status: 'รอเคลม'
                    }
                },
                {
                    $addFields: {
                        lastUpdateDate: {
                            $let: {
                                vars: { lastUpdate: { $arrayElemAt: ['$updates', -1] } },
                                in: {
                                    $ifNull: ['$$lastUpdate.date', { $ifNull: ['$claimDate', '$createdAt'] }]
                                }
                            }
                        }
                    }
                },
                {
                    $addFields: {
                        daysSinceUpdate: {
                            $floor: {
                                $divide: [{ $subtract: ['$$NOW', '$lastUpdateDate'] }, 86400000]
                            }
                        }
                    }
                },
                { $match: { daysSinceUpdate: { $gte: 5 } } },
                { $count: 'overdueClaims' }
            ]),
            Warranty.aggregate([
                { $match: warrantyMatch },
                {
                    $group: {
                        _id: { $ifNull: ['$package.plan', 'ไม่ระบุแพ็กเกจ'] },
                        count: { $sum: 1 }
                    }
                },
                { $project: { _id: 0, plan: '$_id', count: 1 } },
                { $sort: { count: -1, plan: 1 } }
            ]),
            Claim.aggregate([
                { $match: claimMatch },
                { $group: { _id: { $ifNull: ['$status', 'ไม่ระบุสถานะ'] }, count: { $sum: 1 } } },
                { $project: { _id: 0, status: '$_id', count: 1 } },
                { $sort: { count: -1, status: 1 } }
            ]),
            Warranty.aggregate([
                { $match: warrantyMatch },
                {
                    $group: {
                        _id: {
                            year: { $year: '$createdAt' },
                            month: { $month: '$createdAt' }
                        },
                        revenue: { $sum: { $ifNull: ['$package.price', 0] } }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        year: '$_id.year',
                        month: '$_id.month',
                        revenue: 1
                    }
                },
                { $sort: { year: 1, month: 1 } }
            ]),
            Claim.aggregate([
                { $match: claimMatch },
                {
                    $group: {
                        _id: {
                            year: { $year: '$claimDate' },
                            month: { $month: '$claimDate' }
                        },
                        claimCost: { $sum: { $ifNull: ['$totalCost', 0] } }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        year: '$_id.year',
                        month: '$_id.month',
                        claimCost: 1
                    }
                },
                { $sort: { year: 1, month: 1 } }
            ])
        ]);

        const totalRevenue = Number(revenueAgg?.[0]?.totalRevenue || 0);
        const totalClaimCost = Number(claimCostAgg?.[0]?.totalClaimCost || 0);
        const activeWarranties = Number(activeAgg?.[0]?.activeWarranties || 0);
        const overdueClaims = Number(overdueAgg?.[0]?.overdueClaims || 0);

        const trendMap = new Map();
        (Array.isArray(warrantyTrendAgg) ? warrantyTrendAgg : []).forEach(r => {
            const key = `${r.year}-${String(r.month).padStart(2, '0')}`;
            trendMap.set(key, { month: key, revenue: Number(r.revenue || 0), claimCost: 0 });
        });
        (Array.isArray(claimTrendAgg) ? claimTrendAgg : []).forEach(r => {
            const key = `${r.year}-${String(r.month).padStart(2, '0')}`;
            const existing = trendMap.get(key) || { month: key, revenue: 0, claimCost: 0 };
            existing.claimCost = Number(r.claimCost || 0);
            trendMap.set(key, existing);
        });
        const trend = Array.from(trendMap.values()).sort((a, b) => a.month.localeCompare(b.month));

        return res.json({
            success: true,
            kpi: { totalRevenue, totalClaimCost, activeWarranties, overdueClaims },
            charts: {
                trend,
                packages: Array.isArray(packagesAgg) ? packagesAgg : [],
                claimStatus: Array.isArray(claimStatusAgg) ? claimStatusAgg : []
            }
        });
    } catch (err) {
        return res.status(500).json({ success: false, message: err.message });
    }
});

// ═══════════════════════════════════════════════════════════════════
// STAFF CRUD API ROUTES (Admin Only)
// ═══════════════════════════════════════════════════════════════════

// Get all staff
app.get('/api/staff', checkAdminRole, async (req, res) => {
    try {
        const staff = await Staff.find({}, { password: 0 }).sort({ createdAt: -1 });
        res.json(staff);
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Create new staff
app.post('/api/staff', checkAdminRole, async (req, res) => {
    try {
        const { username, password, staffName, role } = req.body;

        const existingStaff = await Staff.findOne({ username });
        if (existingStaff) {
            return res.status(400).json({ success: false, message: 'Username already exists' });
        }

        const staffId = 'STF' + Math.floor(Math.random() * 1000).toString().padStart(3, '0');
        // Derive staffPosition from role for backward compatibility
        let staffPosition = 'เจ้าหน้าที่';
        if (role === 'admin') staffPosition = 'ผู้ดูแลระบบ';
        else if (role === 'approver') staffPosition = 'ผู้อนุมัติ';
        else staffPosition = 'พนักงานขาย';

        const newStaff = new Staff({ staffId, staffName, staffPosition, username, password, role });
        await newStaff.save();

        const insertedStaff = await Staff.findById(newStaff._id, { password: 0 });
        res.status(201).json({ success: true, staff: insertedStaff });
    } catch (err) {
        res.status(400).json({ success: false, message: err.message });
    }
});

// Update staff
app.put('/api/staff/:id', checkAdminRole, async (req, res) => {
    try {
        const { staffName, role, password } = req.body;

        // Build update object
        const updateData = { staffName, role };

        if (role === 'admin') updateData.staffPosition = 'ผู้ดูแลระบบ';
        else if (role === 'approver') updateData.staffPosition = 'ผู้อนุมัติ';
        else updateData.staffPosition = 'พนักงานขาย';

        if (password && password.trim() !== '') {
            updateData.password = password;
        }

        const updatedStaff = await Staff.findByIdAndUpdate(
            req.params.id,
            updateData,
            { new: true, runValidators: true, select: '-password' }
        );

        if (!updatedStaff) return res.status(404).json({ success: false, message: 'Staff not found' });

        res.json({ success: true, staff: updatedStaff });
    } catch (err) {
        res.status(400).json({ success: false, message: err.message });
    }
});

// Delete staff
app.delete('/api/staff/:id', checkAdminRole, async (req, res) => {
    try {
        const deletedStaff = await Staff.findByIdAndDelete(req.params.id);
        if (!deletedStaff) return res.status(404).json({ success: false, message: 'Staff not found' });
        res.json({ success: true, message: 'Staff deleted successfully' });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Get all warranties (Enriched with Member Data)
app.get('/api/warranties', async (req, res) => {
    try {
        try {
            await expireOverdueInstallments();
        } catch (e) {
            console.error('expireOverdueInstallments failed:', e);
        }

        // Build dynamic filter from query params
        const filterMatch = buildWarrantyFilterMatch(req.query);

        // Handle status filter for dashboard
        const dashStatus = req.query.status;
        if (dashStatus && dashStatus !== 'all') {
            const now = new Date();
            if (dashStatus === 'active') {
                filterMatch.approvalStatus = 'approved';
                filterMatch['warrantyDates.end'] = { $gte: now };
                filterMatch.claimStatus = 'normal';
            } else if (dashStatus === 'expired') {
                filterMatch['warrantyDates.end'] = { ...(filterMatch['warrantyDates.end'] || {}), $lt: now };
            } else if (dashStatus === 'approval_pending') {
                filterMatch.approvalStatus = 'pending';
            } else if (dashStatus === 'approval_approved') {
                filterMatch.approvalStatus = 'approved';
            } else if (dashStatus === 'approval_rejected') {
                filterMatch.approvalStatus = 'rejected';
            } else if (dashStatus === 'claim_pending') {
                filterMatch.claimStatus = 'pending';
            } else if (dashStatus === 'claim_completed') {
                filterMatch.claimStatus = 'completed';
            }
        }

        const pipeline = [
            ...(Object.keys(filterMatch).length > 0 ? [{ $match: filterMatch }] : []),
            { $sort: { createdAt: -1 } },
            {
                $lookup: {
                    from: 'members',
                    localField: 'memberId',
                    foreignField: 'memberId',
                    as: 'memberInfo'
                }
            },
            {
                $lookup: {
                    from: 'claims',
                    localField: '_id',
                    foreignField: 'warrantyId',
                    as: 'claims'
                }
            },
            {
                $addFields: {
                    'customer.citizenId': { $arrayElemAt: ['$memberInfo.citizenId', 0] },
                    'customer.facebook': { $arrayElemAt: ['$memberInfo.facebook', 0] },
                    'customer.id': '$memberId',
                    'totalClaimAmount': { $sum: '$claims.totalCost' }
                }
            },
            {
                $addFields: {
                    devicePrice: { $ifNull: ['$devicePrice', '$device.deviceValue'] },
                    installmentsPaid: {
                        $let: {
                            vars: {
                                paidCount: {
                                    $size: {
                                        $filter: {
                                            input: { $ifNull: ['$payment.schedule', []] },
                                            as: 's',
                                            cond: { $eq: ['$$s.status', 'Paid'] }
                                        }
                                    }
                                }
                            },
                            in: {
                                $cond: [
                                    { $eq: ['$payment.method', 'Installment'] },
                                    { $min: [3, '$$paidCount'] },
                                    3
                                ]
                            }
                        }
                    },
                    usedCoverage: { $ifNull: ['$usedCoverage', '$totalClaimAmount'] }
                }
            },
            {
                $addFields: {
                    maxLimit: { $floor: { $multiply: ['$devicePrice', 0.70] } }
                }
            },
            {
                $addFields: {
                    currentLimit: {
                        $switch: {
                            branches: [
                                { case: { $gte: ['$installmentsPaid', 3] }, then: { $floor: { $multiply: ['$maxLimit', 1.0] } } },
                                { case: { $eq: ['$installmentsPaid', 2] }, then: { $floor: { $multiply: ['$maxLimit', 0.30] } } },
                                { case: { $eq: ['$installmentsPaid', 1] }, then: { $floor: { $multiply: ['$maxLimit', 0.10] } } }
                            ],
                            default: { $floor: { $multiply: ['$maxLimit', 0.10] } }
                        }
                    }
                }
            },
            {
                $addFields: {
                    remainingLimit: { $subtract: ['$currentLimit', '$usedCoverage'] }
                }
            },
            { $project: { memberInfo: 0, claims: 0 } }
        ];

        const warranties = await Warranty.aggregate(pipeline);
        res.json(warranties);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Create new warranty
app.post('/api/warranties', async (req, res) => {
    try {
        const { memberId, device } = req.body;

        // Generate Unique 7-digit Policy Number
        let policyNumber;
        let isUnique = false;
        while (!isUnique) {
            policyNumber = Math.floor(1000000 + Math.random() * 9000000).toString(); // 7 digits
            const existingPolicy = await Warranty.findOne({ policyNumber });
            if (!existingPolicy) isUnique = true;
        }

        const existingSerial = await Warranty.findOne({ 'device.serial': device.serial });
        if (existingSerial) return res.status(400).json({ message: 'เรือนนี้ได้ลงทะเบียนไปแล้ว (Serial Number already registered)' });

        const newWarranty = new Warranty({
            ...req.body,
            policyNumber,
            approvalStatus: 'pending'
        });

        // Enforce installmentsPaid from payment data in DB (do not trust client input)
        try {
            if (newWarranty.payment && newWarranty.payment.method === 'Installment') {
                const paidCount = (Array.isArray(newWarranty.payment.schedule) ? newWarranty.payment.schedule : [])
                    .filter(s => s && s.status === 'Paid').length;
                newWarranty.installmentsPaid = Math.min(3, Math.max(0, paidCount));
            } else {
                newWarranty.installmentsPaid = 3;
            }
        } catch (e) {
            console.error('Failed to calc installmentsPaid on create:', e);
        }
        await newWarranty.save();

        if (io && newWarranty.approvalStatus === 'pending') {
            const firstName = (newWarranty.customer && newWarranty.customer.firstName) ? newWarranty.customer.firstName : '';
            const lastName = (newWarranty.customer && newWarranty.customer.lastName) ? newWarranty.customer.lastName : '';
            const customerName = `${firstName} ${lastName}`.trim() || '-';
            io.emit('urgent_approval_needed', {
                warrantyId: newWarranty._id.toString(),
                policyNumber: newWarranty.policyNumber,
                customerName
            });
        }

        // Create FinanceTransaction immediately after creation if payment is recorded
        if (newWarranty.payment && (newWarranty.payment.paidCash > 0 || newWarranty.payment.paidTransfer > 0 || (newWarranty.payment.schedule && newWarranty.payment.schedule[0] && (newWarranty.payment.schedule[0].paidCash > 0 || newWarranty.payment.schedule[0].paidTransfer > 0)))) {
            const isInstallment = newWarranty.payment.method === 'Installment';
            const initialPayment = isInstallment && newWarranty.payment.schedule && newWarranty.payment.schedule[0] ? newWarranty.payment.schedule[0] : newWarranty.payment;

            const cash = Number(initialPayment.paidCash || 0);
            const transfer = Number(initialPayment.paidTransfer || 0);
            // Frontend might send cashReceived, transferAmount, changeAmount at root or we use paidCash
            const change = req.body.changeAmount ? Number(req.body.changeAmount) : 0;
            const net = (cash - change) + transfer;

            if (net > 0) {
                const firstName = (newWarranty.customer && newWarranty.customer.firstName) ? newWarranty.customer.firstName : '';
                const lastName = (newWarranty.customer && newWarranty.customer.lastName) ? newWarranty.customer.lastName : '';

                let pMethod = 'ไม่ระบุ';
                if (cash > 0 && transfer > 0) pMethod = 'เงินสด+โอนเงิน';
                else if (cash > 0) pMethod = 'เงินสด';
                else if (transfer > 0) pMethod = 'โอนเงิน';

                try {
                    await FinanceTransaction.create({
                        policyNumber: newWarranty.policyNumber,
                        customerName: `${firstName} ${lastName}`.trim() || '-',
                        actionType: 'ซื้อแพ็กเกจใหม่',
                        paymentMethod: pMethod,
                        cashReceived: cash,
                        transferAmount: transfer,
                        changeAmount: change,
                        netTotal: net,
                        evidenceUrl: req.body.evidenceUrl || null,
                        recordedBy: req.body.staffName || newWarranty.staffName || 'System'
                    });
                } catch (e) {
                    console.error('Failed to create FinanceTransaction:', e);
                }
            }
        }

        res.status(201).json(newWarranty);
    } catch (err) {
        res.status(400).json({ message: err.message });
    }
});

// Get warranties filtered by approvalStatus (Enriched with Member Data)
app.get('/api/warranties/pending', async (req, res) => {
    try {
        const status = req.query.status || 'pending';
        const baseMatch = {};
        if (status !== 'all') {
            baseMatch.approvalStatus = status;
        }

        // Merge with search/date filters
        const matchQuery = buildWarrantyFilterMatch(req.query, baseMatch);

        const warranties = await Warranty.aggregate([
            { $match: matchQuery },
            { $sort: { createdAt: -1 } },
            {
                $lookup: {
                    from: 'members',
                    localField: 'memberId',
                    foreignField: 'memberId',
                    as: 'memberInfo'
                }
            },
            {
                $lookup: {
                    from: 'claims',
                    localField: '_id',
                    foreignField: 'warrantyId',
                    as: 'claims'
                }
            },
            {
                $addFields: {
                    'customer.citizenId': { $arrayElemAt: ['$memberInfo.citizenId', 0] },
                    'customer.id': '$memberId',
                    'totalClaimAmount': { $sum: '$claims.totalCost' }
                }
            },
            {
                $addFields: {
                    devicePrice: { $ifNull: ['$devicePrice', '$device.deviceValue'] },
                    installmentsPaid: {
                        $let: {
                            vars: {
                                paidCount: {
                                    $size: {
                                        $filter: {
                                            input: { $ifNull: ['$payment.schedule', []] },
                                            as: 's',
                                            cond: { $eq: ['$$s.status', 'Paid'] }
                                        }
                                    }
                                }
                            },
                            in: {
                                $cond: [
                                    { $eq: ['$payment.method', 'Installment'] },
                                    { $min: [3, '$$paidCount'] },
                                    3
                                ]
                            }
                        }
                    },
                    usedCoverage: { $ifNull: ['$usedCoverage', '$totalClaimAmount'] }
                }
            },
            {
                $addFields: {
                    maxLimit: { $floor: { $multiply: ['$devicePrice', 0.70] } }
                }
            },
            {
                $addFields: {
                    currentLimit: {
                        $switch: {
                            branches: [
                                { case: { $gte: ['$installmentsPaid', 3] }, then: { $floor: { $multiply: ['$maxLimit', 1.0] } } },
                                { case: { $eq: ['$installmentsPaid', 2] }, then: { $floor: { $multiply: ['$maxLimit', 0.30] } } },
                                { case: { $eq: ['$installmentsPaid', 1] }, then: { $floor: { $multiply: ['$maxLimit', 0.10] } } }
                            ],
                            default: { $floor: { $multiply: ['$maxLimit', 0.10] } }
                        }
                    }
                }
            },
            {
                $addFields: {
                    remainingLimit: { $subtract: ['$currentLimit', '$usedCoverage'] }
                }
            },
            { $project: { memberInfo: 0, claims: 0 } }
        ]);
        res.json(warranties);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Get pending warranty count for badge
app.get('/api/warranties/pending-count', async (req, res) => {
    try {
        const count = await Warranty.countDocuments({ approvalStatus: 'pending' });
        res.json({ count });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// ═══════════════════════════════════════════════════════════════════
// SALES DASHBOARD SUMMARY API
// ═══════════════════════════════════════════════════════════════════
app.get('/api/dashboard/sales/summary', async (req, res) => {
    try {
        const now = new Date();
        const fiveDaysAgo = new Date(now.getTime() - 5 * 24 * 60 * 60 * 1000);

        // 1. Overdue Claims (not เสร็จสิ้น and updatedAt > 5 days ago)
        const overdueClaims = await Claim.countDocuments({
            status: { $ne: 'เสร็จสิ้น' },
            updatedAt: { $lt: fiveDaysAgo }
        });

        // 2. Pending Approvals
        const pendingApprovals = await Warranty.countDocuments({ approvalStatus: 'pending' });

        // 3. Unpaid Packages (payment status not Paid, or no payment recorded)
        const unpaidPackages = await Warranty.countDocuments({
            approvalStatus: { $ne: 'rejected' },
            $or: [
                { 'payment.status': 'Pending' },
                { 'payment.status': { $exists: false } },
                { 'payment.paidCash': { $in: [0, null] }, 'payment.paidTransfer': { $in: [0, null] }, 'payment.method': { $ne: 'Installment' } }
            ]
        });

        // 4. Due Installments (installments with status Pending and dueDate <= today)
        const dueInstallmentsResult = await Warranty.aggregate([
            { $match: { 'payment.method': 'Installment' } },
            { $unwind: '$payment.schedule' },
            {
                $match: {
                    'payment.schedule.status': 'Pending',
                    'payment.schedule.dueDate': { $lte: now }
                }
            },
            { $count: 'total' }
        ]);
        const installmentOverdue = (dueInstallmentsResult.length > 0) ? dueInstallmentsResult[0].total : 0;

        res.json({
            overdueClaims,
            pendingApprovals,
            unpaidPackages,
            installmentOverdue
        });
    } catch (err) {
        console.error('Sales summary error:', err);
        res.status(500).json({ message: err.message });
    }
});

// ═══════════════════════════════════════════════════════════════════
// APPROVER DASHBOARD SUMMARY API
// ═══════════════════════════════════════════════════════════════════
app.get('/api/dashboard/approver/summary', async (req, res) => {
    try {
        const now = new Date();
        const threeDaysAgo = new Date(now.getTime() - 3 * 24 * 60 * 60 * 1000);

        // Start of today (Thai time context usually, but local DB time for simplicity)
        const startOfToday = new Date();
        startOfToday.setHours(0, 0, 0, 0);

        // 1. All Pending
        const pendingApprovals = await Warranty.countDocuments({ approvalStatus: 'pending' });

        // 2. Urgent Pending (> 3 days)
        const urgentPending = await Warranty.countDocuments({
            approvalStatus: 'pending',
            createdAt: { $lt: threeDaysAgo }
        });

        // 3. Approved Today
        const approvedToday = await Warranty.countDocuments({
            approvalStatus: 'approved',
            updatedAt: { $gte: startOfToday }
        });

        // 4. Rejected Today
        const rejectedToday = await Warranty.countDocuments({
            approvalStatus: 'rejected',
            updatedAt: { $gte: startOfToday }
        });

        // 5. Recent Pending (Top 5 Oldest)
        const recentPending = await Warranty.find({ approvalStatus: 'pending' })
            .sort({ createdAt: 1 })
            .limit(5)
            .select('policyNumber customer staffName createdAt');

        res.json({
            pendingApprovals,
            urgentPending,
            approvedToday,
            rejectedToday,
            recentPending
        });
    } catch (err) {
        console.error('Approver summary error:', err);
        res.status(500).json({ message: err.message });
    }
});

// Approve a warranty
app.put('/api/warranties/:id/approve', async (req, res) => {
    try {
        const { approver } = req.body;
        const warranty = await Warranty.findByIdAndUpdate(
            req.params.id,
            {
                approvalStatus: 'approved',
                approver: approver,
                approvalDate: new Date()
            },
            { new: true }
        );
        if (!warranty) return res.status(404).json({ message: 'Record not found' });
        res.json({ success: true, warranty });
    } catch (err) {
        res.status(400).json({ message: err.message });
    }
});

// Reject a warranty
app.put('/api/warranties/:id/reject', async (req, res) => {
    try {
        const { reason, rejectBy } = req.body;
        const warranty = await Warranty.findByIdAndUpdate(
            req.params.id,
            {
                approvalStatus: 'rejected',
                rejectReason: reason,
                rejectBy: rejectBy,
                rejectDate: new Date()
            },
            { new: true }
        );
        if (!warranty) return res.status(404).json({ message: 'Record not found' });
        res.json({ success: true, warranty });
    } catch (err) {
        res.status(400).json({ message: err.message });
    }
});

// Check for duplicate Serial or IMEI
app.get('/api/warranties/check-duplicate', async (req, res) => {
    try {
        const { type, value, excludeId } = req.query;
        if (!type || !value) return res.json({ exists: false });

        const query = {};
        if (type === 'serial') {
            query['device.serial'] = value;
        } else if (type === 'imei') {
            query['device.imei'] = value;
        } else {
            return res.status(400).json({ message: 'Invalid type' });
        }

        // If editing, exclude the current record
        if (excludeId && mongoose.Types.ObjectId.isValid(excludeId)) {
            query._id = { $ne: excludeId };
        }

        const existing = await Warranty.findOne(query);
        res.json({ exists: !!existing });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Get active warranties only (approved) — for claim menu, including expired
app.get('/api/warranties/active', async (req, res) => {
    try {
        const baseMatch = {
            approvalStatus: 'approved'
        };

        // Merge with search/date filters
        const matchQuery = buildWarrantyFilterMatch(req.query, baseMatch);

        const warranties = await Warranty.aggregate([
            { $match: matchQuery },
            { $sort: { createdAt: -1 } },
            {
                $lookup: {
                    from: 'members',
                    localField: 'memberId',
                    foreignField: 'memberId',
                    as: 'memberInfo'
                }
            },
            {
                $lookup: {
                    from: 'claims',
                    localField: '_id',
                    foreignField: 'warrantyId',
                    as: 'claims'
                }
            },
            {
                $addFields: {
                    'customer.citizenId': { $arrayElemAt: ['$memberInfo.citizenId', 0] },
                    'customer.facebook': { $arrayElemAt: ['$memberInfo.facebook', 0] },
                    'customer.id': '$memberId',
                    'customer.idCardAddress': { $arrayElemAt: ['$memberInfo.idCardAddress', 0] },
                    'customer.shippingAddress': { $arrayElemAt: ['$memberInfo.shippingAddress', 0] },
                    'totalClaimAmount': { $sum: '$claims.totalCost' }
                }
            },
            { $project: { memberInfo: 0, claims: 0 } }
        ]);
        res.json(warranties);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Get single warranty (Enriched with Member Data)
app.get('/api/warranties/:id', async (req, res) => {
    try {
        const warranties = await Warranty.aggregate([
            { $match: { _id: new mongoose.Types.ObjectId(req.params.id) } },
            {
                $lookup: {
                    from: 'members',
                    localField: 'memberId',
                    foreignField: 'memberId',
                    as: 'memberInfo'
                }
            },
            {
                $lookup: {
                    from: 'claims',
                    localField: '_id',
                    foreignField: 'warrantyId',
                    as: 'claims'
                }
            },
            {
                $addFields: {
                    'customer.citizenId': { $arrayElemAt: ['$memberInfo.citizenId', 0] },
                    'customer.facebook': { $arrayElemAt: ['$memberInfo.facebook', 0] },
                    'customer.id': '$memberId',
                    'customer.idCardAddress': { $arrayElemAt: ['$memberInfo.idCardAddress', 0] },
                    'customer.shippingAddress': { $arrayElemAt: ['$memberInfo.shippingAddress', 0] },
                    'totalClaimAmount': { $sum: '$claims.totalCost' }
                }
            },
            {
                $addFields: {
                    devicePrice: { $ifNull: ['$devicePrice', '$device.deviceValue'] },
                    installmentsPaid: {
                        $let: {
                            vars: {
                                paidCount: {
                                    $size: {
                                        $filter: {
                                            input: { $ifNull: ['$payment.schedule', []] },
                                            as: 's',
                                            cond: { $eq: ['$$s.status', 'Paid'] }
                                        }
                                    }
                                }
                            },
                            in: {
                                $cond: [
                                    { $eq: ['$payment.method', 'Installment'] },
                                    { $min: [3, '$$paidCount'] },
                                    3
                                ]
                            }
                        }
                    },
                    usedCoverage: { $ifNull: ['$usedCoverage', '$totalClaimAmount'] }
                }
            },
            {
                $addFields: {
                    maxLimit: { $floor: { $multiply: ['$devicePrice', 0.70] } }
                }
            },
            {
                $addFields: {
                    currentLimit: {
                        $switch: {
                            branches: [
                                { case: { $gte: ['$installmentsPaid', 3] }, then: { $floor: { $multiply: ['$maxLimit', 1.0] } } },
                                { case: { $eq: ['$installmentsPaid', 2] }, then: { $floor: { $multiply: ['$maxLimit', 0.30] } } },
                                { case: { $eq: ['$installmentsPaid', 1] }, then: { $floor: { $multiply: ['$maxLimit', 0.10] } } }
                            ],
                            default: { $floor: { $multiply: ['$maxLimit', 0.10] } }
                        }
                    }
                }
            },
            {
                $addFields: {
                    remainingLimit: { $subtract: ['$currentLimit', '$usedCoverage'] }
                }
            },
            { $project: { memberInfo: 0, claims: 0 } }
        ]);

        if (!warranties || warranties.length === 0) {
            return res.status(404).json({ message: 'Record not found' });
        }
        res.json(warranties[0]);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Update warranty
app.put('/api/warranties/:id', async (req, res) => {
    try {
        const { memberId, ...updateData } = req.body;
        // memberId is immutable as per requirement

        const updated = await Warranty.findByIdAndUpdate(
            req.params.id,
            updateData,
            { new: true, runValidators: true }
        );

        if (!updated) return res.status(404).json({ message: 'Record not found' });
        res.json(updated);
    } catch (err) {
        res.status(400).json({ message: err.message });
    }
});

// Update Payment Status
app.patch('/api/warranties/:id/payment', async (req, res) => {
    try {
        const { installmentNo, payAllRemaining, paidCash, paidTransfer, refId, changeAmount, evidenceUrl, staffName } = req.body;
        const warranty = await Warranty.findById(req.params.id);
        if (!warranty) return res.status(404).json({ message: 'Record not found' });

        if (payAllRemaining) {
            // Update all pending installments
            warranty.payment.status = 'Paid';
            warranty.payment.paidDate = new Date();
            warranty.payment.paidCash = (warranty.payment.paidCash || 0) + (paidCash || 0);
            warranty.payment.paidTransfer = (warranty.payment.paidTransfer || 0) + (paidTransfer || 0);

            warranty.payment.schedule.forEach(inst => {
                if (inst.status !== 'Paid') {
                    inst.status = 'Paid';
                    inst.paidDate = new Date();
                    inst.paidCash = paidCash; // Note: Usually shared or total is recorded
                    inst.paidTransfer = paidTransfer;
                    inst.refId = refId;
                }
            });
        } else if (installmentNo) {
            // Update specific installment
            const inst = warranty.payment.schedule.find(s => s.installmentNo === installmentNo);
            if (inst) {
                inst.status = 'Paid';
                inst.paidDate = new Date();
                inst.paidCash = paidCash;
                inst.paidTransfer = paidTransfer;
                inst.refId = refId;
            }

            // Check if all are paid
            const allPaid = warranty.payment.schedule.every(s => s.status === 'Paid');
            if (allPaid) {
                warranty.payment.status = 'Paid';
                warranty.payment.paidDate = new Date();
            }
        } else {
            // Update full payment
            warranty.payment.status = 'Paid';
            warranty.payment.paidDate = new Date();
            warranty.payment.paidCash = paidCash;
            warranty.payment.paidTransfer = paidTransfer;
            warranty.payment.refId = refId;
        }

        await warranty.save();

        // Recalculate installmentsPaid based on DB payment schedule
        try {
            if (warranty.payment && warranty.payment.method === 'Installment') {
                const paidCount = (Array.isArray(warranty.payment.schedule) ? warranty.payment.schedule : [])
                    .filter(s => s && s.status === 'Paid').length;
                warranty.installmentsPaid = Math.min(3, Math.max(0, paidCount));
            } else {
                warranty.installmentsPaid = 3;
            }
            await warranty.save();
        } catch (e) {
            // If this fails, do not block payment update response
            console.error('Failed to recalc installmentsPaid:', e);
        }

        // Process Finance Transaction
        const cash = Number(paidCash || 0);
        const transfer = Number(paidTransfer || 0);
        const change = Number(changeAmount || 0);
        const net = (cash - change) + transfer;

        if (net > 0) {
            const firstName = (warranty.customer && warranty.customer.firstName) ? warranty.customer.firstName : '';
            const lastName = (warranty.customer && warranty.customer.lastName) ? warranty.customer.lastName : '';

            let pMethod = 'ไม่ระบุ';
            if (cash > 0 && transfer > 0) pMethod = 'เงินสด+โอนเงิน';
            else if (cash > 0) pMethod = 'เงินสด';
            else if (transfer > 0) pMethod = 'โอนเงิน';

            let actType = 'ชำระเต็มจำนวน';
            if (payAllRemaining) actType = 'ชำระปิดยอด/จ่ายเต็ม';
            else if (installmentNo) actType = `ชำระค่างวดที่ ${installmentNo}`;

            try {
                await FinanceTransaction.create({
                    policyNumber: warranty.policyNumber,
                    customerName: `${firstName} ${lastName}`.trim() || '-',
                    actionType: actType,
                    paymentMethod: pMethod,
                    cashReceived: cash,
                    transferAmount: transfer,
                    changeAmount: change,
                    netTotal: net,
                    evidenceUrl: evidenceUrl || null,
                    recordedBy: staffName || warranty.staffName || 'System'
                });
            } catch (e) {
                console.error('Failed to create FinanceTransaction:', e);
            }
        }

        res.json({ success: true, warranty });
    } catch (err) {
        res.status(400).json({ message: err.message });
    }
});

// Customer Portal: Get Member Data, Warranties, and Claims
app.post('/api/public/customer/portal', async (req, res) => {
    try {
        const { idCard, memberId } = req.body;

        if (!idCard || !memberId) {
            return res.status(400).json({ success: false, message: 'กรุณากรอกข้อมูลให้ครบถ้วน' });
        }

        // 1. Authenticate Member
        // Search by both citizenId (idCard) and memberId
        const member = await Member.findOne({ citizenId: idCard, memberId: memberId });

        if (!member) {
            return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลสมาชิก หรือข้อมูลไม่ถูกต้อง' });
        }

        // 2. Fetch Warranties for this member
        const warranties = await Warranty.aggregate([
            { $match: { memberId: memberId } },
            { $sort: { createdAt: -1 } },
            {
                $lookup: {
                    from: 'claims',
                    localField: '_id',
                    foreignField: 'warrantyId',
                    as: 'claims'
                }
            },
            {
                $addFields: {
                    'totalClaimAmount': { $sum: '$claims.totalCost' }
                }
            },
            { $project: { claims: 0 } } // Exclude claims here, we'll fetch them separately or structured differently
        ]);

        // 3. Fetch all Claims for these warranties
        // We can actually just use the lookup from step 2, but if we want a flat list of claims for the claims section:
        const warrantyIds = warranties.map(w => w._id);
        const claims = await Claim.aggregate([
            { $match: { warrantyId: { $in: warrantyIds } } },
            { $sort: { claimDate: -1 } }, // Newest claims first
            {
                $lookup: {
                    from: 'warranties',
                    localField: 'warrantyId',
                    foreignField: '_id',
                    as: 'warrantyInfo'
                }
            },
            {
                $addFields: {
                    'deviceModel': { $arrayElemAt: ['$warrantyInfo.device.model', 0] },
                    'color': { $arrayElemAt: ['$warrantyInfo.device.color', 0] }
                }
            },
            { $project: { warrantyInfo: 0 } }
        ]);

        res.json({
            success: true,
            member: member,
            warranties: warranties,
            claims: claims
        });

    } catch (err) {
        console.error('Portal Error:', err);
        res.status(500).json({ success: false, message: 'Server Error: ' + err.message });
    }
});

// Delete warranty
app.delete('/api/warranties/:id', async (req, res) => {
    try {
        const deleted = await Warranty.findByIdAndDelete(req.params.id);
        if (!deleted) return res.status(404).json({ message: 'Record not found' });
        res.json({ success: true, message: 'Record deleted successfully' });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// ═══════════════════════════════════════════════════════════════════
// CLAIM API ROUTES
// ═══════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════
// FINANCE API ROUTES
// ═══════════════════════════════════════════════════════════════════

app.post('/api/upload', genericUpload.single('file'), (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ message: 'No file uploaded' });
        res.json({ url: req.file.path });
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});


// ═══════════════════════════════════════════════════════════════════
// FINANCE EXPENSE API (Detailed Claim Expenses)
// ═══════════════════════════════════════════════════════════════════

app.get('/api/finance/expenses', async (req, res) => {
    try {
        const matchQuery = buildExpenseFilterMatch(req.query);

        // Convert __expenseDate to updates.date for matching
        if (matchQuery.__expenseDate) {
            matchQuery['updates.date'] = matchQuery.__expenseDate;
            delete matchQuery.__expenseDate;
        }

        const expenses = await Claim.aggregate([
            { $unwind: '$updates' },
            { $match: { 'updates.cost': { $gt: 0 } } },
            { $match: matchQuery },
            { $sort: { 'updates.date': -1 } },
            {
                $project: {
                    _id: 0,
                    claimId: 1,
                    policyNumber: 1,
                    customerName: 1,
                    deviceModel: 1,
                    claimShopName: 1,
                    expenseDate: '$updates.date',
                    expenseTitle: '$updates.title',
                    centerName: '$updates.centerName',
                    amount: '$updates.cost'
                }
            }
        ]);
        res.json(expenses);
    } catch (err) {
        console.error('Fetch finance expenses error:', err);
        res.status(500).json({ message: err.message });
    }
});

app.get('/api/finance/expenses/summary', async (req, res) => {
    try {
        const matchQuery = buildExpenseFilterMatch(req.query);

        // Convert __expenseDate to updates.date for matching
        if (matchQuery.__expenseDate) {
            matchQuery['updates.date'] = matchQuery.__expenseDate;
            delete matchQuery.__expenseDate;
        }

        const summary = await Claim.aggregate([
            { $unwind: '$updates' },
            { $match: { 'updates.cost': { $gt: 0 } } },
            { $match: matchQuery },
            {
                $group: {
                    _id: null,
                    totalExpense: { $sum: '$updates.cost' }
                }
            }
        ]);

        res.json({
            totalExpense: (summary && summary.length > 0) ? summary[0].totalExpense : 0
        });
    } catch (err) {
        console.error('Fetch finance expenses summary error:', err);
        res.status(500).json({ message: err.message });
    }
});

app.get('/api/finance/transactions', async (req, res) => {
    try {
        const transactions = await FinanceTransaction.find().sort({ transactionDate: -1 });
        res.json(transactions);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

app.get('/api/finance/summary', async (req, res) => {
    try {
        const aggr = await FinanceTransaction.aggregate([
            {
                $group: {
                    _id: null,
                    totalCashReceived: { $sum: "$cashReceived" },
                    totalChangeAmount: { $sum: "$changeAmount" },
                    totalTransferAmount: { $sum: "$transferAmount" },
                    totalRevenue: { $sum: "$netTotal" }
                }
            }
        ]);

        if (aggr && aggr.length > 0) {
            const data = aggr[0];
            const netCash = (data.totalCashReceived || 0) - (data.totalChangeAmount || 0);
            res.json({
                totalCash: netCash,
                totalTransfer: data.totalTransferAmount || 0,
                totalRevenue: data.totalRevenue || 0,
                totalChange: data.totalChangeAmount || 0
            });
        } else {
            res.json({ totalCash: 0, totalTransfer: 0, totalRevenue: 0, totalChange: 0 });
        }
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

app.get('/api/finance/export/excel', async (req, res) => {
    try {
        const { startDate, endDate, fields, includeSummary, paymentMethod } = req.query || {};

        const match = {};
        if (startDate) {
            match.transactionDate = { ...(match.transactionDate || {}), $gte: new Date(String(startDate)) };
        }
        if (endDate) {
            match.transactionDate = { ...(match.transactionDate || {}), $lte: new Date(String(endDate) + 'T23:59:59.999Z') };
        }
        if (paymentMethod && String(paymentMethod) !== 'all') {
            match.paymentMethod = String(paymentMethod);
        }

        const selectedFields = String(fields || '')
            .split(',')
            .map(s => String(s || '').trim())
            .filter(Boolean);

        const fieldMeta = {
            transactionDate: { header: 'วันที่', width: 22 },
            actionType: { header: 'ประเภทรายการ', width: 18 },
            policyNumber: { header: 'เลขที่สัญญา', width: 16 },
            customerName: { header: 'ชื่อลูกค้า', width: 20 },
            paymentMethod: { header: 'วิธีชำระ', width: 16 },
            cashReceived: { header: 'รับเงินสด', width: 14 },
            transferAmount: { header: 'เงินโอน', width: 14 },
            changeAmount: { header: 'เงินทอน', width: 14 },
            netTotal: { header: 'ยอดสุทธิ', width: 14 },
            evidenceUrl: { header: 'หลักฐาน', width: 28 },
            recordedBy: { header: 'ผู้ทำรายการ', width: 18 }
        };

        const defaultFieldOrder = [
            'transactionDate',
            'actionType',
            'policyNumber',
            'customerName',
            'paymentMethod',
            'cashReceived',
            'transferAmount',
            'changeAmount',
            'netTotal',
            'evidenceUrl',
            'recordedBy'
        ];

        const finalFields = (selectedFields.length > 0 ? selectedFields : defaultFieldOrder)
            .filter(f => Object.prototype.hasOwnProperty.call(fieldMeta, f));

        const transactions = await FinanceTransaction.find(match).sort({ transactionDate: -1 }).lean();

        const workbook = new ExcelJS.Workbook();
        workbook.creator = 'EasyCare';
        workbook.created = new Date();

        const ws = workbook.addWorksheet('Transactions');
        ws.columns = finalFields.map(f => ({ key: f, ...fieldMeta[f] }));
        ws.getRow(1).font = { bold: true };

        const moneyFields = new Set(['cashReceived', 'transferAmount', 'changeAmount', 'netTotal']);

        for (const tx of (Array.isArray(transactions) ? transactions : [])) {
            const rowData = {};
            for (const f of finalFields) {
                if (f === 'transactionDate') {
                    rowData[f] = tx.transactionDate ? new Date(tx.transactionDate) : null;
                } else if (moneyFields.has(f)) {
                    rowData[f] = Number(tx[f] || 0);
                } else {
                    rowData[f] = tx[f] ?? '';
                }
            }
            ws.addRow(rowData);
        }

        ws.columns.forEach(col => {
            if (col && col.key === 'transactionDate') {
                col.numFmt = 'dd/mm/yyyy hh:mm';
            }
            if (col && moneyFields.has(col.key)) {
                col.numFmt = '#,##0.00';
            }
        });

        if (String(includeSummary || '1') !== '0') {
            const aggr = await FinanceTransaction.aggregate([
                ...(Object.keys(match).length > 0 ? [{ $match: match }] : []),
                {
                    $group: {
                        _id: null,
                        totalCashReceived: { $sum: "$cashReceived" },
                        totalChangeAmount: { $sum: "$changeAmount" },
                        totalTransferAmount: { $sum: "$transferAmount" },
                        totalRevenue: { $sum: "$netTotal" }
                    }
                }
            ]);

            const data = aggr && aggr.length > 0 ? aggr[0] : {};
            const netCash = Number((data.totalCashReceived || 0) - (data.totalChangeAmount || 0));
            const totalTransfer = Number(data.totalTransferAmount || 0);
            const totalRevenue = Number(data.totalRevenue || 0);

            const wsSum = workbook.addWorksheet('Summary');
            wsSum.columns = [
                { header: 'รายการ', key: 'label', width: 22 },
                { header: 'ยอดรวม', key: 'value', width: 18 }
            ];
            wsSum.getRow(1).font = { bold: true };
            wsSum.addRow({ label: 'ยอดรวมเงินสด', value: netCash });
            wsSum.addRow({ label: 'ยอดรวมเงินโอน', value: totalTransfer });
            wsSum.addRow({ label: 'รายรับรวมทั้งหมด', value: totalRevenue });
            wsSum.getColumn('value').numFmt = '#,##0.00';
        }

        const safeStart = startDate ? String(startDate) : '';
        const safeEnd = endDate ? String(endDate) : '';
        const fileName = `finance_${safeStart || 'all'}_${safeEnd || 'all'}.xlsx`;

        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
        await workbook.xlsx.write(res);
        res.end();
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Create new claim (with image upload)
app.post('/api/claims', claimUpload.array('images', 10), async (req, res) => {
    try {
        const {
            warrantyId, policyNumber, memberId, customerName, customerPhone,
            deviceModel, imei, serialNumber, color, symptoms, staffName,
            claimShopName,
            returnMethod, pickupBranch, deliveryAddressType, deliveryAddressDetail,
            devicePowerState
        } = req.body;

        // Generate unique Claim ID: SML + 6 digits
        let claimId;
        let isUnique = false;
        while (!isUnique) {
            const randomNum = Math.floor(100000 + Math.random() * 900000);
            claimId = `SML${randomNum}`;
            const existing = await Claim.findOne({ claimId });
            if (!existing) isUnique = true;
        }

        // Collect uploaded file paths (Cloudinary URLs)
        const images = req.files ? req.files.map(f => f.path) : [];

        const claimData = {
            claimId,
            warrantyId,
            policyNumber,
            memberId,
            claimShopName: String(claimShopName || '').trim(),
            customerName,
            customerPhone,
            deviceModel,
            devicePowerState: devicePowerState === 'off' ? 'off' : 'on',
            imei,
            serialNumber,
            color,
            claimDate: new Date(),
            symptoms,
            images,
            staffName,
            returnMethod,
            pickupBranch: returnMethod === 'pickup' ? pickupBranch : '',
            deliveryAddressType: returnMethod === 'delivery' ? deliveryAddressType : undefined,
            deliveryAddressDetail: returnMethod === 'delivery' ? deliveryAddressDetail : ''
        };

        // Parse deviceCondition if provided
        if (req.body.deviceCondition) {
            try {
                claimData.deviceCondition = JSON.parse(req.body.deviceCondition);
            } catch (e) {
                console.error('Error parsing deviceCondition:', e);
            }
        }

        const newClaim = new Claim(claimData);

        await newClaim.save();

        // Update Warranty status to 'Wait for Claim'
        await Warranty.findByIdAndUpdate(warrantyId, { claimStatus: 'pending' });

        res.status(201).json({ success: true, claim: newClaim });
    } catch (err) {
        res.status(400).json({ success: false, message: err.message });
    }
});

// Get all claims
app.get('/api/claims', async (req, res) => {
    try {
        const claims = await Claim.aggregate([
            { $sort: { createdAt: -1 } },
            {
                $lookup: {
                    from: 'warranties',
                    localField: 'warrantyId',
                    foreignField: '_id',
                    as: 'warrantyInfo'
                }
            },
            {
                $addFields: {
                    'imei': { $ifNull: ['$imei', { $arrayElemAt: ['$warrantyInfo.device.imei', 0] }] },
                    'serialNumber': { $ifNull: ['$serialNumber', { $arrayElemAt: ['$warrantyInfo.device.serial', 0] }] },
                    'color': { $ifNull: ['$color', { $arrayElemAt: ['$warrantyInfo.device.color', 0] }] }
                }
            },
            { $project: { warrantyInfo: 0 } }
        ]);
        res.json(claims);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Get claim by Warranty ID (for printing receipt)
app.get('/api/claims/warranty/:warrantyId', async (req, res) => {
    try {
        const claim = await Claim.findOne({ warrantyId: req.params.warrantyId }).sort({ createdAt: -1 });
        if (!claim) return res.status(404).json({ success: false, message: 'Claim not found' });
        res.json({ success: true, claim });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Get pending claims (status = 'รอเคลม')
app.get('/api/claims/pending', async (req, res) => {
    try {
        // Merge base status filter with search/date filters
        const matchQuery = buildClaimFilterMatch(req.query, { status: { $in: ['รอเคลม', 'รอการตัดสินใจจากลูกค้า'] } });

        const claims = await Claim.aggregate([
            { $match: matchQuery },
            { $sort: { createdAt: -1 } },
            {
                $lookup: {
                    from: 'warranties',
                    localField: 'warrantyId',
                    foreignField: '_id',
                    as: 'warrantyInfo'
                }
            },
            {
                $addFields: {
                    'imei': { $ifNull: ['$imei', { $arrayElemAt: ['$warrantyInfo.device.imei', 0] }] },
                    'serialNumber': { $ifNull: ['$serialNumber', { $arrayElemAt: ['$warrantyInfo.device.serial', 0] }] },
                    'color': { $ifNull: ['$color', { $arrayElemAt: ['$warrantyInfo.device.color', 0] }] }
                }
            },
            { $project: { warrantyInfo: 0 } }
        ]);

        const now = Date.now();
        const MS_PER_DAY = 24 * 60 * 60 * 1000;

        const enriched = (claims || []).map(c => {
            const updates = Array.isArray(c.updates) ? c.updates : [];
            const lastUpdate = updates.length > 0 ? updates[updates.length - 1] : null;
            const lastUpdateDateRaw = (lastUpdate && lastUpdate.date) ? lastUpdate.date : (c.claimDate || c.createdAt);
            const lastUpdateTime = lastUpdateDateRaw ? new Date(lastUpdateDateRaw).getTime() : NaN;

            const daysSinceUpdate = Number.isFinite(lastUpdateTime)
                ? Math.floor((now - lastUpdateTime) / MS_PER_DAY)
                : 0;

            const isOverdue = daysSinceUpdate >= 5;

            return {
                ...c,
                isOverdue,
                daysOverdue: isOverdue ? daysSinceUpdate : 0,
            };
        });

        res.json(enriched);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Get claim history by Warranty ID
app.get('/api/claims/history/:warrantyId', async (req, res) => {
    try {
        const claims = await Claim.aggregate([
            { $match: { warrantyId: new mongoose.Types.ObjectId(req.params.warrantyId) } },
            { $sort: { createdAt: -1 } },
            {
                $lookup: {
                    from: 'warranties',
                    localField: 'warrantyId',
                    foreignField: '_id',
                    as: 'warrantyInfo'
                }
            },
            {
                $addFields: {
                    'imei': { $ifNull: ['$imei', { $arrayElemAt: ['$warrantyInfo.device.imei', 0] }] },
                    'serialNumber': { $ifNull: ['$serialNumber', { $arrayElemAt: ['$warrantyInfo.device.serial', 0] }] },
                    'color': { $ifNull: ['$color', { $arrayElemAt: ['$warrantyInfo.device.color', 0] }] }
                }
            },
            { $project: { warrantyInfo: 0 } }
        ]);
        res.json(claims);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Add status update to a claim
app.post('/api/claims/:id/updates', claimUpload.fields([
    { name: 'images', maxCount: 10 },
    { name: 'evidenceImages', maxCount: 10 }
]), async (req, res) => {
    try {
        const claim = await Claim.findById(req.params.id);
        if (!claim) return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลการเคลม' });

        const images = (req.files && req.files.images) ? req.files.images.map(f => f.path) : [];
        const evidenceImages = (req.files && req.files.evidenceImages) ? req.files.evidenceImages.map(f => f.path) : [];
        const cost = parseFloat(req.body.cost) || 0;
        const centerName = String(req.body.centerName || '').trim();
        const centerLocation = String(req.body.centerLocation || '').trim();
        const centerPhone = String(req.body.centerPhone || '').trim();
        const technicianName = String(req.body.technicianName || '').trim();
        const technicianPhone = String(req.body.technicianPhone || '').trim();
        const nextStep = (claim.updates ? claim.updates.length : 0) + 2; // +2 because step 1 = "รับเครื่อง" (auto)

        if (cost > 0 && evidenceImages.length === 0) {
            return res.status(400).json({ success: false, message: 'หากมีค่าใช้จ่าย กรุณาแนบรูปหลักฐานอย่างน้อย 1 รูป' });
        }

        let shouldApplyCost = true;
        let currentCoverageLeft = null;
        if (cost > 0 && claim.warrantyId) {
            const warranty = await Warranty.findById(claim.warrantyId);
            if (warranty) {
                const remaining = Number(warranty.remainingLimit ?? 0);
                currentCoverageLeft = Number.isFinite(remaining) ? remaining : 0;
                if (cost > currentCoverageLeft) {
                    const refundAmount = currentCoverageLeft;
                    const excessCost = cost - currentCoverageLeft;
                    claim.excessCost = excessCost;
                    claim.refundAmount = refundAmount;
                    claim.customerDecision = 'รอตัดสินใจ';
                    claim.status = 'รอการตัดสินใจจากลูกค้า';
                    shouldApplyCost = false;

                    // Append (เกินวงเงิน) to the title
                    if (req.body.title && !req.body.title.includes('(เกินวงเงิน)')) {
                        req.body.title = `${req.body.title} (เกินวงเงิน)`;
                    }
                }
            }
        }

        claim.updates.push({
            step: nextStep,
            title: req.body.title || '',
            date: new Date(),
            cost: cost,
            centerName,
            centerLocation,
            centerPhone,
            technicianName,
            technicianPhone,
            images: images,
            evidenceImages: evidenceImages
        });

        if (shouldApplyCost) {
            claim.totalCost = (claim.totalCost || 0) + cost;
        }
        await claim.save();

        // Sync usedCoverage on Warranty based on total claim cost
        try {
            if (claim.warrantyId) {
                const agg = await Claim.aggregate([
                    { $match: { warrantyId: claim.warrantyId } },
                    { $group: { _id: '$warrantyId', totalUsed: { $sum: { $ifNull: ['$totalCost', 0] } } } }
                ]);
                const totalUsed = agg && agg[0] ? Number(agg[0].totalUsed || 0) : 0;
                await Warranty.findByIdAndUpdate(claim.warrantyId, { usedCoverage: totalUsed });
                await expireWarrantyIfNoRemaining(claim.warrantyId);
            }
        } catch (e) {
            console.error('Failed to sync usedCoverage from claims:', e);
        }

        if (io) io.emit('claimUpdate', { claimId: claim.claimId, id: claim._id.toString(), warrantyId: claim.warrantyId?.toString() });

        res.json({ success: true, claim });
    } catch (err) {
        res.status(400).json({ success: false, message: err.message });
    }
});

// Customer decision when repair cost exceeds remaining coverage
app.post('/api/claims/:id/decision', async (req, res) => {
    try {
        const claim = await Claim.findById(req.params.id);
        if (!claim) return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลการเคลม' });

        const warranty = claim.warrantyId ? await Warranty.findById(claim.warrantyId) : null;
        if (!warranty) return res.status(400).json({ success: false, message: 'ไม่พบข้อมูลสัญญาประกัน' });

        const decision = String(req.body.decision || '').trim();
        const staffName = String(req.body.staffName || claim.staffName || warranty.staffName || 'System').trim();

        const excessCost = Number(claim.excessCost || 0);
        const refundAmount = Number(claim.refundAmount || 0);

        if (claim.status !== 'รอการตัดสินใจจากลูกค้า') {
            return res.status(400).json({ success: false, message: 'สถานะเคลมไม่อยู่ในขั้นตอนรอการตัดสินใจ' });
        }
        if (!Number.isFinite(excessCost) || excessCost < 0 || !Number.isFinite(refundAmount) || refundAmount < 0) {
            return res.status(400).json({ success: false, message: 'ข้อมูลวงเงินส่วนต่างไม่ถูกต้อง' });
        }

        if (decision === 'pay_excess') {
            const paymentMethod = String(req.body.paymentMethod || '').trim();
            const cashReceived = Math.max(0, Number(req.body.cashReceived || 0));
            const transferAmount = Math.max(0, Number(req.body.transferAmount || 0));

            let pMethod = paymentMethod;
            if (!pMethod) {
                if (cashReceived > 0) pMethod = 'เงินสด';
                else if (transferAmount > 0) pMethod = 'โอนเงิน';
            }

            await FinanceTransaction.create({
                policyNumber: claim.policyNumber,
                customerName: claim.customerName || warranty.customer?.firstName || '-',
                actionType: 'ชำระค่าซ่อมส่วนต่าง',
                paymentMethod: pMethod,
                cashReceived: cashReceived,
                transferAmount: transferAmount,
                changeAmount: 0,
                netTotal: excessCost,
                recordedBy: staffName
            });

            claim.customerDecision = 'จ่ายส่วนต่าง';
            claim.status = 'รอเคลม';
            claim.totalCost = (claim.totalCost || 0) + refundAmount;
            claim.updates.push({
                step: (claim.updates ? claim.updates.length : 0) + 2,
                title: 'ลูกค้าตกลงรับเครื่องคืนและชำระเงินส่วนต่าง',
                date: new Date(),
                cost: excessCost,
                images: [],
                evidenceImages: []
            });

            await claim.save();

            const warrantyNewUsed = Number(warranty.usedCoverage || 0) + refundAmount;
            await Warranty.findByIdAndUpdate(warranty._id, { usedCoverage: warrantyNewUsed });
            await expireWarrantyIfNoRemaining(warranty._id);

            if (io) io.emit('claimUpdate', { claimId: claim.claimId, id: claim._id.toString(), warrantyId: claim.warrantyId?.toString() });
            return res.json({ success: true, claim });
        }

        if (decision === 'refund') {
            await FinanceTransaction.create({
                policyNumber: claim.policyNumber,
                customerName: claim.customerName || warranty.customer?.firstName || '-',
                actionType: 'คืนเงินชดเชยสละสิทธิ์เครื่อง',
                paymentMethod: 'คืนเงิน',
                cashReceived: 0,
                transferAmount: 0,
                changeAmount: 0,
                netTotal: -Math.abs(refundAmount),
                recordedBy: staffName
            });

            claim.customerDecision = 'รับเงินชดเชย';
            claim.status = 'ลูกค้าสละสิทธิ์เครื่อง';
            claim.updates.push({
                step: (claim.updates ? claim.updates.length : 0) + 2,
                title: 'ลูกค้าสละสิทธิ์เครื่องและรับเงินชดเชย',
                date: new Date(),
                cost: 0,
                images: [],
                evidenceImages: []
            });
            await claim.save();

            const warrantyNewUsed = Number(warranty.usedCoverage || 0) + refundAmount;
            await Warranty.findByIdAndUpdate(warranty._id, {
                usedCoverage: warrantyNewUsed,
                claimStatus: 'completed'
            });
            await expireWarrantyIfNoRemaining(warranty._id);

            if (io) io.emit('claimUpdate', { claimId: claim.claimId, id: claim._id.toString(), warrantyId: claim.warrantyId?.toString() });
            return res.json({ success: true, claim });
        }

        return res.status(400).json({ success: false, message: 'decision ไม่ถูกต้อง' });
    } catch (err) {
        return res.status(400).json({ success: false, message: err.message });
    }
});

// Complete a claim (ลูกค้ามารับเครื่องแล้ว หรือ จัดส่งกลับ)
app.post('/api/claims/:id/complete', claimUpload.fields([
    { name: 'deviceImage', maxCount: 10 },
    { name: 'boxImage', maxCount: 10 },
    { name: 'receiptImage', maxCount: 10 },
    { name: 'customerImage', maxCount: 10 }
]), async (req, res) => {
    try {
        const claim = await Claim.findById(req.params.id);
        if (!claim) return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลการเคลม' });

        const { returnMethod, pickupBranch, deliveryAddressType, deliveryAddressDetail } = req.body;

        const images = [];
        if (req.files) {
            ['deviceImage', 'boxImage', 'receiptImage', 'customerImage'].forEach(field => {
                if (req.files[field]) {
                    req.files[field].forEach(file => images.push(file.path));
                }
            });
        }

        claim.completedReturnMethod = returnMethod;
        let title = 'ปิดงานเคลม: ';

        if (returnMethod === 'pickup') {
            claim.completedReturnBranch = pickupBranch;
            title += `ลูกค้ามารับเครื่องที่สาขา ${pickupBranch || ''}`;
        } else if (returnMethod === 'delivery') {
            claim.completedDeliveryAddressType = deliveryAddressType;
            claim.completedDeliveryAddressDetail = deliveryAddressDetail;
            title += 'จัดส่งเรียบร้อยแล้ว';
        }

        // Determine next step number
        const nextStep = (claim.updates ? claim.updates.length : 0) + 2;

        // Update claim status to 'รับเครื่องแล้ว' automatically
        claim.status = 'รับเครื่องแล้ว';
        claim.pickupDate = new Date();

        // Add completion update
        claim.updates.push({
            step: nextStep,
            title: title,
            date: new Date(),
            cost: 0,
            images: images
        });

        await claim.save();

        // Update Warranty status back to 'normal' (active)
        await Warranty.findByIdAndUpdate(claim.warrantyId, { claimStatus: 'normal' });

        if (io) io.emit('claimUpdate', { claimId: claim.claimId, id: claim._id.toString(), warrantyId: claim.warrantyId?.toString() });

        res.json({ success: true, claim });
    } catch (err) {
        res.status(400).json({ success: false, message: err.message });
    }
});

// Get public claim tracking info
app.get('/api/public/track/:claimId', async (req, res) => {
    try {
        const { claimId } = req.params;
        const claim = await Claim.findOne({ claimId });

        if (!claim) {
            return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลการเคลม' });
        }

        // Calculate Remaining Balance
        let remainingBalance = 0;
        let coverageLimit = 0;

        if (claim.warrantyId) {
            const warranty = await Warranty.findById(claim.warrantyId);
            if (warranty) {
                // Calculate Total Used Amount (Sum of all claims for this warranty)
                const allClaims = await Claim.find({ warrantyId: claim.warrantyId });
                const totalUsed = allClaims.reduce((sum, c) => sum + (c.totalCost || 0), 0);

                const usedCoverage = Number.isFinite(Number(warranty.usedCoverage))
                    ? Number(warranty.usedCoverage)
                    : totalUsed;

                const basePrice = Number(warranty.devicePrice ?? warranty.device?.deviceValue ?? 0);
                const maxLimit = Math.floor(basePrice * 0.70);
                const paid = Number(warranty.installmentsPaid ?? 1);
                const currentLimit = paid >= 3
                    ? Math.floor(maxLimit * 1.0)
                    : (paid === 2 ? Math.floor(maxLimit * 0.30) : Math.floor(maxLimit * 0.10));

                coverageLimit = currentLimit;
                remainingBalance = coverageLimit - usedCoverage;
            }
        }

        // Return only necessary public info
        const publicData = {
            claimId: claim.claimId,
            deviceModel: claim.deviceModel,
            symptoms: claim.symptoms,
            status: claim.status,
            totalCost: claim.totalCost,
            coverageLimit: coverageLimit,
            remainingBalance: remainingBalance,
            updates: claim.updates.sort((a, b) => new Date(b.date) - new Date(a.date)), // Sort newest first
            timestamp: new Date()
        };

        res.json({ success: true, data: publicData });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Get single claim
app.get('/api/claims/:id', async (req, res) => {
    try {
        const claims = await Claim.aggregate([
            { $match: { _id: new mongoose.Types.ObjectId(req.params.id) } },
            {
                $lookup: {
                    from: 'warranties',
                    localField: 'warrantyId',
                    foreignField: '_id',
                    as: 'warrantyInfo'
                }
            },
            {
                $addFields: {
                    'imei': { $ifNull: ['$imei', { $arrayElemAt: ['$warrantyInfo.device.imei', 0] }] },
                    'serialNumber': { $ifNull: ['$serialNumber', { $arrayElemAt: ['$warrantyInfo.device.serial', 0] }] },
                    'color': { $ifNull: ['$color', { $arrayElemAt: ['$warrantyInfo.device.color', 0] }] }
                }
            },
            { $project: { warrantyInfo: 0 } }
        ]);

        if (!claims || claims.length === 0) {
            return res.status(404).json({ message: 'ไม่พบข้อมูลการเคลม' });
        }
        res.json(claims[0]);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Save signatures for a claim
app.put('/api/claims/:id/signatures', async (req, res) => {
    try {
        const { customerSignature, staffSignature, managerSignature } = req.body;
        const claim = await Claim.findByIdAndUpdate(
            req.params.id,
            { customerSignature, staffSignature, managerSignature },
            { new: true }
        );
        if (!claim) return res.status(404).json({ message: 'ไม่พบข้อมูลการเคลม' });
        res.json({ success: true, claim });
    } catch (err) {
        res.status(400).json({ message: err.message });
    }
});

// Member API Routes

// Get all members
app.get('/api/members', async (req, res) => {
    try {
        const members = await Member.find().sort({ createdAt: -1 }).lean();

        const cutoff = new Date(Date.now() - (5 * 24 * 60 * 60 * 1000));
        const overdueWarranties = await Warranty.find({
            'payment.method': 'Installment',
            'payment.schedule': {
                $elemMatch: {
                    status: 'Pending',
                    dueDate: { $lt: cutoff }
                }
            }
        })
            .select({ memberId: 1, policyNumber: 1, payment: 1 })
            .lean();

        const reasonsByMemberId = new Map();
        for (const w of overdueWarranties) {
            const mId = String(w.memberId || '');
            if (!mId) continue;
            const schedule = (w && w.payment && Array.isArray(w.payment.schedule)) ? w.payment.schedule : [];
            for (const s of schedule) {
                const due = s && s.dueDate ? new Date(s.dueDate) : null;
                if (!due) continue;
                if (s.status === 'Pending' && due < cutoff) {
                    const daysOverdue = Math.floor((Date.now() - due.getTime()) / 86400000);
                    const arr = reasonsByMemberId.get(mId) || [];
                    arr.push({
                        type: 'installment_overdue',
                        policyNumber: w.policyNumber || '-',
                        installmentNo: s.installmentNo,
                        dueDate: s.dueDate,
                        daysOverdue
                    });
                    reasonsByMemberId.set(mId, arr);
                }
            }
        }

        const enriched = members.map(m => {
            const reasons = reasonsByMemberId.get(String(m.memberId || '')) || [];
            return {
                ...m,
                memberStatus: reasons.length > 0 ? 'ไม่ปกติ' : 'ปกติ',
                blacklistReasons: reasons
            };
        });

        res.json(enriched);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// Create new member
app.post('/api/members', async (req, res) => {
    try {
        const { phone, citizenId, postalCode } = req.body;

        const normalizeDigits = (v) => String(v || '').replace(/\D/g, '');
        const phoneDigits = normalizeDigits(phone);
        const postalDigits = normalizeDigits(postalCode);

        if (!phoneDigits || phoneDigits.length !== 10) {
            return res.status(400).json({ success: false, message: 'กรุณากรอกเบอร์โทรศัพท์เป็นตัวเลข 10 หลัก' });
        }
        if (postalDigits && postalDigits.length !== 5) {
            return res.status(400).json({ success: false, message: 'กรุณากรอกรหัสไปรษณีย์เป็นตัวเลข 5 หลัก' });
        }

        if (citizenId) {
            const existingCitizen = await Member.findOne({ citizenId });
            if (existingCitizen) {
                return res.status(400).json({ success: false, message: 'เลขบัตรประชาชนนี้ถูกใช้งานแล้ว' });
            }
        }

        const existingMember = await Member.findOne({ phone: phoneDigits });
        if (existingMember) {
            return res.status(400).json({ success: false, message: 'เบอร์โทรศัพท์นี้ถูกใช้งานแล้ว' });
        }

        // Generate Unique Member ID: SMCxxxxxx
        let memberId;
        let isUnique = false;
        while (!isUnique) {
            const randomNum = Math.floor(100000 + Math.random() * 900000); // 6 digits
            memberId = `SMC${randomNum}`;
            const existingId = await Member.findOne({ memberId });
            if (!existingId) isUnique = true;
        }

        const newMember = new Member({
            ...req.body,
            phone: phoneDigits,
            postalCode: postalDigits || req.body.postalCode,
            memberId
        });
        await newMember.save();
        res.status(201).json({ success: true, member: newMember });
    } catch (err) {
        res.status(400).json({ success: false, message: err.message });
    }
});

// Lookup members by phone, memberId, or Name (Partial match)
app.get('/api/members/lookup', async (req, res) => {
    try {
        const { query } = req.query;
        if (!query) return res.status(400).json({ success: false, message: 'กรุณาระบุข้อมูลสำหรับค้นหา' });

        // Search in multiple fields using case-insensitive regex
        const searchRegex = new RegExp(query, 'i');
        const members = await Member.find({
            $or: [
                { phone: searchRegex },
                { memberId: searchRegex },
                { citizenId: searchRegex },
                { firstName: searchRegex },
                { lastName: searchRegex }
            ]
        }).limit(10).lean(); // Limit results for UI performance

        const enriched = await Promise.all(
            members.map(async (m) => {
                const reasons = await getMemberBlacklistReasonsByMemberId(m.memberId);
                return {
                    ...m,
                    memberStatus: reasons.length > 0 ? 'ไม่ปกติ' : 'ปกติ'
                };
            })
        );

        res.json({ success: true, members: enriched });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Get single member
app.get('/api/members/:id', async (req, res) => {
    try {
        const member = await Member.findById(req.params.id).lean();
        if (!member) return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลสมาชิก' });

        const reasons = await getMemberBlacklistReasonsByMemberId(member.memberId);

        res.json({
            ...member,
            memberStatus: reasons.length > 0 ? 'ไม่ปกติ' : 'ปกติ',
            blacklistReasons: reasons
        });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Update member
app.put('/api/members/:id', async (req, res) => {
    try {
        const { phone, citizenId, postalCode } = req.body;

        const normalizeDigits = (v) => String(v || '').replace(/\D/g, '');
        const phoneDigits = normalizeDigits(phone);
        const postalDigits = normalizeDigits(postalCode);

        if (!phoneDigits || phoneDigits.length !== 10) {
            return res.status(400).json({ success: false, message: 'กรุณากรอกเบอร์โทรศัพท์เป็นตัวเลข 10 หลัก' });
        }
        if (postalDigits && postalDigits.length !== 5) {
            return res.status(400).json({ success: false, message: 'กรุณากรอกรหัสไปรษณีย์เป็นตัวเลข 5 หลัก' });
        }

        // Check if phone unique but not current member
        const existingMember = await Member.findOne({ phone: phoneDigits, _id: { $ne: req.params.id } });
        if (existingMember) {
            return res.status(400).json({ success: false, message: 'เบอร์โทรศัพท์นี้ถูกใช้งานโดยสมาชิกท่านอื่นแล้ว' });
        }

        if (citizenId) {
            const existingCitizen = await Member.findOne({ citizenId, _id: { $ne: req.params.id } });
            if (existingCitizen) {
                return res.status(400).json({ success: false, message: 'เลขบัตรประชาชนนี้ถูกใช้งานโดยสมาชิกท่านอื่นแล้ว' });
            }
        }

        const updatedMember = await Member.findByIdAndUpdate(
            req.params.id,
            { ...req.body, phone: phoneDigits, postalCode: postalDigits || req.body.postalCode },
            { new: true }
        );
        if (!updatedMember) return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลสมาชิก' });
        res.json({ success: true, member: updatedMember });
    } catch (err) {
        res.status(400).json({ success: false, message: err.message });
    }
});

// Delete member
app.delete('/api/members/:id', async (req, res) => {
    try {
        const deleted = await Member.findByIdAndDelete(req.params.id);
        if (!deleted) return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลสมาชิก' });
        res.json({ success: true, message: 'ลบข้อมูลสมาชิกสำเร็จ' });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// --- Shops API ---

// Get all shops
app.get('/api/shops', async (req, res) => {
    try {
        const shops = await Shop.find().sort({ createdAt: -1 });
        res.json(shops);
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Create new shop
app.post('/api/shops', async (req, res) => {
    try {
        // Generate Unique Shop ID: SMP + 6 digits
        let shopId;
        let isUnique = false;
        while (!isUnique) {
            const random = Math.floor(100000 + Math.random() * 900000).toString();
            shopId = 'SMP' + random;
            const existing = await Shop.findOne({ shopId });
            if (!existing) isUnique = true;
        }

        const newShop = new Shop({
            ...req.body,
            shopId
        });
        await newShop.save();
        res.status(201).json({ success: true, shop: newShop });
    } catch (err) {
        res.status(400).json({ success: false, message: err.message });
    }
});

// Update shop
app.put('/api/shops/:id', async (req, res) => {
    try {
        const updatedShop = await Shop.findByIdAndUpdate(
            req.params.id,
            req.body,
            { new: true }
        );
        if (!updatedShop) return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลร้านค้า' });
        res.json({ success: true, shop: updatedShop });
    } catch (err) {
        res.status(400).json({ success: false, message: err.message });
    }
});

// Delete shop
app.delete('/api/shops/:id', async (req, res) => {
    try {
        const deleted = await Shop.findByIdAndDelete(req.params.id);
        if (!deleted) return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลร้านค้า' });
        res.json({ success: true, message: 'ลบข้อมูลร้านค้าสำเร็จ' });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Global Error Handler
app.use((err, req, res, next) => {
    console.error('SERVER ERROR:', err);
    if (err instanceof multer.MulterError) {
        return res.status(400).json({ success: false, message: 'Upload Error: ' + err.message });
    }
    res.status(500).json({ success: false, message: 'Server Error: ' + err.message });
});

// Serve frontend SPA (Fallback)
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// Server Startup Section (Usually at the end)
const startServer = () => {
    const server = http.createServer(app);
    io = new Server(server, { cors: { origin: '*' } });
    server.listen(PORT, () => {
        console.log(`Server running on http://localhost:${PORT}`);
    });
};

startServer();
