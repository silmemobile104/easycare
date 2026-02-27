require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const http = require('http');
const { Server } = require('socket.io');

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
    status: { type: String, default: 'รอเคลม', enum: ['รอเคลม', 'รับเครื่องแล้ว'] },
    totalCost: { type: Number, default: 0 },
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

// ═══════════════════════════════════════════════════════════════════
// FILTER HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════

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

// Get active warranties only (approved + not expired) — for claim menu
app.get('/api/warranties/active', async (req, res) => {
    try {
        const now = new Date();
        const baseMatch = {
            approvalStatus: 'approved',
            'warrantyDates.end': { $gte: now }
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
        const { installmentNo, payAllRemaining, paidCash, paidTransfer, refId } = req.body;
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
        const matchQuery = buildClaimFilterMatch(req.query, { status: 'รอเคลม' });

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

        claim.totalCost = (claim.totalCost || 0) + cost;
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
        const members = await Member.find().sort({ createdAt: -1 });
        res.json(members);
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
        }).limit(10); // Limit results for UI performance

        res.json({ success: true, members });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Get single member
app.get('/api/members/:id', async (req, res) => {
    try {
        const member = await Member.findById(req.params.id);
        if (!member) return res.status(404).json({ success: false, message: 'ไม่พบข้อมูลสมาชิก' });
        res.json(member);
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
