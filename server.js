require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const multer = require('multer');

const app = express();
const PORT = process.env.PORT || 3005;

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
    username: { type: String, unique: true, index: true },
    password: { type: String, required: true }
}, { timestamps: true });

const Staff = mongoose.model('Staff', StaffSchema);

// Claim Schema
const ClaimSchema = new mongoose.Schema({
    claimId: { type: String, unique: true, index: true },
    warrantyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Warranty' },
    policyNumber: String,
    memberId: String,
    customerName: String,
    customerPhone: String,
    deviceModel: String,
    claimDate: { type: Date, default: Date.now },
    symptoms: String,
    images: [String],
    staffName: String,
    returnMethod: { type: String, enum: ['pickup', 'delivery'] },
    pickupBranch: String,
    customerSignature: String,
    staffSignature: String,
    managerSignature: String,
    status: { type: String, default: 'pending', enum: ['pending', 'in_progress', 'completed', 'rejected'] }
}, { timestamps: true });

const Claim = mongoose.model('Claim', ClaimSchema);

// API Routes

// Staff Registration
app.post('/api/register', async (req, res) => {
    try {
        const { staffName, username, password } = req.body;

        // Check if username exists
        const existingStaff = await Staff.findOne({ username });
        if (existingStaff) {
            return res.status(400).json({ success: false, message: 'Username already exists' });
        }

        const staffId = 'STF' + Math.floor(Math.random() * 1000).toString().padStart(3, '0');
        const newStaff = new Staff({ staffId, staffName, username, password });
        await newStaff.save();

        res.status(201).json({ success: true, user: { staffName, staffId } });
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
                user: { staffName: staff.staffName, staffId: staff.staffId }
            });
        } else {
            // Fallback for admin if no staff exists yet (optional, but keep for convenience as per requirement)
            if (username === 'admin' && password === '1234') {
                return res.json({
                    success: true,
                    user: { staffName: 'Admin', staffId: 'STF000' }
                });
            }
            res.status(401).json({ success: false, message: 'Invalid credentials' });
        }
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// Get all warranties (Enriched with Member Data)
app.get('/api/warranties', async (req, res) => {
    try {
        const warranties = await Warranty.aggregate([
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
                $addFields: {
                    'customer.citizenId': { $arrayElemAt: ['$memberInfo.citizenId', 0] },
                    'customer.facebook': { $arrayElemAt: ['$memberInfo.facebook', 0] },
                    'customer.id': '$memberId'
                }
            },
            { $project: { memberInfo: 0 } }
        ]);
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
        await newWarranty.save();
        res.status(201).json(newWarranty);
    } catch (err) {
        res.status(400).json({ message: err.message });
    }
});

// Get warranties filtered by approvalStatus (Enriched with Member Data)
app.get('/api/warranties/pending', async (req, res) => {
    try {
        const status = req.query.status || 'pending';
        const matchQuery = {};
        if (status !== 'all') {
            matchQuery.approvalStatus = status;
        }

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
                $addFields: {
                    'customer.citizenId': { $arrayElemAt: ['$memberInfo.citizenId', 0] },
                    'customer.id': '$memberId'
                }
            },
            { $project: { memberInfo: 0 } }
        ]);
        res.json(warranties);
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
        const warranties = await Warranty.aggregate([
            {
                $match: {
                    approvalStatus: 'approved',
                    'warrantyDates.end': { $gte: now }
                }
            },
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
                $addFields: {
                    'customer.citizenId': { $arrayElemAt: ['$memberInfo.citizenId', 0] },
                    'customer.facebook': { $arrayElemAt: ['$memberInfo.facebook', 0] },
                    'customer.id': '$memberId'
                }
            },
            { $project: { memberInfo: 0 } }
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
                $addFields: {
                    'customer.citizenId': { $arrayElemAt: ['$memberInfo.citizenId', 0] },
                    'customer.facebook': { $arrayElemAt: ['$memberInfo.facebook', 0] },
                    'customer.id': '$memberId'
                }
            },
            { $project: { memberInfo: 0 } }
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
        res.json({ success: true, warranty });
    } catch (err) {
        res.status(400).json({ message: err.message });
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
        const { warrantyId, policyNumber, memberId, customerName, customerPhone, deviceModel, symptoms, staffName, returnMethod, pickupBranch } = req.body;

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

        const newClaim = new Claim({
            claimId,
            warrantyId,
            policyNumber,
            memberId,
            customerName,
            customerPhone,
            deviceModel,
            claimDate: new Date(),
            symptoms,
            images,
            staffName,
            returnMethod,
            pickupBranch: returnMethod === 'pickup' ? pickupBranch : ''
        });

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
        const claims = await Claim.find().sort({ createdAt: -1 });
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


// Get single claim
app.get('/api/claims/:id', async (req, res) => {
    try {
        const claim = await Claim.findById(req.params.id);
        if (!claim) return res.status(404).json({ message: 'ไม่พบข้อมูลการเคลม' });
        res.json(claim);
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
        const { phone, citizenId } = req.body;

        if (citizenId) {
            const existingCitizen = await Member.findOne({ citizenId });
            if (existingCitizen) {
                return res.status(400).json({ success: false, message: 'เลขบัตรประชาชนนี้ถูกใช้งานแล้ว' });
            }
        }

        const existingMember = await Member.findOne({ phone });
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

        const newMember = new Member({ ...req.body, memberId });
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
        const { phone, citizenId } = req.body;
        // Check if phone unique but not current member
        const existingMember = await Member.findOne({ phone, _id: { $ne: req.params.id } });
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
            req.body,
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
    app.listen(PORT, () => {
        console.log(`Server running on http://localhost:${PORT}`);
    });
};

startServer();
