document.addEventListener('DOMContentLoaded', () => {
    // ═══════════════════════════════════════════════════════════════════
    // SWEETALERT2 HELPER FUNCTIONS - SmileCare Theme
    // ═══════════════════════════════════════════════════════════════════

    // Base SweetAlert2 configuration matching SmileCare theme
    const SwalTheme = Swal.mixin({
        customClass: {
            popup: 'swal-popup-custom',
            title: 'swal-title-custom',
            confirmButton: 'swal-confirm-btn',
            cancelButton: 'swal-cancel-btn',
            denyButton: 'swal-deny-btn'
        },
        buttonsStyling: false,
        confirmButtonColor: '#0d9488',
        cancelButtonColor: '#ef4444',
        background: 'rgba(255, 255, 255, 0.95)',
        backdrop: 'rgba(15, 23, 42, 0.6)'
    });

    /**
     * Show a styled alert notification
     * @param {string} type - 'success' | 'error' | 'warning' | 'info'
     * @param {string} message - Message to display
     * @param {string} [title] - Optional title (auto-generated if not provided)
     */
    window.showAlert = function (type, message, title = null) {
        const titles = {
            success: 'สำเร็จ!',
            error: 'เกิดข้อผิดพลาด!',
            warning: 'คำเตือน!',
            info: 'แจ้งเตือน'
        };

        return SwalTheme.fire({
            icon: type,
            title: title || titles[type] || '',
            text: message,
            confirmButtonText: 'ตกลง',
            timer: type === 'success' ? 3000 : undefined,
            timerProgressBar: type === 'success'
        });
    };

    /**
     * Show a confirmation dialog with Confirm/Cancel buttons
     * @param {string} title - Title of the dialog
     * @param {string} message - Warning message
     * @param {string} [confirmText='ยืนยัน'] - Confirm button text
     * @param {string} [cancelText='ยกเลิก'] - Cancel button text
     * @returns {Promise<boolean>} - Returns true if confirmed, false otherwise
     */
    window.showConfirm = async function (title, message, confirmText = 'ยืนยัน', cancelText = 'ยกเลิก') {
        const result = await SwalTheme.fire({
            icon: 'warning',
            title: title,
            text: message,
            showCancelButton: true,
            confirmButtonText: confirmText,
            cancelButtonText: cancelText,
            reverseButtons: true
        });
        return result.isConfirmed;
    };

    /**
     * Show a delete confirmation dialog (red themed)
     * @param {string} [message] - Custom warning message
     * @returns {Promise<boolean>} - Returns true if confirmed
     */
    window.showDeleteConfirm = async function (message = 'การกระทำนี้ไม่สามารถย้อนกลับได้') {
        const result = await SwalTheme.fire({
            icon: 'warning',
            title: 'คุณแน่ใจหรือไม่?',
            text: message,
            showCancelButton: true,
            confirmButtonText: '🗑️ ลบข้อมูล',
            cancelButtonText: 'ยกเลิก',
            confirmButtonColor: '#ef4444',
            reverseButtons: true
        });
        return result.isConfirmed;
    };

    /**
     * Show a toast notification (top-end position)
     * @param {string} type - 'success' | 'error' | 'warning' | 'info'
     * @param {string} message - Message to display
     */
    window.showToast = function (type, message) {
        const Toast = Swal.mixin({
            toast: true,
            position: 'top-end',
            showConfirmButton: false,
            timer: 3000,
            timerProgressBar: true,
            didOpen: (toast) => {
                toast.addEventListener('mouseenter', Swal.stopTimer);
                toast.addEventListener('mouseleave', Swal.resumeTimer);
            }
        });

        Toast.fire({
            icon: type,
            title: message
        });
    };

    // ═══════════════════════════════════════════════════════════════════
    // GLOBAL LOADER FUNCTIONS
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Show the global loading overlay
     * @param {string} [text='กำลังโหลด...'] - Optional loading text
     */
    window.showLoader = function (text = 'กำลังโหลด...') {
        const loader = document.getElementById('global-loader');
        if (loader) {
            const textEl = loader.querySelector('.loader-text');
            if (textEl) textEl.textContent = text;
            loader.style.display = 'flex';
            // Force reflow for animation
            loader.offsetHeight;
            loader.classList.add('active');
        }
    };

    /**
     * Hide the global loading overlay
     */
    window.hideLoader = function () {
        const loader = document.getElementById('global-loader');
        if (loader) {
            loader.classList.remove('active');
            setTimeout(() => {
                loader.style.display = 'none';
            }, 300); // Match CSS transition
        }
    };

    // --- APP STATE ---
    let currentUser = JSON.parse(localStorage.getItem('smilecare_staff_session'));
    const canApproveContracts = !!(
        currentUser &&
        (currentUser.staffPosition === 'อนุมัติสัญญา' || currentUser.staffId === 'STF000' || currentUser.staffName === 'Admin')
    );

    // --- SOCKET.IO (for real-time notifications) ---
    let socket = null;
    try {
        if (typeof io !== 'undefined') {
            socket = io();
        }
    } catch (e) {
        console.warn('Socket.io init failed:', e);
    }

    const urgentApprovalQueue = [];
    let urgentApprovalProcessing = false;

    function playUrgentApprovalSound() {
        try {
            const audio = new Audio('/alert.mp3');
            audio.currentTime = 0;
            const p = audio.play();
            if (p && typeof p.catch === 'function') {
                p.catch(() => { /* ignore autoplay block */ });
            }
        } catch (e) {
            // ignore
        }
    }

    async function updateStatusTrackingBadge({ showToastIfOverdue = false } = {}) {
        if (!currentUser) return;
        try {
            const res = await fetch('/api/claims/pending');
            const claims = await res.json();
            const overdueCount = (Array.isArray(claims) ? claims : []).filter(c => c && c.isOverdue).length;

            const badge = document.getElementById('statusTrackingBadge');
            if (badge) {
                if (overdueCount > 0) {
                    badge.textContent = overdueCount > 99 ? '99+' : overdueCount;
                    badge.style.display = 'inline-flex';
                } else {
                    badge.style.display = 'none';
                }
            }

            if (showToastIfOverdue && overdueCount > 0) {
                const lastToastAt = parseInt(localStorage.getItem('smilecare_overdue_claim_toast_ts') || '0', 10);
                const now = Date.now();
                if (!Number.isFinite(lastToastAt) || (now - lastToastAt) > (60 * 60 * 1000)) {
                    showToast('warning', `แจ้งเตือน: มีงานเคลม ${overdueCount} รายการที่ไม่ได้อัปเดตสถานะเกิน 5 วัน!`);
                    localStorage.setItem('smilecare_overdue_claim_toast_ts', String(now));
                }
            }
        } catch (err) {
            console.error('Error fetching status tracking overdue count:', err);
        }
    }

    async function processUrgentApprovalQueue() {
        if (urgentApprovalProcessing) return;
        urgentApprovalProcessing = true;

        try {
            while (urgentApprovalQueue.length > 0) {
                const payload = urgentApprovalQueue.shift();
                const customerName = (payload && payload.customerName) ? payload.customerName : '-';

                playUrgentApprovalSound();

                const result = await SwalTheme.fire({
                    title: 'มีรายการรออนุมัติใหม่!',
                    text: `ลูกค้า: ${customerName} กำลังรอการอนุมัติสัญญา`,
                    icon: 'warning',
                    allowOutsideClick: false,
                    allowEscapeKey: false,
                    showCancelButton: true,
                    confirmButtonText: 'ไปหน้าอนุมัติเดี๋ยวนี้',
                    cancelButtonText: 'ปิดแจ้งเตือน',
                    confirmButtonColor: '#0d9488'
                });

                if (result.isConfirmed) {
                    if (typeof showView === 'function') {
                        showView('approval');
                    } else if (approvalNavLink) {
                        approvalNavLink.click();
                    }
                }
            }
        } finally {
            urgentApprovalProcessing = false;
        }
    }
    let isEditMode = false;
    let currentEditData = null;
    let allRecords = []; // Global state for filtering
    const PACKAGE_PRICES = {
        'Plan A': 799, 'Plan B': 1499, 'Plan C': 1699, 'Plan D': 2399,
        'Plan a': 599, 'Plan b': 999, 'Plan c': 1299,
        'Plan 1': 399, 'Plan 2': 799, 'Plan 3': 1099
    };
    const formatDate = (dateStr) => dateStr ? new Date(dateStr).toISOString().split('T')[0] : '';

    // --- DOM ELEMENTS ---
    const views = {
        login: document.getElementById('loginView'),
        dashboard: document.getElementById('dashboardView'),
        registration: document.getElementById('registrationView'),
        staffRegister: document.getElementById('staffRegisterView')
    };

    // --- VIEW CONTROLLER ---
    function showView(viewName) {
        if (viewName === 'approval' && !canApproveContracts) {
            showAlert('warning', 'บัญชีของคุณไม่มีสิทธิ์เข้าเมนูอนุมัติสัญญา');
            viewName = 'dashboard';
        }
        Object.keys(views).forEach(key => {
            if (views[key]) views[key].style.display = 'none';
        });

        // Sub-view logic for dashboard/members
        const shopsMain = document.getElementById('shopsMain');
        const dashNavLink = document.getElementById('dashNavLink');
        const membersNavLink = document.getElementById('membersNavLink');
        const shopsNavLink = document.getElementById('shopsNavLink');

        if (viewName === 'dashboard' || viewName === 'members' || viewName === 'shops' || viewName === 'claims' || viewName === 'statusTracking' || viewName === 'approval') {
            views.dashboard.style.display = 'block';

            // Hide all sub-views first
            dashMain.style.display = 'none';
            membersMain.style.display = 'none';
            shopsMain.style.display = 'none';
            const claimsMain = document.getElementById('claimsMain');
            if (claimsMain) claimsMain.style.display = 'none';
            const statusTrackingMain = document.getElementById('statusTrackingMain');
            if (statusTrackingMain) statusTrackingMain.style.display = 'none';
            const approvalMain = document.getElementById('approvalMain');
            if (approvalMain) approvalMain.style.display = 'none';

            // Remove active class from all nav links
            if (dashNavLink) dashNavLink.classList.remove('active');
            if (membersNavLink) membersNavLink.classList.remove('active');
            if (shopsNavLink) shopsNavLink.classList.remove('active');
            const claimsNavLinkEl = document.getElementById('claimsNavLink');
            if (claimsNavLinkEl) claimsNavLinkEl.classList.remove('active');
            const statusTrackingNavLinkEl = document.getElementById('statusTrackingNavLink');
            if (statusTrackingNavLinkEl) statusTrackingNavLinkEl.classList.remove('active');
            const approvalNavLinkEl = document.getElementById('approvalNavLink');
            if (approvalNavLinkEl) approvalNavLinkEl.classList.remove('active');

            if (viewName === 'dashboard') {
                dashMain.style.display = 'block';
                if (dashNavLink) dashNavLink.classList.add('active');
                fetchWarranties();
            } else if (viewName === 'members') {
                membersMain.style.display = 'block';
                if (membersNavLink) membersNavLink.classList.add('active');
                fetchMembers();
            } else if (viewName === 'shops') {
                shopsMain.style.display = 'block';
                if (shopsNavLink) shopsNavLink.classList.add('active');
                fetchShops();
            } else if (viewName === 'claims') {
                const claimsMainEl = document.getElementById('claimsMain');
                if (claimsMainEl) claimsMainEl.style.display = 'block';
                const claimsNavLinkEl2 = document.getElementById('claimsNavLink');
                if (claimsNavLinkEl2) claimsNavLinkEl2.classList.add('active');
                fetchActivePolicies();
            } else if (viewName === 'statusTracking') {
                if (statusTrackingMain) statusTrackingMain.style.display = 'block';
                if (statusTrackingNavLinkEl) statusTrackingNavLinkEl.classList.add('active');
                fetchPendingClaims();
                stopApprovalAutoRefresh(); // Stop approval refresh if leaving that view
            } else if (viewName === 'approval') {
                const approvalMain = document.getElementById('approvalMain');
                if (approvalMain) approvalMain.style.display = 'block';
                const approvalNavLinkEl = document.getElementById('approvalNavLink');
                if (approvalNavLinkEl) approvalNavLinkEl.classList.add('active');

                // Initialize approval view
                fetchApprovalWarranties('pending');
                startApprovalAutoRefresh();
            } else {
                // For other dashboard views, ensure approval refresh is stopped
                stopApprovalAutoRefresh();
            }
        } else if (views[viewName]) {
            views[viewName].style.display = 'block';
            if (viewName === 'registration') initRegistrationForm();
        }

        // Always try to update badge if logged in
        if (currentUser) {
            updateApprovalBadge();
            startBadgeInterval();
        }
    }

    // --- BADGE LOGIC ---
    let badgeInterval;
    let statusTrackingBadgeInterval;

    async function updateApprovalBadge() {
        if (!currentUser) return;
        try {
            const res = await fetch('/api/warranties/pending-count');
            const data = await res.json();

            const badge = document.getElementById('approvalBadge');
            if (badge) {
                if (data.count > 0) {
                    badge.textContent = data.count > 99 ? '99+' : data.count;
                    badge.style.display = 'inline-flex';
                } else {
                    badge.style.display = 'none';
                }
            }
        } catch (err) {
            console.error('Error fetching badge count:', err);
        }
    }

    function startBadgeInterval() {
        if (badgeInterval) clearInterval(badgeInterval);
        badgeInterval = setInterval(updateApprovalBadge, 60000); // Update every 60 seconds

        if (statusTrackingBadgeInterval) clearInterval(statusTrackingBadgeInterval);
        statusTrackingBadgeInterval = setInterval(() => updateStatusTrackingBadge(), 60000);
    }

    // --- AUTH LOGIC ---
    const loginForm = document.getElementById('loginForm');
    if (loginForm) {
        loginForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;

            showLoader('กำลังเข้าสู่ระบบ...');
            try {
                const res = await fetch('/api/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ username, password })
                });
                const data = await res.json();

                if (data && data.success) {
                    currentUser = data.user;
                    localStorage.setItem('smilecare_staff_session', JSON.stringify(currentUser));
                    updateStaffInfo();
                    showView('dashboard');

                    updateStatusTrackingBadge({ showToastIfOverdue: true });
                } else {
                    const loginError = document.getElementById('loginError');
                    if (loginError) loginError.style.display = 'block';
                }
            } catch (err) {
                console.error('Fetch error:', err);
            } finally {
                hideLoader();
            }
        });
    }

    // Image preview for evidence images
    const updateEvidenceInput = document.getElementById('updateEvidenceImages');
    if (updateEvidenceInput) {
        updateEvidenceInput.addEventListener('change', function () {
            const preview = document.getElementById('updateEvidencePreview');
            if (!preview) return;
            preview.innerHTML = '';
            Array.from(this.files).forEach(file => {
                const reader = new FileReader();
                reader.onload = (e) => {
                    preview.innerHTML += `<img src="${e.target.result}" style="width: 80px; height: 80px; object-fit: cover; border-radius: 8px; border: 2px solid #f59e0b;">`;
                };
                reader.readAsDataURL(file);
            });
        });
    }

    // --- STAFF REGISTRATION LOGIC ---
    const staffRegisterForm = document.getElementById('staffRegisterForm');
    if (staffRegisterForm) {
        staffRegisterForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const staffName = document.getElementById('regStaffName').value;
            const staffPosition = document.getElementById('regStaffPosition').value;
            const username = document.getElementById('regUsername').value;
            const password = document.getElementById('regPassword').value;

            console.log('Sending registration data:', { staffName, staffPosition, username });
            showLoader('กำลังลงทะเบียน...');
            try {
                const res = await fetch('/api/register', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ staffName, staffPosition, username, password })
                });
                const data = await res.json();
                if (data.success) {
                    showAlert('success', 'ลงทะเบียนสำเร็จ! กรุณาเข้าสู่ระบบ');
                    showView('login');
                } else {
                    const errorElem = document.getElementById('regError');
                    errorElem.textContent = data.message || 'การลงทะเบียนล้มเหลว';
                    errorElem.style.display = 'block';
                }
            } catch (err) {
                console.error('Registration error:', err);
            } finally {
                hideLoader();
            }
        });
    }

    // --- VIEW NAVIGATION ---
    const toRegisterBtn = document.getElementById('toRegister');
    if (toRegisterBtn) toRegisterBtn.addEventListener('click', (e) => { e.preventDefault(); showView('staffRegister'); });

    const toLoginBtn = document.getElementById('toLogin');
    if (toLoginBtn) toLoginBtn.addEventListener('click', (e) => { e.preventDefault(); showView('login'); });

    function updateStaffInfo() {
        if (!currentUser) return;
        const nameElem = document.getElementById('displayStaffName');
        const positionElem = document.getElementById('displayStaffPosition');
        const initialElem = document.getElementById('staffInitial');
        if (nameElem) nameElem.textContent = currentUser.staffName;
        if (positionElem) positionElem.textContent = currentUser.staffPosition;
        if (initialElem) initialElem.textContent = currentUser.staffName.charAt(0);
    }

    if (logoutBtn) {
        logoutBtn.addEventListener('click', () => {
            localStorage.removeItem('smilecare_staff_session');
            currentUser = null;
            showView('login');
        });
    }

    // --- NAVIGATION LINKS ---
    const dashNavLink = document.getElementById('dashNavLink');
    if (dashNavLink) dashNavLink.addEventListener('click', (e) => { e.preventDefault(); showView('dashboard'); });

    const membersNavLink = document.getElementById('membersNavLink');
    if (membersNavLink) membersNavLink.addEventListener('click', (e) => { e.preventDefault(); showView('members'); });

    const shopsNavLink = document.getElementById('shopsNavLink');
    if (shopsNavLink) shopsNavLink.addEventListener('click', (e) => { e.preventDefault(); showView('shops'); });

    const claimsNavLink = document.getElementById('claimsNavLink');
    if (claimsNavLink) claimsNavLink.addEventListener('click', (e) => { e.preventDefault(); showView('claims'); });

    const statusTrackingNavLink = document.getElementById('statusTrackingNavLink');
    if (statusTrackingNavLink) statusTrackingNavLink.addEventListener('click', (e) => { e.preventDefault(); showView('statusTracking'); });

    const approvalNavLink = document.getElementById('approvalNavLink');
    if (approvalNavLink) {
        if (!canApproveContracts) {
            approvalNavLink.style.display = 'none';
        } else {
            approvalNavLink.addEventListener('click', (e) => { e.preventDefault(); showView('approval'); });
        }
    }

    // Urgent Approval Notification (queue)
    if (socket) {
        socket.on('urgent_approval_needed', (payload) => {
            if (!canApproveContracts) return;
            urgentApprovalQueue.push(payload || {});
            processUrgentApprovalQueue();
        });
    }

    // --- DASHBOARD LOGIC ---
    async function fetchWarranties() {
        showLoader('กำลังโหลดข้อมูล...');
        try {
            const res = await fetch('/api/warranties');
            const data = await res.json();
            allRecords = data; // Save to global state
            applyFilters(); // Initial render with filters
        } catch (err) {
            console.error('Fetch error:', err);
        } finally {
            hideLoader();
        }
    }

    function applyFilters() {
        const searchTerm = document.getElementById('searchInput').value.toLowerCase();
        const statusFilter = document.getElementById('statusFilter').value;
        const paymentFilter = document.getElementById('paymentFilter').value;

        const filtered = allRecords.filter(r => {
            // 1. Search filter
            const fullName = `${r.customer.firstName} ${r.customer.lastName}`.toLowerCase();
            const matchesSearch = !searchTerm ||
                (r.policyNumber && r.policyNumber.includes(searchTerm)) ||
                (r.memberId && r.memberId.toLowerCase().includes(searchTerm)) ||
                fullName.includes(searchTerm) ||
                r.customer.phone.includes(searchTerm);

            if (!matchesSearch) return false;

            // 2. Status filter
            const isExpired = new Date(r.warrantyDates.end) < new Date();
            if (statusFilter !== 'all') {
                if (statusFilter === 'active') {
                    if (isExpired) return false;
                    if (r.approvalStatus && r.approvalStatus !== 'approved') return false;
                    if (r.claimStatus && r.claimStatus !== 'normal') return false;
                } else if (statusFilter === 'expired') {
                    if (!isExpired) return false;
                } else if (statusFilter === 'approval_pending') {
                    if (r.approvalStatus !== 'pending') return false;
                } else if (statusFilter === 'approval_approved') {
                    if (r.approvalStatus !== 'approved') return false;
                } else if (statusFilter === 'approval_rejected') {
                    if (r.approvalStatus !== 'rejected') return false;
                } else if (statusFilter === 'claim_pending') {
                    if (r.claimStatus !== 'pending') return false;
                } else if (statusFilter === 'claim_completed') {
                    if (r.claimStatus !== 'completed') return false;
                }
            }

            // 3. Payment filter
            if (paymentFilter !== 'all') {
                const method = r.payment.method;
                if (paymentFilter === 'full' && method !== 'Full Payment') return false;
                if (paymentFilter === 'installment' && method !== 'Installment') return false;
                if (paymentFilter === 'installment_complete') {
                    if (method !== 'Installment') return false;
                    const isComplete = r.payment.schedule.every(s => s.status === 'Paid');
                    if (!isComplete) return false;
                }
            }

            return true;
        });

        renderDashboard(filtered);
    }

    function renderDashboard(records) {
        const body = document.getElementById('recordsBody');
        const totalElem = document.getElementById('totalRecords');
        const activeElem = document.getElementById('activeRecords');
        const expiringElem = document.getElementById('expiringSoon');
        const empty = document.getElementById('emptyState');

        // Calculate Stats
        const now = new Date();
        const thirtyDaysLater = new Date();
        thirtyDaysLater.setDate(now.getDate() + 30);

        const totalCount = records.length;
        const activeCount = records.filter(r => new Date(r.warrantyDates.end) >= now).length;
        const expiringSoonCount = records.filter(r => {
            const end = new Date(r.warrantyDates.end);
            return end > now && end <= thirtyDaysLater;
        }).length;

        if (totalElem) totalElem.textContent = totalCount.toLocaleString();
        if (activeElem) activeElem.textContent = activeCount.toLocaleString();
        if (expiringElem) expiringElem.textContent = expiringSoonCount.toLocaleString();

        if (totalCount === 0) {
            empty.style.display = 'block';
            body.innerHTML = '';
            return;
        }

        empty.style.display = 'none';
        body.innerHTML = records.map(r => {
            const isExpired = new Date(r.warrantyDates.end) < new Date();
            let paymentStatus = '';
            if (r.payment.method === 'Full Payment') {
                const statusText = r.payment.status === 'Paid' ? 'ชำระแล้ว' : 'ค้างชำระ';
                const statusClass = r.payment.status === 'Paid' ? 'status-paid' : 'status-pending';
                paymentStatus = `<span class="status-badge ${statusClass}">${statusText}</span>`;
            } else {
                const paidCount = r.payment.schedule.filter(s => s.status === 'Paid').length;
                const totalCount = 3;
                const statusClass = paidCount === totalCount ? 'status-paid' : 'status-pending';
                paymentStatus = `<span class="status-badge ${statusClass}">ชำระแล้ว ${paidCount}/${totalCount}</span>`;
            }

            // Calculation for Remaining Time: X Days, Y Hours
            const now = new Date();
            const end = new Date(r.warrantyDates.end);
            const diffMs = end - now;
            let timeRemainingText = '';
            if (diffMs > 0) {
                const totalHours = Math.floor(diffMs / (1000 * 60 * 60));
                const days = Math.floor(totalHours / 24);
                const hours = totalHours % 24;
                timeRemainingText = `${days} วัน, ${hours} ชม.`;
            } else {
                timeRemainingText = 'หมดอายุ';
            }

            let statusBadge = '';
            if (r.approvalStatus === 'pending') {
                statusBadge = `<span class="status-badge status-pending">รออนุมัติ</span>`;
            } else if (r.approvalStatus === 'rejected') {
                statusBadge = `<span class="status-badge status-expired">ไม่อนุมัติ</span>`;
            } else if (r.claimStatus === 'pending') {
                statusBadge = `<span class="status-badge status-pending" style="background-color: #f59e0b; color: white;">รอเคลม</span>`;
            } else {
                const isExpired = new Date(r.warrantyDates.end) < new Date();
                statusBadge = `<span class="status-badge ${isExpired ? 'status-expired' : 'status-active'}">${isExpired ? 'หมดอายุ' : 'ปกติ'}</span>`;
            }

            return `
                <tr>
                    <td data-label="วันที่ทำรายการ">${r.createdAt ? new Date(r.createdAt).toLocaleString('th-TH') : '-'}</td>
                    <td data-label="เลขกรมธรรม์" style="font-weight: 600; color: var(--primary);">${r.policyNumber || '-'}</td>
                    <td data-label="รหัสสมาชิก">${r.memberId || '-'}</td>
                    <td data-label="ชื่อลูกค้า">${r.customer.firstName} ${r.customer.lastName}</td>
                    <td data-label="เบอร์โทรศัพท์">${r.customer.phone}</td>
                    <td data-label="รุ่นอุปกรณ์">${r.device.model}</td>
                    <td data-label="แพ็กเกจ"><span style="color: var(--primary); font-weight: 500;">${r.package.plan}</span></td>
                    <td data-label="ประกันคงเหลือ">${timeRemainingText}</td>
                    <td data-label="วงเงินคงเหลือ">
                        ${(() => {
                    const deviceValue = r.device.deviceValue || 0;
                    const limit = Math.floor(deviceValue * 0.7); // 70% of device value
                    const used = r.totalClaimAmount || 0;
                    const balance = limit - used;
                    const color = balance < 0 ? '#ef4444' : '#10b981';
                    return `
                            <div style="display: flex; flex-direction: column; align-items: flex-start;">
                                <span style="color: ${color}; font-weight: 600;">${balance.toLocaleString()} บาท</span>
                                <span style="font-size: 0.75rem; color: #64748b;">(วงเงิน ${limit.toLocaleString()})</span>
                            </div>
                        `;
                })()}
                    </td>
                    <td data-label="ร้านค้า">${r.shopName || '-'}</td>
                    <td data-label="ผู้บันทึก">${r.staffName}</td>
                    <td data-label="สถานะ">${statusBadge}</td>
                    <td data-label="การชำระเงิน">${paymentStatus}</td>
                    <td data-label="จัดการ">
                    <div style="display: flex; gap: 0.5rem; justify-content: center;">
                        <button class="edit-btn" data-id="${r._id}" title="แก้ไขข้อมูล">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path></svg>
                        </button>
                        </button>
                        ${(() => {
                    if (r.approvalStatus === 'pending') {
                        return `
                                <button class="print-btn" style="cursor: not-allowed; opacity: 0.8; pointer-events: none;" title="รออนุมัติ...">
                                    <div style="width: 18px; height: 18px; border: 2px solid #e2e8f0; border-top-color: #f59e0b; border-radius: 50%; animation: spinnerRotate 1.5s linear infinite;"></div>
                                </button>`;
                    } else if (r.approvalStatus === 'rejected') {
                        return `
                                <button class="print-btn" style="cursor: not-allowed; opacity: 0.5; color: #ef4444; pointer-events: none;" title="ไม่อนุมัติ">
                                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="15" y1="9" x2="9" y2="15"></line><line x1="9" y1="9" x2="15" y2="15"></line></svg>
                                </button>`;
                    } else {
                        return `
                                <button class="print-btn" data-id="${r._id}" title="พิมพ์เอกสาร">
                                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 6 2 18 2 18 9"></polyline><path d="M6 18H4a2 2 0 0 1-2-2v-5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-2"></path><rect x="6" y="14" width="12" height="8"></rect></svg>
                                </button>`;
                    }
                })()}
                        <button class="delete-btn" data-id="${r._id}" title="ลบข้อมูล" style="color: #ef4444;">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path><line x1="10" y1="11" x2="10" y2="17"></line><line x1="14" y1="11" x2="14" y2="17"></line></svg>
                        </button>
                    </div>
                </td>
            </tr>
            `;
        }).join('');

        // Add listeners to edit buttons
        document.querySelectorAll('.edit-btn').forEach(btn => {
            btn.addEventListener('click', () => editWarranty(btn.dataset.id));
        });

        // Add listeners to print buttons (Contract Document)
        document.querySelectorAll('.print-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                window.open(`document.html?id=${btn.dataset.id}`, '_blank');
            });
        });

        // Add listeners to delete buttons
        document.querySelectorAll('.delete-btn').forEach(btn => {
            btn.addEventListener('click', () => deleteWarranty(btn.dataset.id));
        });
    }

    async function deleteWarranty(id) {
        const confirmed = await showDeleteConfirm('คุณต้องการลบข้อมูลประกันนี้ใช่หรือไม่? การกระทำนี้ไม่สามารถย้อนกลับได้');
        if (!confirmed) return;

        showLoader('กำลังลบข้อมูล...');
        try {
            const res = await fetch(`/api/warranties/${id}`, { method: 'DELETE' });
            if (res.ok) {
                showAlert('success', 'ลบข้อมูลสำเร็จ');
                fetchWarranties();
            } else {
                const data = await res.json();
                showAlert('error', data.message || 'เกิดข้อผิดพลาดในการลบข้อมูล');
            }
        } catch (err) {
            console.error('Delete error:', err);
        } finally {
            hideLoader();
        }
    }

    function openReceipt(data) {
        localStorage.setItem('smilecare_receipt_data', JSON.stringify(data));
        window.open('vat.html', '_blank');
    }

    document.getElementById('addNewBtn').addEventListener('click', () => {
        isEditMode = false;
        showView('registration');
    });

    async function editWarranty(id) {
        try {
            const res = await fetch(`/api/warranties/${id}`);
            const data = await res.json();
            if (!res.ok) throw new Error(data.message);

            isEditMode = true;
            currentEditData = data;
            document.getElementById('editRecordId').value = id;

            // Update UI
            document.getElementById('regFormTitle').textContent = 'แก้ไขข้อมูลประกันภัย';
            document.getElementById('regFormSubtitle').textContent = `กำลังแก้ไขรหัสสมาชิก: ${data.memberId}`;
            document.getElementById('memberId').readOnly = true;

            // Show view first to ensure elements are available
            showView('registration');

            // Populate Form
            document.getElementById('policyNumber').value = data.policyNumber || '';
            document.getElementById('memberId').value = data.memberId;
            document.getElementById('firstName').value = data.customer.firstName || '';
            document.getElementById('lastName').value = data.customer.lastName || '';
            document.getElementById('phone').value = data.customer.phone || '';
            if (data.customer.dob) {
                const dob = new Date(data.customer.dob);
                document.getElementById('dobDay').value = dob.getDate();
                document.getElementById('dobMonth').value = dob.getMonth() + 1;
                document.getElementById('dobYear').value = dob.getFullYear() + 543; // To B.E.
                updateAge();
            }
            document.getElementById('address').value = data.customer.address;
            document.getElementById('productType').value = data.device.type;
            toggleIMEIField();
            document.getElementById('model').value = data.device.model;
            document.getElementById('color').value = data.device.color;
            document.getElementById('capacity').value = data.device.capacity;
            document.getElementById('serialNumber').value = data.device.serial;
            document.getElementById('imei').value = data.device.imei || '';
            document.getElementById('deviceValue').value = data.device.deviceValue || '';
            document.getElementById('officialWarrantyEnd').value = formatDate(data.device.officialWarrantyEnd);

            // Set Protection Type based on Plan name
            const planName = data.package.plan;
            if (['Plan A', 'Plan B', 'Plan C', 'Plan D'].includes(planName)) {
                document.getElementById('protectionType').value = 'Full';
            } else if (['Plan a', 'Plan b', 'Plan c'].includes(planName)) {
                document.getElementById('protectionType').value = 'ScreenApple';
            } else if (['Plan 1', 'Plan 2', 'Plan 3'].includes(planName)) {
                document.getElementById('protectionType').value = 'ScreenCompany';
            }

            updatePackageOptions();
            document.getElementById('package').value = planName;

            document.getElementById('startDate').value = data.warrantyDates.start;
            document.getElementById('endDate').value = data.warrantyDates.end;
            updateDateDisplay();

            // Set payment method
            const paymentRadio = document.querySelector(`input[name="paymentMethod"][value="${data.payment.method}"]`);
            if (paymentRadio) paymentRadio.checked = true;

            updateRemainingDays();
            updatePaymentUI();
            populateShopsDropdown(data.shopName);

            // Show Payment Management Section
            const pmSection = document.getElementById('paymentManagementSection');
            if (pmSection) pmSection.style.display = 'block';

            const ipc = document.getElementById('initialPaymentCheck');
            if (ipc) ipc.style.display = 'none';

            renderPaymentManagement(data);

        } catch (err) {
            console.error('Edit error:', err);
            showAlert('error', 'ไม่สามารถดึงข้อมูลเพื่อแก้ไขได้');
        }
    }

    function updatePackageOptions() {
        const type = document.getElementById('protectionType').value;
        const packageSelect = document.getElementById('package');
        const currentVal = packageSelect.value;

        let options = '';
        if (type === 'Full') {
            options = `
                <option value="" disabled>เลือกแผนประกัน</option>
                <option value="Plan A">Plan A</option>
                <option value="Plan B">Plan B</option>
                <option value="Plan C">Plan C</option>
                <option value="Plan D">Plan D</option>
            `;
        } else if (type === 'ScreenApple') {
            options = `
                <option value="" disabled>เลือกแผนประกัน</option>
                <option value="Plan a">Plan a</option>
                <option value="Plan b">Plan b</option>
                <option value="Plan c">Plan c</option>
            `;
        } else if (type === 'ScreenCompany') {
            options = `
                <option value="" disabled>เลือกแผนประกัน</option>
                <option value="Plan 1">Plan 1</option>
                <option value="Plan 2">Plan 2</option>
                <option value="Plan 3">Plan 3</option>
            `;
        }

        packageSelect.innerHTML = options;

        // Try to restore value if it still exists in the new list
        if (Array.from(packageSelect.options).some(opt => opt.value === currentVal)) {
            packageSelect.value = currentVal;
        } else {
            packageSelect.selectedIndex = 0;
        }
        updatePaymentUI();
    }

    // --- REGISTRATION LOGIC ---
    function initRegistrationForm() {
        const protectionSelect = document.getElementById('protectionType');
        if (protectionSelect) {
            protectionSelect.addEventListener('change', updatePackageOptions);
        }
        updatePackageOptions(); // Initial call
        if (isEditMode) return; // Don't reset if editing

        const form = document.getElementById('warrantyForm');
        form.reset();

        document.getElementById('regFormTitle').textContent = 'ลงทะเบียนประกันภัย';
        document.getElementById('regFormSubtitle').textContent = 'ประกันคุ้มครองมือถือ iPhone & iPad';
        document.getElementById('policyNumber').value = '';
        document.getElementById('memberId').readOnly = false;
        document.getElementById('memberId').value = ''; // Will be populated by lookup or manual entry
        document.getElementById('paymentManagementSection').style.display = 'none';
        document.getElementById('initialPaymentCheck').style.display = 'block';
        const initialPaidCheck = document.getElementById('initialPaidCheck');
        if (initialPaidCheck) initialPaidCheck.checked = false;

        // Auto-set dates
        const now = new Date();
        const nextYear = new Date(now);
        nextYear.setFullYear(now.getFullYear() + 1);

        document.getElementById('startDate').value = now.toISOString();
        document.getElementById('endDate').value = nextYear.toISOString();
        updateDateDisplay();

        toggleIMEIField();
        updateRemainingDays();
        updatePaymentUI();
        populateShopsDropdown();
    }

    async function populateShopsDropdown(selectedShop = '') {
        const shopSelect = document.getElementById('shopName');
        if (!shopSelect) return;

        try {
            const res = await fetch('/api/shops');
            const shops = await res.json();

            shopSelect.innerHTML = '<option value="" disabled selected>เลือกร้านค้า</option>';
            shops.forEach(shop => {
                const option = document.createElement('option');
                option.value = shop.shopName;
                option.textContent = shop.shopName;
                if (selectedShop && shop.shopName === selectedShop) {
                    option.selected = true;
                }
                shopSelect.appendChild(option);
            });
        } catch (err) {
            console.error('Populate shops error:', err);
            shopSelect.innerHTML = '<option value="" disabled selected>ไม่สามารถโหลดข้อมูลร้านค้าได้</option>';
        }
    }

    function toggleIMEIField() {
        const productType = document.getElementById('productType').value;
        const imeiGroup = document.getElementById('imeiGroup');
        const imeiInput = document.getElementById('imei');
        if (productType === 'iPad') {
            imeiGroup.style.display = 'none';
            imeiInput.required = false;
            imeiInput.value = '';
        } else {
            imeiGroup.style.display = 'block';
            imeiInput.required = true;
        }
    }

    function updateModelOptions() {
        const productTypeEl = document.getElementById('productType');
        const modelEl = document.getElementById('model');
        if (!productTypeEl || !modelEl) return;

        const type = productTypeEl.value;
        const currentVal = modelEl.value;

        const MODELS = {
            iPhone: [
                'iPhone 13',
                'iPhone 14',
                'iPhone 15',
                'iPhone 16',
                'iPhone 16 Plus',
                'iPhone 16 Pro',
                'iPhone 16 Pro Max',
                'iPhone 17 256',
                'iPhone 17 Pro',
                'iPhone 17 Pro Max'
            ],
            iPad: [
                'iPad Gen11',
                'iPad Air7'
            ]
        };

        const list = MODELS[type] || [];
        modelEl.innerHTML = '<option value="" disabled selected>เลือก</option>';
        list.forEach(m => {
            const opt = document.createElement('option');
            opt.value = m;
            opt.textContent = m;
            modelEl.appendChild(opt);
        });

        if (list.includes(currentVal)) {
            modelEl.value = currentVal;
        }
    }

    function updateAge() {
        const day = parseInt(document.getElementById('dobDay').value);
        const month = parseInt(document.getElementById('dobMonth').value);
        const yearBE = parseInt(document.getElementById('dobYear').value);

        if (day && month && yearBE && yearBE > 2400) {
            const now = new Date();
            const currentDay = now.getDate();
            const currentMonth = now.getMonth() + 1;
            const currentYearBE = now.getFullYear() + 543;

            let age = currentYearBE - yearBE;

            // If birthday hasn't occurred yet this year, subtract 1
            if (currentMonth < month || (currentMonth === month && currentDay < day)) {
                age--;
            }

            document.getElementById('age').value = age >= 0 ? age : 0;
        } else {
            document.getElementById('age').value = '';
        }
    }

    function updateDateDisplay() {
        const start = document.getElementById('startDate').value;
        const end = document.getElementById('endDate').value;
        if (start && end) {
            document.getElementById('startDateDisplay').value = new Date(start).toLocaleString('th-TH');
            document.getElementById('endDateDisplay').value = new Date(end).toLocaleString('th-TH');
        }
    }

    function updateRemainingDays() {
        const start = new Date(document.getElementById('startDate').value);
        const end = new Date(document.getElementById('endDate').value);
        const diff = Math.ceil((end - start) / (1000 * 60 * 60 * 24));
        document.getElementById('remainingDays').value = diff > 0 ? diff : 0;
    }

    function updatePaymentUI() {
        const plan = document.getElementById('package').value;
        const method = document.querySelector('input[name="paymentMethod"]:checked').value;
        const price = PACKAGE_PRICES[plan] || 0;

        document.getElementById('packagePriceText').textContent = `${price.toLocaleString()} บาท`;
        document.getElementById('totalPriceText').textContent = `${price.toLocaleString()} บาท`;

        const instContainer = document.getElementById('installmentContainer');
        const instBody = document.getElementById('installmentBody');

        if (method === 'Installment' && price > 0) {
            instContainer.style.display = 'block';
            const perMonth = Math.floor(price / 3);
            const remainder = price % 3;
            const start = new Date(document.getElementById('startDate').value);

            let html = '';
            for (let i = 1; i <= 3; i++) {
                const amt = i === 1 ? perMonth + remainder : perMonth;
                const dueDate = new Date(start); dueDate.setMonth(start.getMonth() + (i - 1));
                const graceDate = new Date(dueDate); graceDate.setDate(dueDate.getDate() + 5);
                html += `<tr><td>#${i}</td><td>${amt.toLocaleString()}</td><td>${dueDate.toLocaleDateString('th-TH')}</td><td><span class="late-deadline">⚠️ ${graceDate.toLocaleDateString('th-TH')}</span></td></tr>`;
            }
            instBody.innerHTML = html;
        } else {
            instContainer.style.display = 'none';
        }
    }

    function renderPaymentManagement(data) {
        const fullSec = document.getElementById('fullPaymentStatus');
        const instSec = document.getElementById('installmentStatus');

        if (data.payment.method === 'Full Payment') {
            fullSec.style.display = 'block';
            instSec.style.display = 'none';
            document.getElementById('fullPaymentAmount').textContent = `${data.package.price.toLocaleString()} บาท`;

            // Debt Summary for Full Payment
            document.getElementById('debtSummaryTotal').textContent = `${data.package.price.toLocaleString()} บาท`;
            document.getElementById('debtSummaryRemaining').textContent = data.payment.status === 'Paid' ? '0 บาท' : `${data.package.price.toLocaleString()} บาท`;

            const statusText = document.getElementById('fullPaymentStatusText');
            const confirmBtn = document.getElementById('confirmFullPaymentBtn');

            if (data.payment.status === 'Paid') {
                statusText.innerHTML = `<span class="status-paid">ชำระแล้วเมื่อ ${new Date(data.payment.paidDate).toLocaleDateString('th-TH')}</span>`;
                confirmBtn.style.display = 'none';

                // Add Print button for full payment
                const printBtn = document.createElement('button');
                printBtn.type = 'button';
                printBtn.className = 'receive-btn';
                printBtn.style.background = 'var(--secondary)';
                printBtn.style.marginTop = '0.5rem';
                printBtn.textContent = 'พิมพ์ใบเสร็จ';
                printBtn.onclick = () => openReceipt({
                    policyNumber: data.policyNumber,
                    receiptNo: 'RC-' + data.policyNumber + '-F',
                    paidDate: new Date(data.payment.paidDate).toLocaleString('th-TH'),
                    shopName: data.shopName,
                    staffName: currentUser ? currentUser.staffName : (data.staffName || '-'),
                    customerName: `${data.customer.firstName} ${data.customer.lastName}`,
                    customerPhone: data.customer.phone,
                    customerAddress: data.customer.address,
                    amount: data.package.price,
                    description: `ชำระเต็มจำนวน แพ็กเกจ ${data.package.plan}`
                });
                statusText.appendChild(document.createElement('br'));
                statusText.appendChild(printBtn);
            } else {
                statusText.innerHTML = `<span class="status-pending">ค้างชำระ</span>`;
                confirmBtn.style.display = 'block';
                confirmBtn.onclick = () => receivePayment(data._id, null, data.package.price);
            }
        } else {
            fullSec.style.display = 'none';
            instSec.style.display = 'block';
            const body = document.getElementById('paymentStatusBody');
            let remainingTotal = 0;
            let pendingCount = 0;

            body.innerHTML = data.payment.schedule.map(s => {
                const isPaid = s.status === 'Paid';
                if (!isPaid) {
                    remainingTotal += s.amount;
                    pendingCount++;
                }
                const isOverdue = !isPaid && new Date(s.dueDate) < new Date();
                const rowClass = isPaid ? 'paid-row' : (isOverdue ? 'overdue-row' : '');

                return `
                    <tr class="${rowClass}">
                        <td>#${s.installmentNo}</td>
                        <td>${s.amount.toLocaleString()}</td>
                        <td>${new Date(s.dueDate).toLocaleDateString('th-TH')}</td>
                        <td>
                            ${isPaid
                        ? `<span class="status-paid">ชำระแล้ว<br><small>${new Date(s.paidDate).toLocaleDateString('th-TH')}</small></span>`
                        : `<span class="status-pending">${isOverdue ? 'เกินกำหนด' : 'ค้างชำระ'}</span>`}
                        </td>
                        <td>
                            ${isPaid ? '-' : `<button type="button" class="receive-btn" onclick="receivePayment('${data._id}', ${s.installmentNo}, ${s.amount})">รับชำระเงิน</button>`}
                        </td>
                        <td>
                            ${isPaid ? `<button type="button" class="print-receipt-btn receive-btn" style="background: var(--secondary); padding: 4px 8px;" data-no="${s.installmentNo}" data-amt="${s.amount}" data-date="${s.paidDate}">พิมพ์</button>` : '-'}
                        </td>
                    </tr>
                `;
            }).join('');

            // Show/Hide Pay All Remaining button
            const payAllContainer = document.getElementById('payAllRemainingContainer');
            const payAllBtn = document.getElementById('payAllRemainingBtn');
            if (pendingCount > 1) {
                payAllContainer.style.display = 'block';
                payAllBtn.onclick = () => receiveAllRemainingPayments(data._id, remainingTotal);
                payAllBtn.textContent = `💰 ชำระเงินงวดที่เหลือทั้งหมด (${pendingCount} งวด: ${remainingTotal.toLocaleString()} บาท)`;
            } else {
                payAllContainer.style.display = 'none';
            }

            // Debt Summary for Installment
            document.getElementById('debtSummaryTotal').textContent = `${data.package.price.toLocaleString()} บาท`;
            document.getElementById('debtSummaryRemaining').textContent = `${remainingTotal.toLocaleString()} บาท`;

            // Individual print listeners
            body.querySelectorAll('.print-receipt-btn').forEach(btn => {
                btn.onclick = () => {
                    openReceipt({
                        policyNumber: data.policyNumber,
                        receiptNo: 'RC-' + data.policyNumber + '-' + btn.dataset.no,
                        paidDate: new Date(btn.dataset.date).toLocaleString('th-TH'),
                        shopName: data.shopName,
                        staffName: currentUser ? currentUser.staffName : (data.staffName || '-'),
                        customerName: `${data.customer.firstName} ${data.customer.lastName}`,
                        customerPhone: data.customer.phone,
                        customerAddress: data.customer.address,
                        amount: parseInt(btn.dataset.amt),
                        description: `ชำระค่าเบี้ยประกัน งวดที่ ${btn.dataset.no}`
                    });
                };
            });
        }
    }

    // --- APPROVAL GUARD FOR PAYMENTS ---
    async function waitForApprovalIfPending(warrantyId) {
        return new Promise((resolve) => {
            Swal.fire({
                title: 'กรุณารออนุมัติสัญญา...',
                html: '<p style="color: #64748b; font-size: 0.9rem;">ระบบกำลังรอการอนุมัติจากหัวหน้างาน<br>กรุณาอย่าปิดหน้านี้</p>',
                allowOutsideClick: false,
                allowEscapeKey: false,
                showConfirmButton: false,
                didOpen: () => {
                    Swal.showLoading();
                    const pollInterval = setInterval(async () => {
                        try {
                            const res = await fetch(`/api/warranties/${warrantyId}`);
                            const data = await res.json();
                            if (data.approvalStatus === 'approved') {
                                clearInterval(pollInterval);
                                Swal.close();
                                resolve('approved');
                            } else if (data.approvalStatus === 'rejected') {
                                clearInterval(pollInterval);
                                Swal.fire({
                                    icon: 'error',
                                    title: 'สัญญาไม่ได้รับการอนุมัติ',
                                    text: 'หัวหน้างานไม่อนุมัติสัญญานี้ กรุณาติดต่อหัวหน้างานเพื่อสอบถามรายละเอียด',
                                    confirmButtonText: 'ตกลง'
                                });
                                resolve('rejected');
                            }
                        } catch (err) {
                            console.error('Approval polling error:', err);
                        }
                    }, 3000);
                }
            });
        });
    }

    window.receivePayment = async function (id, installmentNo, amount) {
        if (currentEditData && currentEditData.approvalStatus === 'pending') {
            const result = await waitForApprovalIfPending(currentEditData._id);
            if (result !== 'approved') return;
            // Refresh data after approval
            const res = await fetch(`/api/warranties/${currentEditData._id}`);
            currentEditData = await res.json();
            renderPaymentManagement(currentEditData);
        }
        startCheckout(currentEditData, null, {
            amountDue: amount,
            installmentNo,
            description: `ชำระค่าเบี้ยประกัน งวดที่ ${installmentNo}`
        });
    };

    window.receiveAllRemainingPayments = async function (id, totalAmount) {
        if (currentEditData && currentEditData.approvalStatus === 'pending') {
            const result = await waitForApprovalIfPending(currentEditData._id);
            if (result !== 'approved') return;
            const res = await fetch(`/api/warranties/${currentEditData._id}`);
            currentEditData = await res.json();
            renderPaymentManagement(currentEditData);
        }
        startCheckout(currentEditData, null, {
            amountDue: totalAmount,
            payAllRemaining: true,
            description: 'ชำระเงินงวดที่เหลือทั้งหมด'
        });
    };

    // Listeners for form updates
    document.getElementById('productType').addEventListener('change', () => {
        toggleIMEIField();
        updateModelOptions();
    });
    document.getElementById('dobDay').addEventListener('input', updateAge);
    document.getElementById('dobMonth').addEventListener('input', updateAge);
    document.getElementById('dobYear').addEventListener('input', updateAge);

    updateModelOptions();
    document.getElementById('package').addEventListener('change', updatePaymentUI);
    document.getElementById('protectionType').addEventListener('change', updatePaymentUI);
    document.getElementsByName('paymentMethod').forEach(r => r.addEventListener('change', updatePaymentUI));

    async function searchMembers(query) {
        const resultsBox = document.getElementById('memberSearchResults');
        if (!query || query.length < 2) {
            resultsBox.style.display = 'none';
            return;
        }

        try {
            const res = await fetch(`/api/members/lookup?query=${encodeURIComponent(query)}`);
            const data = await res.json();

            if (data.success && data.members && data.members.length > 0) {
                resultsBox.innerHTML = data.members.map(m => `
                    <div class="search-result-item" onclick="selectMember(${JSON.stringify(m).replace(/"/g, '&quot;')})">
                        <div class="search-result-info">
                            <span class="search-result-name">${m.firstName} ${m.lastName}</span>
                            <span class="search-result-sub">${m.phone} ${m.citizenId ? `| CID: ${m.citizenId}` : ''}</span>
                        </div>
                        <span class="search-result-tag">${m.memberId}</span>
                    </div>
                `).join('');
                resultsBox.style.display = 'block';
            } else {
                resultsBox.innerHTML = '<div class="search-result-item" style="cursor: default; color: #94a3b8;">ไม่พบข้อมูลสมาชิก</div>';
                resultsBox.style.display = 'block';
            }
        } catch (err) {
            console.error('Member search error:', err);
        }
    }

    window.selectMember = function (member) {
        document.getElementById('memberId').value = member.memberId;
        document.getElementById('firstName').value = member.firstName;
        document.getElementById('lastName').value = member.lastName;
        document.getElementById('phone').value = member.phone;
        document.getElementById('address').value = member.shippingAddress || member.idCardAddress || '';

        if (member.birthdate) {
            const dob = new Date(member.birthdate);
            document.getElementById('dobDay').value = dob.getDate();
            document.getElementById('dobMonth').value = dob.getMonth() + 1;
            document.getElementById('dobYear').value = dob.getFullYear() + 543;
            updateAge();
        }

        document.getElementById('memberSearchResults').style.display = 'none';
        document.getElementById('memberSearchInput').value = `${member.firstName} ${member.lastName} (${member.memberId})`;
    };

    // Member Search Event Listener
    const searchInput = document.getElementById('memberSearchInput');
    if (searchInput) {
        let debounceTimer;
        searchInput.addEventListener('input', (e) => {
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => searchMembers(e.target.value.trim()), 300);
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!searchInput.contains(e.target) && !document.getElementById('memberSearchResults').contains(e.target)) {
                document.getElementById('memberSearchResults').style.display = 'none';
            }
        });
    }

    async function checkDuplicate(field, value) {
        const input = document.getElementById(field === 'serialNumber' ? 'serialNumber' : 'imei');
        const label = input.parentNode.querySelector('label');
        const baseLabel = field === 'serialNumber' ? 'เลขซีเรียล (Serial Number)' : 'เลข IMEI';

        if (!value) {
            input.classList.remove('input-error');
            label.innerHTML = baseLabel;
            label.style.color = '';
            updateSubmitButtonState();
            return false;
        }

        const excludeId = isEditMode ? document.getElementById('editRecordId').value : null;
        const type = field === 'serialNumber' ? 'serial' : 'imei';

        try {
            const url = `/api/warranties/check-duplicate?type=${type}&value=${encodeURIComponent(value)}${excludeId ? `&excludeId=${excludeId}` : ''}`;
            const res = await fetch(url);
            const data = await res.json();

            if (data.exists) {
                input.classList.add('input-error');
                label.innerHTML = `${baseLabel} <span style="color: #ef4444;">*เลขนี้เคยลงทะเบียนแล้ว</span>`;
                label.style.color = '#ef4444';
                updateSubmitButtonState();
                return true;
            } else {
                input.classList.remove('input-error');
                label.innerHTML = baseLabel;
                label.style.color = '';
                updateSubmitButtonState();
                return false;
            }
        } catch (err) {
            console.error('Duplicate check error:', err);
            return false;
        }
    }

    function updateSubmitButtonState() {
        const submitBtn = document.querySelector('#warrantyForm button[type="submit"]');
        const hasErrors = document.querySelectorAll('#warrantyForm .input-error').length > 0;

        if (hasErrors) {
            submitBtn.disabled = true;
            submitBtn.style.opacity = '0.5';
            submitBtn.style.cursor = 'not-allowed';
        } else {
            submitBtn.disabled = false;
            submitBtn.style.opacity = '1';
            submitBtn.style.cursor = 'pointer';
        }
    }

    // Duplicate Check Event Listeners
    const serialInput = document.getElementById('serialNumber');
    if (serialInput) {
        serialInput.addEventListener('blur', () => {
            checkDuplicate('serialNumber', serialInput.value.trim());
        });
    }

    const imeiInputReg = document.getElementById('imei');
    if (imeiInputReg) {
        imeiInputReg.addEventListener('blur', () => {
            checkDuplicate('imei', imeiInputReg.value.trim());
        });
    }

    document.getElementById('backToDashBtn').addEventListener('click', () => showView('dashboard'));

    document.getElementById('warrantyForm').addEventListener('submit', async (e) => {
        e.preventDefault();

        const plan = document.getElementById('package').value;
        const method = document.querySelector('input[name="paymentMethod"]:checked').value;
        const price = PACKAGE_PRICES[plan];

        // Auto-generate coverage dates strictly at submission
        const start = isEditMode ? new Date(document.getElementById('startDate').value) : new Date();
        const end = isEditMode ? new Date(document.getElementById('endDate').value) : new Date(start);
        if (!isEditMode) end.setFullYear(start.getFullYear() + 1);

        const schedule = [];
        if (method === 'Installment') {
            const perMonth = Math.floor(price / 3);
            const remainder = price % 3;
            for (let i = 1; i <= 3; i++) {
                const dueDate = new Date(start); dueDate.setMonth(start.getMonth() + (i - 1));
                const graceDate = new Date(dueDate); graceDate.setDate(dueDate.getDate() + 5);

                // Merge status and paidDate if editing and settings match
                let status = 'Pending';
                let paidDate = undefined;

                if (isEditMode && currentEditData && currentEditData.payment.method === 'Installment') {
                    const existing = currentEditData.payment.schedule.find(s => s.installmentNo === i);
                    // Only preserve if the installment number exists
                    // We can be more strict (checking amount/dueDate), but usually user just wants to keep "Paid"
                    if (existing && existing.status === 'Paid') {
                        status = 'Paid';
                        paidDate = existing.paidDate;
                    }
                }

                schedule.push({
                    installmentNo: i,
                    amount: i === 1 ? perMonth + remainder : perMonth,
                    dueDate: dueDate,
                    graceDate: graceDate,
                    status: status,
                    paidDate: paidDate
                });
            }
        }

        const payment = {
            method,
            schedule,
            status: 'Pending',
            paidDate: undefined
        };

        if (isEditMode && currentEditData) {
            if (method === 'Full Payment' && currentEditData.payment.method === 'Full Payment') {
                payment.status = currentEditData.payment.status;
                payment.paidDate = currentEditData.payment.paidDate;
            }
        } else {
            // New Registration - Check for "Mark as Paid"
            const markPaid = document.getElementById('initialPaidCheck')?.checked;
            if (markPaid) {
                if (method === 'Full Payment') {
                    payment.status = 'Paid';
                    payment.paidDate = new Date();
                } else if (method === 'Installment') {
                    payment.schedule[0].status = 'Paid';
                    payment.schedule[0].paidDate = new Date();
                }
            }
        }

        // Create DOB date object
        const dobDay = parseInt(document.getElementById('dobDay').value);
        const dobMonth = parseInt(document.getElementById('dobMonth').value);
        const dobYearBE = parseInt(document.getElementById('dobYear').value);
        const dob = new Date(dobYearBE - 543, dobMonth - 1, dobDay);

        const payload = {
            memberId: document.getElementById('memberId').value,
            shopName: document.getElementById('shopName').value,
            protectionType: document.getElementById('protectionType').value,
            staffName: currentUser.staffName,
            customer: {
                firstName: document.getElementById('firstName').value,
                lastName: document.getElementById('lastName').value,
                phone: document.getElementById('phone').value,
                dob: dob.toISOString(),
                age: parseInt(document.getElementById('age').value),
                address: document.getElementById('address').value
            },
            device: {
                type: document.getElementById('productType').value,
                model: document.getElementById('model').value,
                color: document.getElementById('color').value,
                capacity: document.getElementById('capacity').value,
                serial: document.getElementById('serialNumber').value,
                imei: document.getElementById('imei').value || undefined,
                deviceValue: parseFloat(document.getElementById('deviceValue').value) || 0,
                officialWarrantyEnd: document.getElementById('officialWarrantyEnd').value
            },
            package: { plan, price },
            warrantyDates: {
                start: start.toISOString(),
                end: end.toISOString()
            },
            payment: payment
        };

        try {
            const url = isEditMode ? `/api/warranties/${document.getElementById('editRecordId').value}` : '/api/warranties';
            const reqMethod = isEditMode ? 'PUT' : 'POST';

            const res = await fetch(url, {
                method: reqMethod,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await res.json();
            if (res.ok) {
                const modalTitle = document.getElementById('successModalTitle');
                const modalText = document.getElementById('successModalText');
                const printBtn = document.getElementById('printReceiptBtn');
                const finishBtn = document.getElementById('finishProcessBtn');
                const closeBtn = document.getElementById('closeModal');
                const immediatePayment = document.getElementById('immediatePaymentSection');

                if (isEditMode) {
                    modalTitle.textContent = 'อัปเดตข้อมูลสำเร็จ!';
                    modalText.textContent = 'ข้อมูลประกันภัยได้ถูกอัปเดตลงในระบบเรียบร้อยแล้ว';
                    hideCheckout();
                    closeBtn.style.display = 'block';
                    showView('dashboard');
                    fetchWarranties();
                } else {
                    // --- NEW: Approval Polling Flow ---
                    // Show SweetAlert2 loading spinner and poll for approval
                    const savedWarrantyId = data._id;
                    const savedData = data;
                    const savedPayload = payload;
                    const markPaid = document.getElementById('initialPaidCheck')?.checked;

                    Swal.fire({
                        title: 'กรุณารออนุมัติสัญญา...',
                        html: '<p style="color: #64748b; font-size: 0.9rem;">ระบบกำลังรอการอนุมัติจากหัวหน้างาน<br>กรุณาอย่าปิดหน้านี้</p>',
                        allowOutsideClick: false,
                        allowEscapeKey: false,
                        showConfirmButton: false,
                        didOpen: () => {
                            Swal.showLoading();

                            // Poll every 3 seconds
                            const pollInterval = setInterval(async () => {
                                try {
                                    const pollRes = await fetch(`/api/warranties/${savedWarrantyId}`);
                                    const pollData = await pollRes.json();

                                    if (pollData.approvalStatus === 'approved') {
                                        clearInterval(pollInterval);
                                        Swal.close();

                                        showView('dashboard');
                                        fetchWarranties();

                                        // Resume original flow
                                        const modalTitle = document.getElementById('successModalTitle');
                                        const modalText = document.getElementById('successModalText');
                                        const closeBtn = document.getElementById('closeModal');

                                        modalTitle.textContent = 'ลงทะเบียนสำเร็จ!';
                                        modalText.textContent = 'สัญญาได้รับการอนุมัติแล้ว';

                                        if (markPaid) {
                                            modalText.textContent = 'สัญญาอนุมัติแล้ว - บันทึกสถานะการชำระเงินเรียบร้อย';
                                            hideCheckout();
                                            document.getElementById('checkoutStep3').style.display = 'block';
                                            setupPrintButton(savedData, savedPayload);
                                        } else {
                                            startCheckout(savedData, savedPayload);
                                        }
                                        document.getElementById('successModal').style.display = 'flex';

                                    } else if (pollData.approvalStatus === 'rejected') {
                                        clearInterval(pollInterval);
                                        Swal.fire({
                                            icon: 'error',
                                            title: 'สัญญาไม่ได้รับการอนุมัติ',
                                            text: 'หัวหน้างานไม่อนุมัติสัญญานี้ กรุณาติดต่อหัวหน้างานเพื่อสอบถามรายละเอียด',
                                            confirmButtonText: 'ตกลง'
                                        });
                                    }
                                    // If still 'pending', do nothing and wait for next poll
                                } catch (pollErr) {
                                    console.error('Polling error:', pollErr);
                                }
                            }, 3000);
                        }
                    });
                }
            } else {
                showAlert('error', data.message || 'เกิดข้อผิดพลาดในการบันทึกข้อมูล');
            }
        } catch (err) {
            console.error('Submit error:', err);
        }
    });

    function setupPrintButton(recordData, payload, extraData = {}) {
        const printBtn = document.getElementById('printReceiptBtn');
        const reprintBtn = document.getElementById('reprintReceiptBtn');

        const onPrint = () => {
            const isInstallment = (payload && payload.payment.method === 'Installment') ||
                (recordData && recordData.payment.method === 'Installment');
            const amount = extraData.amount || (payload ? (isInstallment ? payload.payment.schedule[0].amount : payload.package.price) : recordData.package.price);

            const receiptData = {
                policyNumber: recordData.policyNumber,
                receiptNo: 'RC-' + recordData.policyNumber + '-' + (checkoutData.payAllRemaining ? 'ALL' : (checkoutData.installmentNo || (isInstallment ? '1' : 'F'))),
                paidDate: new Date().toLocaleString('th-TH'),
                shopName: recordData.shopName,
                staffName: currentUser ? currentUser.staffName : (payload ? payload.staffName : (recordData.staffName || '-')),
                customerName: `${recordData.customer.firstName} ${recordData.customer.lastName}`,
                customerPhone: recordData.customer.phone,
                customerAddress: recordData.customer.address,
                amount: checkoutData.amountDue || amount,
                cashReceived: extraData.paidCash || 0,
                transferAmount: extraData.paidTransfer || 0,
                change: (extraData.paidCash || 0) + (extraData.paidTransfer || 0) - (checkoutData.amountDue || amount),
                refId: extraData.refId || '',
                description: checkoutData.description || (isInstallment ? `ชำระค่าเบี้ยประกัน งวดที่ 1` : `ชำระเต็มจำนวน แพ็กเกจ ${payload.package.plan}`)
            };
            openReceipt(receiptData);
        };

        if (printBtn) printBtn.onclick = onPrint;
        if (reprintBtn) reprintBtn.onclick = onPrint;
    }


    function hideCheckout() {
        document.querySelectorAll('.checkout-step').forEach(s => s.style.display = 'none');
    }

    let checkoutData = { record: null, payload: null, amountDue: 0, method: 'Cash', installmentNo: null, payAllRemaining: false, description: '' };

    function startCheckout(record, payload, extra = null) {
        checkoutData.record = record;
        checkoutData.payload = payload;

        if (extra) {
            checkoutData.amountDue = extra.amountDue;
            checkoutData.installmentNo = extra.installmentNo || null;
            checkoutData.payAllRemaining = extra.payAllRemaining || false;
            checkoutData.description = extra.description || '';
        } else {
            checkoutData.amountDue = payload.payment.method === 'Full Payment' ? payload.package.price : payload.payment.schedule[0].amount;
            checkoutData.installmentNo = payload.payment.method === 'Installment' ? 1 : null;
            checkoutData.payAllRemaining = false;
            checkoutData.description = checkoutData.installmentNo ? `ชำระค่าเบี้ยประกัน งวดที่ 1` : `ชำระเต็มจำนวน แพ็กเกจ ${payload.package.plan}`;
        }

        hideCheckout();
        document.getElementById('checkoutStep1').style.display = 'block';
        document.getElementById('successModal').style.display = 'flex';
        document.getElementById('closeModal').style.display = 'block';

        const modalTitle = document.getElementById('successModalTitle');
        const modalText = document.getElementById('successModalText');

        if (extra) {
            modalTitle.textContent = 'ชำระเงินงวดประกัน';
            modalText.textContent = extra.description;
        }
        document.getElementById('checkoutTotalDue').textContent = `${checkoutData.amountDue.toLocaleString('th-TH', { minimumFractionDigits: 2 })} บาท`;

        // Reset Inputs
        document.getElementById('cashReceivedInput').value = '';
        document.getElementById('transferAmountInput').value = checkoutData.amountDue;
        document.getElementById('transferRefInput').value = '';
        document.getElementById('splitCashInput').value = '';
        document.getElementById('splitTransferInput').value = '';

        updateCheckoutCalculations();
    }

    // Checkout Tab Switching
    document.querySelectorAll('.pay-tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.pay-tab-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            checkoutData.method = btn.dataset.method;

            document.getElementById('cashInputGroup').style.display = checkoutData.method === 'Cash' ? 'block' : 'none';
            document.getElementById('transferInputGroup').style.display = checkoutData.method === 'Transfer' ? 'block' : 'none';
            document.getElementById('splitInputGroup').style.display = checkoutData.method === 'Split' ? 'block' : 'none';

            updateCheckoutCalculations();
        });
    });

    function updateCheckoutCalculations() {
        const totalDue = checkoutData.amountDue;
        let totalPaid = 0;
        let change = 0;
        let isValid = false;

        if (checkoutData.method === 'Cash') {
            const received = parseFloat(document.getElementById('cashReceivedInput').value) || 0;
            change = received - totalDue;
            totalPaid = received;
            document.getElementById('cashChangeAmount').textContent = `${(change > 0 ? change : 0).toLocaleString('th-TH', { minimumFractionDigits: 2 })} บาท`;
            isValid = received >= totalDue;
        } else if (checkoutData.method === 'Transfer') {
            const amount = parseFloat(document.getElementById('transferAmountInput').value) || 0;
            totalPaid = amount;
            isValid = amount >= totalDue;
        } else if (checkoutData.method === 'Split') {
            const cash = parseFloat(document.getElementById('splitCashInput').value) || 0;
            const transfer = parseFloat(document.getElementById('splitTransferInput').value) || 0;
            totalPaid = cash + transfer;
            const balance = totalPaid - totalDue;

            document.getElementById('splitTotalPaid').textContent = `${totalPaid.toLocaleString('th-TH', { minimumFractionDigits: 2 })} บาท`;
            document.getElementById('splitBalance').textContent = `${(balance < 0 ? balance : 0).toLocaleString('th-TH', { minimumFractionDigits: 2 })} บาท`;
            document.getElementById('splitBalance').style.color = balance < 0 ? '#ef4444' : '#22c55e';

            const changeElem = document.getElementById('splitChangeAmount');
            const changeRow = document.getElementById('splitChangeRow');
            if (balance > 0) {
                changeRow.style.display = 'flex';
                changeElem.textContent = `${balance.toLocaleString('th-TH', { minimumFractionDigits: 2 })} บาท`;
            } else {
                changeRow.style.display = 'none';
            }

            isValid = totalPaid >= totalDue;
        }

        document.getElementById('goToSummaryBtn').disabled = !isValid;
    }

    // Input Listeners
    ['cashReceivedInput', 'transferAmountInput', 'splitCashInput', 'splitTransferInput'].forEach(id => {
        document.getElementById(id).addEventListener('input', updateCheckoutCalculations);
    });

    // Step Navigation
    document.getElementById('goToSummaryBtn').addEventListener('click', () => {
        const totalDue = checkoutData.amountDue;
        let cashPaid = 0;
        let transferPaid = 0;
        let change = 0;

        if (checkoutData.method === 'Cash') {
            cashPaid = parseFloat(document.getElementById('cashReceivedInput').value) || 0;
            change = cashPaid - totalDue;
        } else if (checkoutData.method === 'Transfer') {
            transferPaid = parseFloat(document.getElementById('transferAmountInput').value) || 0;
        } else if (checkoutData.method === 'Split') {
            cashPaid = parseFloat(document.getElementById('splitCashInput').value) || 0;
            transferPaid = parseFloat(document.getElementById('splitTransferInput').value) || 0;
            change = (cashPaid + transferPaid) - totalDue;
        }

        document.getElementById('summaryTotalDue').textContent = `${totalDue.toLocaleString('th-TH', { minimumFractionDigits: 2 })} บาท`;
        document.getElementById('summaryCashPaid').textContent = `${cashPaid.toLocaleString('th-TH', { minimumFractionDigits: 2 })} บาท`;
        document.getElementById('summaryTransferPaid').textContent = `${transferPaid.toLocaleString('th-TH', { minimumFractionDigits: 2 })} บาท`;
        document.getElementById('summaryChangeAmount').textContent = `${(change > 0 ? change : 0).toLocaleString('th-TH', { minimumFractionDigits: 2 })} บาท`;
        document.getElementById('summaryChangeRow').style.display = change > 0 ? 'flex' : 'none';

        hideCheckout();
        document.getElementById('checkoutStep2').style.display = 'block';
    });

    document.getElementById('backToInputBtn').addEventListener('click', () => {
        hideCheckout();
        document.getElementById('checkoutStep1').style.display = 'block';
    });

    document.getElementById('finalConfirmBtn').addEventListener('click', async () => {
        const btn = document.getElementById('finalConfirmBtn');
        const originalText = btn.innerHTML;

        try {
            btn.disabled = true;
            btn.innerHTML = '⌛ กำลังยืนยัน...';

            const totalDue = checkoutData.amountDue;
            let cashPaid = 0;
            let transferPaid = 0;
            const refId = document.getElementById('transferRefInput').value;

            if (checkoutData.method === 'Cash') {
                cashPaid = parseFloat(document.getElementById('cashReceivedInput').value) || 0;
            } else if (checkoutData.method === 'Transfer') {
                transferPaid = parseFloat(document.getElementById('transferAmountInput').value) || 0;
            } else if (checkoutData.method === 'Split') {
                cashPaid = parseFloat(document.getElementById('splitCashInput').value) || 0;
                transferPaid = parseFloat(document.getElementById('splitTransferInput').value) || 0;
            }

            const res = await fetch(`/api/warranties/${checkoutData.record._id}/payment`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    installmentNo: checkoutData.installmentNo,
                    payAllRemaining: checkoutData.payAllRemaining,
                    paidCash: cashPaid,
                    paidTransfer: transferPaid,
                    refId: refId
                })
            });

            if (res.ok) {
                const totalDue = checkoutData.amountDue;
                const isInstallment = (checkoutData.payload && checkoutData.payload.payment.method === 'Installment') ||
                    (checkoutData.record && checkoutData.record.payment.method === 'Installment');

                const receiptData = {
                    policyNumber: checkoutData.record.policyNumber,
                    receiptNo: 'RC-' + checkoutData.record.policyNumber + '-' + (checkoutData.payAllRemaining ? 'ALL' : (checkoutData.installmentNo || (isInstallment ? '1' : 'F'))),
                    paidDate: new Date().toLocaleString('th-TH'),
                    shopName: checkoutData.record.shopName,
                    staffName: currentUser ? currentUser.staffName : (checkoutData.payload ? checkoutData.payload.staffName : (checkoutData.record.staffName || '-')),
                    customerName: `${checkoutData.record.customer.firstName} ${checkoutData.record.customer.lastName}`,
                    customerPhone: checkoutData.record.customer.phone,
                    customerAddress: checkoutData.record.customer.address,
                    amount: totalDue,
                    cashReceived: cashPaid,
                    transferAmount: transferPaid,
                    change: (cashPaid + transferPaid) - totalDue,
                    refId: refId,
                    description: checkoutData.description
                };

                // Refresh UI if in edit mode
                if (isEditMode) {
                    const updatedRes = await fetch(`/api/warranties/${checkoutData.record._id}`);
                    const updatedData = await updatedRes.json();
                    currentEditData = updatedData;
                    renderPaymentManagement(updatedData);
                    fetchWarranties();
                }

                openReceipt(receiptData);
                setupPrintButton(checkoutData.record, checkoutData.payload, {
                    paidCash: cashPaid,
                    paidTransfer: transferPaid,
                    refId: refId
                });
                hideCheckout();
                document.getElementById('checkoutStep3').style.display = 'block';
            } else {
                const data = await res.json();
                showAlert('error', data.message || 'เกิดข้อผิดพลาดในการบันทึกการชำระเงิน');
            }
        } catch (err) {
            console.error('Final confirm error:', err);
            showAlert('error', 'เกิดข้อผิดพลาดในการเชื่อมต่อ');
        } finally {
            btn.disabled = false;
            btn.innerHTML = originalText;
        }
    });

    document.getElementById('finishProcessBtn').addEventListener('click', () => {
        document.getElementById('successModal').style.display = 'none';
        resetRegistrationModal();
        showView('dashboard');
        fetchWarranties();
    });

    function resetRegistrationModal() {
        hideCheckout();
        document.getElementById('closeModal').style.display = 'block';
        // Reset tabs
        document.querySelectorAll('.pay-tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelector('.pay-tab-btn[data-method="Cash"]').classList.add('active');
        checkoutData.method = 'Cash';
        document.getElementById('cashInputGroup').style.display = 'block';
        document.getElementById('transferInputGroup').style.display = 'none';
        document.getElementById('splitInputGroup').style.display = 'none';
    }

    document.getElementById('closeModal').addEventListener('click', () => {
        document.getElementById('successModal').style.display = 'none';
        resetRegistrationModal();
        showView('dashboard');
        fetchWarranties();
    });

    // Dashboard Filters listeners
    document.getElementById('searchInput').addEventListener('input', applyFilters);
    document.getElementById('statusFilter').addEventListener('change', applyFilters);
    document.getElementById('paymentFilter').addEventListener('change', applyFilters);

    // --- MEMBERS LOGIC ---
    async function fetchMembers() {
        try {
            const res = await fetch('/api/members');
            const data = await res.json();
            renderMembers(data);
        } catch (err) {
            console.error('Fetch members error:', err);
        }
    }

    function renderMembers(members) {
        const body = document.getElementById('membersBody');
        const empty = document.getElementById('membersEmptyState');

        if (members.length === 0) {
            empty.style.display = 'block';
            body.innerHTML = '';
            return;
        }

        empty.style.display = 'none';
        body.innerHTML = members.map(m => `
            <tr>
                <td data-label="รหัสสมาชิก" style="font-weight: 600;">${m.memberId || '-'}</td>
                <td data-label="ชื่อ-นามสกุล">${m.firstName} ${m.lastName}</td>
                <td data-label="เบอร์โทรศัพท์">${m.phone}</td>
                <td data-label="ที่อยู่ตามบัตร">${m.idCardAddress || '-'}${m.postalCode ? ` (${m.postalCode})` : ''}</td>
                <td data-label="จัดการ">
                    <div style="display: flex; gap: 0.5rem; justify-content: center;">
                        <button class="edit-member-btn edit-btn" data-id="${m._id}" title="แก้ไข">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path></svg>
                        </button>
                        <button class="delete-member-btn edit-btn" data-id="${m._id}" title="ลบ" style="color: #ef4444;">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path><line x1="10" y1="11" x2="10" y2="17"></line><line x1="14" y1="11" x2="14" y2="17"></line></svg>
                        </button>
                    </div>
                </td>
            </tr>
        `).join('');

        // Add event listeners for edit buttons
        body.querySelectorAll('.edit-member-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const id = btn.getAttribute('data-id');
                editMember(id);
            });
        });

        // Add event listeners for delete buttons
        body.querySelectorAll('.delete-member-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const id = btn.getAttribute('data-id');
                deleteMember(id);
            });
        });
    }

    async function deleteMember(id) {
        const confirmed = await showDeleteConfirm('คุณต้องการลบสมาชิกนี้ใช่หรือไม่? บัญชีและประวัติจะถูกลบออกถาวร');
        if (!confirmed) return;

        try {
            const res = await fetch(`/api/members/${id}`, { method: 'DELETE' });
            if (res.ok) {
                showAlert('success', 'ลบสมาชิกสำเร็จ');
                fetchMembers();
            } else {
                const data = await res.json();
                showAlert('error', data.message || 'เกิดข้อผิดพลาดในการลบสมาชิก');
            }
        } catch (err) {
            console.error('Delete member error:', err);
        }
    }

    async function editMember(id) {
        try {
            const res = await fetch(`/api/members/${id}`);
            const member = await res.json();

            document.getElementById('memberModalTitle').textContent = 'แก้ไขข้อมูลสมาชิก';
            document.getElementById('editMemberId').value = member._id;
            document.getElementById('memberIdDisplay').value = member.memberId;
            document.getElementById('memberFirstName').value = member.firstName;
            document.getElementById('memberLastName').value = member.lastName;
            document.getElementById('memberPhone').value = member.phone;
            document.getElementById('memberBirthdate').value = member.birthdate ? member.birthdate.split('T')[0] : '';
            document.getElementById('memberIdCardAddress').value = member.idCardAddress || '';
            document.getElementById('memberShippingAddress').value = member.shippingAddress || '';

            // New Thai ID fields
            document.getElementById('memberPrefix').value = member.prefix || '';
            document.getElementById('memberGender').value = member.gender || '';
            document.getElementById('memberFirstNameEn').value = member.firstNameEn || '';
            document.getElementById('memberLastNameEn').value = member.lastNameEn || '';
            document.getElementById('memberCitizenId').value = member.citizenId || '';
            document.getElementById('memberIssueDate').value = member.issueDate ? member.issueDate.split('T')[0] : '';
            document.getElementById('memberExpiryDate').value = member.expiryDate ? member.expiryDate.split('T')[0] : '';
            document.getElementById('memberFacebook').value = member.facebook || '';
            document.getElementById('memberFacebookLink').value = member.facebookLink || '';
            const memberPostalCode = document.getElementById('memberPostalCode');
            if (memberPostalCode) memberPostalCode.value = member.postalCode || '';

            document.getElementById('memberCitizenId').readOnly = false;
            document.getElementById('memberPrefix').readOnly = false;
            document.getElementById('memberFirstName').readOnly = false;
            document.getElementById('memberLastName').readOnly = false;
            document.getElementById('memberFirstNameEn').readOnly = false;
            document.getElementById('memberLastNameEn').readOnly = false;
            document.getElementById('memberGender').readOnly = false;
            document.getElementById('memberBirthdate').readOnly = false;
            document.getElementById('memberIssueDate').readOnly = false;
            document.getElementById('memberExpiryDate').readOnly = false;
            document.getElementById('memberIdCardAddress').readOnly = false;

            const photoContainer = document.getElementById('smartCardPhotoContainer');
            const photoImg = document.getElementById('smartCardPhoto');
            if (member.photo) {
                photoImg.src = member.photo;
                photoContainer.style.display = 'block';
            } else {
                photoContainer.style.display = 'none';
            }

            document.getElementById('memberModal').style.display = 'flex';
        } catch (err) {
            console.error('Fetch member error:', err);
            showAlert('error', 'ไม่สามารถดึงข้อมูลสมาชิกได้');
        }
    }

    const addMemberBtn = document.getElementById('addMemberBtn');
    if (addMemberBtn) {
        addMemberBtn.addEventListener('click', () => {
            document.getElementById('memberModalTitle').textContent = 'เพิ่มข้อมูลสมาชิก';
            document.getElementById('memberForm').reset();
            document.getElementById('editMemberId').value = '';
            document.getElementById('memberIdDisplay').value = ''; // Clear display
            document.getElementById('memberCitizenId').value = '';
            document.getElementById('memberPrefix').value = '';
            document.getElementById('memberGender').value = '';
            document.getElementById('memberFirstNameEn').value = '';
            document.getElementById('memberLastNameEn').value = '';
            document.getElementById('memberIssueDate').value = '';
            document.getElementById('memberExpiryDate').value = '';
            document.getElementById('memberFacebook').value = '';
            document.getElementById('memberFacebookLink').value = '';
            const memberPostalCode = document.getElementById('memberPostalCode');
            if (memberPostalCode) memberPostalCode.value = '';
            document.getElementById('smartCardPhotoContainer').style.display = 'none';
            document.getElementById('smartCardPhoto').src = '';

            // Re-enable fields if they were disabled (though we keep them readonly for SC integration)
            document.getElementById('memberCitizenId').readOnly = false;
            document.getElementById('memberPrefix').readOnly = false;
            document.getElementById('memberFirstName').readOnly = false;
            document.getElementById('memberLastName').readOnly = false;
            document.getElementById('memberFirstNameEn').readOnly = false;
            document.getElementById('memberLastNameEn').readOnly = false;
            document.getElementById('memberGender').readOnly = false;
            document.getElementById('memberBirthdate').readOnly = false;
            document.getElementById('memberIssueDate').readOnly = false;
            document.getElementById('memberExpiryDate').readOnly = false;
            document.getElementById('memberIdCardAddress').readOnly = false;

            document.getElementById('memberModal').style.display = 'flex';
        });
    }

    const closeMemberModal = document.getElementById('closeMemberModal');
    if (closeMemberModal) { 
        closeMemberModal.addEventListener('click', () => {
            document.getElementById('memberModal').style.display = 'none';
        });
    }

    const copyAddressBtn = document.getElementById('copyAddressBtn');
    if (copyAddressBtn) {
        copyAddressBtn.addEventListener('click', () => {
            const idAddress = document.getElementById('memberIdCardAddress').value;
            document.getElementById('memberShippingAddress').value = idAddress;
        });
    }

    const memberForm = document.getElementById('memberForm');
    if (memberForm) {
        memberForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const editId = document.getElementById('editMemberId').value;

            const normalizeDigits = (v) => String(v || '').replace(/\D/g, '');
            const phoneDigits = normalizeDigits(document.getElementById('memberPhone').value);
            const postalDigits = normalizeDigits(document.getElementById('memberPostalCode') ? document.getElementById('memberPostalCode').value : '');

            if (phoneDigits.length !== 10) {
                showAlert('warning', 'กรุณากรอกเบอร์โทรศัพท์เป็นตัวเลข 10 หลัก');
                return;
            }
            if (postalDigits && postalDigits.length !== 5) {
                showAlert('warning', 'กรุณากรอกรหัสไปรษณีย์เป็นตัวเลข 5 หลัก');
                return;
            }

            const payload = {
                firstName: document.getElementById('memberFirstName').value,
                lastName: document.getElementById('memberLastName').value,
                phone: phoneDigits,
                birthdate: document.getElementById('memberBirthdate').value,
                idCardAddress: document.getElementById('memberIdCardAddress').value,
                address: document.getElementById('memberIdCardAddress').value,
                shippingAddress: document.getElementById('memberShippingAddress').value,
                postalCode: postalDigits || undefined,
                citizenId: document.getElementById('memberCitizenId').value,
                prefix: document.getElementById('memberPrefix').value,
                firstNameEn: document.getElementById('memberFirstNameEn').value,
                lastNameEn: document.getElementById('memberLastNameEn').value,
                gender: document.getElementById('memberGender').value,
                issueDate: document.getElementById('memberIssueDate').value,
                expiryDate: document.getElementById('memberExpiryDate').value,
                facebook: document.getElementById('memberFacebook').value,
                facebookLink: document.getElementById('memberFacebookLink').value,
                photo: document.getElementById('smartCardPhoto').src.startsWith('data:image') ? document.getElementById('smartCardPhoto').src : undefined
            };

            try {
                const url = editId ? `/api/members/${editId}` : '/api/members';
                const method = editId ? 'PUT' : 'POST';

                const res = await fetch(url, {
                    method: method,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                if (data.success) {
                    document.getElementById('memberModal').style.display = 'none';
                    fetchMembers();
                } else {
                    showAlert('error', data.message || 'เกิดข้อผิดพลาด');
                }
            } catch (err) {
                console.error('Submit member error:', err);
                showAlert('error', 'เกิดข้อผิดพลาดในการเชื่อมต่อ');
            }
        });
    }

    // --- MOBILE MENU LOGIC ---
    const menuToggle = document.getElementById('mobileMenuBtn');
    const sidebar = document.getElementById('sidebar');
    const sidebarOverlay = document.getElementById('sidebarOverlay');

    if (menuToggle && sidebar && sidebarOverlay) {
        const toggleMenu = () => {
            sidebar.classList.toggle('active');
            sidebarOverlay.classList.toggle('active');
        };

        menuToggle.addEventListener('click', toggleMenu);
        sidebarOverlay.addEventListener('click', toggleMenu);

        // Close menu when navigation links are clicked (on mobile)
        sidebar.querySelectorAll('nav a').forEach(link => {
            link.addEventListener('click', () => {
                if (window.innerWidth <= 1024) {
                    toggleMenu();
                }
            });
        });
    }

    // --- SHOPS LOGIC ---
    async function fetchShops() {
        try {
            const res = await fetch('/api/shops');
            const data = await res.json();
            renderShops(data);
        } catch (err) {
            console.error('Fetch shops error:', err);
        }
    }

    function renderShops(shops) {
        const body = document.getElementById('shopsBody');
        const empty = document.getElementById('shopsEmptyState');

        if (shops.length === 0) {
            if (empty) empty.style.display = 'block';
            body.innerHTML = '';
            return;
        }

        if (empty) empty.style.display = 'none';
        body.innerHTML = shops.map(s => `
            <tr>
                <td data-label="รหัสร้านค้า" style="font-weight: 600;">${s.shopId}</td>
                <td data-label="ชื่อร้านค้า">${s.shopName}</td>
                <td data-label="สถานที่ตั้ง">${s.location || '-'}</td>
                <td data-label="จัดการ">
                    <div style="display: flex; gap: 0.5rem; justify-content: center;">
                        <button class="edit-shop-btn edit-btn" data-id="${s._id}" title="แก้ไข">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path></svg>
                        </button>
                        <button class="delete-shop-btn edit-btn" data-id="${s._id}" title="ลบ" style="color: #ef4444;">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path><line x1="10" y1="11" x2="10" y2="17"></line><line x1="14" y1="11" x2="14" y2="17"></line></svg>
                        </button>
                    </div>
                </td>
            </tr>
        `).join('');

        // Add event listeners for edit buttons
        document.querySelectorAll('.edit-shop-btn').forEach(btn => {
            btn.addEventListener('click', () => editShop(btn.dataset.id));
        });

        // Add event listeners for delete buttons
        document.querySelectorAll('.delete-shop-btn').forEach(btn => {
            btn.addEventListener('click', () => deleteShop(btn.dataset.id));
        });
    }

    async function deleteShop(id) {
        const confirmed = await showDeleteConfirm('คุณต้องการลบร้านค้านี้ใช่หรือไม่?');
        if (!confirmed) return;

        try {
            const res = await fetch(`/api/shops/${id}`, { method: 'DELETE' });
            if (res.ok) {
                showAlert('success', 'ลบร้านค้าสำเร็จ');
                fetchShops();
            } else {
                const data = await res.json();
                showAlert('error', data.message || 'เกิดข้อผิดพลาดในการลบร้านค้า');
            }
        } catch (err) {
            console.error('Delete shop error:', err);
        }
    }

    const shopModal = document.getElementById('shopModal');
    const shopForm = document.getElementById('shopForm');

    const addShopBtn = document.getElementById('addShopBtn');
    if (addShopBtn) {
        addShopBtn.addEventListener('click', () => {
            isEditMode = false;
            shopForm.reset();
            document.getElementById('editShopId').value = '';
            document.getElementById('shopModalTitle').textContent = 'เพิ่มข้อมูลร้านค้า';
            document.getElementById('shopIdDisplay').value = 'ออกโดยระบบอัตโนมัติ';
            shopModal.style.display = 'flex';
        });
    }

    const closeShopModal = document.getElementById('closeShopModal');
    if (closeShopModal) {
        closeShopModal.addEventListener('click', () => {
            shopModal.style.display = 'none';
        });
    }

    if (shopForm) {
        shopForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const editId = document.getElementById('editShopId').value;
            const payload = {
                shopName: document.getElementById('shopModalName').value,
                location: document.getElementById('shopLocation').value
            };

            try {
                const url = editId ? `/api/shops/${editId}` : '/api/shops';
                const method = editId ? 'PUT' : 'POST';

                const res = await fetch(url, {
                    method: method,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });

                const data = await res.json();
                if (data.success) {
                    shopModal.style.display = 'none';
                    fetchShops();
                } else {
                    showAlert('error', data.message || 'ไม่สามารถบันทึกข้อมูลได้');
                }
            } catch (err) {
                console.error('Shop save error:', err);
            }
        });
    }

    async function editShop(id) {
        try {
            const res = await fetch('/api/shops');
            const shops = await res.json();
            const shop = shops.find(s => s._id === id);

            if (shop) {
                isEditMode = true;
                const editIdElem = document.getElementById('editShopId');
                const titleElem = document.getElementById('shopModalTitle');
                const idDisplayElem = document.getElementById('shopIdDisplay');
                const modalNameElem = document.getElementById('shopModalName');
                const locElem = document.getElementById('shopLocation');

                if (editIdElem) editIdElem.value = shop._id;
                if (titleElem) titleElem.textContent = 'แก้ไขข้อมูลร้านค้า';
                if (idDisplayElem) idDisplayElem.value = shop.shopId;
                if (modalNameElem) modalNameElem.value = shop.shopName;
                if (locElem) locElem.value = shop.location || '';

                shopModal.style.display = 'flex';
            }
        } catch (err) {
            console.error('Edit shop error:', err);
        }
    }

    // --- SMART CARD INTEGRATION ---
    async function connectSmartCard() {
        const btn = document.getElementById('readSmartCardBtn');
        const originalText = btn.innerHTML;

        try {
            btn.disabled = true;
            btn.innerHTML = '⌛ กำลังเรียกอ่านข้อมูลจาก Smart Card Agent...';

            const response = await fetch('http://localhost:3001/api/read-card');

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.message || 'ไม่สามารถอ่านข้อมูลจากบัตรได้');
            }

            const result = await response.json();

            if (result.success && result.data) {
                populateMemberFormFromCard(result.data);
                showAlert('success', 'อ่านข้อมูลจากบัตรเรียบร้อยแล้ว');
            } else {
                throw new Error(result.message || 'ข้อมูลไม่สมบูรณ์');
            }

        } catch (err) {
            console.error('Smart Card Error:', err);
            // Specific error message if agent is not running
            if (err.name === 'TypeError' && err.message.toLowerCase().includes('failed to fetch')) {
                showAlert('warning', 'ไม่สามารถเชื่อมต่อกับ Smart Card Agent ได้ กรุณารันคำสั่ง npm start ในโฟลเดอร์ smartcard-agent', 'ไม่สามารถเชื่อมต่อ');
            } else {
                showAlert('error', err.message);
            }
        } finally {
            btn.disabled = false;
            btn.innerHTML = originalText;
        }
    }

    function populateMemberFormFromCard(data) {
        document.getElementById('memberCitizenId').value = data.citizenId;
        document.getElementById('memberPrefix').value = data.prefix;
        document.getElementById('memberFirstName').value = data.firstName;
        document.getElementById('memberLastName').value = data.lastName;
        document.getElementById('memberFirstNameEn').value = data.firstNameEn;
        document.getElementById('memberLastNameEn').value = data.lastNameEn;
        document.getElementById('memberBirthdate').value = data.birthdate;
        document.getElementById('memberGender').value = data.gender;
        document.getElementById('memberIdCardAddress').value = data.address;
        document.getElementById('memberIssueDate').value = data.issueDate;
        document.getElementById('memberExpiryDate').value = data.expiryDate;

        const photoContainer = document.getElementById('smartCardPhotoContainer');
        const photoImg = document.getElementById('smartCardPhoto');
        if (data.photo) {
            photoImg.src = data.photo;
            photoContainer.style.display = 'block';
        } else {
            photoContainer.style.display = 'none';
        }
    }

    // Helper to decode TIS-620 to UTF-8 (Common for Thai ID Card reading)
    function decodeThai(buffer) {
        const decoder = new TextDecoder('tis-620');
        return decoder.decode(buffer).trim();
    }

    const readSmartCardBtn = document.getElementById('readSmartCardBtn');
    if (readSmartCardBtn) {
        readSmartCardBtn.addEventListener('click', connectSmartCard);
    }

    // ═══════════════════════════════════════════════════════════════════
    // CLAIM FEATURE - Complete Implementation
    // ═══════════════════════════════════════════════════════════════════

    let currentClaimData = null; // Store current claim after save

    // Fetch active policies for claim menu
    async function fetchActivePolicies() {
        showLoader('กำลังโหลดรายการกรมธรรม์...');
        try {
            const res = await fetch('/api/warranties/active');
            const data = await res.json();
            renderClaimsTable(data);
        } catch (err) {
            console.error('Fetch active policies error:', err);
        } finally {
            hideLoader();
        }
    }

    // Render active policies table with "ทำรายการเคลม" button
    function renderClaimsTable(records) {
        const body = document.getElementById('claimsBody');
        const empty = document.getElementById('claimsEmptyState');
        if (!body) return;

        records = Array.isArray(records) ? records : [];
        records = records.filter(r => r && r.approvalStatus === 'approved');

        if (records.length === 0) {
            if (empty) empty.style.display = 'block';
            body.innerHTML = '';
            return;
        }

        if (empty) empty.style.display = 'none';
        body.innerHTML = records.map(r => {
            const now = new Date();
            const end = new Date(r.warrantyDates.end);
            const diffMs = end - now;
            const totalHours = Math.floor(diffMs / (1000 * 60 * 60));
            const days = Math.floor(totalHours / 24);
            const hours = totalHours % 24;
            const timeRemainingText = `${days} วัน, ${hours} ชม.`;

            return `
                <tr>
                    <td data-label="วันที่ลงทะเบียน">${r.createdAt ? new Date(r.createdAt).toLocaleString('th-TH') : '-'}</td>
                    <td data-label="เลขกรมธรรม์" style="font-weight: 600; color: var(--primary);">${r.policyNumber || '-'}</td>
                    <td data-label="รหัสสมาชิก">${r.memberId || '-'}</td>
                    <td data-label="ชื่อลูกค้า">${r.customer.firstName} ${r.customer.lastName}</td>
                    <td data-label="เบอร์โทรศัพท์">${r.customer.phone}</td>
                    <td data-label="รุ่นอุปกรณ์">${r.device.model}</td>
                    <td data-label="แพ็กเกจ"><span style="color: var(--primary); font-weight: 500;">${r.package.plan}</span></td>
                    <td data-label="ประกันคงเหลือ">${timeRemainingText}</td>
                    <td data-label="สถานะ">
                        ${r.claimStatus === 'pending'
                    ? `<span class="status-badge status-pending" style="background-color: #f59e0b; color: white;">รอเคลม</span>`
                    : `<span class="status-badge status-active">ปกติ</span>`
                }
                    </td>
                    <td data-label="ทำรายการ">
                        <div style="display: flex; gap: 0.5rem; justify-content: center; align-items: center; flex-wrap: wrap;">
                            ${r.claimStatus === 'pending'
                    ? `<button onclick="printClaimByWarrantyId('${r._id}')" style="background: linear-gradient(135deg, #3b82f6, #2563eb); color: white; border: none; padding: 6px 12px; border-radius: 8px; cursor: pointer; font-size: 0.85rem; font-weight: 600; white-space: nowrap;">
                                    🖨️ พิมพ์เอกสาร
                                   </button>`
                    : `<button class="claim-action-btn" data-id="${r._id}" style="background: linear-gradient(135deg, #f59e0b, #d97706); color: white; border: none; padding: 6px 12px; border-radius: 8px; cursor: pointer; font-size: 0.85rem; font-weight: 600; white-space: nowrap;">
                                    📋 แจ้งเคลม
                                   </button>`
                }
                            <button class="btn-history" onclick="openClaimHistoryModal('${r._id}')">
                                📜 ประวัติ
                            </button>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');

        // Attach event listeners to claim buttons
        document.querySelectorAll('.claim-action-btn').forEach(btn => {
            btn.addEventListener('click', () => openClaimForm(btn.dataset.id));
        });
    }

    // Function to print claim receipt from menu
    window.printClaimByWarrantyId = async (warrantyId) => {
        try {
            showLoader('กำลังดึงข้อมูลเอกสาร...');
            const res = await fetch(`/api/claims/warranty/${warrantyId}`);
            const result = await res.json();

            if (result.success) {
                localStorage.setItem('smilecare_current_claim', JSON.stringify(result.claim));
                const claimMongoId = result.claim && result.claim._id ? String(result.claim._id) : '';
                if (claimMongoId) {
                    window.open(`claim_receipt.html?claimId=${encodeURIComponent(claimMongoId)}`, '_blank');
                } else {
                    window.open('claim_receipt.html', '_blank');
                }
            } else {
                showAlert('error', 'ไม่พบข้อมูลการเคลม');
            }
        } catch (err) {
            console.error(err);
            showAlert('error', 'ไม่สามารถดึงข้อมูลเอกสารได้');
        } finally {
            hideLoader();
        }
    };

    // Print claim receipt by claim MongoDB _id (history)
    window.printClaimById = function (claimId) {
        if (!claimId) return;
        window.open(`claim_receipt.html?claimId=${encodeURIComponent(String(claimId))}`, '_blank');
    };

    // Open claim form modal with warranty details
    async function openClaimForm(warrantyId) {
        showLoader('กำลังโหลดข้อมูลกรมธรรม์...');
        try {
            const res = await fetch(`/api/warranties/${warrantyId}`);
            const data = await res.json();
            if (!res.ok) throw new Error(data.message);

            // Populate policy details
            document.getElementById('claimWarrantyId').value = data._id;
            document.getElementById('claimPolicyNumber').value = data.policyNumber || '';
            document.getElementById('claimMemberId').value = data.memberId || '';
            document.getElementById('claimCustomerName').value = `${data.customer.firstName} ${data.customer.lastName}`;
            document.getElementById('claimCustomerPhone').value = data.customer.phone || '';
            document.getElementById('claimDeviceModel').value = data.device.model || '';
            document.getElementById('claimColor').value = data.device.color || '';
            document.getElementById('claimSerial').value = data.device.serial || '';
            document.getElementById('claimIMEI').value = data.device.imei || '';
            document.getElementById('claimPackage').value = data.package.plan || '';
            document.getElementById('claimStartDate').value = data.warrantyDates.start ? new Date(data.warrantyDates.start).toLocaleDateString('th-TH') : '';
            document.getElementById('claimEndDate').value = data.warrantyDates.end ? new Date(data.warrantyDates.end).toLocaleDateString('th-TH') : '';

            // Store member addresses for delivery options
            document.getElementById('displayCardAddress').textContent = data.customer.idCardAddress || 'ไม่พบข้อมูลที่อยู่ตามหน้าบัตร';
            document.getElementById('displayShippingAddress').textContent = data.customer.shippingAddress || 'ไม่พบข้อมูลที่อยู่จัดส่ง';
            document.getElementById('displayCardAddress').dataset.address = data.customer.idCardAddress || '';
            document.getElementById('displayShippingAddress').dataset.address = data.customer.shippingAddress || '';

            // Auto-fill claim details
            document.getElementById('claimDate').value = new Date().toLocaleDateString('th-TH');
            document.getElementById('claimIdDisplay').value = 'สร้างอัตโนมัติเมื่อบันทึก';
            document.getElementById('claimStaffName').value = currentUser ? currentUser.staffName : '';

            // Reset form fields
            document.getElementById('claimSymptoms').value = '';
            document.getElementById('claimImages').value = '';
            document.getElementById('claimImagePreview').innerHTML = '';
            const pickupRadio = document.querySelector('input[name="returnMethod"][value="pickup"]');
            if (pickupRadio) pickupRadio.checked = false;
            const deliveryRadio = document.querySelector('input[name="returnMethod"][value="delivery"]');
            if (deliveryRadio) deliveryRadio.checked = false;
            document.getElementById('claimPickupBranch').value = '';
            document.getElementById('pickupBranchGroup').style.display = 'none';
            document.getElementById('deliveryOptionsGroup').style.display = 'none';
            document.getElementById('newAddressGroup').style.display = 'none';
            document.getElementById('claimNewAddress').value = '';
            document.querySelectorAll('input[name="deliveryAddressType"]').forEach(rb => rb.checked = false);

            // Reset checklist
            const devicePowerOnEl = document.getElementById('devicePowerOn');
            if (devicePowerOnEl) {
                devicePowerOnEl.checked = true;
                devicePowerOnEl.dispatchEvent(new Event('change'));
            }
            document.querySelectorAll('#deviceConditionChecklist .condition-row').forEach(row => {
                row.className = 'condition-row';
            });
            document.querySelectorAll('#deviceConditionChecklist textarea').forEach(t => {
                t.value = '';
            });

            // Show the modal
            document.getElementById('claimFormModal').style.display = 'flex';
        } catch (err) {
            console.error('Open claim form error:', err);
            showAlert('error', 'ไม่สามารถดึงข้อมูลกรมธรรม์ได้');
        } finally {
            hideLoader();
        }
    }

    // Image upload preview
    const claimImagesInput = document.getElementById('claimImages');
    if (claimImagesInput) {
        claimImagesInput.addEventListener('change', function () {
            const preview = document.getElementById('claimImagePreview');
            preview.innerHTML = '';
            Array.from(this.files).forEach(file => {
                const reader = new FileReader();
                reader.onload = (e) => {
                    const img = document.createElement('img');
                    img.src = e.target.result;
                    img.style.cssText = 'width: 80px; height: 80px; object-fit: cover; border-radius: 8px; border: 2px solid #d1d5db;';
                    preview.appendChild(img);
                };
                reader.readAsDataURL(file);
            });
        });
    }

    // Return method radio toggle
    document.querySelectorAll('input[name="returnMethod"]').forEach(radio => {
        radio.addEventListener('change', function () {
            const branchGroup = document.getElementById('pickupBranchGroup');
            const deliveryGroup = document.getElementById('deliveryOptionsGroup');

            if (this.value === 'pickup') {
                if (branchGroup) branchGroup.style.display = 'block';
                if (deliveryGroup) deliveryGroup.style.display = 'none';
            } else if (this.value === 'delivery') {
                if (branchGroup) branchGroup.style.display = 'none';
                if (deliveryGroup) deliveryGroup.style.display = 'block';
            }
        });
    });

    // Delivery address type radio toggle
    document.querySelectorAll('input[name="deliveryAddressType"]').forEach(radio => {
        radio.addEventListener('change', function () {
            const newAddressGroup = document.getElementById('newAddressGroup');
            if (newAddressGroup) {
                newAddressGroup.style.display = this.value === 'new' ? 'block' : 'none';
            }
        });
    });

    // Close claim form modal
    const closeClaimFormBtn = document.getElementById('closeClaimFormModal');
    if (closeClaimFormBtn) {
        closeClaimFormBtn.addEventListener('click', () => {
            document.getElementById('claimFormModal').style.display = 'none';
        });
    }

    // Submit claim form
    const claimForm = document.getElementById('claimForm');
    if (claimForm) {
        claimForm.addEventListener('submit', async (e) => {
            e.preventDefault();

            const symptoms = document.getElementById('claimSymptoms').value.trim();
            if (!symptoms) {
                showAlert('warning', 'กรุณาระบุรายละเอียดอาการเสีย');
                return;
            }

            const returnMethodEl = document.querySelector('input[name="returnMethod"]:checked');
            if (!returnMethodEl) {
                showAlert('warning', 'กรุณาเลือกวิธีการคืนเครื่อง');
                return;
            }

            const formData = new FormData();
            formData.append('warrantyId', document.getElementById('claimWarrantyId').value);
            formData.append('policyNumber', document.getElementById('claimPolicyNumber').value);
            formData.append('memberId', document.getElementById('claimMemberId').value);
            formData.append('customerName', document.getElementById('claimCustomerName').value);
            formData.append('customerPhone', document.getElementById('claimCustomerPhone').value);
            formData.append('deviceModel', document.getElementById('claimDeviceModel').value);
            formData.append('imei', document.getElementById('claimIMEI').value);
            formData.append('serialNumber', document.getElementById('claimSerial').value);
            formData.append('color', document.getElementById('claimColor').value);
            const devicePowerStateEl2 = document.querySelector('input[name="devicePowerState"]:checked');
            const devicePowerState2 = devicePowerStateEl2 ? devicePowerStateEl2.value : 'on';
            formData.append('devicePowerState', devicePowerState2);
            formData.append('symptoms', symptoms);
            formData.append('staffName', document.getElementById('claimStaffName').value);
            formData.append('returnMethod', returnMethodEl.value);
            formData.append('pickupBranch', document.getElementById('claimPickupBranch').value);

            if (returnMethodEl.value === 'delivery') {
                const addrTypeEl = document.querySelector('input[name="deliveryAddressType"]:checked');
                if (!addrTypeEl) {
                    showAlert('warning', 'กรุณาเลือกที่อยู่สำหรับจัดส่ง');
                    return;
                }

                let addrDetail = '';
                if (addrTypeEl.value === 'card') {
                    addrDetail = document.getElementById('displayCardAddress').dataset.address;
                } else if (addrTypeEl.value === 'memberShipping') {
                    addrDetail = document.getElementById('displayShippingAddress').dataset.address;
                } else if (addrTypeEl.value === 'new') {
                    addrDetail = document.getElementById('claimNewAddress').value.trim();
                    if (!addrDetail) {
                        showAlert('warning', 'กรุณากรอกข้อมูลที่อยู่ใหม่');
                        return;
                    }
                }

                formData.append('deliveryAddressType', addrTypeEl.value);
                formData.append('deliveryAddressDetail', addrDetail);
            }

            // Collect device condition checklist (new button-based format)
            const devicePowerStateEl = document.querySelector('input[name="devicePowerState"]:checked');
            const devicePowerState = devicePowerStateEl ? devicePowerStateEl.value : 'on';

            const conditionKeys = (devicePowerState === 'off')
                ? ['exterior', 'screen', 'assembly', 'appleLogo', 'chargingPort', 'simTray']
                : ['exterior', 'screen', 'assembly', 'appleLogo', 'buttons', 'chargingPort',
                    'simTray', 'imeiMatch', 'modelMatch', 'screenTouch', 'faceIdTouchId', 'cameras',
                    'speakerMic', 'connectivity', 'battery', 'warrantyVoid'];
            const conditionLabels = {
                exterior: 'ตัวเครื่องภายนอก', screen: 'หน้าจอ', assembly: 'การประกอบ',
                appleLogo: 'โลโก้ Apple', buttons: 'ปุ่มต่างๆ', chargingPort: 'พอร์ตชาร์จ',
                simTray: 'ช่องใส่ซิม', imeiMatch: 'IMEI ตรงกัน', modelMatch: 'Model เครื่อง',
                screenTouch: 'หน้าจอ/ทัชสกรีน', faceIdTouchId: 'Face ID / Touch ID',
                cameras: 'กล้องหน้า/กล้องหลัง', speakerMic: 'ลำโพง/ไมค์',
                connectivity: 'การเชื่อมต่อ', battery: 'แบตเตอรี่', warrantyVoid: 'ประกัน/วอยด์',
                other: 'อื่นๆ'
            };
            const deviceCondition = {};
            let conditionErrors = [];
            for (const key of conditionKeys) {
                const row = document.querySelector(`#deviceConditionChecklist .condition-row[data-key="${key}"]`);
                const isNormal = row && row.classList.contains('is-normal');
                const isAbnormal = row && row.classList.contains('is-abnormal');
                if (!isNormal && !isAbnormal) {
                    conditionErrors.push(conditionLabels[key]);
                    continue;
                }
                const status = isNormal ? 'normal' : 'abnormal';
                let reason = '';
                if (isAbnormal) {
                    const reasonEl = document.getElementById(`reason-text-${key}`);
                    reason = reasonEl ? reasonEl.value.trim() : '';
                    if (!reason) { conditionErrors.push(`${conditionLabels[key]} (ต้องระบุเหตุผล)`); }
                }
                deviceCondition[key] = { status, reason };
            }

            // Optional "other" (free text, no normal/abnormal selection)
            const otherReasonEl = document.getElementById('reason-text-other');
            const otherReason = otherReasonEl ? otherReasonEl.value.trim() : '';
            if (otherReason) {
                deviceCondition.other = { status: 'abnormal', reason: otherReason };
            }

            if (conditionErrors.length > 0) {
                showAlert('warning', `กรุณาเลือกสภาพและระบุเหตุผลให้ครบทุกรายการ:\n• ${conditionErrors.join('\n• ')}`);
                return;
            }
            formData.append('deviceCondition', JSON.stringify(deviceCondition));

            // Append image files
            const imageFiles = document.getElementById('claimImages').files;
            for (let i = 0; i < imageFiles.length; i++) {
                formData.append('images', imageFiles[i]);
            }

            showLoader('กำลังบันทึกรายการเคลม...');
            try {
                const res = await fetch('/api/claims', {
                    method: 'POST',
                    body: formData // No Content-Type header — browser sets it with boundary
                });
                const result = await res.json();

                if (result.success) {
                    // Close claim form modal
                    document.getElementById('claimFormModal').style.display = 'none';
                    showAlert('success', `บันทึกรายการเคลมสำเร็จ! รหัส: ${result.claim.claimId}`);
                    currentClaimData = result.claim;

                    // Open contract/signature modal
                    openClaimContract(result.claim);
                } else {
                    showAlert('error', result.message || 'เกิดข้อผิดพลาดในการบันทึก');
                }
            } catch (err) {
                console.error('Submit claim error:', err);
                showAlert('error', 'เกิดข้อผิดพลาดในการเชื่อมต่อเซิร์ฟเวอร์');
            } finally {
                hideLoader();
            }
        });
    }

    // ═══════════════════════════════════════════════════════════════════
    // CLAIM CONTRACT & SIGNATURE MODAL
    // ═══════════════════════════════════════════════════════════════════

    function openClaimContract(claim) {
        // Populate contract details
        document.getElementById('contractClaimId').textContent = claim.claimId;
        document.getElementById('contractClaimDate').textContent = new Date(claim.claimDate).toLocaleDateString('th-TH');
        document.getElementById('contractPolicyNumber').textContent = claim.policyNumber;
        document.getElementById('contractMemberId').textContent = claim.memberId;
        document.getElementById('contractCustomerName').textContent = claim.customerName;
        document.getElementById('contractCustomerPhone').textContent = claim.customerPhone;
        document.getElementById('contractDeviceModel').textContent = claim.deviceModel;
        document.getElementById('contractStaffName').textContent = claim.staffName;
        document.getElementById('contractSymptoms').textContent = claim.symptoms;

        // Return method
        const returnText = claim.returnMethod === 'pickup' ? 'มารับเองที่ร้าน' : 'จัดส่ง';
        document.getElementById('contractReturnMethod').textContent = returnText;
        const branchRow = document.getElementById('contractBranchRow');

        if (claim.returnMethod === 'pickup' && claim.pickupBranch) {
            branchRow.style.display = 'block';
            document.getElementById('contractPickupBranch').textContent = claim.pickupBranch;
        } else if (claim.returnMethod === 'delivery' && claim.deliveryAddressDetail) {
            branchRow.style.display = 'block';
            const label = branchRow.querySelector('strong');
            if (label) label.textContent = 'ที่อยู่จัดส่ง:';
            document.getElementById('contractPickupBranch').textContent = claim.deliveryAddressDetail;
        } else {
            branchRow.style.display = 'none';
        }

        // Populate Device Condition Checklist
        const conditionMap = {
            exterior: 'ตัวเครื่องภายนอก',
            screen: 'หน้าจอ',
            assembly: 'การประกอบ',
            appleLogo: 'โลโก้ Apple',
            buttons: 'ปุ่มต่างๆ',
            chargingPort: 'พอร์ตชาร์จ',
            simTray: 'ช่องใส่ซิม',
            imeiMatch: 'IMEI ตรงกัน',
            modelMatch: 'Model เครื่อง',
            screenTouch: 'หน้าจอ/ทัชสกรีน',
            faceIdTouchId: 'Face ID / Touch ID',
            cameras: 'กล้องหน้า/กล้องหลัง',
            speakerMic: 'ลำโพง/ไมค์',
            connectivity: 'การเชื่อมต่อ',
            battery: 'แบตเตอรี่',
            warrantyVoid: 'ประกัน/วอยด์'
        };

        const conditions = claim.deviceCondition || {};
        const conditionGrid = document.getElementById('contractConditionGrid');
        if (conditionGrid) {
            conditionGrid.innerHTML = Object.entries(conditionMap).map(([key, label]) => {
                const item = conditions[key];
                // Support both old boolean format and new {status, reason} format
                const isNormal = item === true || (item && item.status === 'normal');
                const isAbnormal = item === false || (item && item.status === 'abnormal');
                const reason = (item && item.reason) ? item.reason : '';
                let badge = `<span style="color:#94a3b8;font-size:0.8rem;">—</span>`;
                if (isNormal) badge = `<span style="color:#16a34a;font-weight:700;">✅ ปกติ</span>`;
                if (isAbnormal) badge = `<span style="color:#dc2626;font-weight:700;">❌ ไม่ปกติ</span>`;
                const reasonHtml = (isAbnormal && reason) ? `<div style="font-size:0.78rem;color:#dc2626;margin-top:2px;padding-left:4px;">↳ ${reason}</div>` : '';
                return `<div style="font-size:0.85rem;padding:2px 0;">${badge} ${label}${reasonHtml}</div>`;
            }).join('');
        }

        // Uploaded images
        const imagesSection = document.getElementById('contractImagesSection');
        const imagesContainer = document.getElementById('contractImages');
        if (claim.images && claim.images.length > 0) {
            imagesSection.style.display = 'block';
            imagesContainer.innerHTML = claim.images.map(src =>
                `<img src="${src}" style="width: 80px; height: 80px; object-fit: cover; border-radius: 8px; border: 2px solid #d1d5db;">`
            ).join('');
        } else {
            imagesSection.style.display = 'none';
        }

        // Reset CEO signature state
        // document.getElementById('ceoSignatureArea').style.display = 'block';
        // document.getElementById('managerSigCanvas').style.display = 'none';
        // document.getElementById('clearManagerSigBtn').style.display = 'none';

        // Initialize signature canvases
        // initSignaturePad('customerSigCanvas');
        // initSignaturePad('staffSigCanvas');
        // initSignaturePad('managerSigCanvas');

        // Show modal
        document.getElementById('claimContractModal').style.display = 'flex';
    }

    // Signature Pad Logic (mouse + touch)
    function initSignaturePad(canvasId) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        const ctx = canvas.getContext('2d');

        // Resize canvas to match display size
        const rect = canvas.getBoundingClientRect();
        canvas.width = rect.width || 250;
        canvas.height = rect.height || 150;

        ctx.strokeStyle = '#1e293b';
        ctx.lineWidth = 2;
        ctx.lineCap = 'round';
        ctx.lineJoin = 'round';

        let isDrawing = false;

        function getPos(e) {
            const rect = canvas.getBoundingClientRect();
            const scaleX = canvas.width / rect.width;
            const scaleY = canvas.height / rect.height;
            if (e.touches) {
                return {
                    x: (e.touches[0].clientX - rect.left) * scaleX,
                    y: (e.touches[0].clientY - rect.top) * scaleY
                };
            }
            return {
                x: (e.clientX - rect.left) * scaleX,
                y: (e.clientY - rect.top) * scaleY
            };
        }

        function startDraw(e) {
            e.preventDefault();
            isDrawing = true;
            const pos = getPos(e);
            ctx.beginPath();
            ctx.moveTo(pos.x, pos.y);
        }

        function draw(e) {
            e.preventDefault();
            if (!isDrawing) return;
            const pos = getPos(e);
            ctx.lineTo(pos.x, pos.y);
            ctx.stroke();
        }

        function stopDraw(e) {
            if (e) e.preventDefault();
            isDrawing = false;
        }

        // Remove old listeners to avoid duplicates
        canvas.onmousedown = startDraw;
        canvas.onmousemove = draw;
        canvas.onmouseup = stopDraw;
        canvas.onmouseleave = stopDraw;

        canvas.ontouchstart = startDraw;
        canvas.ontouchmove = draw;
        canvas.ontouchend = stopDraw;
    }

    // Clear signature buttons
    document.querySelectorAll('.clear-sig-btn').forEach(btn => {
        btn.addEventListener('click', function () {
            const canvasId = this.dataset.canvas;
            const canvas = document.getElementById(canvasId);
            if (canvas) {
                const ctx = canvas.getContext('2d');
                ctx.clearRect(0, 0, canvas.width, canvas.height);
            }
        });
    });

    // Toggle CEO signature (use image vs. manual sign)
    const toggleCeoSigBtn = document.getElementById('toggleCeoSig');
    if (toggleCeoSigBtn) {
        toggleCeoSigBtn.addEventListener('click', function () {
            const ceoArea = document.getElementById('ceoSignatureArea');
            const managerCanvas = document.getElementById('managerSigCanvas');
            const clearBtn = document.getElementById('clearManagerSigBtn');

            if (managerCanvas.style.display === 'none') {
                // Switch to manual sign
                ceoArea.style.display = 'none';
                managerCanvas.style.display = 'block';
                clearBtn.style.display = 'inline-block';
                this.textContent = 'ใช้รูป CEO';
                initSignaturePad('managerSigCanvas');
            } else {
                // Switch back to CEO image
                ceoArea.style.display = 'block';
                managerCanvas.style.display = 'none';
                clearBtn.style.display = 'none';
                this.textContent = 'เซ็นใหม่';
            }
        });
    }

    // Save signatures
    const saveSignaturesBtn = document.getElementById('saveSignaturesBtn');
    if (saveSignaturesBtn) {
        saveSignaturesBtn.addEventListener('click', async () => {
            if (!currentClaimData) return;

            const customerSig = document.getElementById('customerSigCanvas').toDataURL();
            const staffSig = document.getElementById('staffSigCanvas').toDataURL();

            // Manager signature: use canvas if visible, otherwise use CEO image path
            let managerSig;
            const managerCanvas = document.getElementById('managerSigCanvas');
            if (managerCanvas.style.display !== 'none') {
                managerSig = managerCanvas.toDataURL();
            } else {
                managerSig = 'ceo.png'; // Reference to the CEO image file
            }

            showLoader('กำลังบันทึกลายเซ็น...');
            try {
                const res = await fetch(`/api/claims/${currentClaimData._id}/signatures`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ customerSignature: customerSig, staffSignature: staffSig, managerSignature: managerSig })
                });
                const result = await res.json();
                if (result.success) {
                    showAlert('success', 'บันทึกลายเซ็นสำเร็จ');
                } else {
                    showAlert('error', result.message || 'เกิดข้อผิดพลาด');
                }
            } catch (err) {
                console.error('Save signatures error:', err);
                showAlert('error', 'เกิดข้อผิดพลาดในการบันทึกลายเซ็น');
            } finally {
                hideLoader();
            }
        });
    }

    // Print contract document
    const printContractBtn = document.getElementById('printContractBtn');
    if (printContractBtn) {
        printContractBtn.addEventListener('click', () => {
            if (currentClaimData) {
                localStorage.setItem('smilecare_current_claim', JSON.stringify(currentClaimData));
                window.open('claim_receipt.html', '_blank');
            } else {
                showAlert('error', 'ไม่พบข้อมูลการเคลม');
            }
        });
    }

    // Close contract modal
    const closeContractBtn = document.getElementById('closeContractModal');
    if (closeContractBtn) {
        closeContractBtn.addEventListener('click', () => {
            document.getElementById('claimContractModal').style.display = 'none';
            currentClaimData = null;
        });
    }

    // ═══════════════════════════════════════════════════════════════════
    // STATUS TRACKING LOGIC
    // ═══════════════════════════════════════════════════════════════════

    async function fetchPendingClaims() {
        showLoader('กำลังโหลดรายการเคลม...');
        try {
            const res = await fetch('/api/claims/pending');
            const claims = await res.json();
            renderStatusTracking(claims);

            updateStatusTrackingBadge();
        } catch (err) {
            console.error('Fetch pending claims error:', err);
            showAlert('error', 'ไม่สามารถโหลดข้อมูลรายการเคลมได้');
        } finally {
            hideLoader();
        }
    }

    function renderStatusTracking(claims) {
        const body = document.getElementById('statusTrackingBody');
        const empty = document.getElementById('statusTrackingEmptyState');
        if (!body) return;

        if (!claims || claims.length === 0) {
            body.innerHTML = '';
            if (empty) empty.style.display = 'block';
            return;
        }
        if (empty) empty.style.display = 'none';

        const safeDays = (n) => {
            const x = parseInt(n, 10);
            return Number.isFinite(x) && x >= 0 ? x : 0;
        };

        body.innerHTML = claims.map(c => `
            <tr class="${c && c.isOverdue ? 'row-overdue' : ''}">
                <td data-label="รหัสเคลม" style="font-weight: 600; color: var(--primary);">${c.claimId || '-'}</td>
                <td data-label="วันที่แจ้งเคลม">${c.claimDate ? new Date(c.claimDate).toLocaleDateString('th-TH', { year: 'numeric', month: 'short', day: 'numeric' }) : '-'}</td>
                <td data-label="ชื่อลูกค้า">${c.customerName || '-'}</td>
                <td data-label="เบอร์โทรศัพท์">${c.customerPhone || '-'}</td>
                <td data-label="อุปกรณ์">${c.deviceModel || '-'}</td>
                <td data-label="สถานะ">
                    ${c && c.isOverdue
                ? `<span class="status-badge status-overdue">⚠️ เกินกำหนดอัปเดต (${safeDays(c.daysOverdue)} วัน)</span>`
                : `<span class="status-badge status-pending" style="background-color: #f59e0b; color: white;">รอเคลม</span>`}
                </td>
                <td data-label="ทำรายการ">
                    <div style="display: flex; gap: 0.5rem; justify-content: center;">
                        <button class="status-update-btn submit-btn" data-id="${c._id}" style="padding: 0.4rem 1rem; font-size: 0.85rem; background: linear-gradient(135deg, #3b82f6, #2563eb);">
                            📋 อัปเดตสถานะ
                        </button>
                    </div>
                </td>
            </tr>
        `).join('');

        // Wire up buttons
        document.querySelectorAll('.status-update-btn').forEach(btn => {
            btn.addEventListener('click', () => openStatusUpdateModal(btn.dataset.id));
        });
    }

    async function openStatusUpdateModal(claimId) {
        showLoader('กำลังโหลดข้อมูล...');
        try {
            const res = await fetch(`/api/claims/${claimId}`);
            const claim = await res.json();
            if (!res.ok) throw new Error(claim.message);

            const isIPadModel = (model) => {
                if (!model) return false;
                return String(model).toLowerCase().includes('ipad');
            };

            // Store claimId
            document.getElementById('statusUpdateClaimId').value = claim._id;

            // Claim info bar
            const infoBar = document.getElementById('statusClaimInfo');
            if (infoBar) {
                const imeiHtml = isIPadModel(claim.deviceModel)
                    ? ''
                    : `<div><strong>IMEI:</strong> ${claim.imei}</div>`;
                infoBar.innerHTML = `
                    <div><strong>รหัสเคลม:</strong> ${claim.claimId}</div>
                    <div><strong>ชื่อลูกค้า:</strong> ${claim.customerName}</div>
                    <div><strong>เบอร์โทร:</strong> ${claim.customerPhone}</div>
                    <div><strong>อุปกรณ์:</strong> ${claim.deviceModel}</div>
                    ${imeiHtml}
                    <div><strong>Serial Number:</strong> ${claim.serialNumber}</div>
                    <div><strong>สี:</strong> ${claim.color}</div>
                    <div><strong>อาการ:</strong> ${claim.symptoms || '-'}</div>
                    <div><strong>วันที่แจ้งเคลม:</strong> ${new Date(claim.claimDate).toLocaleDateString('th-TH', { year: 'numeric', month: 'long', day: 'numeric' })}</div>
                `;
            }

            // Render timeline
            renderTimeline(claim);

            // Reset form
            document.getElementById('updateTitle').value = '';
            document.getElementById('updateCost').value = '0';
            const centerNameEl = document.getElementById('updateCenterName');
            if (centerNameEl) centerNameEl.value = '';
            const centerLocationEl = document.getElementById('updateCenterLocation');
            if (centerLocationEl) centerLocationEl.value = '';
            const centerPhoneEl = document.getElementById('updateCenterPhone');
            if (centerPhoneEl) centerPhoneEl.value = '';
            const techNameEl = document.getElementById('updateTechnicianName');
            if (techNameEl) techNameEl.value = '';
            const techPhoneEl = document.getElementById('updateTechnicianPhone');
            if (techPhoneEl) techPhoneEl.value = '';

            const costSection = document.getElementById('updateCostSection');
            const toggleCostBtn = document.getElementById('toggleCostBtn');
            if (costSection) costSection.style.display = 'none';
            if (toggleCostBtn) toggleCostBtn.style.display = 'block';

            const updateImagesInput = document.getElementById('updateImages');
            if (updateImagesInput) updateImagesInput.value = '';
            const updatePreview = document.getElementById('updateImagePreview');
            if (updatePreview) updatePreview.innerHTML = '';

            const evidenceGroup = document.getElementById('updateEvidenceGroup');
            if (evidenceGroup) evidenceGroup.style.display = 'none';
            const evidenceInput = document.getElementById('updateEvidenceImages');
            if (evidenceInput) evidenceInput.value = '';
            const evidencePreview = document.getElementById('updateEvidencePreview');
            if (evidencePreview) evidencePreview.innerHTML = '';

            const completeImagesInput = document.getElementById('completeImages');
            if (completeImagesInput) completeImagesInput.value = '';
            const completePreview = document.getElementById('completeImagePreview');
            if (completePreview) completePreview.innerHTML = '';

            // Show modal
            document.getElementById('statusUpdateModal').style.display = 'flex';
        } catch (err) {
            console.error('Open status update modal error:', err);
            showAlert('error', 'ไม่สามารถโหลดข้อมูลเคลมได้');
        } finally {
            hideLoader();
        }
    }

    function renderTimeline(claim) {
        const timeline = document.getElementById('statusTimeline');
        if (!timeline) return;

        let html = '';

        // Step 1: "รับเครื่อง" (auto-generated from claim data)
        const claimDateStr = new Date(claim.claimDate).toLocaleDateString('th-TH', { year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit' });
        html += `
            <div class="timeline-item">
                <div class="timeline-marker">
                    <div class="timeline-circle">1</div>
                    <div class="timeline-line"></div>
                </div>
                <div class="timeline-content">
                    <div class="timeline-header">
                        <h4>รับเครื่อง</h4>
                        <span class="timeline-date">${claimDateStr}</span>
                    </div>
                    <p class="timeline-cost">ค่าใช้จ่าย: <strong>0 บาท</strong></p>
                    ${claim.images && claim.images.length > 0 ? `
                        <div class="timeline-images">
                            ${claim.images.map(img => `<img src="${img}" alt="ภาพตอนรับเครื่อง" onclick="window.open('${img}', '_blank')">`).join('')}
                        </div>
                    ` : ''}
                </div>
            </div>
        `;

        // Subsequent updates from the updates array
        if (claim.updates && claim.updates.length > 0) {
            claim.updates.forEach(u => {
                const dateStr = new Date(u.date).toLocaleDateString('th-TH', { year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit' });
                html += `
                    <div class="timeline-item">
                        <div class="timeline-marker">
                            <div class="timeline-circle">${u.step}</div>
                            <div class="timeline-line"></div>
                        </div>
                        <div class="timeline-content">
                            <div class="timeline-header">
                                <h4>${u.title || '-'}</h4>
                                <span class="timeline-date">${dateStr}</span>
                            </div>
                            <p class="timeline-cost">ค่าใช้จ่าย: <strong>${(u.cost || 0).toLocaleString()} บาท</strong></p>
                            ${(u.centerName || u.centerLocation || u.centerPhone || u.technicianName || u.technicianPhone) ? `
                                <div style="margin-top: 0.45rem; font-size: 0.85rem; color: #334155;">
                                    ${u.centerName ? `<div><strong>ศูนย์:</strong> ${u.centerName}</div>` : ''}
                                    ${u.centerLocation ? `<div><strong>ที่ตั้งศูนย์:</strong> ${u.centerLocation}</div>` : ''}
                                    ${u.centerPhone ? `<div><strong>เบอร์ศูนย์:</strong> ${u.centerPhone}</div>` : ''}
                                    ${u.technicianName ? `<div><strong>ช่าง:</strong> ${u.technicianName}</div>` : ''}
                                    ${u.technicianPhone ? `<div><strong>เบอร์ช่าง:</strong> ${u.technicianPhone}</div>` : ''}
                                </div>
                            ` : ''}
                            ${u.images && u.images.length > 0 ? `
                                <div class="timeline-images">
                                    ${u.images.map(img => `<img src="${img}" alt="ภาพประกอบ" onclick="window.open('${img}', '_blank')">`).join('')}
                                </div>
                            ` : ''}
                            ${u.evidenceImages && u.evidenceImages.length > 0 ? `
                                <div style="margin-top: 0.6rem; font-size: 0.85rem; color: #b45309; font-weight: 700;">หลักฐานค่าใช้จ่าย</div>
                                <div class="timeline-images">
                                    ${u.evidenceImages.map(img => `<img src="${img}" alt="หลักฐานค่าใช้จ่าย" onclick="window.open('${img}', '_blank')" style="border: 2px solid #f59e0b;">`).join('')}
                                </div>
                            ` : ''}
                        </div>
                    </div>
                `;
            });
        }

        timeline.innerHTML = html;
    }

    function isCostEnabled() {
        const costSection = document.getElementById('updateCostSection');
        if (!costSection) return false;
        return costSection.style.display !== 'none';
    }

    function getUpdateCostNumber() {
        const costEl = document.getElementById('updateCost');
        const raw = costEl ? costEl.value : '0';
        const n = Number(raw);
        if (!Number.isFinite(n) || n < 0) return 0;
        return n;
    }

    function resetCostUI() {
        const costSection = document.getElementById('updateCostSection');
        const toggleCostBtn = document.getElementById('toggleCostBtn');
        const costEl = document.getElementById('updateCost');
        const evidenceGroup = document.getElementById('updateEvidenceGroup');
        const evidenceInput = document.getElementById('updateEvidenceImages');
        const evidencePreview = document.getElementById('updateEvidencePreview');
        if (costEl) costEl.value = '0';
        if (costSection) costSection.style.display = 'none';
        if (toggleCostBtn) toggleCostBtn.style.display = 'block';
        if (evidenceGroup) evidenceGroup.style.display = 'none';
        if (evidenceInput) evidenceInput.value = '';
        if (evidencePreview) evidencePreview.innerHTML = '';
    }

    const toggleCostBtn = document.getElementById('toggleCostBtn');
    if (toggleCostBtn) {
        toggleCostBtn.addEventListener('click', () => {
            const costSection = document.getElementById('updateCostSection');
            const costEl = document.getElementById('updateCost');
            const evidenceGroup = document.getElementById('updateEvidenceGroup');
            if (costSection) costSection.style.display = 'block';
            toggleCostBtn.style.display = 'none';
            if (evidenceGroup) evidenceGroup.style.display = 'block';
            if (costEl) {
                costEl.value = costEl.value || '0';
                costEl.focus();
            }
        });
    }

    const removeCostBtn = document.getElementById('removeCostBtn');
    if (removeCostBtn) {
        removeCostBtn.addEventListener('click', () => {
            resetCostUI();
        });
    }

    // Image preview for update images
    const updateImagesInput = document.getElementById('updateImages');
    if (updateImagesInput) {
        updateImagesInput.addEventListener('change', function () {
            const preview = document.getElementById('updateImagePreview');
            if (!preview) return;
            preview.innerHTML = '';
            Array.from(this.files).forEach(file => {
                const reader = new FileReader();
                reader.onload = (e) => {
                    preview.innerHTML += `<img src="${e.target.result}" style="width: 80px; height: 80px; object-fit: cover; border-radius: 8px; border: 2px solid #93c5fd;">`;
                };
                reader.readAsDataURL(file);
            });
        });
    }

    // Image preview for complete images
    const completeImagesInput = document.getElementById('completeImages');
    if (completeImagesInput) {
        completeImagesInput.addEventListener('change', function () {
            const preview = document.getElementById('completeImagePreview');
            if (!preview) return;
            preview.innerHTML = '';
            Array.from(this.files).forEach(file => {
                const reader = new FileReader();
                reader.onload = (e) => {
                    preview.innerHTML += `<img src="${e.target.result}" style="width: 80px; height: 80px; object-fit: cover; border-radius: 8px; border: 2px solid #86efac;">`;
                };
                reader.readAsDataURL(file);
            });
        });
    }

    // Submit status update
    const submitUpdateBtn = document.getElementById('submitUpdateBtn');
    if (submitUpdateBtn) {
        submitUpdateBtn.addEventListener('click', async () => {
            const claimId = document.getElementById('statusUpdateClaimId').value;
            const title = document.getElementById('updateTitle').value.trim();
            const costEnabled = isCostEnabled();
            const costNumber = costEnabled ? getUpdateCostNumber() : 0;
            const cost = String(costNumber);
            const imagesInput = document.getElementById('updateImages');
            const evidenceInput = document.getElementById('updateEvidenceImages');

            if (!title) {
                showAlert('warning', 'กรุณาระบุรายละเอียดการอัปเดต');
                return;
            }

            if (costNumber > 0 && (!evidenceInput || evidenceInput.files.length === 0)) {
                showAlert('warning', 'หากมีค่าใช้จ่าย กรุณาแนบรูปหลักฐานอย่างน้อย 1 รูป');
                return;
            }

            const formData = new FormData();
            formData.append('title', title);
            formData.append('cost', cost);
            const centerName = (document.getElementById('updateCenterName')?.value || '').trim();
            const centerLocation = (document.getElementById('updateCenterLocation')?.value || '').trim();
            const centerPhone = (document.getElementById('updateCenterPhone')?.value || '').trim();
            const technicianName = (document.getElementById('updateTechnicianName')?.value || '').trim();
            const technicianPhone = (document.getElementById('updateTechnicianPhone')?.value || '').trim();

            formData.append('centerName', centerName);
            formData.append('centerLocation', centerLocation);
            formData.append('centerPhone', centerPhone);
            formData.append('technicianName', technicianName);
            formData.append('technicianPhone', technicianPhone);
            if (imagesInput && imagesInput.files.length > 0) {
                Array.from(imagesInput.files).forEach(file => {
                    formData.append('images', file);
                });
            }

            if (evidenceInput && evidenceInput.files.length > 0) {
                Array.from(evidenceInput.files).forEach(file => {
                    formData.append('evidenceImages', file);
                });
            }

            showLoader('กำลังบันทึกการอัปเดต...');
            try {
                const res = await fetch(`/api/claims/${claimId}/updates`, {
                    method: 'POST',
                    body: formData
                });
                const data = await res.json();
                if (data.success) {
                    showToast('success', 'บันทึกการอัปเดตสำเร็จ');
                    // Re-open the modal to refresh data
                    await openStatusUpdateModal(claimId);
                } else {
                    showAlert('error', data.message || 'ไม่สามารถบันทึกการอัปเดตได้');
                }
            } catch (err) {
                console.error('Submit update error:', err);
                showAlert('error', 'เกิดข้อผิดพลาดในการบันทึก');
            } finally {
                hideLoader();
            }
        });
    }

    // Complete claim (ลูกค้ามารับเครื่องแล้ว)
    const completeClaimBtn = document.getElementById('completeClaimBtn');
    if (completeClaimBtn) {
        completeClaimBtn.addEventListener('click', async () => {
            const claimId = document.getElementById('statusUpdateClaimId').value;
            const confirmed = await showConfirm(
                'ยืนยันการปิดงาน',
                'คุณแน่ใจหรือไม่ว่าลูกค้ามารับเครื่องแล้ว? สถานะจะเปลี่ยนเป็น "รับเครื่องแล้ว" และไม่สามารถเปลี่ยนกลับได้',
                '✅ ยืนยันปิดงาน',
                'ยกเลิก'
            );
            if (!confirmed) return;

            const formData = new FormData();
            const imagesInput = document.getElementById('completeImages');
            if (imagesInput && imagesInput.files.length > 0) {
                Array.from(imagesInput.files).forEach(file => {
                    formData.append('images', file);
                });
            }

            showLoader('กำลังปิดงาน...');
            try {
                const res = await fetch(`/api/claims/${claimId}/complete`, {
                    method: 'POST',
                    body: formData
                });
                const data = await res.json();
                if (data.success) {
                    showAlert('success', 'ปิดงานสำเร็จ! สถานะเปลี่ยนเป็น "รับเครื่องแล้ว"');
                    document.getElementById('statusUpdateModal').style.display = 'none';
                    fetchPendingClaims(); // Refresh list
                } else {
                    showAlert('error', data.message || 'ไม่สามารถปิดงานได้');
                }
            } catch (err) {
                console.error('Complete claim error:', err);
                showAlert('error', 'เกิดข้อผิดพลาดในการปิดงาน');
            } finally {
                hideLoader();
            }
        });
    }

    // Close status update modal
    const closeStatusUpdateModalBtn = document.getElementById('closeStatusUpdateModal');
    if (closeStatusUpdateModalBtn) {
        closeStatusUpdateModalBtn.addEventListener('click', () => {
            document.getElementById('statusUpdateModal').style.display = 'none';
        });
    }

    // Close claim history modal
    const closeClaimHistoryModalBtn = document.getElementById('closeClaimHistoryModal');
    if (closeClaimHistoryModalBtn) {
        closeClaimHistoryModalBtn.addEventListener('click', () => {
            document.getElementById('claimHistoryModal').style.display = 'none';
        });
    }

    // ═══════════════════════════════════════════════════════════════════
    // CLAIM HISTORY LOGIC
    // ═══════════════════════════════════════════════════════════════════

    window.openClaimHistoryModal = async function (warrantyId) {
        showLoader('กำลังโหลดประวัติการเคลม...');
        try {
            const res = await fetch(`/api/claims/history/${warrantyId}`);
            const claims = await res.json();
            renderClaimHistory(claims);
            document.getElementById('claimHistoryModal').style.display = 'flex';
        } catch (err) {
            console.error('Fetch claim history error:', err);
            showAlert('error', 'ไม่สามารถโหลดประวัติการเคลมได้');
        } finally {
            hideLoader();
        }
    };

    function renderClaimHistory(claims) {
        const listContainer = document.getElementById('claimHistoryList');
        const emptyState = document.getElementById('claimHistoryEmpty');

        if (!claims || claims.length === 0) {
            listContainer.style.display = 'none';
            emptyState.style.display = 'block';
            return;
        }

        listContainer.style.display = 'flex';
        emptyState.style.display = 'none';

        listContainer.innerHTML = claims.map(c => {
            const totalCost = c.totalCost || 0;
            const statusColor = c.status === 'รอเคลม' ? '#f59e0b' : '#10b981';
            const statusText = c.status === 'รอเคลม' ? 'รอเคลม / กำลังซ่อม' : 'รับเครื่องแล้ว (เสร็จสิ้น)';
            const claimMongoId = c && c._id ? String(c._id) : '';

            // Helper to generate image HTML
            const generateImagesHtml = (images) => {
                if (!images || images.length === 0) return '';
                return `
                    <div class="history-images">
                        ${images.map(img => `<img src="${img}" onclick="window.open('${img}', '_blank')" alt="รูปประกอบ">`).join('')}
                    </div>
                `;
            };

            // Generate timeline HTML for history
            let timelineHtml = `
                <div class="history-timeline-item">
                    <div class="history-dot"></div>
                    <div class="history-content">
                        <div style="flex: 1; display: flex; flex-wrap: wrap; gap: 0.5rem; align-items: baseline;">
                            <strong>รับเครื่อง</strong>
                            <span class="history-date">${new Date(c.claimDate).toLocaleString('th-TH')}</span>
                        </div>
                        ${generateImagesHtml(c.images)}
                    </div>
                </div>
            `;

            if (c.updates && c.updates.length > 0) {
                timelineHtml += c.updates.map(u => `
                    <div class="history-timeline-item">
                        <div class="history-dot"></div>
                        <div class="history-content">
                            <div style="display: flex; justify-content: space-between; width: 100%; align-items: baseline; flex-wrap: wrap; gap: 0.5rem;">
                                <strong>${u.title}</strong>
                                <span class="history-date">${new Date(u.date).toLocaleString('th-TH')}</span>
                                ${u.cost > 0 ? `<div class="history-cost">+${u.cost.toLocaleString()} บาท</div>` : ''}
                            </div>
                            ${generateImagesHtml(u.images)}
                        </div>
                    </div>
                `).join('');
            }

            return `
                <div class="history-card">
                    <div class="history-header">
                        <div>
                            <span style="font-weight: 700; color: var(--primary);">#${c.claimId}</span>
                            <span style="font-size: 0.9rem; color: #64748b; margin-left: 8px;">${new Date(c.claimDate).toLocaleDateString('th-TH')}</span>
                        </div>
                        <span class="status-badge" style="background-color: ${statusColor}; color: white;">${statusText}</span>
                    </div>
                    <div class="history-body">
                            <div style="display:flex; justify-content:flex-end; margin-bottom: 0.75rem;">
                                <button type="button" onclick="printClaimById('${claimMongoId}')" ${claimMongoId ? '' : 'disabled'}
                                    style="background: linear-gradient(135deg, #3b82f6, #2563eb); color: white; border: none; padding: 6px 12px; border-radius: 8px; cursor: pointer; font-size: 0.85rem; font-weight: 600; white-space: nowrap;">
                                    🖨️ พิมพ์เอกสารรับเครื่อง
                                </button>
                            </div>
                            <strong>อุปกรณ์:</strong> ${c.deviceModel} | <strong>สี:</strong> ${c.color || '-'}
                        </div>
                        <div style="margin-bottom: 0.5rem; font-size: 0.9rem; color: #64748b;">
                            <strong>IMEI:</strong> ${c.imei || '-'} | <strong>Serial:</strong> ${c.serialNumber || '-'}
                        </div>
                        <div style="margin-bottom: 0.5rem;">
                            <strong>อาการ:</strong> ${c.symptoms}
                        </div>
                        <div style="margin-bottom: 0.5rem;">
                            <strong>การคืนเครื่อง:</strong> ${c.returnMethod === 'pickup' ? 'รับเองที่สาขา' : 'จัดส่งพัสดุ'}
                        </div>
                        
                        <div class="history-timeline">
                            ${timelineHtml}
                        </div>

                        <div class="history-footer">
                            <span>ค่าใช้จ่ายรวมทั้งสิ้น</span>
                            <span class="history-total">${totalCost.toLocaleString()} บาท</span>
                        </div>
                    </div>
                </div>
            `;
        }).join('');
    }

    // ═══════════════════════════════════════════════════════════════════
    // APPROVAL MENU LOGIC
    // ═══════════════════════════════════════════════════════════════════

    // --- State for Approval View ---
    let approvalCurrentFilter = 'pending';
    let approvalAllWarranties = [];
    let approvalAutoRefreshInterval = null;
    let approvalIsAutoRefreshPaused = false;
    let approvalIsSilentFetching = false;

    // --- Helper: Check if a SweetAlert modal is open ---
    function isModalOpen() {
        return Swal.isVisible();
    }

    // --- Helper: Extract sorted IDs ---
    function getApprovalIdSet(warranties) {
        return warranties.map(w => w._id).sort().join(',');
    }

    // --- Fetch Stats Counts (silent) ---
    async function fetchApprovalCounts() {
        try {
            const [pendingRes, approvedRes, rejectedRes] = await Promise.all([
                fetch('/api/warranties/pending?status=pending'),
                fetch('/api/warranties/pending?status=approved'),
                fetch('/api/warranties/pending?status=rejected')
            ]);
            const pending = await pendingRes.json();
            const approved = await approvedRes.json();
            const rejected = await rejectedRes.json();

            if (document.getElementById('badgePending')) document.getElementById('badgePending').textContent = pending.length;
            if (document.getElementById('badgeApproved')) document.getElementById('badgeApproved').textContent = approved.length;
            if (document.getElementById('badgeRejected')) document.getElementById('badgeRejected').textContent = rejected.length;
        } catch (err) {
            console.error('Error fetching approval counts:', err);
        }
    }

    // --- Full Fetch (with loader) ---
    async function fetchApprovalWarranties(status) {
        showLoader();
        try {
            const res = await fetch(`/api/warranties/pending?status=${status}`);
            approvalAllWarranties = await res.json();
            renderApprovalTable(approvalAllWarranties);
            fetchApprovalCounts();
        } catch (err) {
            console.error('Error fetching approval warranties:', err);
        } finally {
            hideLoader();
        }
    }

    // --- Silent Fetch (auto-refresh) ---
    async function silentFetchApprovalWarranties() {
        // Skip if already fetching, or if a modal is open, or paused
        if (approvalIsSilentFetching || isModalOpen() || approvalIsAutoRefreshPaused) return;

        approvalIsSilentFetching = true;
        try {
            const res = await fetch(`/api/warranties/pending?status=${approvalCurrentFilter}`);
            const freshData = await res.json();

            // Skip DOM update if modal opened while we were fetching
            if (isModalOpen()) return;

            const oldIds = getApprovalIdSet(approvalAllWarranties);
            const newIds = getApprovalIdSet(freshData);

            // Only update DOM if data has actually changed
            if (oldIds !== newIds) {
                // Find which IDs are new (not in previous set)
                const previousIdSet = new Set(approvalAllWarranties.map(w => w._id));
                const newItemIds = freshData.filter(w => !previousIdSet.has(w._id)).map(w => w._id);

                approvalAllWarranties = freshData;
                renderApprovalTable(approvalAllWarranties, newItemIds);
            }

            // Always silently refresh counts
            fetchApprovalCounts();
        } catch (err) {
            console.error('Silent refresh error:', err);
        } finally {
            approvalIsSilentFetching = false;
        }
    }

    // --- Render Table ---
    function renderApprovalTable(records, highlightIds = []) {
        const body = document.getElementById('approvalBody');
        const empty = document.getElementById('approvalEmptyState');

        if (!body) return; // Guard clause

        if (records.length === 0) {
            body.innerHTML = '';
            if (empty) empty.style.display = 'block';
            return;
        }

        if (empty) empty.style.display = 'none';
        const highlightSet = new Set(highlightIds);

        body.innerHTML = records.map(w => {
            let st = { text: 'รออนุมัติ', class: 'pending' };
            if (w.approvalStatus === 'approved') {
                const isExpired = new Date(w.warrantyDates?.end) < new Date();
                st = isExpired ? { text: 'หมดอายุ', class: 'expired' } : { text: 'ปกติ', class: 'active' };
            } else if (w.approvalStatus === 'rejected') {
                st = { text: 'ไม่อนุมัติ', class: 'rejected' };
            }
            const rowClass = highlightSet.has(w._id) ? ' class="new-row-highlight"' : '';

            return `
                <tr${rowClass}>
                    <td data-label="เลขกรมธรรม์" style="font-weight: 600;">${w.policyNumber || '-'}</td>
                    <td data-label="ชื่อลูกค้า">${w.customer?.firstName || ''} ${w.customer?.lastName || ''}</td>
                    <td data-label="รุ่นอุปกรณ์">${w.device?.model || '-'}</td>
                    <td data-label="แพ็กเกจ">${w.package?.plan || '-'} (${w.package?.price?.toLocaleString() || 0} บาท)</td>
                    <td data-label="ร้านค้า">${w.shopName || '-'}</td>
                    <td data-label="สถานะ"><span class="status-badge ${st.class}">${st.text}</span></td>
                    <td data-label="ดำเนินการ">
                        <div class="approval-actions">
                            <button class="btn-view" onclick="viewApprovalDetails('${w._id}')">👁 ดูรายละเอียด</button>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    }

    function escapeHtml(s) {
        return String(s).replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function formatThaiDateTime(d) {
        if (!d) return '-';
        const dd = new Date(d);
        if (Number.isNaN(dd.getTime())) return '-';
        return dd.toLocaleString('th-TH');
    }

    async function openApprovalLogAllModal() {
        try {
            if (typeof SwalTheme === 'undefined') {
                alert('SwalTheme library is missing!');
                return;
            }

            showLoader('กำลังโหลด Log...');
            let all = [];
            try {
                const res = await fetch('/api/warranties/pending?status=all');
                all = await res.json();
            } finally {
                hideLoader();
            }

            const rows = (Array.isArray(all) ? all : [])
                .slice()
                .sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0))
                .map(w => {
                    const createdAt = formatThaiDateTime(w.createdAt);
                    const createdBy = w.staffName || '-';
                    const policy = w.policyNumber || '-';
                    const customerName = `${w.customer?.firstName || ''} ${w.customer?.lastName || ''}`.trim() || '-';

                    let action = 'รออนุมัติ';
                    let actionTime = '-';
                    let actor = '-';
                    if (w.approvalStatus === 'approved') {
                        action = 'อนุมัติ';
                        actionTime = formatThaiDateTime(w.approvalDate);
                        actor = w.approver || '-';
                    } else if (w.approvalStatus === 'rejected') {
                        action = 'ไม่อนุมัติ';
                        actionTime = formatThaiDateTime(w.rejectDate);
                        actor = w.rejectBy || '-';
                    }

                    return `
                        <tr>
                            <td style="white-space:nowrap; font-weight:600;">${escapeHtml(policy)}</td>
                            <td>${escapeHtml(customerName)}</td>
                            <td style="white-space:nowrap;">${escapeHtml(createdAt)}</td>
                            <td>${escapeHtml(createdBy)}</td>
                            <td style="white-space:nowrap; font-weight:700;">${escapeHtml(action)}</td>
                            <td style="white-space:nowrap;">${escapeHtml(actionTime)}</td>
                            <td>${escapeHtml(actor)}</td>
                        </tr>
                    `;
                })
                .join('');

            SwalTheme.fire({
                title: '🕒 Log การดำเนินงานสัญญาทั้งหมด',
                icon: 'info',
                width: 980,
                confirmButtonText: 'ปิด',
                html: `
                    <div style="text-align:left; font-size:0.9rem; margin-bottom:8px; color:#475569;">
                        แสดงย้อนหลัง: วันที่สร้างรายการ / การอนุมัติ-ไม่อนุมัติ / ผู้ดำเนินการ
                    </div>
                    <div style="max-height: 65vh; overflow:auto; border: 1px solid #e2e8f0; border-radius: 10px;">
                        <table style="width:100%; border-collapse: collapse;">
                            <thead style="position: sticky; top: 0; background: #f8fafc;">
                                <tr>
                                    <th style="text-align:left; padding:10px; border-bottom:1px solid #e2e8f0;">เลขกรมธรรม์</th>
                                    <th style="text-align:left; padding:10px; border-bottom:1px solid #e2e8f0;">ชื่อลูกค้า</th>
                                    <th style="text-align:left; padding:10px; border-bottom:1px solid #e2e8f0;">สร้างเมื่อ</th>
                                    <th style="text-align:left; padding:10px; border-bottom:1px solid #e2e8f0;">ผู้บันทึก</th>
                                    <th style="text-align:left; padding:10px; border-bottom:1px solid #e2e8f0;">ผล</th>
                                    <th style="text-align:left; padding:10px; border-bottom:1px solid #e2e8f0;">เวลาที่ทำ</th>
                                    <th style="text-align:left; padding:10px; border-bottom:1px solid #e2e8f0;">ผู้อนุมัติ/ผู้ไม่อนุมัติ</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${rows || '<tr><td colspan="7" style="padding: 12px; color:#64748b;">ไม่พบข้อมูล</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                `
            });
        } catch (err) {
            console.error('openApprovalLogAllModal error:', err);
            alert('เกิดข้อผิดพลาดในการเปิด Log');
        }
    }

    const btnApprovalViewLog = document.getElementById('btnApprovalViewLog');
    if (btnApprovalViewLog) {
        btnApprovalViewLog.addEventListener('click', (e) => {
            e.preventDefault();
            openApprovalLogAllModal();
        });
    }

    // --- Actions ---
    // Make functions globally available for inline onclick
    window.viewApprovalDetails = function (id) {
        console.log('viewApprovalDetails called with id:', id);
        try {
            const w = approvalAllWarranties.find(r => r._id === id);
            if (!w) {
                console.error('Warranty not found for id:', id);
                alert('ไม่พบข้อมูลสัญญา (Warranty not found)');
                return;
            }

            let st = { text: 'รออนุมัติ', class: 'pending' };
            if (w.approvalStatus === 'approved') {
                const isExpired = new Date(w.warrantyDates?.end) < new Date();
                st = isExpired ? { text: 'หมดอายุ', class: 'expired' } : { text: 'ปกติ', class: 'active' };
            } else if (w.approvalStatus === 'rejected') {
                st = { text: 'ไม่อนุมัติ', class: 'rejected' };
            }

            if (typeof SwalTheme === 'undefined') {
                alert('SwalTheme library is missing!');
                return;
            }

            SwalTheme.fire({
                title: `📋 สัญญา #${w.policyNumber}`,
                html: `
                    <div class="approval-detail-grid">
                        <div class="approval-detail-item">
                            <label>รหัสลูกค้า</label>
                            <span>${w.customer?.id || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>เลขบัตรประชาชน</label>
                            <span>${w.customer?.citizenId || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>ชื่อลูกค้า</label>
                            <span>${w.customer?.firstName || ''} ${w.customer?.lastName || ''}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>อายุ</label>
                            <span>${w.customer?.age || ''}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>เบอร์โทรศัพท์</label>
                            <span>${w.customer?.phone || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>ประเภทอุปกรณ์</label>
                            <span>${w.device?.type || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>รุ่น (Model)</label>
                            <span>${w.device?.model || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>สี</label>
                            <span>${w.device?.color || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>ความจุ</label>
                            <span>${w.device?.capacity || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>Serial Number</label>
                            <span>${w.device?.serial || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>IMEI</label>
                            <span>${w.device?.imei || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>มูลค่าเครื่อง</label>
                            <span>${w.device?.deviceValue?.toLocaleString() || 0} บาท</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>แพ็กเกจ</label>
                            <span>${w.package?.plan || '-'} (${w.package?.price?.toLocaleString() || 0} บาท)</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>วิธีชำระ</label>
                            <span>${w.payment?.method || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>ร้านค้า</label>
                            <span>${w.shopName || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>ผู้บันทึก</label>
                            <span>${w.staffName || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>สถานะ</label>
                            <span class="status-badge ${st.class}">${st.text}</span>
                        </div>
                        ${w.approvalStatus === 'approved' ? `
                        <div class="approval-detail-item">
                            <label>ผู้อนุมัติ</label>
                            <span>${w.approver || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>วันที่อนุมัติ</label>
                            <span>${w.approvalDate ? new Date(w.approvalDate).toLocaleString('th-TH') : '-'}</span>
                        </div>
                        ` : ''}
                        ${w.approvalStatus === 'rejected' ? `
                        <div class="approval-detail-item">
                            <label>ผู้ไม่อนุมัติ</label>
                            <span>${w.rejectBy || '-'}</span>
                        </div>
                        <div class="approval-detail-item">
                            <label>วันที่ทำรายการ</label>
                            <span>${w.rejectDate ? new Date(w.rejectDate).toLocaleString('th-TH') : '-'}</span>
                        </div>
                        <div class="approval-detail-item" style="grid-column: span 2;">
                            <label>เหตุผลที่ไม่อนุมัติ</label>
                            <span style="color: #ef4444;">${w.rejectReason || '-'}</span>
                        </div>
                        ` : ''}
                    </div>

                    <div style="display:flex; justify-content:center; gap:10px; margin-top: 14px;">
                        ${w.approvalStatus === 'pending' ? `
                        <button type="button" id="approvalApproveBtn" class="submit-btn"
                            style="width:auto; padding: 10px 14px; background: linear-gradient(135deg, #10b981, #059669);">
                            ✅ อนุมัติ
                        </button>
                        <button type="button" id="approvalRejectBtn" class="submit-btn"
                            style="width:auto; padding: 10px 14px; background: linear-gradient(135deg, #ef4444, #dc2626);">
                            ❌ ไม่อนุมัติ
                        </button>
                        ` : ''}
                        <button type="button" id="approvalPrintContractBtn" class="submit-btn"
                            style="width:auto; padding: 10px 14px; background: linear-gradient(135deg, #3b82f6, #2563eb);">
                            🖨️ พิมพ์เอกสารสัญญา
                        </button>
                    </div>
                `,
                width: 600,
                showConfirmButton: true,
                confirmButtonText: 'ปิด',
                didOpen: () => {
                    const btn = document.getElementById('approvalPrintContractBtn');
                    if (btn) {
                        btn.addEventListener('click', () => {
                            window.open(`document.html?id=${encodeURIComponent(w._id)}`, '_blank');
                        });
                    }

                    const approveBtn = document.getElementById('approvalApproveBtn');
                    if (approveBtn) {
                        approveBtn.addEventListener('click', () => {
                            Swal.close();
                            approveWarranty(w._id);
                        });
                    }

                    const rejectBtn = document.getElementById('approvalRejectBtn');
                    if (rejectBtn) {
                        rejectBtn.addEventListener('click', () => {
                            Swal.close();
                            rejectWarranty(w._id);
                        });
                    }
                },
            });
        } catch (err) {
            console.error('viewApprovalDetails error:', err);
            alert('Error accessing details: ' + err.message);
        }
    };

    window.approveWarranty = async function (id) {
        console.log('approveWarranty called for id:', id);
        try {
            if (typeof SwalTheme === 'undefined') {
                alert('SwalTheme library is missing!');
                return;
            }

            const result = await SwalTheme.fire({
                title: 'ยืนยันการอนุมัติ?',
                text: 'คุณต้องการอนุมัติสัญญานี้หรือไม่?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: '✅ อนุมัติ',
                cancelButtonText: 'ยกเลิก',
            });

            if (!result.isConfirmed) return;

            // Use global currentUser
            const approver = currentUser ? currentUser.staffName : 'Unknown';

            showLoader('กำลังอนุมัติ...');
            try {
                const res = await fetch(`/api/warranties/${id}/approve`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ approver })
                });
                const data = await res.json();
                if (res.ok) {
                    SwalTheme.fire({
                        icon: 'success',
                        title: 'อนุมัติสำเร็จ!',
                        text: 'สัญญาได้รับการอนุมัติแล้ว',
                        timer: 2000,
                        showConfirmButton: false
                    });
                    fetchApprovalWarranties(approvalCurrentFilter);
                } else {
                    SwalTheme.fire('เกิดข้อผิดพลาด', data.message || 'ไม่สามารถอนุมัติได้', 'error');
                }
            } catch (err) {
                console.error('Approve fetch error:', err);
                SwalTheme.fire('เกิดข้อผิดพลาด', 'ไม่สามารถเชื่อมต่อเซิร์ฟเวอร์ได้: ' + err.message, 'error');
            } finally {
                hideLoader();
            }
        } catch (err) {
            console.error('approveWarranty outer error:', err);
            alert('Error in approveWarranty: ' + err.message);
        }
    };

    window.rejectWarranty = async function (id) {
        console.log('rejectWarranty called for id:', id);
        try {
            if (typeof SwalTheme === 'undefined') {
                alert('SwalTheme library is missing!');
                return;
            }

            const result = await SwalTheme.fire({
                title: 'ยืนยันไม่อนุมัติ?',
                text: 'กรุณาระบุเหตุผลที่ไม่อนุมัติสัญญา',
                input: 'text',
                inputPlaceholder: 'ระบุเหตุผล...',
                icon: 'warning',
                showCancelButton: true,
                confirmButtonText: '❌ ไม่อนุมัติ',
                cancelButtonText: 'ยกเลิก',
                confirmButtonColor: '#ef4444',
                inputValidator: (value) => {
                    if (!value) {
                        return 'กรุณาระบุเหตุผล!';
                    }
                }
            });

            if (!result.isConfirmed) return;

            const reason = result.value;
            // Use global currentUser
            const rejectBy = currentUser ? currentUser.staffName : 'Unknown';

            showLoader('กำลังดำเนินการ...');
            try {
                const res = await fetch(`/api/warranties/${id}/reject`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ reason, rejectBy })
                });
                const data = await res.json();
                if (res.ok) {
                    SwalTheme.fire({
                        icon: 'info',
                        title: 'ไม่อนุมัติสัญญา',
                        text: 'สัญญาถูกปฏิเสธเรียบร้อยแล้ว',
                        timer: 2000,
                        showConfirmButton: false
                    });
                    fetchApprovalWarranties(approvalCurrentFilter);
                } else {
                    SwalTheme.fire('เกิดข้อผิดพลาด', data.message || 'ไม่สามารถดำเนินการได้', 'error');
                }
            } catch (err) {
                console.error('Reject fetch error:', err);
                SwalTheme.fire('เกิดข้อผิดพลาด', 'ไม่สามารถเชื่อมต่อเซิร์ฟเวอร์ได้: ' + err.message, 'error');
            } finally {
                hideLoader();
            }
        } catch (err) {
            console.error('rejectWarranty outer error:', err);
            alert('Error in rejectWarranty: ' + err.message);
        }
    };

    // --- Filter Tab Listeners ---
    document.querySelectorAll('.approval-tab-btn').forEach(btn => {
        if (!btn) return;
        btn.addEventListener('click', () => {
            document.querySelectorAll('.approval-tab-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            approvalCurrentFilter = btn.dataset.status;
            fetchApprovalWarranties(approvalCurrentFilter);
        });
    });

    // ====== AUTO-REFRESH LOGIC ======
    const approvalLiveIndicator = document.getElementById('approvalLiveIndicator');
    const approvalLiveText = document.getElementById('approvalLiveText');
    const btnApprovalToggleRefresh = document.getElementById('btnApprovalToggleRefresh');

    function startApprovalAutoRefresh() {
        stopApprovalAutoRefresh(); // clear any existing interval
        approvalIsAutoRefreshPaused = false;

        // Initial silent fetch
        silentFetchApprovalWarranties();

        approvalAutoRefreshInterval = setInterval(silentFetchApprovalWarranties, 5000); // 5 seconds

        if (approvalLiveIndicator) approvalLiveIndicator.classList.remove('paused');
        if (approvalLiveText) approvalLiveText.textContent = 'Live Update';
        if (btnApprovalToggleRefresh) {
            btnApprovalToggleRefresh.classList.remove('active');
            btnApprovalToggleRefresh.innerHTML = '⏸ หยุดชั่วคราว';
        }
    }

    function stopApprovalAutoRefresh() {
        if (approvalAutoRefreshInterval) {
            clearInterval(approvalAutoRefreshInterval);
            approvalAutoRefreshInterval = null;
        }

        if (approvalLiveIndicator) approvalLiveIndicator.classList.add('paused');
        if (approvalLiveText) approvalLiveText.textContent = 'Paused';
    }

    if (btnApprovalToggleRefresh) {
        btnApprovalToggleRefresh.addEventListener('click', () => {
            if (approvalIsAutoRefreshPaused) {
                // Resume
                startApprovalAutoRefresh();
            } else {
                // Pause
                approvalIsAutoRefreshPaused = true;
                stopApprovalAutoRefresh();

                if (btnApprovalToggleRefresh) {
                    btnApprovalToggleRefresh.classList.add('active');
                    btnApprovalToggleRefresh.innerHTML = '▶ เริ่มใหม่';
                }
            }
        });
    }

    // --- INITIALIZATION ---
    if (currentUser) {
        updateStaffInfo();
        showView('dashboard');

        updateStatusTrackingBadge({ showToastIfOverdue: true });
    } else {
        showView('login');
    }
    console.log('SmileCare script initialized successfully.');
});
