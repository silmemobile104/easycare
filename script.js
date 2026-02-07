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
    let isEditMode = false;
    let currentEditData = null;
    let allRecords = []; // Global state for filtering
    const PACKAGE_PRICES = { 'Plan A': 799, 'Plan B': 1499, 'Plan C': 1699, 'Plan D': 2399, 'Plan a': 599, 'Plan b': 999, 'Plan c': 1299 };
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
        Object.keys(views).forEach(key => {
            if (views[key]) views[key].style.display = 'none';
        });

        // Sub-view logic for dashboard/members
        const shopsMain = document.getElementById('shopsMain');
        const dashNavLink = document.getElementById('dashNavLink');
        const membersNavLink = document.getElementById('membersNavLink');
        const shopsNavLink = document.getElementById('shopsNavLink');

        if (viewName === 'dashboard' || viewName === 'members' || viewName === 'shops') {
            views.dashboard.style.display = 'block';

            // Hide all sub-views first
            dashMain.style.display = 'none';
            membersMain.style.display = 'none';
            shopsMain.style.display = 'none';

            // Remove active class from all nav links
            if (dashNavLink) dashNavLink.classList.remove('active');
            if (membersNavLink) membersNavLink.classList.remove('active');
            if (shopsNavLink) shopsNavLink.classList.remove('active');

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
            }
        } else if (views[viewName]) {
            views[viewName].style.display = 'block';
            if (viewName === 'registration') initRegistrationForm();
        }
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
                if (data.success) {
                    currentUser = data.user;
                    localStorage.setItem('smilecare_staff_session', JSON.stringify(currentUser));
                    updateStaffInfo();
                    showView('dashboard');
                } else {
                    document.getElementById('loginError').style.display = 'block';
                }
            } catch (err) {
                console.error('Login error:', err);
            } finally {
                hideLoader();
            }
        });
    }

    // --- STAFF REGISTRATION LOGIC ---
    const staffRegisterForm = document.getElementById('staffRegisterForm');
    if (staffRegisterForm) {
        staffRegisterForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const staffName = document.getElementById('regStaffName').value;
            const username = document.getElementById('regUsername').value;
            const password = document.getElementById('regPassword').value;

            showLoader('กำลังลงทะเบียน...');
            try {
                const res = await fetch('/api/register', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ staffName, username, password })
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
        const initialElem = document.getElementById('staffInitial');
        if (nameElem) nameElem.textContent = currentUser.staffName;
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
            if (statusFilter === 'active' && isExpired) return false;
            if (statusFilter === 'expired' && !isExpired) return false;

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

            const statusBadge = `<span class="status-badge ${isExpired ? 'status-expired' : 'status-active'}">${isExpired ? 'หมดอายุ' : 'ปกติ'}</span>`;

            return `
                <tr>
                    <td data-label="เลขกรมธรรม์" style="font-weight: 600; color: var(--primary);">${r.policyNumber || '-'}</td>
                    <td data-label="รหัสสมาชิก">${r.memberId || '-'}</td>
                    <td data-label="ชื่อลูกค้า">${r.customer.firstName} ${r.customer.lastName}</td>
                    <td data-label="เบอร์โทรศัพท์">${r.customer.phone}</td>
                    <td data-label="รุ่นอุปกรณ์">${r.device.model}</td>
                    <td data-label="แพ็กเกจ"><span style="color: var(--primary); font-weight: 500;">${r.package.plan}</span></td>
                    <td data-label="ประกันคงเหลือ">${timeRemainingText}</td>
                    <td data-label="ร้านค้า">${r.shopName || '-'}</td>
                    <td data-label="ผู้บันทึก">${r.staffName}</td>
                    <td data-label="สถานะ">${statusBadge}</td>
                    <td data-label="การชำระเงิน">${paymentStatus}</td>
                    <td data-label="จัดการ">
                    <div style="display: flex; gap: 0.5rem; justify-content: flex-end;">
                        <button class="edit-btn" data-id="${r._id}" title="แก้ไขข้อมูล">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path></svg>
                        </button>
                        <button class="print-btn" data-id="${r._id}" title="พิมพ์เอกสาร">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 6 2 18 2 18 9"></polyline><path d="M6 18H4a2 2 0 0 1-2-2v-5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-2"></path><rect x="6" y="14" width="12" height="8"></rect></svg>
                        </button>
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

            // Set Protection Type based on Plan name case (Plan A/B/C/D vs Plan a/b/c)
            const isScreenOnly = ['Plan a', 'Plan b', 'Plan c'].includes(data.package.plan);
            document.getElementById('protectionType').value = isScreenOnly ? 'Screen' : 'Full';
            updatePackageOptions();
            document.getElementById('package').value = data.package.plan;

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
        } else {
            options = `
                <option value="" disabled>เลือกแผนประกัน</option>
                <option value="Plan a">Plan a</option>
                <option value="Plan b">Plan b</option>
                <option value="Plan c">Plan c</option>
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

            shopSelect.innerHTML = '<option value="" disabled selected>เลือกสรรร้านค้า</option>';
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

    window.receivePayment = function (id, installmentNo, amount) {
        startCheckout(currentEditData, null, {
            amountDue: amount,
            installmentNo,
            description: `ชำระค่าเบี้ยประกัน งวดที่ ${installmentNo}`
        });
    };

    window.receiveAllRemainingPayments = function (id, totalAmount) {
        startCheckout(currentEditData, null, {
            amountDue: totalAmount,
            payAllRemaining: true,
            description: 'ชำระเงินงวดที่เหลือทั้งหมด'
        });
    };

    // Listeners for form updates
    document.getElementById('productType').addEventListener('change', toggleIMEIField);
    document.getElementById('dobDay').addEventListener('input', updateAge);
    document.getElementById('dobMonth').addEventListener('input', updateAge);
    document.getElementById('dobYear').addEventListener('input', updateAge);
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
                            <span class="search-result-sub">${m.phone}</span>
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
                } else {
                    modalTitle.textContent = 'ลงทะเบียนสำเร็จ!';
                    modalText.textContent = 'ข้อมูลการลงทะเบียนประกันภัยของคุณถูกบันทึกเรียบร้อยแล้ว';

                    const markPaid = document.getElementById('initialPaidCheck')?.checked;
                    if (markPaid) {
                        modalText.textContent = 'บันทึกข้อมูลและสถานะการชำระเงินเรียบร้อยแล้ว';
                        hideCheckout();
                        document.getElementById('checkoutStep3').style.display = 'block';
                        setupPrintButton(data, payload);
                    } else {
                        startCheckout(data, payload);
                    }
                }
                document.getElementById('successModal').style.display = 'flex';
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
                <td data-label="ที่อยู่ตามบัตร">${m.idCardAddress || '-'}</td>
                <td data-label="จัดการ">
                    <div style="display: flex; gap: 0.5rem; justify-content: flex-end;">
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
            document.getElementById('smartCardPhotoContainer').style.display = 'none';
            document.getElementById('smartCardPhoto').src = '';

            // Re-enable fields if they were disabled (though we keep them readonly for SC integration)
            document.getElementById('memberFirstName').readOnly = true;
            document.getElementById('memberLastName').readOnly = true;
            document.getElementById('memberBirthdate').readOnly = true;
            document.getElementById('memberIdCardAddress').readOnly = true;

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
            const payload = {
                firstName: document.getElementById('memberFirstName').value,
                lastName: document.getElementById('memberLastName').value,
                phone: document.getElementById('memberPhone').value,
                birthdate: document.getElementById('memberBirthdate').value,
                idCardAddress: document.getElementById('memberIdCardAddress').value,
                address: document.getElementById('memberIdCardAddress').value,
                shippingAddress: document.getElementById('memberShippingAddress').value,
                citizenId: document.getElementById('memberCitizenId').value,
                prefix: document.getElementById('memberPrefix').value,
                firstNameEn: document.getElementById('memberFirstNameEn').value,
                lastNameEn: document.getElementById('memberLastNameEn').value,
                gender: document.getElementById('memberGender').value,
                issueDate: document.getElementById('memberIssueDate').value,
                expiryDate: document.getElementById('memberExpiryDate').value,
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
                    <div style="display: flex; gap: 0.5rem; justify-content: flex-end;">
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

    // --- INITIALIZATION ---
    if (currentUser) {
        updateStaffInfo();
        showView('dashboard');
    } else {
        showView('login');
    }
    console.log('SmileCare script initialized successfully.');
});
