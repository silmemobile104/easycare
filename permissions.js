/**
 * EasyCare Menu Permissions Management System
 * Dynamic Role & Visibility Controller
 */

// 16 Standard Default Menus in EasyCare
const defaultMenuPermissionsList = [
    { id: 'nav-executive', name: 'แดชบอร์ด (ผู้บริหาร)', category: 'dashboard', roles: ['admin'], enabled: true, order: 1 },
    { id: 'nav-dashboard-sales', name: 'แดชบอร์ด (ฝ่ายขาย)', category: 'dashboard', roles: ['admin', 'sales'], enabled: true, order: 2 },
    { id: 'nav-dashboard-approver', name: 'แดชบอร์ด (ฝ่ายอนุมัติ)', category: 'dashboard', roles: ['admin', 'approver'], enabled: true, order: 3 },
    { id: 'nav-packages', name: 'แพ็กเกจ (รายการสัญญา)', category: 'operations', roles: ['admin', 'sales', 'approver', 'finance'], enabled: true, order: 4 },
    { id: 'nav-members', name: 'สมาชิก', category: 'operations', roles: ['admin', 'sales', 'approver', 'finance'], enabled: true, order: 5 },
    { id: 'nav-shops', name: 'ร้านค้า', category: 'operations', roles: ['admin'], enabled: true, order: 6 },
    { id: 'nav-claims', name: 'แจ้งเคลม', category: 'claims', roles: ['admin', 'sales'], enabled: true, order: 7 },
    { id: 'nav-tracking', name: 'ติดตามสถานะงานเคลม', category: 'claims', roles: ['admin', 'sales'], enabled: true, order: 8 },
    { id: 'nav-approval', name: 'อนุมัติสัญญา', category: 'claims', roles: ['admin', 'approver'], enabled: true, order: 9 },
    { id: 'nav-staff', name: 'จัดการพนักงาน', category: 'admin', roles: ['admin'], enabled: true, order: 10 },
    { id: 'nav-finance-companies', name: 'จัดการไฟแนนซ์', category: 'admin', roles: ['admin'], enabled: true, order: 11 },
    { id: 'nav-products', name: 'จัดการสินค้า', category: 'admin', roles: ['admin'], enabled: true, order: 12 },
    { id: 'nav-finance', name: 'การเงิน (Finance)', category: 'finance', roles: ['admin', 'approver', 'finance'], enabled: true, order: 13 },
    { id: 'nav-deposit', name: 'การมัดจำ', category: 'finance', roles: ['admin', 'sales', 'finance'], enabled: true, order: 14 },
    { id: 'nav-calculator', name: 'คำนวณราคา', category: 'tools', roles: ['admin', 'sales', 'approver', 'finance'], enabled: true, order: 15 },
    { id: 'nav-permissions', name: 'จัดการสิทธิ์เมนู', category: 'admin', roles: ['admin'], enabled: true, order: 16 },
    { id: 'nav-audit-logs', name: 'ประวัติการใช้งาน (Audit Log)', category: 'admin', roles: ['admin'], enabled: true, order: 17 }
];

// Helper to convert list to lookup map: { [id]: { roles: [...], enabled: true, order: n } }
function buildPermissionMap(list) {
    const map = {};
    if (Array.isArray(list)) {
        list.forEach(item => {
            if (item && item.id) {
                map[item.id] = {
                    roles: Array.isArray(item.roles) ? item.roles : [],
                    enabled: item.enabled !== false,
                    order: typeof item.order === 'number' ? item.order : 99
                };
            }
        });
    }
    return map;
}

// Active in-memory state initialized from localStorage cache or defaults
let activeMenuPermissionsList = (function () {
    try {
        if (typeof localStorage !== 'undefined') {
            const cached = localStorage.getItem('easycare_menu_permissions');
            if (cached) {
                const parsed = JSON.parse(cached);
                if (Array.isArray(parsed) && parsed.length > 0) {
                    // Merge any newly introduced default menus that may not exist in older cache
                    const existingIds = new Set(parsed.map(p => p.id));
                    defaultMenuPermissionsList.forEach(def => {
                        if (!existingIds.has(def.id)) {
                            parsed.push(JSON.parse(JSON.stringify(def)));
                        }
                    });
                    return parsed;
                }
            }
        }
    } catch (e) {
        console.error('Error loading cached menu permissions:', e);
    }
    return JSON.parse(JSON.stringify(defaultMenuPermissionsList));
})();

let activePermissionMap = buildPermissionMap(activeMenuPermissionsList);

/**
 * Get current list of menu permissions
 */
function getMenuPermissionsList() {
    return JSON.parse(JSON.stringify(activeMenuPermissionsList));
}

/**
 * Update in-memory and local storage menu permissions
 */
function setMenuPermissions(list) {
    if (Array.isArray(list) && list.length > 0) {
        activeMenuPermissionsList = list;
        activePermissionMap = buildPermissionMap(list);
        try {
            if (typeof localStorage !== 'undefined') {
                localStorage.setItem('easycare_menu_permissions', JSON.stringify(list));
            }
        } catch (e) {
            console.error('Error saving menu permissions to localStorage:', e);
        }
    }
}

/**
 * Check if the user has permission to view a specific menu
 * @param {string} permissionKey - The menu key to check
 * @returns {boolean}
 */
function hasPermission(permissionKey) {
    let userRole = 'sales';

    try {
        const explicitRole = typeof localStorage !== 'undefined' ? localStorage.getItem('userRole') : null;
        const session = typeof localStorage !== 'undefined' ? localStorage.getItem('smilecare_staff_session') : null;

        if (explicitRole) {
            userRole = explicitRole;
        } else if (session) {
            const user = JSON.parse(session);
            if (user && user.role) {
                userRole = user.role;
            }
        }
    } catch (e) {
        console.error('Error reading user role in hasPermission:', e);
    }

    // Safety guard: Admin always has access to menu permissions
    if (userRole === 'admin' && permissionKey === 'nav-permissions') {
        return true;
    }

    const permConfig = activePermissionMap[permissionKey];
    if (!permConfig) {
        return true; // Backward compatibility for unlisted items
    }

    if (permConfig.enabled === false) {
        return false;
    }

    return permConfig.roles.includes(userRole);
}

/**
 * Applies menu permissions to DOM elements that have data-permission
 * and sorts sidebar navigation items by their configured order
 */
function applyMenuPermissions() {
    if (typeof document === 'undefined') return;

    const menuElements = document.querySelectorAll('[data-permission]');

    menuElements.forEach(element => {
        const permissionKey = element.getAttribute('data-permission');
        if (!hasPermission(permissionKey)) {
            element.style.display = 'none';
        } else {
            element.style.display = '';
        }
    });

    // Re-order sidebar nav elements based on configured order
    try {
        const sidebarNav = document.querySelector('aside.sidebar nav');
        if (sidebarNav) {
            const navLinks = Array.from(sidebarNav.querySelectorAll('a[data-permission]'));
            if (navLinks.length > 0) {
                navLinks.sort((a, b) => {
                    const keyA = a.getAttribute('data-permission');
                    const keyB = b.getAttribute('data-permission');
                    const orderA = (activePermissionMap[keyA] && typeof activePermissionMap[keyA].order === 'number') ? activePermissionMap[keyA].order : 99;
                    const orderB = (activePermissionMap[keyB] && typeof activePermissionMap[keyB].order === 'number') ? activePermissionMap[keyB].order : 99;
                    return orderA - orderB;
                });
                navLinks.forEach(link => sidebarNav.appendChild(link));
            }
        }
    } catch (e) {
        console.error('Error sorting sidebar nav items:', e);
    }
}

/**
 * Fetch latest menu permissions from server asynchronously
 */
async function fetchMenuPermissions() {
    try {
        const res = await fetch('/api/system/menu-permissions');
        if (res.ok) {
            const json = await res.json();
            if (json && json.success && Array.isArray(json.data)) {
                setMenuPermissions(json.data);
                applyMenuPermissions();
            }
        }
    } catch (err) {
        console.warn('Could not fetch menu permissions from server, using cached/defaults:', err.message);
    }
}

// Backward compatibility global object
const menuPermissions = new Proxy({}, {
    get(target, prop) {
        if (activePermissionMap[prop]) {
            return activePermissionMap[prop].roles;
        }
        return ['sales', 'admin', 'approver', 'finance'];
    }
});

// Export globally on window object
if (typeof window !== 'undefined') {
    window.menuPermissions = menuPermissions;
    window.defaultMenuPermissionsList = defaultMenuPermissionsList;
    window.hasPermission = hasPermission;
    window.applyMenuPermissions = applyMenuPermissions;
    window.getMenuPermissionsList = getMenuPermissionsList;
    window.setMenuPermissions = setMenuPermissions;
    window.fetchMenuPermissions = fetchMenuPermissions;

    // Fetch from server on load
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', fetchMenuPermissions);
    } else {
        fetchMenuPermissions();
    }
}

if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        defaultMenuPermissionsList,
        hasPermission,
        applyMenuPermissions,
        getMenuPermissionsList,
        setMenuPermissions
    };
}
