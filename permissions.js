const menuPermissions = {
    'nav-packages': ['sales', 'admin'],
    'nav-members': ['sales', 'admin'],
    'nav-shops': ['sales', 'admin'],
    'nav-claims': ['sales', 'admin'],
    'nav-tracking': ['sales', 'admin'],
    'nav-approval': ['approver', 'admin']
};

/**
 * Check if the user has permission to view a specific menu
 * @param {string} permissionKey - The menu key to check
 * @returns {boolean}
 */
function hasPermission(permissionKey) {
    const session = localStorage.getItem('smilecare_staff_session');
    const explicitRole = localStorage.getItem('userRole');
    let userRole = explicitRole || 'sales'; // Default fallback

    if (!explicitRole && session) {
        try {
            const user = JSON.parse(session);
            if (user && user.role) {
                userRole = user.role;
            }
        } catch (e) {
            console.error('Error parsing session data', e);
        }
    }

    if (!menuPermissions[permissionKey]) {
        return true;
    }

    return menuPermissions[permissionKey].includes(userRole);
}

/**
 * Applies menu permissions to DOM elements that have data-permission
 */
function applyMenuPermissions() {
    const menuElements = document.querySelectorAll('[data-permission]');

    menuElements.forEach(element => {
        const permissionKey = element.getAttribute('data-permission');
        if (!hasPermission(permissionKey)) {
            element.style.display = 'none';
        } else {
            element.style.display = '';
        }
    });
}
