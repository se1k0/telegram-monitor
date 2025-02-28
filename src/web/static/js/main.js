document.addEventListener('DOMContentLoaded', function() {
    // 初始化点赞功能
    initLikeButtons();
    
    // 根据当前URL高亮导航项
    highlightActiveNavItem();
    
    // 根据URL参数选择筛选器选项
    selectFilterOptions();
    
    // 初始化工具提示
    initTooltips();
    
    // 初始化导航高亮
    highlightCurrentNavItem();
    
    // 初始化状态卡片计数动画
    initCounterAnimations();
    
    // 注册搜索表单事件
    registerSearchEvents();
});

/**
 * 初始化所有点赞按钮的事件监听
 */
function initLikeButtons() {
    document.querySelectorAll('.like-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const chain = this.dataset.chain;
            const contract = this.dataset.contract;
            const likeCount = this.querySelector('.like-count');
            
            fetch('/api/like', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ chain, contract })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    likeCount.textContent = data.likes_count;
                    this.classList.add('btn-success');
                    this.classList.remove('btn-outline-success');
                }
            })
            .catch(error => console.error('Error:', error));
        });
    });
}

/**
 * 根据当前URL高亮相应的导航项
 */
function highlightActiveNavItem() {
    const currentPath = window.location.pathname;
    const navLinks = document.querySelectorAll('.nav-link');
    
    navLinks.forEach(link => {
        link.classList.remove('active');
        const href = link.getAttribute('href');
        
        if (href === '/') {
            // 首页只在当前路径完全匹配时高亮
            if (currentPath === '/') {
                link.classList.add('active');
            }
        } else if (currentPath.startsWith(href)) {
            // 其他页面在路径以链接URL开头时高亮
            link.classList.add('active');
        }
    });
}

/**
 * 根据URL参数设置表单选项
 */
function selectFilterOptions() {
    // 获取URL参数
    const urlParams = new URLSearchParams(window.location.search);
    const chainParam = urlParams.get('chain');
    const searchParam = urlParams.get('search');
    
    // 设置链筛选器
    if (chainParam) {
        const chainSelector = document.getElementById('chainSelector');
        if (chainSelector) {
            chainSelector.value = chainParam;
        }
    }
    
    // 设置搜索框
    if (searchParam) {
        const searchInput = document.querySelector('input[name="search"]');
        if (searchInput) {
            searchInput.value = searchParam;
        }
    }
}

/**
 * 复制文本到剪贴板
 * @param {string} text - 要复制的文本
 */
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        // 显示复制成功提示
        const tooltip = document.createElement('div');
        tooltip.className = 'copy-tooltip';
        tooltip.innerText = '已复制!';
        document.body.appendChild(tooltip);
        
        // 2秒后移除提示
        setTimeout(() => {
            tooltip.remove();
        }, 2000);
    });
}

// 初始化工具提示
function initTooltips() {
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    if (tooltipTriggerList.length) {
        [...tooltipTriggerList].map(tooltipTriggerEl => {
            return new bootstrap.Tooltip(tooltipTriggerEl);
        });
    }
}

// 高亮当前导航项
function highlightCurrentNavItem() {
    const currentPath = window.location.pathname;
    const navLinks = document.querySelectorAll('.navbar .nav-link');
    
    navLinks.forEach(link => {
        link.classList.remove('active');
        
        const href = link.getAttribute('href');
        if (href === currentPath || 
            (href !== '/' && currentPath.startsWith(href)) ||
            (href === '/' && currentPath === '/')) {
            link.classList.add('active');
        }
    });
    
    // 如果没有活跃项，默认高亮首页
    if (!document.querySelector('.navbar .nav-link.active') && document.querySelector('.navbar .nav-link[href="/"]')) {
        document.querySelector('.navbar .nav-link[href="/"]').classList.add('active');
    }
}

// 初始化卡片动画
function initCardAnimations() {
    // 已移除动画效果
}

// 初始化表格行动画
function initTableRowAnimations() {
    // 已移除动画效果
}

// 初始化计数动画
function initCounterAnimations() {
    const counters = document.querySelectorAll('.status-value');
    if (counters.length) {
        counters.forEach(counter => {
            const target = parseInt(counter.innerText, 10);
            if (!isNaN(target) && target > 0) {
                animateCounter(counter, 0, target, 1500);
            }
        });
    }
}

// 计数动画函数
function animateCounter(element, start, end, duration) {
    let startTimestamp = null;
    const step = (timestamp) => {
        if (!startTimestamp) startTimestamp = timestamp;
        const progress = Math.min((timestamp - startTimestamp) / duration, 1);
        const currentValue = Math.floor(progress * (end - start) + start);
        element.innerHTML = currentValue.toLocaleString();
        if (progress < 1) {
            window.requestAnimationFrame(step);
        }
    };
    window.requestAnimationFrame(step);
}

// 注册搜索表单事件
function registerSearchEvents() {
    const searchForms = document.querySelectorAll('form[action="/tokens"], form[action="/token_advanced"]');
    if (searchForms.length) {
        searchForms.forEach(form => {
            form.addEventListener('submit', function() {
                showLoadingOverlay();
            });
        });
    }
    
    // 重置按钮事件
    const resetButtons = document.querySelectorAll('button[onclick*="resetForm"]');
    if (resetButtons.length) {
        resetButtons.forEach(button => {
            button.addEventListener('click', showLoadingOverlay);
        });
    }
}

// 显示加载动画
function showLoadingOverlay() {
    let loadingOverlay = document.getElementById('loadingOverlay');
    if (!loadingOverlay) {
        loadingOverlay = document.createElement('div');
        loadingOverlay.id = 'loadingOverlay';
        loadingOverlay.innerHTML = `
            <div class="loading-spinner">
                <div class="spinner-border text-light" role="status">
                    <span class="visually-hidden">加载中...</span>
                </div>
                <p class="mt-2 text-light">加载中...</p>
            </div>
        `;
        loadingOverlay.style.position = 'fixed';
        loadingOverlay.style.top = '0';
        loadingOverlay.style.left = '0';
        loadingOverlay.style.width = '100%';
        loadingOverlay.style.height = '100%';
        loadingOverlay.style.backgroundColor = 'rgba(66, 72, 116, 0.5)';
        loadingOverlay.style.display = 'flex';
        loadingOverlay.style.justifyContent = 'center';
        loadingOverlay.style.alignItems = 'center';
        loadingOverlay.style.zIndex = '9999';
        document.body.appendChild(loadingOverlay);
    } else {
        loadingOverlay.style.display = 'flex';
    }
}

// 隐藏加载动画
function hideLoadingOverlay() {
    const loadingOverlay = document.getElementById('loadingOverlay');
    if (loadingOverlay) {
        loadingOverlay.style.display = 'none';
    }
}

// 收藏功能实现
function addToWatchlist(tokenId) {
    // 此处添加收藏功能的实现
    // 可以使用localStorage存储或发送Ajax请求到后端
    const token = document.querySelector(`tr[data-token-id="${tokenId}"]`);
    if (token) {
        token.classList.add('highlight-row');
        setTimeout(() => {
            token.classList.remove('highlight-row');
        }, 1500);
    }
    
    const watchlistBadge = document.getElementById('watchlistBadge');
    if (watchlistBadge) {
        const currentCount = parseInt(watchlistBadge.textContent, 10) || 0;
        watchlistBadge.textContent = currentCount + 1;
    }
    
    // 显示通知
    showNotification(`代币 ${tokenId} 已添加到监控列表`);
}

// 显示通知
function showNotification(message, type = 'success') {
    let notificationContainer = document.getElementById('notificationContainer');
    if (!notificationContainer) {
        notificationContainer = document.createElement('div');
        notificationContainer.id = 'notificationContainer';
        notificationContainer.style.position = 'fixed';
        notificationContainer.style.top = '20px';
        notificationContainer.style.right = '20px';
        notificationContainer.style.zIndex = '9999';
        document.body.appendChild(notificationContainer);
    }
    
    const notification = document.createElement('div');
    notification.className = `alert alert-${type} alert-dismissible fade show`;
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="关闭"></button>
    `;
    
    notificationContainer.appendChild(notification);
    
    // 3秒后自动关闭
    setTimeout(() => {
        notification.classList.remove('show');
        setTimeout(() => {
            notification.remove();
        }, 300);
    }, 3000);
}

// CSV导出功能
function exportToCSV() {
    const currentUrl = window.location.href;
    const exportUrl = currentUrl + (currentUrl.includes('?') ? '&' : '?') + 'format=csv';
    window.location.href = exportUrl;
    
    showNotification('数据导出中，请稍候...');
}

// 表单重置函数
function resetForm(formId) {
    const form = document.getElementById(formId || 'filter-form');
    if (form) {
        form.reset();
        showLoadingOverlay();
        window.location.href = form.getAttribute('action');
    }
} 