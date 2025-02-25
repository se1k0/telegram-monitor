document.addEventListener('DOMContentLoaded', function() {
    // 初始化点赞功能
    initLikeButtons();
    
    // 根据当前URL高亮导航项
    highlightActiveNavItem();
    
    // 根据URL参数选择筛选器选项
    selectFilterOptions();
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