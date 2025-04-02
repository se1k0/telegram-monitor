document.addEventListener('DOMContentLoaded', function() {
    // 初始化复制功能
    initCopyFunctions();
});

/**
 * 复制文本到剪贴板
 * @param {string} text - 要复制的文本
 * @param {string} description - 复制内容的描述（可选）
 */
function copyToClipboard(text, description = '内容') {
    navigator.clipboard.writeText(text).then(() => {
        // 显示复制成功提示
        showNotification(description + '已复制到剪贴板！', 'success');
    }).catch(err => {
        console.error('复制失败:', err);
        showNotification('复制失败，请手动复制', 'danger');
    });
}

/**
 * 初始化所有复制功能的事件监听
 */
function initCopyFunctions() {
    // 合约地址复制 - 移除这个事件监听器，改为在HTML中直接绑定
    // 这样可以避免重复触发复制操作
    // document.querySelectorAll('.token-address').forEach(element => {
    //     element.addEventListener('click', function() {
    //         const address = this.getAttribute('data-address') || this.textContent.trim().split(' ')[0];
    //         copyToClipboard(address);
    //     });
    // });
}

/**
 * 显示通知
 * @param {string} message - 通知消息
 * @param {string} type - 通知类型（success, warning, danger等）
 */
function showNotification(message, type = 'success') {
    // 检查是否已存在toast容器
    let toastContainer = document.querySelector('.toast-container');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.className = 'toast-container position-fixed top-0 end-0 p-3';
        document.body.appendChild(toastContainer);
    }
    
    // 创建toast元素
    const toastElement = document.createElement('div');
    toastElement.className = `toast align-items-center text-white bg-${type} border-0`;
    toastElement.role = 'alert';
    toastElement.setAttribute('aria-live', 'assertive');
    toastElement.setAttribute('aria-atomic', 'true');
    
    const toastContent = `
        <div class="d-flex">
            <div class="toast-body">
                ${message}
            </div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="关闭"></button>
        </div>
    `;
    
    toastElement.innerHTML = toastContent;
    toastContainer.appendChild(toastElement);
    
    // 使用Bootstrap的Toast组件显示通知
    const toast = new bootstrap.Toast(toastElement, {
        autohide: true,
        delay: 3000
    });
    toast.show();
    
    // 自动移除toast元素
    toastElement.addEventListener('hidden.bs.toast', function() {
        this.remove();
    });
} 