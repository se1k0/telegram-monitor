/**
 * Token 刷新功能
 * 处理 Token 数据刷新和 UI 更新
 */

// Token刷新功能
function refreshTokenData(button) {
    // 获取代币信息
    const chain = button.dataset.chain;
    const contract = button.dataset.contract;
    const tokenSymbol = button.dataset.tokenSymbol || 'Unknown';
    
    if (!chain || !contract) {
        showToast('错误', '无法获取代币信息', 'error');
        return;
    }
    
    // 显示加载中状态
    const spinner = button.querySelector('.spinner-border');
    const icon = button.querySelector('.bi-arrow-clockwise');
    if (spinner && icon) {
        spinner.classList.remove('d-none');
        icon.classList.add('d-none');
    }
    button.disabled = true;
            
    // 显示加载中提示
    showToast('正在刷新 ' + tokenSymbol + ' 的数据...');
            
    // 调用API刷新数据
    fetch(`/api/refresh_token/${chain}/${contract}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        }
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`请求失败 (${response.status})`);
        }
        return response.json();
    })
    .then(data => {
        // 恢复按钮状态
        if (spinner && icon) {
            spinner.classList.add('d-none');
            icon.classList.remove('d-none');
        }
        button.disabled = false;
                
        if (data.success) {
            // 检查代币是否已被删除
            if (data.deleted) {
                // 显示已删除提示
                showToast(tokenSymbol + ' 已从数据库中删除，因为该代币在DEX上不存在', true, 'warning');
                
                // 从UI中移除该代币行
                const row = button.closest('tr');
                if (row) {
                    // 添加褪色效果
                    row.style.transition = 'opacity 0.8s ease-out';
                    row.style.opacity = '0';
                    
                    // 设置定时器，等动画完成后移除行
                    setTimeout(() => {
                        row.remove();
                        
                        // 检查是否需要更新表格统计信息
                        updateTokensTableCount();
                    }, 800);
                }
                return;
            }
            
            // 更新成功，显示提示
            const marketCapChange = data.market_cap_change && data.market_cap_change.percent ? 
                ` (${data.market_cap_change.percent > 0 ? '+' : ''}${data.market_cap_change.percent.toFixed(2)}%)` : '';
                
            showToast(tokenSymbol + ' 数据已更新' + marketCapChange, true);
                    
            // 更新页面上的数据而不是刷新整个页面
            if (data.token) {
                updateTokenDataInUI(chain, contract, data.token);
            }
        } else {
            // 更新失败
            const errorMessage = data.error || '更新失败，请稍后再试';
            showToast(tokenSymbol + ' 数据更新失败: ' + errorMessage, false);
        }
    })
    .catch(error => {
        console.error('刷新代币数据出错:', error);
        
        // 恢复按钮状态
        if (spinner && icon) {
            spinner.classList.add('d-none');
            icon.classList.remove('d-none');
        }
        button.disabled = false;
        
        // 显示错误提示
        showToast('刷新 ' + tokenSymbol + ' 数据时出错: ' + error.message, false);
    });
}

// 更新UI上的代币数据
function updateTokenDataInUI(chain, contract, tokenData) {
    // 找到对应的行
    const row = document.querySelector(`tr .refresh-token-btn[data-chain="${chain}"][data-contract="${contract}"]`).closest('tr');
    if (!row) return;
    
    // 更新市值
    const marketCapCell = row.querySelector('td:nth-child(3)');
    if (marketCapCell && tokenData.market_cap_formatted) {
        marketCapCell.textContent = tokenData.market_cap_formatted;
    }
    
    // 计算并更新涨跌幅
    const changePctCell = row.querySelector('td:nth-child(4)');
    if (changePctCell && tokenData.market_cap && tokenData.market_cap_1h) {
        const changePct = tokenData.market_cap_1h > 0 ? 
            ((tokenData.market_cap - tokenData.market_cap_1h) / tokenData.market_cap_1h * 100) : 0;
        
        changePctCell.textContent = changePct > 0 ? `+${changePct.toFixed(2)}%` : `${changePct.toFixed(2)}%`;
        
        // 更新颜色
        changePctCell.className = '';  // 清除现有类
        if (changePct > 0) {
            changePctCell.classList.add('text-success');
        } else if (changePct < 0) {
            changePctCell.classList.add('text-danger');
        }
    }
    
    // 更新成交量
    const volumeCell = row.querySelector('td:nth-child(5)');
    if (volumeCell && tokenData.volume_1h) {
        let formatted = '';
        if (tokenData.volume_1h >= 1000000) {
            formatted = `$${(tokenData.volume_1h / 1000000).toFixed(2)}M`;
        } else if (tokenData.volume_1h >= 1000) {
            formatted = `$${(tokenData.volume_1h / 1000).toFixed(2)}K`;
        } else {
            formatted = `$${tokenData.volume_1h.toFixed(2)}`;
        }
        volumeCell.textContent = formatted;
    }
    
    // 更新买入/卖出
    const txnsCell = row.querySelector('td:nth-child(6)');
    if (txnsCell) {
        txnsCell.textContent = `${tokenData.buys_1h || 0}/${tokenData.sells_1h || 0}`;
    }
    
    // 更新持有者数量
    const holdersCell = row.querySelector('td:nth-child(7)');
    if (holdersCell) {
        if (tokenData.holders_count) {
            holdersCell.textContent = tokenData.holders_count;
        } else {
            holdersCell.innerHTML = '<span class="text-muted">未知</span>';
        }
    }
    
    // 更新社群覆盖
    const communityCell = row.querySelector('td:nth-child(8)');
    if (communityCell && tokenData.community_reach !== undefined) {
        communityCell.textContent = tokenData.community_reach || 0;
    }
    
    // 更新消息覆盖
    const spreadCell = row.querySelector('td:nth-child(9)');
    if (spreadCell && tokenData.spread_count !== undefined) {
        spreadCell.textContent = tokenData.spread_count || 0;
    }
    
    // 更新代币图像
    const imgElement = row.querySelector('img');
    if (imgElement && tokenData.image_url) {
        imgElement.src = tokenData.image_url;
    }
    
    // 刷新行的样式
    if (tokenData.market_cap > tokenData.market_cap_1h) {
        row.classList.add('table-success');
        row.classList.remove('table-danger');
    } else if (tokenData.market_cap < tokenData.market_cap_1h) {
        row.classList.add('table-danger');
        row.classList.remove('table-success');
    } else {
        row.classList.remove('table-success', 'table-danger');
    }
}

// 显示提示消息
function showToast(message, success = true, type = null) {
    // 确定消息类型
    if (type === null) {
        type = success ? 'success' : 'error';
    }
    
    // 检查是否已有通知显示函数
    if (typeof window.showNotification === 'function') {
        // 使用main.js中的通知函数
        window.showNotification(message, type === 'error' ? 'danger' : type);
        return;
    }
    
    // 自己处理通知显示
    let toastContainer = document.querySelector('.toast-container');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.className = 'toast-container position-fixed bottom-0 end-0 p-3';
        document.body.appendChild(toastContainer);
    }
    
    // 创建toast元素
    const toastId = 'toast-' + Date.now();
    const toastEl = document.createElement('div');
    toastEl.id = toastId;
    toastEl.className = `toast align-items-center text-white bg-${type === 'error' ? 'danger' : type === 'success' ? 'success' : 'info'} border-0`;
    toastEl.setAttribute('role', 'alert');
    toastEl.setAttribute('aria-live', 'assertive');
    toastEl.setAttribute('aria-atomic', 'true');
    
    const toastBody = document.createElement('div');
    toastBody.className = 'd-flex';
    
    const messageDiv = document.createElement('div');
    messageDiv.className = 'toast-body';
    messageDiv.textContent = message;
    
    const closeButton = document.createElement('button');
    closeButton.type = 'button';
    closeButton.className = 'btn-close btn-close-white me-2 m-auto';
    closeButton.setAttribute('data-bs-dismiss', 'toast');
    closeButton.setAttribute('aria-label', '关闭');
    
    toastBody.appendChild(messageDiv);
    toastBody.appendChild(closeButton);
    toastEl.appendChild(toastBody);
    
    toastContainer.appendChild(toastEl);
    
    const toast = new bootstrap.Toast(toastEl, {
        delay: 3000
    });
    toast.show();
    
    toastEl.addEventListener('hidden.bs.toast', function() {
        toastEl.remove();
    });
}

// 更新代币表格计数
function updateTokensTableCount() {
    // 查找计数元素
    const countElements = document.querySelectorAll('.tokens-count');
    if (countElements.length === 0) return;
    
    // 计算当前显示的代币行数
    const visibleRows = document.querySelectorAll('table.tokens-table tbody tr').length;
    
    // 更新所有计数元素
    countElements.forEach(el => {
        el.textContent = visibleRows;
    });
    
    // 如果没有代币显示空状态
    const emptyState = document.querySelector('.no-tokens-message');
    const tokenTable = document.querySelector('.tokens-table');
    
    if (visibleRows === 0 && tokenTable) {
        // 隐藏表格
        tokenTable.classList.add('d-none');
        
        // 显示空状态消息
        if (!emptyState) {
            const container = tokenTable.parentElement;
            const noTokensDiv = document.createElement('div');
            noTokensDiv.className = 'alert alert-info no-tokens-message text-center my-4';
            noTokensDiv.innerHTML = '没有代币数据可显示。<a href="/tokens/new">添加新代币</a>？';
            container.appendChild(noTokensDiv);
        } else {
            emptyState.classList.remove('d-none');
        }
    }
} 