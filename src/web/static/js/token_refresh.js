/**
 * Token 刷新功能
 * 处理 Token 数据刷新和 UI 更新
 */

// Token刷新功能
function refreshTokenData(row) {
    // 直接从tr行获取代币信息
    const chain = row.getAttribute('data-chain');
    const contract = row.getAttribute('data-contract');
    const tokenSymbol = row.getAttribute('data-token-symbol') || 'Unknown';
    // 刷新时立即更新时间戳，防止1分钟内重复自动刷新
    row.dataset.lastAutoRefresh = Date.now().toString();
    // 自动刷新时，理论上不会出现无效数据
    if (!chain || !contract) {
        return;
    }
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
        if (data.success) {
            // 检查代币是否已被删除
            if (data.deleted) {
                // 从UI中移除该代币行
                row.style.transition = 'opacity 0.8s ease-out';
                row.style.opacity = '0';
                setTimeout(() => {
                    row.remove();
                    updateTokensTableCount();
                }, 800);
                return;
            }
            // 更新页面上的数据
            if (data.token) {
                updateTokenDataInUI(chain, contract, data.token);
            }
        }
        // 失败时不做任何UI提示
    })
    .catch(error => {
        // 自动刷新失败时静默处理
    });
}

// 数值变化闪烁动画
function flashIfChanged(cell, newValue) {
    if (!cell) return;
    if (cell.textContent !== newValue) {
        cell.textContent = newValue;
        cell.classList.add('value-flash');
        setTimeout(() => cell.classList.remove('value-flash'), 4000);
    }
}

// 更新UI上的代币数据
function updateTokenDataInUI(chain, contract, tokenData) {
    // 直接通过tr的data属性查找对应行
    const row = document.querySelector(`tr.token-row[data-chain="${chain}"][data-contract="${contract}"]`);
    if (!row) return;
    
    // 更新市值
    const marketCapCell = row.querySelector('td:nth-child(3)');
    if (marketCapCell && tokenData.market_cap_formatted) {
        flashIfChanged(marketCapCell, tokenData.market_cap_formatted);
    }
    
    // 计算并更新涨跌幅
    const changePctCell = row.querySelector('td:nth-child(4)');
    if (changePctCell) {
        const currentMarketCap = parseFloat(tokenData.market_cap) || 0;
        const firstMarketCap = parseFloat(tokenData.first_market_cap) || 0;
        let changeValue = 0;
        if (firstMarketCap > 0) {
            changeValue = ((currentMarketCap - firstMarketCap) / firstMarketCap) * 100;
        }
        const pctText = (changeValue > 0 ? '+' : '') + changeValue.toFixed(2) + '%';
        // 先移除正负色类，避免动画被覆盖
        changePctCell.classList.remove('positive-change', 'negative-change');
        flashIfChanged(changePctCell, pctText);
        // 再加正负色
        if (changeValue > 0) {
            changePctCell.classList.add('positive-change');
        } else if (changeValue < 0) {
            changePctCell.classList.add('negative-change');
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
        flashIfChanged(volumeCell, formatted);
    }
    
    // 更新买入/卖出
    const txnsCell = row.querySelector('td:nth-child(6)');
    if (txnsCell) {
        const buysElem = txnsCell.querySelector('.buy-count');
        const sellsElem = txnsCell.querySelector('.sell-count');
        if (buysElem) {
            const buysHtml = `${tokenData.buys_1h || '0'}<i class="bi bi-arrow-up-short"></i>`;
            if (buysElem.innerHTML !== buysHtml) {
                buysElem.innerHTML = buysHtml;
                buysElem.classList.add('value-flash');
                setTimeout(() => buysElem.classList.remove('value-flash'), 4000);
            }
        }
        if (sellsElem) {
            const sellsHtml = `${tokenData.sells_1h || '0'}<i class="bi bi-arrow-down-short"></i>`;
            if (sellsElem.innerHTML !== sellsHtml) {
                sellsElem.innerHTML = sellsHtml;
                sellsElem.classList.add('value-flash');
                setTimeout(() => sellsElem.classList.remove('value-flash'), 4000);
            }
        }
    }
    
    // 更新持有者数量
    const holdersCell = row.querySelector('td:nth-child(7)');
    if (holdersCell) {
        if (tokenData.holders_count) {
            flashIfChanged(holdersCell, tokenData.holders_count.toString());
        } else {
            flashIfChanged(holdersCell, '未知');
            holdersCell.innerHTML = '<span class="text-muted">未知</span>';
        }
    }
    
    // 更新社群覆盖
    const communityCell = row.querySelector('td:nth-child(8)');
    if (communityCell && tokenData.community_reach !== undefined) {
        flashIfChanged(communityCell, formatNumber(tokenData.community_reach || 0));
    }
    
    // 更新消息覆盖
    const spreadCell = row.querySelector('td:nth-child(9)');
    if (spreadCell && tokenData.spread_count !== undefined) {
        flashIfChanged(spreadCell, (tokenData.spread_count || 0).toString());
    }
    
    // 更新代币图像
    const imgElement = row.querySelector('img');
    if (imgElement && tokenData.image_url) {
        if (imgElement.src !== tokenData.image_url) {
            imgElement.src = tokenData.image_url;
            imgElement.classList.add('value-flash');
            setTimeout(() => imgElement.classList.remove('value-flash'), 4000);
        }
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
    // 保证token条背景色始终为白色
    row.classList.remove('table-success', 'table-danger');
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