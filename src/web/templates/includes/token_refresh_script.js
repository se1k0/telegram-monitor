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
    showToast('处理中', `正在刷新 ${tokenSymbol} 的数据...`, 'info');
    
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
            // 更新成功，显示提示
            const marketCapChange = data.market_cap_change.percent ? 
                ` (${data.market_cap_change.percent > 0 ? '+' : ''}${data.market_cap_change.percent.toFixed(2)}%)` : '';
                
            showToast('成功', `${tokenSymbol} 数据已更新${marketCapChange}`, 'success');
            
            // 更新页面上的数据
            updateTokenDataInUI(chain, contract, data.token);
        } else {
            // 更新失败，显示错误信息
            const errorMessage = data.error || '更新失败，请稍后再试';
            showToast('失败', `${tokenSymbol} 数据更新失败: ${errorMessage}`, 'error');
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
        showToast('错误', `刷新 ${tokenSymbol} 数据时出错: ${error.message}`, 'error');
    });
}

// 更新UI上的代币数据
function updateTokenDataInUI(chain, contract, tokenData) {
    // 找到对应的行
    const row = document.querySelector(`tr .token-refresh-btn[data-chain="${chain}"][data-contract="${contract}"]`).closest('tr');
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
    
    // 更新交易量
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
    
    // 更新社群覆盖 - 增强选择器并添加调试日志
    const communityCell = row.querySelector('.community-reach-cell') || row.querySelector('td:nth-child(8)');
    if (communityCell) {
        // 检查是否有社群覆盖数据
        if (tokenData.community_reach !== undefined) {
            console.log(`更新社群覆盖: ${tokenData.community_reach}`);
            communityCell.textContent = tokenData.community_reach || 0;
        } else {
            console.warn(`未找到社群覆盖数据`);
        }
    } else {
        console.warn(`未找到社群覆盖单元格`);
        // 尝试备用选择器
        const backupCommunityCell = row.querySelector('td.community-reach') || 
                                   Array.from(row.querySelectorAll('td')).find(cell => 
                                       cell.textContent.trim().match(/^\d+$/) && 
                                       cell.previousElementSibling && 
                                       cell.previousElementSibling.textContent.includes('持有者'));
        if (backupCommunityCell && tokenData.community_reach !== undefined) {
            console.log(`使用备用选择器更新社群覆盖: ${tokenData.community_reach}`);
            backupCommunityCell.textContent = tokenData.community_reach || 0;
        }
    }
    
    // 更新传播次数 - 增强选择器并添加调试日志
    const spreadCell = row.querySelector('.spread-count-cell') || row.querySelector('td:nth-child(9)');
    if (spreadCell) {
        // 检查是否有传播次数数据
        if (tokenData.spread_count !== undefined) {
            console.log(`更新传播次数: ${tokenData.spread_count}`);
            spreadCell.textContent = tokenData.spread_count || 0;
        } else {
            console.warn(`未找到传播次数数据`);
        }
    } else {
        console.warn(`未找到传播次数单元格`);
        // 尝试备用选择器
        const backupSpreadCell = row.querySelector('td.spread-count') || 
                               Array.from(row.querySelectorAll('td')).find(cell => 
                                   cell.textContent.trim().match(/^\d+$/) && 
                                   cell.previousElementSibling && 
                                   cell.previousElementSibling.textContent.includes('社群覆盖'));
        if (backupSpreadCell && tokenData.spread_count !== undefined) {
            console.log(`使用备用选择器更新传播次数: ${tokenData.spread_count}`);
            backupSpreadCell.textContent = tokenData.spread_count || 0;
        }
    }
    
    // 更新代币图像
    const imgElement = row.querySelector('.token-img');
    const placeholderElement = row.querySelector('.token-placeholder');
    if (tokenData.image_url) {
        if (imgElement) {
            imgElement.src = tokenData.image_url;
        } else if (placeholderElement) {
            // 如果之前没有图像，创建新的图像元素替换占位符
            const newImg = document.createElement('img');
            newImg.src = tokenData.image_url;
            newImg.className = 'me-2 token-img';
            newImg.alt = tokenData.token_symbol;
            placeholderElement.parentNode.replaceChild(newImg, placeholderElement);
        }
    }
    
    // 刷新行的颜色样式
    if (tokenData.market_cap > tokenData.market_cap_1h) {
        row.className = 'table-profit';
    } else if (tokenData.market_cap < tokenData.market_cap_1h) {
        row.className = 'table-loss';
    }
}

// 显示提示消息
function showToast(title, message, type = 'info') {
    // 检查是否已经有toast容器
    let toastContainer = document.getElementById('toast-container');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.id = 'toast-container';
        toastContainer.className = 'position-fixed bottom-0 end-0 p-3';
        document.body.appendChild(toastContainer);
    }
    
    // 创建新的toast
    const toastId = 'toast-' + Date.now();
    const toastHtml = `
        <div id="${toastId}" class="toast" role="alert" aria-live="assertive" aria-atomic="true">
            <div class="toast-header ${type === 'error' ? 'bg-danger text-white' : type === 'success' ? 'bg-success text-white' : 'bg-info text-white'}">
                <strong class="me-auto">${title}</strong>
                <small>${new Date().toLocaleTimeString()}</small>
                <button type="button" class="btn-close ${type === 'error' || type === 'success' ? 'btn-close-white' : ''}" data-bs-dismiss="toast" aria-label="Close"></button>
            </div>
            <div class="toast-body">
                ${message}
            </div>
        </div>
    `;
    
    // 添加到容器
    toastContainer.insertAdjacentHTML('beforeend', toastHtml);
    
    // 初始化并显示toast
    const toastElement = document.getElementById(toastId);
    const toast = new bootstrap.Toast(toastElement, { delay: 5000 });
    toast.show();
    
    // 自动移除
    toastElement.addEventListener('hidden.bs.toast', function() {
        toastElement.remove();
    });
} 