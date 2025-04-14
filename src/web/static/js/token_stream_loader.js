/**
 * Token 流式加载模块
 * 用于在页面加载后逐条加载代币数据，提升用户体验
 */

// 全局变量
let isLoading = false;        // 是否正在加载数据
let hasMoreTokens = true;     // 是否还有更多代币
let lastTokenId = 0;          // 上一次请求的最后一个token ID
let loadingDelay = 300;       // 每条代币加载的延迟时间（毫秒）
let batchSize = 100;           // 每批加载的代币数量，从5改为20，加快加载速度
let totalLoaded = 0;          // 已加载的代币总数
const maxTokensToAutoLoad = 200; // 最大自动加载的代币数量，避免过多加载

// DOM 元素
let tokenListBody;            // 代币列表的tbody元素
let loadingIndicator;         // 加载指示器
let allLoadedIndicator;       // 全部加载完成的指示器

/**
 * 初始化流式加载功能
 */
function initTokenStreamLoader() {
    console.log("初始化Token流式加载器");
    
    // 初始化DOM元素引用
    tokenListBody = document.getElementById('token-list-body');
    loadingIndicator = document.getElementById('tokens-loading-indicator');
    allLoadedIndicator = document.getElementById('tokens-all-loaded');
    
    if (!tokenListBody || !loadingIndicator || !allLoadedIndicator) {
        console.error("找不到必要的DOM元素");
        return;
    }
    
    // 占位行保留，首批数据加载后会自动移除
    
    // 开始加载第一批代币
    loadTokensBatch();
    
    // 添加滚动加载事件
    window.addEventListener('scroll', checkAndLoadMoreTokens);
}

/**
 * 加载一批代币数据
 */
function loadTokensBatch() {
    if (isLoading || !hasMoreTokens || totalLoaded >= maxTokensToAutoLoad) {
        if (totalLoaded >= maxTokensToAutoLoad && hasMoreTokens) {
            // 如果达到最大加载数但还有更多代币，显示加载更多按钮
            showLoadMoreButton();
        }
        return;
    }
    
    isLoading = true;
    
    // 获取当前URL中的筛选参数
    const urlParams = new URLSearchParams(window.location.search);
    const chain = urlParams.get('chain') || 'all';
    const search = urlParams.get('search') || '';
    
    // 构建API请求URL
    const url = `/api/tokens/stream?chain=${chain}&search=${search}&last_id=${lastTokenId}&batch_size=${batchSize}`;
    
    console.log(`正在请求代币数据: ${url}`);
    
    // 发送API请求
    fetch(url)
        .then(response => {
            if (!response.ok) {
                throw new Error(`请求失败: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            if (!data.success) {
                throw new Error(data.error || '获取代币数据失败');
            }
            
            console.log(`获取到 ${data.tokens.length} 个代币`);
            
            // 更新是否还有更多代币的标志
            hasMoreTokens = data.has_more;
            
            // 更新最后一个token ID
            if (data.next_id > 0) {
                lastTokenId = data.next_id;
            }
            
            // 如果是首次加载数据，先清除占位行
            if (totalLoaded === 0) {
                // 清除占位行
                const placeholderRows = tokenListBody.querySelectorAll('.placeholder-row');
                placeholderRows.forEach(row => row.remove());
            }
            
            // 逐条添加代币到表格
            if (data.tokens.length > 0) {
                addTokensSequentially(data.tokens);
            } else {
                // 如果没有获取到代币，显示全部加载完成
                showAllLoadedIndicator();
            }
        })
        .catch(error => {
            console.error('获取代币数据时出错:', error);
            showErrorMessage(error.message);
            isLoading = false;
        });
}

/**
 * 逐条添加代币到表格
 * @param {Array} tokens 代币数据数组
 * @param {Number} index 当前处理的索引
 */
function addTokensSequentially(tokens, index = 0) {
    if (index >= tokens.length) {
        // 所有代币都已添加，更新状态
        isLoading = false;
        totalLoaded += tokens.length;
        
        // 如果已经达到最大自动加载数量但还有更多代币，显示加载更多按钮
        if (totalLoaded >= maxTokensToAutoLoad && hasMoreTokens) {
            showLoadMoreButton();
            return;
        }
        
        // 如果没有更多代币，显示全部加载完成
        if (!hasMoreTokens) {
            showAllLoadedIndicator();
            return;
        }
        
        // 如果还有更多代币且未超过最大数量，继续加载下一批
        setTimeout(loadTokensBatch, 300);
        return;
    }
    
    // 获取当前代币数据
    const token = tokens[index];
    
    // 创建新的表格行
    const row = document.createElement('tr');
    row.className = `${token.is_profit ? 'table-profit' : 'table-loss'} token-fade-in`;
    row.style.opacity = '0';  // 初始设置为不可见，用于淡入动画
    row.dataset.id = token.id;
    row.dataset.chain = token.chain;
    row.dataset.contract = token.contract;
    
    // 设置行内容
    row.innerHTML = `
        <td>
            <div class="d-flex align-items-center">
                ${token.image_url ? 
                `<img src="${token.image_url}" class="me-2 token-img" alt="${token.token_symbol}">` : 
                `<div class="token-placeholder me-2">${token.token_symbol.charAt(0)}</div>`}
                <div>
                    <div class="fw-bold token-name-link" onclick="openTokenDetailModal('${token.chain}', '${token.contract}')">${token.token_symbol}</div>
                    <div class="small text-muted">${token.contract.substring(0, 10)}...</div>
                </div>
            </div>
        </td>
        <td>${token.chain}</td>
        <td>${token.market_cap_formatted}</td>
        <td class="${token.change_pct_value > 0 ? 'text-success' : token.change_pct_value < 0 ? 'text-danger' : ''}">
            ${token.change_percentage}
        </td>
        <td>${token.volume_1h_formatted || '$0'}</td>
        <td>${token.buys_1h || 0}/${token.sells_1h || 0}</td>
        <td>
            ${token.holders_count ? token.holders_count : '<span class="text-muted">未知</span>'}
        </td>
        <td>${token.community_reach || 0}</td>
        <td>${token.spread_count || 0}</td>
        <td>${token.first_update_formatted}</td>
        <td>
            <div class="btn-group">
                <a href="${getDexscreenerUrl(token.chain, token.contract)}" target="_blank" class="btn btn-sm btn-outline-primary">
                    <i class="bi bi-graph-up"></i>
                </a>
                <button class="btn btn-sm btn-outline-success like-btn" data-chain="${token.chain}" data-contract="${token.contract}">
                    <i class="bi bi-heart"></i> <span class="like-count">${token.likes_count || 0}</span>
                </button>
                <button class="btn btn-sm btn-outline-info token-detail-btn" data-chain="${token.chain}" data-contract="${token.contract}">
                    <i class="bi bi-info-circle"></i>
                </button>
                <button class="btn btn-sm btn-outline-warning token-refresh-btn" 
                        data-chain="${token.chain}" 
                        data-contract="${token.contract}" 
                        data-token-symbol="${token.token_symbol}"
                        onclick="refreshTokenData(this)" 
                        title="刷新代币数据">
                    <i class="bi bi-arrow-clockwise"></i>
                    <span class="spinner-border spinner-border-sm d-none" role="status" aria-hidden="true"></span>
                </button>
            </div>
        </td>
    `;
    
    // 添加到表格
    tokenListBody.appendChild(row);
    
    // 添加淡入效果
    setTimeout(() => {
        row.style.opacity = '1';
    }, 10);
    
    // 绑定事件
    bindTokenRowEvents(row);
    
    // 延迟处理下一个代币
    setTimeout(() => {
        addTokensSequentially(tokens, index + 1);
    }, loadingDelay);
}

/**
 * 绑定代币行的事件处理
 * @param {HTMLElement} row 代币行元素
 */
function bindTokenRowEvents(row) {
    // 绑定详情按钮点击事件
    const detailBtn = row.querySelector('.token-detail-btn');
    if (detailBtn) {
        detailBtn.addEventListener('click', function(event) {
            event.preventDefault();
            const chain = this.getAttribute('data-chain');
            const contract = this.getAttribute('data-contract');
            
            if (chain && contract) {
                openTokenDetailModal(chain, contract);
            }
        });
    }
}

/**
 * 显示全部加载完成的指示器
 */
function showAllLoadedIndicator() {
    if (loadingIndicator && allLoadedIndicator) {
        loadingIndicator.classList.add('d-none');
        allLoadedIndicator.classList.remove('d-none');
    }
}

/**
 * 显示加载更多按钮
 */
function showLoadMoreButton() {
    // 隐藏加载指示器
    if (loadingIndicator) {
        loadingIndicator.classList.add('d-none');
    }
    
    // 检查是否已存在加载更多按钮
    let loadMoreButton = document.getElementById('load-more-button');
    
    if (!loadMoreButton) {
        // 创建加载更多按钮
        loadMoreButton = document.createElement('div');
        loadMoreButton.id = 'load-more-button';
        loadMoreButton.className = 'text-center my-4';
        loadMoreButton.innerHTML = `
            <button class="btn btn-primary load-more-btn">
                <i class="bi bi-arrow-down-circle me-2"></i>加载更多代币
            </button>
            <p class="small text-muted mt-2">已加载 ${totalLoaded} 个代币，点击加载更多</p>
        `;
        
        // 添加到DOM
        loadingIndicator.parentNode.insertBefore(loadMoreButton, loadingIndicator);
        
        // 绑定点击事件
        const button = loadMoreButton.querySelector('.load-more-btn');
        if (button) {
            button.addEventListener('click', function() {
                // 隐藏加载更多按钮
                loadMoreButton.classList.add('d-none');
                
                // 显示加载指示器
                loadingIndicator.classList.remove('d-none');
                
                // 加载更多代币
                loadTokensBatch();
            });
        }
    } else {
        // 更新已加载数量
        const countText = loadMoreButton.querySelector('.text-muted');
        if (countText) {
            countText.textContent = `已加载 ${totalLoaded} 个代币，点击加载更多`;
        }
        
        // 显示按钮
        loadMoreButton.classList.remove('d-none');
    }
}

/**
 * 显示错误消息
 * @param {String} message 错误消息
 */
function showErrorMessage(message) {
    if (loadingIndicator) {
        loadingIndicator.innerHTML = `
            <div class="alert alert-danger">
                <i class="bi bi-exclamation-triangle-fill"></i>
                加载失败: ${message}
            </div>
            <button id="retry-load-btn" class="btn btn-warning mt-2">
                <i class="bi bi-arrow-repeat"></i> 重试
            </button>
        `;
        
        // 绑定重试按钮点击事件
        const retryBtn = document.getElementById('retry-load-btn');
        if (retryBtn) {
            retryBtn.addEventListener('click', function() {
                // 恢复加载指示器
                loadingIndicator.innerHTML = `
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">正在加载代币...</span>
                    </div>
                    <p class="mt-2">正在加载代币数据，请稍候...</p>
                `;
                // 重试加载
                loadTokensBatch();
            });
        }
    }
}

/**
 * 检查滚动位置并加载更多代币
 */
function checkAndLoadMoreTokens() {
    // 如果正在加载或没有更多代币，直接返回
    if (isLoading || !hasMoreTokens || totalLoaded >= maxTokensToAutoLoad) {
        return;
    }
    
    // 检查是否滚动到页面底部附近
    const scrollY = window.scrollY || window.pageYOffset;
    const windowHeight = window.innerHeight;
    const documentHeight = document.documentElement.scrollHeight;
    
    // 如果滚动到距离底部300px的位置，加载更多
    if (scrollY + windowHeight >= documentHeight - 300) {
        loadTokensBatch();
    }
}

/**
 * 辅助函数 - 生成DexScreener URL
 */
function getDexscreenerUrl(chain, contract) {
    if (chain === 'SOL') {
        return `https://dexscreener.com/solana/${contract}`;
    } else if (chain === 'ETH') {
        return `https://dexscreener.com/ethereum/${contract}`;
    } else if (chain === 'BSC') {
        return `https://dexscreener.com/bsc/${contract}`;
    } else {
        return `https://dexscreener.com/${chain.toLowerCase()}/${contract}`;
    }
}

// 在页面加载完成后初始化
document.addEventListener('DOMContentLoaded', initTokenStreamLoader); 