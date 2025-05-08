/**
 * 代币流式加载器
 * 负责渐进式加载代币数据并更新UI
 */

// 状态变量
let isLoading = false;         // 是否正在加载中
let lastId = 0;                // 上一次加载的最后一个代币ID
let hasMoreTokens = true;      // 是否还有更多代币可加载
let tokenCount = 0;            // 已加载的代币总数
let chain = 'all';             // 当前选择的链
let searchQuery = '';          // 当前搜索关键字
let loadedTokenIds = new Set(); // 已加载的代币ID集合，防止重复加载

// 初始化默认代币图片
const defaultTokenImage = new Image();
defaultTokenImage.src = '/static/img/default-token.png';

/**
 * 初始化代币流式加载器
 */
function initTokenStreamLoader() {
    // 防止重复初始化
    if (window.tokenLoaderInitialized) {
        console.log("代币加载器已初始化，跳过");
        return;
    }
    
    console.log("初始化代币流式加载器");
    window.tokenLoaderInitialized = true;
    
    // 从URL获取当前链过滤条件
    const urlParams = new URLSearchParams(window.location.search);
    chain = urlParams.get('chain') || 'all';
    searchQuery = urlParams.get('search') || '';
    
    // 重置状态
    isLoading = false;
    lastId = 0;
    hasMoreTokens = true;
    tokenCount = 0;
    loadedTokenIds.clear();
    
    // 确保DOM元素存在
    const initialLoading = document.getElementById('initial-loading');
    const tokenTableContainer = document.getElementById('token-table-container');
    const noTokensMessage = document.getElementById('no-tokens-message');
    const loadMoreIndicator = document.getElementById('load-more');
    
    if (initialLoading) initialLoading.style.display = 'flex';
    if (tokenTableContainer) tokenTableContainer.style.display = 'none';
    if (noTokensMessage) noTokensMessage.style.display = 'none';
    if (loadMoreIndicator) loadMoreIndicator.style.display = 'none';
    
    // 清空可能已存在的代币容器
    const container = document.getElementById('tokens-container');
    if (container) {
        container.innerHTML = '';
    }
    
    // 加载第一批代币
    loadTokens();
    
    // 添加滚动事件监听（确保只添加一次）
    window.removeEventListener('scroll', handleScroll);
    window.addEventListener('scroll', handleScroll);
    
    console.log("代币加载器初始化完成");
}

/**
 * 处理滚动事件，实现无限滚动加载
 */
function handleScroll() {
    // 如果已经没有更多代币，或者正在加载中，则不处理
    if (!hasMoreTokens || isLoading) return;
    
    // 检查是否滚动到接近页面底部
    const scrollY = window.scrollY;
    const windowHeight = window.innerHeight;
    const documentHeight = document.documentElement.scrollHeight;
    
    // 当距离底部300px时，触发加载更多
    if (scrollY + windowHeight > documentHeight - 300) {
        loadTokens();
    }
}

/**
 * 加载代币数据
 */
function loadTokens() {
    // 防止重复加载
    if (isLoading) {
        console.log("已有加载任务进行中，跳过");
        return;
    }
    
    // 如果已经没有更多代币，则不加载
    if (!hasMoreTokens) {
        console.log("没有更多代币数据，跳过加载");
        return;
    }
    
    // 设置加载状态
    isLoading = true;
    
    console.log(`开始加载代币数据: last_id=${lastId}, chain=${chain}`);
    
    // 显示加载更多指示器(非初次加载时)
    const loadMoreIndicator = document.getElementById('load-more');
    if (loadMoreIndicator && lastId > 0) {
        loadMoreIndicator.style.display = 'flex';
    }
    
    // 构建API请求参数
    const params = new URLSearchParams();
    params.append('last_id', lastId);
    
    if (chain && chain !== 'all') {
        params.append('chain', chain);
    }
    if (searchQuery) {
        params.append('search', searchQuery);
    }
    params.append('batch_size', 20); // 每次加载20条
    
    // 设置超时，确保请求不会无限期挂起
    const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('请求超时')), 15000);
    });
    
    // 使用正确的API端点
    Promise.race([
        fetch(`/api/tokens/stream?${params.toString()}`),
        timeoutPromise
    ])
        .then(response => {
            if (!response.ok) {
            throw new Error(`请求失败(${response.status})`);
            }
            return response.json();
        })
        .then(data => {
        // 更新加载状态
        isLoading = false;
        
        // 隐藏加载指示器
        const loadMoreIndicator = document.getElementById('load-more');
        const initialLoading = document.getElementById('initial-loading');
        
        if (loadMoreIndicator) loadMoreIndicator.style.display = 'none';
        if (initialLoading) initialLoading.style.display = 'none';
        
        // 处理返回的代币数据
        if (data.success && data.tokens && data.tokens.length > 0) {
            // 显示表格容器
            const tokenTableContainer = document.getElementById('token-table-container');
            if (tokenTableContainer) {
                tokenTableContainer.style.display = 'block';
            }
            
            // 过滤掉已加载的代币，避免重复
            const newTokens = data.tokens.filter(token => !loadedTokenIds.has(token.id));
            
            if (newTokens.length > 0) {
                // 渲染代币到列表
                renderTokens(newTokens);
                
                // 记录已加载的代币ID
                newTokens.forEach(token => {
                    if (token.id) loadedTokenIds.add(token.id);
                });
                
                // 更新最后一个代币ID
                if (data.next_id) {
                    lastId = data.next_id;
                } else if (newTokens.length > 0 && newTokens[newTokens.length - 1].id) {
                    lastId = newTokens[newTokens.length - 1].id;
                }
                
                // 更新是否有更多代币
                hasMoreTokens = data.has_more === true;
                
                // 更新代币计数器
                tokenCount += newTokens.length;
                updateTokenCounter();
                
                console.log(`加载了${newTokens.length}个新代币，总计${tokenCount}个，next_id=${lastId}，has_more=${hasMoreTokens}`);
            } else {
                console.log("没有新的代币数据，所有返回的代币都已加载");
                
                // 仍然更新lastId和hasMoreTokens
                if (data.next_id) {
                    lastId = data.next_id;
                }
                hasMoreTokens = data.has_more === true;
            }
        } else {
            // 无更多代币可加载
            hasMoreTokens = false;
            console.log("没有更多代币数据或数据为空");
            
            // 如果是首次加载且无数据，显示无数据提示
            if (tokenCount === 0) {
                const noTokensMessage = document.getElementById('no-tokens-message');
                const tokenTableContainer = document.getElementById('token-table-container');
                
                if (noTokensMessage) noTokensMessage.style.display = 'block';
                if (tokenTableContainer) tokenTableContainer.style.display = 'none';
            }
        }
    })
    .catch(error => {
        console.error('加载代币数据出错:', error);
        isLoading = false;
        
        // 隐藏加载指示器
        const loadMoreIndicator = document.getElementById('load-more');
        const initialLoading = document.getElementById('initial-loading');
        
        if (loadMoreIndicator) loadMoreIndicator.style.display = 'none';
        if (initialLoading) initialLoading.style.display = 'none';
        
        // 显示错误提示
        if (typeof showToast === 'function') {
            showToast('加载代币数据失败: ' + error.message, 'error');
        } else {
            alert('加载代币数据失败: ' + error.message);
        }
    });
}

/**
 * 处理图片加载错误
 * @param {HTMLImageElement} img - 图片元素
 */
function handleImageError(img) {
    if (img && img.src !== defaultTokenImage.src) {
        img.src = defaultTokenImage.src;
        // 防止再次触发错误处理，避免循环
        img.onerror = null;
    }
}

/**
 * 渲染代币到UI
 * @param {Array} tokens 代币数据数组
 */
function renderTokens(tokens) {
    // 确保tokens是数组且有内容
    if (!Array.isArray(tokens) || tokens.length === 0) {
        console.log("没有代币数据可渲染");
        return;
    }
    
    const container = document.getElementById('tokens-container');
    if (!container) {
        console.error('找不到tokens-container元素');
        return;
    }
    
    const template = document.getElementById('token-row-template');
    if (!template) {
        console.error('找不到token-row-template元素');
        return;
    }
    
    // 创建文档片段，提高性能
    const fragment = document.createDocumentFragment();
    
    tokens.forEach(token => {
        try {
            // 跳过无效的代币数据
            if (!token || !token.contract || !token.chain) {
                console.warn('跳过无效的代币数据', token);
                return;
            }
            
            // 克隆模板
            const row = document.importNode(template.content, true);
            
            // 填充代币数据
            const trElement = row.querySelector('tr');
            if (trElement) {
                trElement.dataset.id = token.id || '';
            }
            
            // 代币名称和图标
            const imgElement = row.querySelector('img');
            if (imgElement) {
                // 设置默认图片
                imgElement.src = token.image_url || defaultTokenImage.src;
                imgElement.alt = token.name || token.symbol || 'Token';
                // 添加错误处理
                imgElement.onerror = function() { handleImageError(this); };
            }
            
            // 修复代币名称显示（解决显示Unknown问题）
            const nameElement = row.querySelector('.token-name');
            if (nameElement) {
                const tokenName = token.name || '';
                const tokenSymbol = token.token_symbol || token.symbol || '';
                nameElement.innerHTML = tokenName + (tokenSymbol ? ` <span class="token-symbol">${tokenSymbol}</span>` : '');
            }
            
            // 合约地址
            const addressElem = row.querySelector('[data-address]');
            if (addressElem) {
                addressElem.textContent = formatAddress(token.contract);
                addressElem.dataset.address = token.contract;
            }
            
            const copyBtn = row.querySelector('.copy-btn');
            if (copyBtn) {
                copyBtn.dataset.address = token.contract;
            }
            
            // 创建社交媒体链接 - 第一行
            const socialMediaRow1 = row.querySelector('.social-links-row1');
            if (socialMediaRow1) {
                // 清空原有的链接
                socialMediaRow1.innerHTML = '';
                
                // 1. 推特地址
                if (token.twitter) {
                    socialMediaRow1.appendChild(createSocialLink(token.twitter, 'bi-twitter-x', 'Twitter'));
                }
                
                // 2. 网站
                if (token.website) {
                    socialMediaRow1.appendChild(createSocialLink(token.website, 'bi-globe', '网站'));
                }
                
                // 3. Telegram
                if (token.telegram) {
                    socialMediaRow1.appendChild(createSocialLink(token.telegram, 'bi-telegram', 'Telegram'));
                }
                
                // 4. 推特搜索链接
                const tokenSymbol = token.token_symbol || token.symbol || token.name;
                if (tokenSymbol) {
                    const twitterSearchUrl = `https://x.com/search?q=(${encodeURIComponent('$' + tokenSymbol)}%20OR%20${encodeURIComponent(token.contract)})&src=typed_query&f=live`;
                    socialMediaRow1.appendChild(createSocialLink(twitterSearchUrl, 'bi-search', '推特搜索'));
                }
            }
            
            // 创建社交媒体链接 - 第二行
            const socialMediaRow2 = row.querySelector('.social-links-row2');
            if (socialMediaRow2) {
                // 清空原有的链接
                socialMediaRow2.innerHTML = '';
                
                // 添加链特定的链接
                const chainLower = token.chain.toLowerCase();
                
                // 1. Axiom（仅SOL链）
                if (chainLower === 'sol') {
                    const axiomUrl = `https://axiom.trade/meme/${token.contract}`;
                    socialMediaRow2.appendChild(createSocialLink(axiomUrl, 'bi-bar-chart', 'Axiom'));
                }
                
                // 2. Debot（支持solana、bsc、base链）
                if (['sol', 'solana', 'bsc', 'base'].includes(chainLower)) {
                    const debotChain = chainLower === 'sol' ? 'solana' : chainLower;
                    const debotUrl = `https://debot.ai/token/${debotChain}/${token.contract}`;
                    socialMediaRow2.appendChild(createSocialLink(debotUrl, 'bi-robot', 'Debot'));
                }
                
                // 3. GMGN（支持sol、bsc、base链）
                if (['sol', 'bsc', 'base'].includes(chainLower)) {
                    const gmgnUrl = `https://gmgn.ai/${chainLower}/token/${token.contract}`;
                    socialMediaRow2.appendChild(createSocialLink(gmgnUrl, 'bi-graph-up', 'GMGN'));
                }
                
                // 4. PumpFun（仅SOL链）
                if (chainLower === 'sol') {
                    const pumpfunUrl = `https://pump.fun/coin/${token.contract}`;
                    socialMediaRow2.appendChild(createSocialLink(pumpfunUrl, 'bi-rocket', 'PumpFun'));
                }
            }
            
            // 链
            const chainBadge = row.querySelector('.badge-chain');
            if (chainBadge) {
                chainBadge.textContent = token.chain.toUpperCase();
                chainBadge.classList.add(getChainClass(token.chain));
            }
            
            // 设置各种数值
            safeSetTextContent(row.querySelector('td:nth-child(3)'), token.market_cap_formatted || formatMarketCap(token.market_cap));
            
            // 24h变化
            const changeElem = row.querySelector('td:nth-child(4)');
            if (changeElem) {
                const changeValue = parseFloat(token.change_pct_value) || 0;
                changeElem.textContent = formatPercentage(changeValue);
                changeElem.className = `d-none d-lg-table-cell ${changeValue > 0 ? 'positive-change' : changeValue < 0 ? 'negative-change' : ''}`;
            }
            
            // 成交量
            safeSetTextContent(row.querySelector('td:nth-child(5)'), formatVolume(token.volume_1h));
            
            // 买入/卖出
            safeSetTextContent(row.querySelector('.positive-change'), (token.buys_1h || '0'));
            safeSetTextContent(row.querySelector('.negative-change'), (token.sells_1h || '0'));
            
            // 持有者
            safeSetTextContent(row.querySelector('td:nth-child(7)'), formatNumber(token.holders || 0));
            
            // 社区覆盖
            safeSetTextContent(row.querySelector('td:nth-child(8)'), formatNumber(token.community_reach || 0));
            
            // 消息覆盖
            safeSetTextContent(row.querySelector('td:nth-child(9)'), formatNumber(token.spread_count || 0));
            
            // 首次发现
            safeSetTextContent(row.querySelector('td:nth-child(10) .small'), formatDate(token.first_update_formatted || token.first_seen));
            
            // 操作按钮
            const refreshBtn = row.querySelector('.refresh-token-btn');
            if (refreshBtn) {
                refreshBtn.dataset.tokenId = token.id || '';
                refreshBtn.dataset.chain = token.chain;
                refreshBtn.dataset.contract = token.contract;
                refreshBtn.dataset.tokenSymbol = token.token_symbol || token.symbol || '';
            }
            
            const viewBtn = row.querySelector('.view-token-btn');
            if (viewBtn) {
                viewBtn.dataset.tokenId = token.id || '';
            }
            
            // 追加到片段
            fragment.appendChild(row);
        } catch (error) {
            console.error('渲染代币行时出错:', error, token);
        }
    });
    
    // 一次性添加所有元素到DOM
    container.appendChild(fragment);
    
    // 初始化新添加元素上的工具提示
    try {
        if (typeof initTooltips === 'function') {
            initTooltips();
        }
    } catch (error) {
        console.error('初始化工具提示失败:', error);
    }
}

/**
 * 创建社交媒体链接
 * @param {string} url - 链接URL
 * @param {string} icon - 图标类名
 * @param {string} title - 提示文本
 * @returns {HTMLElement} - 链接元素
 */
function createSocialLink(url, icon, title) {
    if (!url) return null;
    
    const link = document.createElement('a');
    link.href = url;
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    link.className = 'social-link';
    link.setAttribute('data-bs-toggle', 'tooltip');
    link.setAttribute('data-bs-placement', 'top');
    link.setAttribute('title', title);
    
    const iconElement = document.createElement('i');
    iconElement.className = `bi ${icon}`;
    link.appendChild(iconElement);
    
    return link;
}

/**
 * 安全设置元素文本内容
 * @param {HTMLElement} element - 目标元素
 * @param {string} content - 文本内容
 */
function safeSetTextContent(element, content) {
    if (element) {
        element.textContent = content || '';
    }
}

/**
 * 更新代币计数器
 */
function updateTokenCounter() {
    const counter = document.getElementById('token-counter');
    if (counter) {
        counter.textContent = tokenCount;
    }
}

/**
 * 重新加载代币数据
 * @returns {Promise} 返回一个Promise，加载完成时解析
 */
function reloadTokens() {
    return new Promise((resolve, reject) => {
        // 重置状态
        isLoading = false;
        lastId = 0;
        hasMoreTokens = true;
        tokenCount = 0;
        loadedTokenIds.clear();
        
        // 清空代币容器
        const container = document.getElementById('tokens-container');
        if (container) {
            container.innerHTML = '';
        }
        
        // 显示加载指示器
        const initialLoading = document.getElementById('initial-loading');
        const tokenTableContainer = document.getElementById('token-table-container');
        const noTokensMessage = document.getElementById('no-tokens-message');
        
        if (initialLoading) initialLoading.style.display = 'flex';
        if (tokenTableContainer) tokenTableContainer.style.display = 'none';
        if (noTokensMessage) noTokensMessage.style.display = 'none';
        
        // 设置一个超时，确保在加载超时时能够解决Promise
        const timeoutId = setTimeout(() => {
            if (isLoading) {
                console.error('加载超时');
                isLoading = false;
                
                if (initialLoading) initialLoading.style.display = 'none';
                
                if (typeof showToast === 'function') {
                    showToast('加载超时，请稍后再试', 'error');
                }
                
                reject(new Error('加载超时'));
            }
        }, 15000); // 15秒超时
        
        // 加载数据
        loadTokens();
        
        // 每秒检查一次是否加载完成
        const checkInterval = setInterval(() => {
            if (!isLoading) {
                clearInterval(checkInterval);
                clearTimeout(timeoutId);
                resolve();
            }
        }, 1000);
    });
}

// 辅助函数

/**
 * 格式化地址，显示前6位和后4位
 * @param {string} address - 完整地址
 * @returns {string} - 格式化后的地址
 */
function formatAddress(address) {
    if (!address || address.length < 10) return address;
    return `${address.substring(0, 6)}...${address.substring(address.length - 4)}`;
}

/**
 * 获取链对应的CSS类名
 * @param {string} chain 链名称
 * @returns {string} CSS类名
 */
function getChainClass(chain) {
    if (!chain) return 'bg-secondary';
    
    const chainMapping = {
        'eth': 'bg-primary',
        'sol': 'bg-purple',
        'bsc': 'bg-warning',
        'arb': 'bg-info',
        'base': 'bg-success',
        'avax': 'bg-danger'
    };
    
    try {
        return chainMapping[chain.toLowerCase()] || 'bg-secondary';
    } catch (e) {
        return 'bg-secondary';
    }
}

/**
 * 格式化市值显示
 * @param {number} value 市值
 * @returns {string} 格式化后的市值
 */
function formatMarketCap(value) {
    if (!value) return '$0';
    if (isNaN(parseFloat(value))) return '$0';
    
    value = parseFloat(value);
    
    try {
        // 格式化为美元表示
        if (value >= 1000000000) {
            return '$' + (value / 1000000000).toFixed(2) + 'B';
        } else if (value >= 1000000) {
            return '$' + (value / 1000000).toFixed(2) + 'M';
        } else if (value >= 1000) {
            return '$' + (value / 1000).toFixed(2) + 'K';
        } else {
            return '$' + value.toFixed(2);
        }
    } catch (e) {
        return '$0';
    }
}

/**
 * 格式化百分比显示
 * @param {number} value 百分比值
 * @returns {string} 格式化后的百分比
 */
function formatPercentage(value) {
    if (value === null || value === undefined || isNaN(parseFloat(value))) return '0%';
    
    value = parseFloat(value);
    
    try {
        return (value > 0 ? '+' : '') + value.toFixed(2) + '%';
    } catch (e) {
        return '0%';
    }
}

/**
 * 格式化交易量显示
 * @param {number} value 交易量
 * @returns {string} 格式化后的交易量
 */
function formatVolume(value) {
    if (!value) return '$0';
    if (isNaN(parseFloat(value))) return '$0';
    
    value = parseFloat(value);
    
    try {
        // 格式化为美元表示
        if (value >= 1000000000) {
            return '$' + (value / 1000000000).toFixed(2) + 'B';
        } else if (value >= 1000000) {
            return '$' + (value / 1000000).toFixed(2) + 'M';
        } else if (value >= 1000) {
            return '$' + (value / 1000).toFixed(2) + 'K';
        } else {
            return '$' + value.toFixed(2);
        }
    } catch (e) {
        return '$0';
    }
}

/**
 * 格式化数字显示
 * @param {number} value 数字
 * @returns {string} 格式化后的数字
 */
function formatNumber(value) {
    if (!value) return '0';
    if (isNaN(parseFloat(value))) return '0';
    
    value = parseFloat(value);
    
    try {
        if (value >= 1000000) {
            return (value / 1000000).toFixed(1) + 'M';
        } else if (value >= 1000) {
            return (value / 1000).toFixed(1) + 'K';
        } else {
            return value.toString();
        }
    } catch (e) {
        return '0';
    }
}

/**
 * 格式化日期显示
 * @param {string} dateString 日期字符串
 * @returns {string} 格式化后的日期
 */
function formatDate(dateString) {
    if (!dateString) return '未知';
    
    try {
        const date = new Date(dateString);
        if (isNaN(date.getTime())) return '未知';
        
        const now = new Date();
        const diffTime = Math.abs(now - date);
        const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
        
        if (diffDays === 0) {
            // 今天，显示小时和分钟
            return '今天 ' + date.getHours().toString().padStart(2, '0') + ':' + 
                   date.getMinutes().toString().padStart(2, '0');
        } else if (diffDays === 1) {
            return '昨天';
        } else if (diffDays < 7) {
            return diffDays + '天前';
        } else {
            // 显示年月日
            return date.getFullYear() + '/' + 
                   (date.getMonth() + 1).toString().padStart(2, '0') + '/' + 
                   date.getDate().toString().padStart(2, '0');
        }
    } catch (e) {
        return '未知';
    }
} 