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
let newestTokenId = 0;         // 最新的token ID，用于检查新token
let checkNewTokensInterval = null; // 检查新token的定时器

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
    newestTokenId = 0;
    
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
    
    // 添加滚动事件监听（确保只添加一次）
    window.removeEventListener('scroll', handleScroll);
    window.addEventListener('scroll', handleScroll);
    
    // 加载第一批代币
    console.log("开始加载首批代币数据");
    loadTokens();
    
    // 创建新token更新提示条（如果不存在）
    createNewTokensNotification();
    
    // 初始化定时检查新token
    startCheckingNewTokens();
    
    console.log("代币加载器初始化完成");
    
    // 调试信息：监控滚动状态
    setInterval(() => {
        if (!isLoading && hasMoreTokens && tokenCount > 0) {
            const scrollY = window.scrollY;
            const windowHeight = window.innerHeight;
            const documentHeight = document.documentElement.scrollHeight;
            const distanceToBottom = documentHeight - (scrollY + windowHeight);
            console.log(`滚动监控：距底部${distanceToBottom}px，已加载${tokenCount}个代币，hasMore=${hasMoreTokens}`);
        }
    }, 5000); // 每5秒记录一次
}

/**
 * 处理滚动事件，实现无限滚动加载
 */
function handleScroll() {
    // 如果已经没有更多代币，或者正在加载中，则不处理
    if (!hasMoreTokens || isLoading) {
        return;
    }
    
    // 检查是否滚动到接近页面底部
    const scrollY = window.scrollY;
    const windowHeight = window.innerHeight;
    const documentHeight = document.documentElement.scrollHeight;
    const distanceToBottom = documentHeight - (scrollY + windowHeight);
    
    // 当距离底部300px时，触发加载更多
    if (distanceToBottom < 300) {
        console.log(`触发滚动加载，距离底部${distanceToBottom}px`);
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
    
    // 每次请求20条数据
    const batchSize = 20;
    params.append('batch_size', batchSize);
    
    console.log(`加载代币数据: last_id=${lastId}, chain=${chain}, batch_size=${batchSize}, 当前已加载${tokenCount}个代币`);
    
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
        if (data.success) {
            // 显示表格容器
            const tokenTableContainer = document.getElementById('token-table-container');
            if (tokenTableContainer) {
                tokenTableContainer.style.display = 'block';
            }
            
            if (data.tokens && data.tokens.length > 0) {
                // 过滤掉已加载的代币，避免重复
                const newTokens = data.tokens.filter(token => !loadedTokenIds.has(token.id));
                
                console.log(`服务器返回${data.tokens.length}个代币，过滤重复后剩余${newTokens.length}个新代币，服务器has_more=${data.has_more}`);
                
                if (newTokens.length > 0) {
                    // 第一批数据时，记录最新的token ID
                    if (lastId === 0 && newTokens[0].id) {
                        newestTokenId = newTokens[0].id;
                        console.log(`更新最新token ID为: ${newestTokenId}`);
                    }
                    
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
                    
                    // 如果没有新代币但服务器返回has_more=true，尝试继续加载下一批
                    // 这种情况可能是因为当前批次的代币都已经加载过，需要继续获取更多
                    if (data.has_more === true) {
                        // 更新ID以获取下一批数据
                        let shouldLoadMore = false;

                        // 优先使用服务器返回的next_id
                        if (data.next_id && data.next_id !== lastId) {
                            lastId = data.next_id;
                            shouldLoadMore = true;
                            console.log(`使用服务器返回的next_id: ${lastId}`);
                        } 
                        // 其次使用返回数据中最后一个代币的ID
                        else if (data.tokens.length > 0 && data.tokens[data.tokens.length - 1].id) {
                            // 使用最后一个代币ID + 1作为下一批查询的起点
                            const lastTokenId = data.tokens[data.tokens.length - 1].id;
                            // 如果这个ID比当前lastId大，使用它
                            if (lastTokenId > lastId) {
                                lastId = lastTokenId;
                                shouldLoadMore = true;
                                console.log(`使用最后一个返回代币的ID: ${lastId}`);
                            } else {
                                // 否则直接递增lastId
                                lastId = lastId + 1;
                                shouldLoadMore = true;
                                console.log(`递增lastId: ${lastId}`);
                            }
                        } 
                        // 最后兜底方案，直接递增lastId
                        else if (lastId > 0) {
                            lastId = lastId + 1;
                            shouldLoadMore = true;
                            console.log(`递增lastId: ${lastId}`);
                        }
                        
                        hasMoreTokens = data.has_more === true;
                        
                        // 如果需要加载更多，设置延迟
                        if (shouldLoadMore) {
                            setTimeout(() => {
                                if (hasMoreTokens && !isLoading) {
                                    loadTokens();
                                }
                            }, 300);
                        }
                    } else {
                        // 服务器明确表示没有更多数据
                        hasMoreTokens = false;
                        console.log("服务器明确表示没有更多数据");
                    }
                }
            } else {
                // 返回的数据为空，但服务器可能仍然表示有更多数据
                console.log(`服务器返回空数据，has_more=${data.has_more}`);
                
                // 保持hasMoreTokens不变，除非服务器明确表示没有更多
                if (data.has_more === false) {
                    hasMoreTokens = false;
                } else if (data.has_more === true && lastId > 0) {
                    // 如果服务器说有更多数据但返回空列表，递增lastId继续查询
                    lastId = lastId + 1;
                    console.log(`空数据但需要继续加载，递增lastId: ${lastId}`);
                    
                    // 短暂延迟后加载下一批
                    setTimeout(() => {
                        if (hasMoreTokens && !isLoading) {
                            loadTokens();
                        }
                    }, 300);
                }
                
                // 如果是首次加载且无数据，显示无数据提示
                if (tokenCount === 0) {
                    const noTokensMessage = document.getElementById('no-tokens-message');
                    const tokenTableContainer = document.getElementById('token-table-container');
                    
                    if (noTokensMessage) noTokensMessage.style.display = 'block';
                    if (tokenTableContainer) tokenTableContainer.style.display = 'none';
                }
            }
        } else {
            // 服务器返回错误
            console.error("服务器返回错误:", data.error);
            if (typeof showToast === 'function') {
                showToast(data.error || '加载失败', 'error');
            }
        }
        
        // 检查是否需要继续加载（如果屏幕足够大）
        setTimeout(() => {
            // 如果页面内容不足以填满屏幕，且还有更多数据可加载，则继续加载
            if (hasMoreTokens && !isLoading && document.body.clientHeight < window.innerHeight) {
                console.log("页面内容不足以填满屏幕，继续加载更多");
                loadTokens();
            }
        }, 200);
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
                trElement.setAttribute('data-chain', token.chain);
                trElement.setAttribute('data-contract', token.contract);
                trElement.setAttribute('data-token-symbol', token.token_symbol || token.symbol || '');
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
                // 绑定点击事件，复制合约地址，兼容所有系统
                copyBtn.onclick = function(event) {
                    event.preventDefault();
                    event.stopPropagation();
                    const address = this.dataset.address;
                    if (!address) return;
                    copyToClipboard(address).then(success => {
                        if (success) {
                            // 复制成功，按钮变色并显示提示
                            this.setAttribute('title', '已复制！');
                            this.classList.add('text-success');
                            if (typeof showToast === 'function') {
                                showToast('合约地址已复制', true);
                            }
                            setTimeout(() => {
                                this.setAttribute('title', '复制合约地址');
                                this.classList.remove('text-success');
                            }, 2000);
                        } else {
                            if (typeof showToast === 'function') {
                                showToast('复制失败，请手动复制', false);
                            }
                        }
                    });
                };
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
            
            // 涨跌幅
            const changeElem = row.querySelector('td:nth-child(4)');
            if (changeElem) {
                const currentMarketCap = parseFloat(token.market_cap) || 0;
                const firstMarketCap = parseFloat(token.first_market_cap) || 0;
                let changeValue = 0;
                
                if (firstMarketCap > 0) {
                    changeValue = ((currentMarketCap - firstMarketCap) / firstMarketCap) * 100;
                }
                
                changeElem.textContent = formatPercentage(changeValue);
                changeElem.className = `d-none d-lg-table-cell ${changeValue > 0 ? 'positive-change' : changeValue < 0 ? 'negative-change' : ''}`;
            }
            
            // 成交量
            safeSetTextContent(row.querySelector('td:nth-child(5)'), formatVolume(token.volume_1h));
            
            // 买入/卖出
            const buysElem = row.querySelector('.buy-count');
            const sellsElem = row.querySelector('.sell-count');
            if (buysElem) buysElem.innerHTML = `${token.buys_1h || '0'}<i class="bi bi-arrow-up-short"></i>`;
            if (sellsElem) sellsElem.innerHTML = `${token.sells_1h || '0'}<i class="bi bi-arrow-down-short"></i>`;
            
            // 持有者
            safeSetTextContent(row.querySelector('td:nth-child(7)'), formatNumber(token.holders_count));
            
            // 社区覆盖
            safeSetTextContent(row.querySelector('td:nth-child(8)'), formatNumber(token.community_reach || 0));
            
            // 消息覆盖
            safeSetTextContent(row.querySelector('td:nth-child(9)'), formatNumber(token.spread_count || 0));
            
            // 首次发现
            const firstSeenElem = row.querySelector('td:nth-child(10) .small');
            if (firstSeenElem) {
                firstSeenElem.innerHTML = renderFirstSeenWithStyle(token.first_update);
            }
            
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
    
    // 添加对详情按钮的事件处理
    attachTokenDetailClickHandlers();
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

    // 检查是否已经是以d结尾的天数格式（如"5d"或"<1d"）
    if (typeof dateString === 'string' && 
        (dateString.endsWith('d') || dateString === '<1d')) {
        return dateString;  // 直接返回已格式化的天数字符串
    }

    try {
        let date;
        // 兼容 'YYYY-MM-DD HH:mm:ss' 格式，强制按本地时间解析
        const match = typeof dateString === 'string' && dateString.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/);
        if (match) {
            date = new Date(
                parseInt(match[1]),           // 年
                parseInt(match[2], 10) - 1,   // 月（0-11）
                parseInt(match[3], 10),       // 日
                parseInt(match[4], 10),       // 时
                parseInt(match[5], 10),       // 分
                parseInt(match[6], 10)        // 秒
            );
        } else {
            date = new Date(dateString);
        }
        if (isNaN(date.getTime())) return '未知';

        const now = new Date();
        const diffMs = now - date;
        const diffSec = Math.floor(diffMs / 1000);
        const diffMin = Math.floor(diffSec / 60);
        const diffHour = Math.floor(diffMin / 60);
        const diffDay = Math.floor(diffHour / 24);
        const diffYear = Math.floor(diffDay / 365);

        if (diffSec < 60) {
            return '刚刚';
        } else if (diffMin < 60) {
            return `${diffMin}m`;
        } else if (diffHour < 24) {
            return `${diffHour}h`;
        } else if (diffDay < 365) {
            return `${diffDay}d`;
        } else {
            return `${diffYear}y`;
        }
    } catch (e) {
        return '未知';
    }
}

/**
 * 创建新token悬浮提示条显示函数
 */
function showNotificationBar(count, newTokens) {
    let bar = document.getElementById('new-tokens-notification-bar');
    if (!bar) {
        bar = document.createElement('div');
        bar.id = 'new-tokens-notification-bar';
        document.body.appendChild(bar);
    }
    let displayCount = count;
    if (count >= 100) displayCount = '99+';
    bar.innerHTML = `<i class="bi bi-arrow-clockwise me-2"></i>【点击更新 <span id="new-tokens-count">${displayCount}</span> 条新信号】`;
    bar.style.display = 'block';
    // 保存新token数据到bar元素
    bar.dataset.newTokens = JSON.stringify(newTokens);
    bar.onclick = function() {
        bar.style.display = 'none';
        loadNewTokens();
    };
}

/**
 * 移除原有表格tr提示条的创建函数
 */
function createNewTokensNotification() {
    // 不再需要表格内tr提示条，保留空实现
}

/**
 * 初始化定时检查新token
 */
function startCheckingNewTokens() {
    // 清除可能存在的旧定时器
    if (checkNewTokensInterval) {
        clearInterval(checkNewTokensInterval);
    }
    
    // 每10秒检查一次新token
    checkNewTokensInterval = setInterval(() => {
        // 只在有已加载token时检测
        if (tokenCount === 0) return;
        
        // 获取当前最新token的ID
        const firstRow = document.querySelector('#tokens-container tr.token-row');
        if (!firstRow) return;
        const latestId = firstRow.dataset.id;
        if (!latestId) return;

        // 构建参数
        const params = new URLSearchParams();
        params.append('check_new', '1');
        params.append('last_id', latestId);
        params.append('ajax', '1');
        if (chain && chain !== 'all') params.append('chain', chain);
        if (searchQuery) params.append('search', searchQuery);

        fetch('/?' + params.toString())
            .then(res => res.json())
            .then(data => {
                if (data.success && data.new_tokens && data.new_tokens.length > 0) {
                    // 确保通知栏显示
                    updateNewTokensNotification(data.count, data.new_tokens);
                    console.log(`发现${data.count}个新token，已显示通知`);
                }
            })
            .catch(error => {
                console.error('检查新token时出错:', error);
            });
    }, 10000); // 改为10秒检查一次
}

/**
 * 更新新token通知，调用悬浮提示条
 */
function updateNewTokensNotification(count, newTokens) {
    if (!count || !newTokens || newTokens.length === 0) return;
    
    // 确保通知栏显示
    showNotificationBar(count, newTokens);
}

/**
 * 加载新token到列表顶部，适配悬浮div
 */
function loadNewTokens() {
    const bar = document.getElementById('new-tokens-notification-bar');
    if (!bar || !bar.dataset.newTokens) {
        return;
    }
    try {
        const newTokens = JSON.parse(bar.dataset.newTokens);
        if (newTokens && newTokens.length > 0) {
            const container = document.getElementById('tokens-container');
            if (container) {
                // 过滤掉已加载的token
                const filteredTokens = newTokens.filter(token => !loadedTokenIds.has(token.id));
                
                // 按照first_update降序排序，确保最新的token在最前面
                filteredTokens.sort((a, b) => {
                    const timeA = new Date(a.first_update || 0).getTime();
                    const timeB = new Date(b.first_update || 0).getTime();
                    return timeB - timeA; // 降序排列
                });
                
                // 创建文档片段，提高性能
                const fragment = document.createDocumentFragment();
                
                // 按顺序创建并添加token行
                filteredTokens.forEach(token => {
                    const row = createTokenRow(token);
                    if (row) {
                        fragment.appendChild(row);
                        row.classList.add('new-token-highlight');
                        setTimeout(() => {
                            row.classList.remove('new-token-highlight');
                        }, 5000);
                        
                        if (token.id) {
                            loadedTokenIds.add(token.id);
                        }
                    }
                });
                
                // 一次性插入所有新token到容器顶部
                if (container.firstChild) {
                    container.insertBefore(fragment, container.firstChild);
                } else {
                    container.appendChild(fragment);
                }
                
                // 关键修复：每次插入新token行后，必须重新绑定所有详情按钮的点击事件
                // 否则新插入的token行的按钮不会有正确的事件处理，导致跳转异常
                attachTokenDetailClickHandlers();
                
                // 更新计数器和状态
                tokenCount += filteredTokens.length;
                updateTokenCounter();
                
                // 更新最新token ID
                if (filteredTokens.length > 0 && filteredTokens[0].id > newestTokenId) {
                    newestTokenId = filteredTokens[0].id;
                }
                
                // 清理通知栏
                bar.style.display = 'none';
                bar.dataset.newTokens = '';
                
                // 初始化工具提示
                initTooltips();
                
                // 显示成功提示
                if (typeof showToast === 'function') {
                    showToast(`已更新 ${filteredTokens.length} 个新token`, 'success');
                }
            }
        }
    } catch (error) {
        console.error('加载新token出错:', error);
        if (typeof showToast === 'function') {
            showToast('加载新token失败：' + error.message, 'error');
        }
    }
}

/**
 * 创建token行元素
 */
function createTokenRow(token) {
    const template = document.getElementById('token-row-template');
    if (!template) {
        console.error('找不到token-row-template元素');
        return null;
    }
    
    // 克隆模板
    const row = document.importNode(template.content, true);
    
    // 填充代币数据
    const trElement = row.querySelector('tr');
    if (trElement) {
        trElement.dataset.id = token.id || '';
        trElement.setAttribute('data-chain', token.chain);
        trElement.setAttribute('data-contract', token.contract);
        trElement.setAttribute('data-token-symbol', token.token_symbol || token.symbol || '');
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
        // 绑定点击事件，复制合约地址，兼容所有系统
        copyBtn.onclick = function(event) {
            event.preventDefault();
            event.stopPropagation();
            const address = this.dataset.address;
            if (!address) return;
            copyToClipboard(address).then(success => {
                if (success) {
                    // 复制成功，按钮变色并显示提示
                    this.setAttribute('title', '已复制！');
                    this.classList.add('text-success');
                    if (typeof showToast === 'function') {
                        showToast('合约地址已复制', true);
                    }
                    setTimeout(() => {
                        this.setAttribute('title', '复制合约地址');
                        this.classList.remove('text-success');
                    }, 2000);
                } else {
                    if (typeof showToast === 'function') {
                        showToast('复制失败，请手动复制', false);
                    }
                }
            });
        };
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
    
    // 涨跌幅
    const changeElem = row.querySelector('td:nth-child(4)');
    if (changeElem) {
        const currentMarketCap = parseFloat(token.market_cap) || 0;
        const firstMarketCap = parseFloat(token.first_market_cap) || 0;
        let changeValue = 0;
        
        if (firstMarketCap > 0) {
            changeValue = ((currentMarketCap - firstMarketCap) / firstMarketCap) * 100;
        }
        
        changeElem.textContent = formatPercentage(changeValue);
        changeElem.className = `d-none d-lg-table-cell ${changeValue > 0 ? 'positive-change' : changeValue < 0 ? 'negative-change' : ''}`;
    }
    
    // 成交量
    safeSetTextContent(row.querySelector('td:nth-child(5)'), token.volume_formatted || formatVolume(token.volume_24h));
    
    // 买入/卖出
    const buysElem = row.querySelector('.buy-count');
    const sellsElem = row.querySelector('.sell-count');
    if (buysElem) buysElem.innerHTML = `${token.buys_1h || '0'}<i class="bi bi-arrow-up-short"></i>`;
    if (sellsElem) sellsElem.innerHTML = `${token.sells_1h || '0'}<i class="bi bi-arrow-down-short"></i>`;
    
    // 持有者数量
    safeSetTextContent(row.querySelector('td:nth-child(7)'), formatNumber(token.holders_count));
    
    // 社区覆盖
    safeSetTextContent(row.querySelector('td:nth-child(8)'), formatNumber(token.community_members));
    
    // 消息覆盖
    safeSetTextContent(row.querySelector('td:nth-child(9)'), formatNumber(token.spread_count));
    
    // 首次发现时间
    const firstSeenElem = row.querySelector('td:nth-child(10) .small');
    if (firstSeenElem) {
        firstSeenElem.innerHTML = renderFirstSeenWithStyle(token.first_update);
    }
    
    // 操作按钮数据
    const refreshBtn = row.querySelector('.refresh-token-btn');
    if (refreshBtn) {
        refreshBtn.dataset.tokenId = token.id;
        refreshBtn.dataset.chain = token.chain;
        refreshBtn.dataset.contract = token.contract;
        refreshBtn.dataset.tokenSymbol = token.token_symbol || token.symbol || '';
    }
    
    const viewBtn = row.querySelector('.view-token-btn');
    if (viewBtn) {
        viewBtn.dataset.tokenId = token.id;
    }
    
    return trElement;
}

// 添加新函数，用于给详情按钮添加点击事件
function attachTokenDetailClickHandlers() {
    // 查找所有token详情按钮
    const detailButtons = document.querySelectorAll('.view-token-btn');
    
    // 为每个按钮添加点击事件
    detailButtons.forEach(button => {
        // 移除可能已存在的点击事件处理器，避免重复绑定
        button.removeEventListener('click', handleTokenDetailClick);
        // 添加新的点击事件处理器
        button.addEventListener('click', handleTokenDetailClick);
    });
    
    console.log(`已为${detailButtons.length}个token详情按钮添加点击事件处理器`);
}

// 处理token详情按钮点击
function handleTokenDetailClick(event) {
    // 阻止默认行为，避免可能的页面跳转
    event.preventDefault();
    
    // 获取按钮元素
    const button = event.currentTarget;
    
    // 获取所在行
    const row = button.closest('tr');
    if (!row) {
        console.error('无法找到token行元素');
        return;
    }
    
    // 从行中获取链和合约信息
    // 首先尝试从刷新按钮获取，因为它有完整的数据属性
    const refreshBtn = row.querySelector('.refresh-token-btn');
    let chain = '';
    let contract = '';
    
    if (refreshBtn) {
        chain = refreshBtn.dataset.chain;
        contract = refreshBtn.dataset.contract;
    }
    
    // 如果上面的方法获取不到，尝试从其他元素获取
    if (!chain || !contract) {
        // 尝试从链徽章获取
        const chainBadge = row.querySelector('.badge-chain');
        if (chainBadge) {
            chain = chainBadge.textContent.trim();
        }
        
        // 尝试从地址复制按钮获取合约地址
        const copyBtn = row.querySelector('.copy-btn');
        if (copyBtn && copyBtn.dataset.address) {
            contract = copyBtn.dataset.address;
        }
        
        // 如果还是获取不到，尝试从合约地址显示元素获取
        if (!contract) {
            const addressSpan = row.querySelector('[data-address]');
            if (addressSpan && addressSpan.dataset.address) {
                contract = addressSpan.dataset.address;
            }
        }
    }
    
    // 检查chain和contract是否有效
    if (!chain || !contract) {
        console.error('获取chain或contract失败', { chain, contract, button, row });
        if (typeof showToast === 'function') {
            showToast('无法获取代币信息，请尝试刷新页面', 'error');
        }
        return;
    }
    
    console.log(`打开代币详情: ${chain}/${contract}`);
    
    // 调用全局openTokenDetailModal函数
    if (typeof window.openTokenDetailModal === 'function') {
        window.openTokenDetailModal(chain, contract);
    } else {
        console.error('找不到openTokenDetailModal函数');
        if (typeof showToast === 'function') {
            showToast('详情功能不可用，请刷新页面再试', 'error');
        }
    }
}

// 新增：渲染首次发现带样式
function renderFirstSeenWithStyle(dateString) {
    const formatted = formatDate(dateString);
    if (formatted === '刚刚') {
        return `<span class="first-seen-badge first-seen-now">${formatted}</span>`;
    } else if (/^\d+m$/.test(formatted)) {
        return `<span class="first-seen-badge first-seen-min">${formatted}</span>`;
    } else {
        return formatted;
    }
}

// 工具函数：通用复制到剪贴板，兼容 http/https
function copyToClipboard(text) {
    // 优先用 Clipboard API（仅 HTTPS/localhost 可用）
    if (navigator.clipboard && window.isSecureContext) {
        return navigator.clipboard.writeText(text).then(() => true, () => false);
    } else {
        // 兜底方案：用 textarea + execCommand
        const textarea = document.createElement('textarea');
        textarea.value = text;
        textarea.style.position = 'fixed';  // 防止页面跳动
        textarea.style.top = '-1000px';
        textarea.style.left = '-1000px';
        document.body.appendChild(textarea);
        textarea.focus();
        textarea.select();
        let success = false;
        try {
            success = document.execCommand('copy');
        } catch (err) {
            success = false;
        }
        document.body.removeChild(textarea);
        return Promise.resolve(success);
    }
} 