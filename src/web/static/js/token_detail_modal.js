/**
 * 代币详情弹出层处理脚本
 * 负责加载和显示代币详情弹出层的数据
 */

// 修复模态框遮罩层问题
function fixModalBackdropIssue() {
    // 监听所有模态框打开事件
    document.addEventListener('shown.bs.modal', function(event) {
        // 查找所有模态框背景
        const backdrops = document.querySelectorAll('.modal-backdrop');
        
        // 设置遮罩层的pointer-events为none，使其不会阻止点击
        backdrops.forEach(backdrop => {
            backdrop.style.pointerEvents = 'none';
        });
        
        console.log('已修复模态框遮罩层交互问题');
    }, true);
    
    // 监听模态框关闭事件，确保遮罩层被正确移除
    document.addEventListener('hidden.bs.modal', function(event) {
        const backdrops = document.querySelectorAll('.modal-backdrop');
        
        // 如果依然存在backdrop但没有打开的模态框，则移除它们
        if (backdrops.length > 0 && !document.querySelector('.modal.show')) {
            backdrops.forEach(backdrop => {
                backdrop.remove();
            });
            // 移除body上的modal-open类
            document.body.classList.remove('modal-open');
            console.log('已移除残留的模态框遮罩层');
        }
    }, true);
}

// 显示复制成功的提示函数（美化版本）
function showCopySuccessToast(content) {
    // 如果已有提示，先移除
    const existingTooltip = document.querySelector('.copy-tooltip');
    if (existingTooltip) {
        document.body.removeChild(existingTooltip);
    }
    
    // 设置显示内容
    const shortContent = content.length > 10 ? 
        content.substring(0, 6) + '...' + content.substring(content.length - 4) : 
        content;
    
    // 创建新的提示元素
    const tooltip = document.createElement('div');
    tooltip.className = 'copy-tooltip';
    tooltip.innerHTML = `
        <i class="bi bi-check-circle-fill"></i>
        <span>已复制: ${shortContent}</span>
    `;
    
    // 添加到body
    document.body.appendChild(tooltip);
    
    // 触发显示动画
    setTimeout(() => {
        tooltip.classList.add('show');
    }, 10);
    
    // 2秒后移除
    setTimeout(() => {
        tooltip.classList.remove('show');
        setTimeout(() => {
            if (tooltip.parentNode) {
                document.body.removeChild(tooltip);
            }
        }, 300); // 等待过渡效果完成后移除DOM
    }, 2000);
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

// 当文档加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    // 输出调试信息
    console.log('代币详情模态框脚本已加载 - 版本 1.2');
    
    // 检查复制API是否可用
    if (navigator.clipboard && navigator.clipboard.writeText) {
        console.log('浏览器支持现代剪贴板API');
    } else {
        console.warn('浏览器不支持现代剪贴板API，将使用备用方案');
    }
    
    // 初始化模态框
    const modalElement = document.getElementById('tokenDetailModal');
    
    // 如果模态框不存在，尝试加载
    if (!modalElement) {
        console.error('找不到代币详情模态框元素');
        return;
    }
    
    // 获取模态框实例
    let tokenModal;
    try {
        tokenModal = new bootstrap.Modal(modalElement, {
            backdrop: false // 禁用遮罩层
        });
    } catch (error) {
        console.error('初始化Bootstrap模态框失败:', error);
        return;
    }
    
    // 查找所有打开代币详情的按钮
    const tokenDetailButtons = document.querySelectorAll('.token-detail-btn');
    tokenDetailButtons.forEach(button => {
        button.addEventListener('click', function(event) {
            event.preventDefault();
            const chain = this.getAttribute('data-chain');
            const contract = this.getAttribute('data-contract');
            
            if (chain && contract) {
                openTokenDetailModal(chain, contract);
            } else {
                console.error('按钮缺少必要的数据属性: data-chain 或 data-contract');
            }
        });
    });
    
    // 模态框内的复制按钮事件处理
    modalElement.addEventListener('click', function(event) {
        // 检查是否点击了复制按钮
        const copyBtn = event.target.closest('.copy-address-btn');
        if (copyBtn && !copyBtn.hasAttribute('data-event-bound')) {
            event.preventDefault();
            event.stopPropagation();
            
            console.log('全局事件处理：复制按钮被点击，但该按钮应该已经有自己的事件处理器');
            // 注意：现在每个按钮在renderTokenDetail中都有单独的事件绑定，这里只是为了兼容性而保留
        }
    });
    
    // 全局函数 - 打开代币详情模态框
    window.openTokenDetailModal = function(chain, contract) {
        console.log(`准备打开token详情模态框: ${chain}/${contract}`);
        
        // 检查模态框是否存在
        const modalElement = document.getElementById('tokenDetailModal');
        if (!modalElement) {
            console.error('找不到token详情模态框元素(#tokenDetailModal)');
            alert('系统错误：找不到模态框元素');
            return;
        }
        
        // 存储当前查询的链和合约地址，用于重试
        modalElement.setAttribute('data-chain', chain);
        modalElement.setAttribute('data-contract', contract);
        
        // 检查必要的DOM元素
        const requiredElements = [
            'tokenDetailLoader', 'tokenDetailContent', 'tokenDetailError',
            'tokenSymbol', 'tokenSymbolShort', 'tokenChain', 'tokenContract',
            'tokenMarketCap', 'tokenPriceChange', 'viewDexscreenerBtn',
            'marketCapChart', 'mentionHistoryBody'
        ];
        
        let missingElements = [];
        for (const id of requiredElements) {
            if (!document.getElementById(id)) {
                missingElements.push(id);
            }
        }
        
        if (missingElements.length > 0) {
            console.error(`模态框缺少以下必要元素: ${missingElements.join(', ')}`);
            
            // 即使缺少元素，我们也尝试显示模态框，并在内部显示错误
            tokenModal.show();
            
            // 手动创建错误提示
            const modalBody = modalElement.querySelector('.modal-body');
            if (modalBody) {
                modalBody.innerHTML = `
                    <div class="alert alert-danger">
                        <h5><i class="bi bi-exclamation-triangle-fill me-2"></i>UI错误</h5>
                        <p>模态框缺少必要元素，无法正常显示。请联系管理员。</p>
                        <small>缺失元素: ${missingElements.join(', ')}</small>
                    </div>
                `;
            }
            return;
        }
        
        // 获取当前滚动位置
        const scrollTop = window.scrollY || document.documentElement.scrollTop;
        
        // 设置模态框的位置
        modalElement.style.top = `${scrollTop}px`;
        
        // 显示模态框
        tokenModal.show();
        
        // 显示加载中状态
        document.getElementById('tokenDetailLoader').classList.remove('d-none');
        document.getElementById('tokenDetailContent').classList.add('d-none');
        document.getElementById('tokenDetailError').classList.add('d-none');
        
        // 设置模态框标题为加载中状态
        document.getElementById('tokenDetailModalLabel').textContent = '代币详情加载中...';
        document.getElementById('tokenDetailModalLabel').classList.remove('text-danger');
        
        // 加载代币数据
        setTimeout(() => {
            // 延迟10ms执行，确保模态框已完全显示
            fetchTokenDetail(chain, contract);
        }, 10);
    };
    
    // 获取代币详情数据
    async function fetchTokenDetail(chain, contract) {
        console.log(`开始获取代币详情: ${chain}/${contract}`);
        
        // 添加请求状态记录
        const modalElement = document.getElementById('tokenDetailModal');
        if (modalElement) {
            modalElement.setAttribute('data-loading', 'true');
        }
        
        // 设置请求超时计时器
        let requestTimer = setTimeout(() => {
            console.error('请求超时，可能是服务器响应慢或网络问题');
            if (modalElement && modalElement.getAttribute('data-loading') === 'true') {
                showError('请求超时，未能获取数据。请检查网络连接后重试。');
            }
        }, 20000); // 20秒超时，比AbortController更早触发
        
        try {
            // 构建URL，添加随机参数以避免缓存问题
            const url = `/api/token_detail/${chain}/${contract}?t=${Date.now()}&nocache=${Math.random()}`;
            console.log(`请求URL: ${url}`);
            
            // 添加超时处理
            const controller = new AbortController();
            const timeoutId = setTimeout(() => {
                controller.abort();
                console.error('请求被AbortController终止');
            }, 30000); // 30秒超时
            
            try {
                console.log('发送API请求...');
                const response = await fetch(url, {
                    signal: controller.signal,
                    headers: {
                        'Accept': 'application/json',
                        'Cache-Control': 'no-cache, no-store, must-revalidate',
                        'Pragma': 'no-cache',
                        'Expires': '0'
                    }
                });
                
                // 清除超时计时器
                clearTimeout(timeoutId);
                clearTimeout(requestTimer);
                
                console.log(`收到响应: 状态码=${response.status}, 状态文本=${response.statusText}`);
                
                if (!response.ok) {
                    // 处理HTTP错误
                    const errorText = await response.text();
                    console.error(`服务器返回错误: 状态码=${response.status}, 响应=${errorText}`);
                    throw new Error(`服务器返回错误状态码: ${response.status} - ${response.statusText}`);
                }
                
                // 尝试解析JSON响应
                const responseText = await response.text();
                console.log(`原始响应文本: ${responseText.substring(0, 200)}...`);
                
                let data;
                try {
                    data = JSON.parse(responseText);
                } catch (parseError) {
                    console.error('解析JSON失败:', parseError);
                    console.error('原始响应:', responseText);
                    throw new Error(`解析JSON失败: ${parseError.message}`);
                }
                
                console.log(`成功解析响应数据: success=${data.success}, token symbol=${data.token?.token_symbol || 'undefined'}`);
                
                if (data.success && data.token) {
                    // 清除加载状态标记
                    if (modalElement) {
                        modalElement.setAttribute('data-loading', 'false');
                    }
                    
                    // 渲染数据到模态框
                    renderTokenDetail(data, chain, contract);
                } else {
                    // 显示错误信息
                    const errorMessage = data.error || '获取代币数据失败';
                    console.error(`API错误: ${errorMessage}`);
                    showError(errorMessage);
                }
            } catch (fetchError) {
                // 清除超时计时器
                clearTimeout(timeoutId);
                clearTimeout(requestTimer);
                
                if (fetchError.name === 'AbortError') {
                    console.error('请求超时');
                    showError('请求超时，服务器响应时间过长。请稍后重试或联系管理员。');
                } else {
                    console.error('请求失败:', fetchError);
                    showError(`网络请求失败: ${fetchError.message}`);
                }
            }
        } catch (error) {
            // 清除超时计时器
            clearTimeout(requestTimer);
            
            console.error('获取代币详情时出错:', error);
            showError(`获取数据时发生错误: ${error.message}`);
        }
    }
    
    // 渲染代币详情到模态框
    function renderTokenDetail(data, chain, contract) {
        console.log("开始渲染代币详情");
        
        try {
            const token = data.token;
            
            if (!token) {
                throw new Error("响应数据中缺少token对象");
            }
            
            // 更新模态框标题
            document.getElementById('tokenDetailModalLabel').textContent = `${token.token_symbol || 'Unknown'} 详情`;
            
            // 更新基本信息
            document.getElementById('tokenSymbol').textContent = token.token_symbol || 'Unknown';
            document.getElementById('tokenSymbolShort').textContent = token.token_symbol ? token.token_symbol.charAt(0) : '?';
            document.getElementById('tokenChain').textContent = token.chain || 'Unknown';
            
            // 更新合约地址，显示完整地址并添加复制按钮
            const contractElement = document.getElementById('tokenContract');
            
            // 检查合约地址是否存在
            let displayAddress = token.contract || 'Unknown';
            if (displayAddress && displayAddress !== 'Unknown' && displayAddress.length > 15) {
                displayAddress = `${token.contract.substring(0, 8)}...${token.contract.substring(token.contract.length - 6)}`;
            }
            
            contractElement.innerHTML = `
                <span title="${token.contract || ''}">${displayAddress}</span>
                <button class="btn btn-outline-secondary copy-address-btn" data-contract="${token.contract || ''}" title="复制合约地址">
                    <i class="bi bi-clipboard"></i>
                </button>
            `;
            
            // 绑定复制按钮事件（确保每次渲染都重新绑定）
            let copyBtn = contractElement.querySelector('.copy-address-btn');
            if (copyBtn) {
                // 先用 cloneNode(true) 替换原按钮，彻底移除所有旧事件监听，防止重复绑定或失效
                const newCopyBtn = copyBtn.cloneNode(true);
                copyBtn.parentNode.replaceChild(newCopyBtn, copyBtn);
                newCopyBtn.addEventListener('click', function(event) {
                    event.preventDefault();
                    event.stopPropagation();
                    console.log('复制按钮被点击'); // 调试输出
                    const contractAddr = this.getAttribute('data-contract');
                    if (!contractAddr) {
                        console.error('复制按钮缺少data-contract属性');
                        return;
                    }
                    
                    // 使用异步方式调用复制函数
                    copyToClipboard(contractAddr).then(success => {
                        if (success) {
                            // 显示复制成功提示
                            const originalTitle = this.getAttribute('title');
                            this.setAttribute('title', '已复制！');
                            this.classList.add('text-success');
                            showCopySuccessToast(contractAddr);
                            setTimeout(() => {
                                this.setAttribute('title', originalTitle);
                                this.classList.remove('text-success');
                            }, 2000);
                        } else {
                            alert('复制失败，请手动复制');
                        }
                    });
                });
                newCopyBtn.setAttribute('data-event-bound', 'true');
            }
            
            // 更新市场数据
            document.getElementById('tokenMarketCap').textContent = token.market_cap_formatted || '$0';
            
            // 设置价格变化样式和值
            const tokenPriceChange = document.getElementById('tokenPriceChange');
            const currentMarketCap = parseFloat(token.market_cap) || 0;
            const firstMarketCap = parseFloat(token.first_market_cap) || 0;
            let changeValue = 0;
            if (firstMarketCap > 0) {
                changeValue = ((currentMarketCap - firstMarketCap) / firstMarketCap) * 100;
            }
            tokenPriceChange.textContent = (changeValue > 0 ? '+' : '') + changeValue.toFixed(2) + '%';
            if (changeValue > 0) {
                tokenPriceChange.classList.add('text-success');
                tokenPriceChange.classList.remove('text-danger');
            } else if (changeValue < 0) {
                tokenPriceChange.classList.add('text-danger');
                tokenPriceChange.classList.remove('text-success');
            } else {
                tokenPriceChange.classList.remove('text-success', 'text-danger');
            }
            
            // 更新DEX查看按钮链接
            try {
                const dexscreenerUrl = getDexscreenerUrl(token.chain || 'unknown', token.contract || '');
                document.getElementById('viewDexscreenerBtn').href = dexscreenerUrl;
            } catch (e) {
                console.error('设置DEX链接时出错:', e);
                document.getElementById('viewDexscreenerBtn').href = '#';
            }
            
            // 渲染提及历史记录
            try {
                renderMentionHistory(data.mention_history || []);
            } catch (e) {
                console.error('渲染提及历史时出错:', e);
            }
            
            // 渲染市值图表
            try {
                renderMarketCapChart(data.market_cap_history || []);
            } catch (e) {
                console.error('渲染市值图表时出错:', e);
            }
            
            console.log("代币详情渲染完成，隐藏加载指示器");
        } catch (error) {
            console.error('渲染代币详情时出错:', error);
        } finally {
            // 确保无论发生什么情况，都隐藏加载中状态，显示内容
            document.getElementById('tokenDetailLoader').classList.add('d-none');
            document.getElementById('tokenDetailContent').classList.remove('d-none');
        }
    }
    
    // 渲染提及历史记录
    function renderMentionHistory(mentions) {
        const mentionHistoryBody = document.getElementById('mentionHistoryBody');
        const mentionHistoryEmpty = document.getElementById('mentionHistoryEmpty');
        const mentionHistoryTable = document.getElementById('mentionHistoryTable');
        
        // 清空现有内容
        mentionHistoryBody.innerHTML = '';
        
        if (!mentions || mentions.length === 0) {
            // 显示无数据提示
            mentionHistoryEmpty.classList.remove('d-none');
            mentionHistoryTable.classList.add('d-none');
            return;
        }
        
        // 隐藏无数据提示，显示表格
        mentionHistoryEmpty.classList.add('d-none');
        mentionHistoryTable.classList.remove('d-none');
        
        // 添加提及记录
        mentions.forEach(mention => {
            const row = document.createElement('tr');
            
            // 格式化时间
            let formattedTime = mention.mention_time;
            if (mention.mention_time) {
                try {
                    const date = new Date(mention.mention_time);
                    formattedTime = date.toLocaleString();
                } catch (e) {
                    console.warn('时间格式化失败:', e);
                }
            }
            
            row.innerHTML = `
                <td>${mention.channel_name || '未知频道'}</td>
                <td>${formattedTime}</td>
                <td>${mention.market_cap_formatted || '$0'}</td>
                <td>
                    <a href="/message/${mention.chain}/${mention.message_id}" class="btn btn-sm btn-outline-primary" target="_blank">
                        查看消息
                    </a>
                </td>
            `;
            
            mentionHistoryBody.appendChild(row);
        });
    }
    
    // 渲染市值图表
    function renderMarketCapChart(marketCapHistory) {
        // 检查Canvas是否存在
        const chartCanvas = document.getElementById('marketCapChart');
        if (!chartCanvas) {
            console.error('找不到市值图表Canvas元素');
            return;
        }
        
        // 销毁现有图表实例(如果有)
        if (window.marketCapChartInstance) {
            window.marketCapChartInstance.destroy();
        }
        
        // 如果没有数据，不绘制图表
        if (!marketCapHistory || marketCapHistory.length < 2) {
            const ctx = chartCanvas.getContext('2d');
            ctx.clearRect(0, 0, chartCanvas.width, chartCanvas.height);
            ctx.font = '14px Arial';
            ctx.fillStyle = '#666';
            ctx.textAlign = 'center';
            ctx.fillText('暂无足够的市值历史数据', chartCanvas.width / 2, chartCanvas.height / 2);
            return;
        }
        
        // 准备图表数据
        const chartData = marketCapHistory.map(item => ({
            x: new Date(item.time),
            y: item.value
        }));
        
        // 绘制图表
        const ctx = chartCanvas.getContext('2d');
        window.marketCapChartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [{
                    label: '市值 (USD)',
                    data: chartData,
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.1)',
                    fill: true,
                    tension: 0.2,
                    pointRadius: 4,
                    pointHoverRadius: 7
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            unit: 'day',
                            displayFormats: {
                                day: 'YYYY-MM-DD'
                            }
                        },
                        title: {
                            display: true,
                            text: '日期'
                        }
                    },
                    y: {
                        beginAtZero: false,
                        title: {
                            display: true,
                            text: '市值 (USD)'
                        },
                        ticks: {
                            callback: function(value) {
                                // 格式化Y轴数值
                                if (value >= 1000000000) {
                                    return '$' + (value / 1000000000).toFixed(2) + 'B';
                                } else if (value >= 1000000) {
                                    return '$' + (value / 1000000).toFixed(2) + 'M';
                                } else if (value >= 1000) {
                                    return '$' + (value / 1000).toFixed(2) + 'K';
                                } else {
                                    return '$' + value.toFixed(2);
                                }
                            }
                        }
                    }
                },
                plugins: {
                    tooltip: {
                        callbacks: {
                            title: function(tooltipItems) {
                                return moment(tooltipItems[0].parsed.x).format('YYYY-MM-DD HH:mm:ss');
                            },
                            label: function(context) {
                                const value = context.parsed.y;
                                let formattedValue = '';
                                if (value >= 1000000000) {
                                    formattedValue = '$' + (value / 1000000000).toFixed(2) + 'B';
                                } else if (value >= 1000000) {
                                    formattedValue = '$' + (value / 1000000).toFixed(2) + 'M';
                                } else if (value >= 1000) {
                                    formattedValue = '$' + (value / 1000).toFixed(2) + 'K';
                                } else {
                                    formattedValue = '$' + value.toFixed(2);
                                }
                                return '市值: ' + formattedValue;
                            }
                        }
                    },
                    legend: {
                        display: false
                    }
                }
            }
        });
    }
    
    // 显示错误信息
    function showError(message) {
        console.error(`显示错误信息: ${message}`);
        
        // 清除加载状态
        const modalElement = document.getElementById('tokenDetailModal');
        if (modalElement) {
            modalElement.setAttribute('data-loading', 'false');
        }
        
        try {
            // 确保隐藏加载指示器
            const loader = document.getElementById('tokenDetailLoader');
            if (loader) {
                loader.classList.add('d-none');
            } else {
                console.error('未找到加载指示器元素');
            }
            
            // 隐藏内容区域
            const content = document.getElementById('tokenDetailContent');
            if (content) {
                content.classList.add('d-none');
            } else {
                console.error('未找到内容区域元素');
            }
            
            // 显示错误区域
            const errorArea = document.getElementById('tokenDetailError');
            if (errorArea) {
                errorArea.classList.remove('d-none');
                
                // 获取当前chain和contract值
                const chain = modalElement ? modalElement.getAttribute('data-chain') || 'unknown' : 'unknown';
                const contract = modalElement ? modalElement.getAttribute('data-contract') || 'unknown' : 'unknown';
                
                // 直接设置错误区域的内容，确保显示最新的错误信息
                errorArea.innerHTML = `
                    <div class="alert alert-danger">
                        <div class="d-flex align-items-center">
                            <i class="bi bi-exclamation-triangle-fill me-2"></i>
                            <div>
                                <strong>加载失败</strong>
                                <p class="mb-0">${message || '加载数据时出错'}</p>
                            </div>
                        </div>
                    </div>
                    <div class="text-center mt-3">
                        <button type="button" class="btn btn-outline-primary retry-button" data-chain="${chain}" data-contract="${contract}">
                            <i class="bi bi-arrow-clockwise me-1"></i> 重试
                        </button>
                    </div>
                `;
                
                // 为重试按钮添加事件监听器
                const retryButton = errorArea.querySelector('.retry-button');
                if (retryButton) {
                    retryButton.addEventListener('click', function() {
                        const btnChain = this.getAttribute('data-chain');
                        const btnContract = this.getAttribute('data-contract');
                        if (btnChain && btnContract) {
                            console.log(`点击重试按钮: ${btnChain}/${btnContract}`);
                            openTokenDetailModal(btnChain, btnContract);
                        } else {
                            console.error('重试按钮缺少data-chain或data-contract属性');
                        }
                    });
                }
            } else {
                console.error('未找到错误区域元素');
                // 如果错误区域不存在，尝试创建一个
                try {
                    const modalBody = document.querySelector('#tokenDetailModal .modal-body');
                    if (modalBody) {
                        const errorDiv = document.createElement('div');
                        errorDiv.id = 'tokenDetailError';
                        errorDiv.className = 'alert alert-danger';
                        errorDiv.innerHTML = `
                            <div class="d-flex align-items-center">
                                <i class="bi bi-exclamation-triangle-fill me-2"></i>
                                <div>
                                    <strong>加载失败</strong>
                                    <p class="mb-0">${message || '加载数据时出错'}</p>
                                </div>
                            </div>
                        `;
                        modalBody.appendChild(errorDiv);
                    }
                } catch (e) {
                    console.error('创建错误提示元素失败:', e);
                }
            }
            
            // 更新模态框标题，显示错误状态
            const modalTitle = document.getElementById('tokenDetailModalLabel');
            if (modalTitle) {
                modalTitle.textContent = '加载失败';
                modalTitle.classList.add('text-danger');
            }
        } catch (e) {
            console.error('显示错误信息时发生错误:', e);
            alert(`加载失败: ${message || '未知错误'}`);
        }
    }
    
    // 辅助函数 - 生成DexScreener URL
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
}); 