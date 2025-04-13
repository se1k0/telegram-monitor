/**
 * 代币详情弹出层处理脚本
 * 负责加载和显示代币详情弹出层的数据
 */

// 当文档加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    // 初始化模态框
    const modalElement = document.getElementById('tokenDetailModal');
    
    // 如果模态框不存在，尝试加载
    if (!modalElement) {
        console.error('找不到代币详情模态框元素');
        return;
    }
    
    // 获取模态框实例
    const tokenModal = new bootstrap.Modal(modalElement);
    
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
    
    // 全局函数 - 打开代币详情模态框
    window.openTokenDetailModal = function(chain, contract) {
        // 显示模态框
        tokenModal.show();
        
        // 显示加载中状态
        document.getElementById('tokenDetailLoader').classList.remove('d-none');
        document.getElementById('tokenDetailContent').classList.add('d-none');
        document.getElementById('tokenDetailError').classList.add('d-none');
        
        // 设置模态框标题为加载中状态
        document.getElementById('tokenDetailModalLabel').textContent = '代币详情加载中...';
        
        // 加载代币数据
        fetchTokenDetail(chain, contract);
    };
    
    // 获取代币详情数据
    async function fetchTokenDetail(chain, contract) {
        try {
            const response = await fetch(`/api/token_detail/${chain}/${contract}`);
            const data = await response.json();
            
            if (data.success) {
                // 渲染数据到模态框
                renderTokenDetail(data, chain, contract);
            } else {
                // 显示错误信息
                showError(data.error || '获取代币数据失败');
            }
        } catch (error) {
            console.error('获取代币详情时出错:', error);
            showError('网络请求失败，请稍后重试');
        }
    }
    
    // 渲染代币详情到模态框
    function renderTokenDetail(data, chain, contract) {
        const token = data.token;
        
        // 更新模态框标题
        document.getElementById('tokenDetailModalLabel').textContent = `${token.token_symbol} 详情`;
        
        // 更新基本信息
        document.getElementById('tokenSymbol').textContent = token.token_symbol;
        document.getElementById('tokenSymbolShort').textContent = token.token_symbol.charAt(0);
        document.getElementById('tokenChain').textContent = token.chain;
        document.getElementById('tokenContract').textContent = `${token.contract.substring(0, 6)}...${token.contract.substring(token.contract.length - 4)}`;
        
        // 更新市场数据
        document.getElementById('tokenMarketCap').textContent = token.market_cap_formatted;
        
        // 设置价格变化样式和值
        const tokenPriceChange = document.getElementById('tokenPriceChange');
        tokenPriceChange.textContent = token.change_percentage;
        if (token.change_pct_value > 0) {
            tokenPriceChange.classList.add('text-success');
            tokenPriceChange.classList.remove('text-danger');
        } else if (token.change_pct_value < 0) {
            tokenPriceChange.classList.add('text-danger');
            tokenPriceChange.classList.remove('text-success');
        } else {
            tokenPriceChange.classList.remove('text-success', 'text-danger');
        }
        
        // 更新DEX查看按钮链接
        const dexscreenerUrl = getDexscreenerUrl(token.chain, token.contract);
        document.getElementById('viewDexscreenerBtn').href = dexscreenerUrl;
        
        // 渲染提及历史记录
        renderMentionHistory(data.mention_history);
        
        // 渲染市值图表
        renderMarketCapChart(data.market_cap_history);
        
        // 隐藏加载中状态，显示内容
        document.getElementById('tokenDetailLoader').classList.add('d-none');
        document.getElementById('tokenDetailContent').classList.remove('d-none');
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
        document.getElementById('tokenDetailLoader').classList.add('d-none');
        document.getElementById('tokenDetailContent').classList.add('d-none');
        document.getElementById('tokenDetailError').classList.remove('d-none');
        document.getElementById('errorMessage').textContent = message;
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