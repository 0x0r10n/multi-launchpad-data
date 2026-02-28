import React, { useState, useEffect, useRef } from 'react';
import {
    Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
    Paper, Avatar, Chip, Typography, Box, IconButton
} from '@mui/material';
import {
    RefreshCw,
    TrendingUp,
    Clock as ClockIcon,
    Twitter as TwitterIcon,
    Send as TelegramIcon,
    Globe as WebIcon,
    Maximize2 as MaximizeIcon,
    Filter as FilterIcon,
    ChevronDown as ChevronDownIcon,
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, AreaChart, Area, ComposedChart, Bar } from 'recharts';
import type { OHLCV, PriceTick } from '../utils/ohlcv';
import { getCandleData } from '../utils/ohlcv';
import { Modal, Backdrop, Fade } from '@mui/material';

interface TokenData {
    mint: string;
    name: string;
    symbol: string;
    image: string;
    mcapUsd: number;
    volumeSol: number;
    curvePct: number;
    launchpad: string;
    createdTime: number;
    hasTwitter: boolean;
    hasTelegram: boolean;
    hasWebsite: boolean;
    devPct: number;
    top10Pct: number;
    sniperPct: number;
    signature: string;
    priceHistory?: PriceTick[];
    ohlcv1h?: OHLCV[];
    events?: Record<string, { priceChangePercentage: number }>;
}

const TokenRow = React.memo(({ token, now, onSelect }: { token: TokenData, now: number, onSelect: (t: TokenData) => void }) => {
    const getTimeAge = (timestamp: number) => {
        const seconds = Math.floor((now - timestamp) / 1000);
        if (seconds < 60) return `${seconds}s`;
        if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
        return `${Math.floor(seconds / 3600)}h`;
    };

    const lastPrice = token.ohlcv1h && token.ohlcv1h.length > 0 ? token.ohlcv1h[token.ohlcv1h.length - 1].close : 0;
    const prevPrice = token.ohlcv1h && token.ohlcv1h.length > 1 ? token.ohlcv1h[token.ohlcv1h.length - 2].close : lastPrice;
    const isUp = lastPrice >= prevPrice;
    const p1h = token.events?.["1h"]?.priceChangePercentage || 0;

    return (
        <TableRow
            component={motion.tr}
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            sx={{
                '&:hover': { bgcolor: 'rgba(255,255,255,0.02)' },
                cursor: 'pointer',
                transition: 'background 0.2s',
                borderBottom: '1px solid rgba(255, 255, 255, 0.05)'
            }}
            onClick={() => onSelect(token)}
        >
            <TableCell sx={{ border: 'none', py: 1.5 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: { xs: 1, md: 2 } }}>
                    <Avatar
                        src={token.image}
                        variant="rounded"
                        sx={{ width: { xs: 36, md: 44 }, height: { xs: 36, md: 44 }, borderRadius: '10px' }}
                    />
                    <Box>
                        <Typography variant="body2" sx={{ fontWeight: 700, fontSize: { xs: '0.8rem', md: '0.9rem' } }}>{token.name}</Typography>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                            <Typography variant="caption" sx={{ color: 'rgba(161, 161, 170, 1)', fontFamily: 'JetBrains Mono' }}>
                                {token.symbol} · <span style={{ color: '#22c55e' }}>{token.mint.slice(0, 4)}</span>
                            </Typography>
                        </Box>
                    </Box>
                </Box>
            </TableCell>

            <TableCell sx={{ border: 'none' }}>
                <Box sx={{ display: 'flex', alignItems: 'flex-start', flexDirection: 'column' }}>
                    <Typography variant="body2" sx={{ fontWeight: 700 }}>
                        ${token.mcapUsd > 1000 ? `${(token.mcapUsd / 1000).toFixed(1)}k` : token.mcapUsd.toFixed(0)}
                    </Typography>
                    <Chip
                        label={`${p1h > 0 ? '+' : ''}${p1h.toFixed(1)}%`}
                        size="small"
                        sx={{
                            height: 14,
                            fontSize: '0.55rem',
                            fontWeight: 800,
                            bgcolor: p1h >= 0 ? 'rgba(34, 197, 94, 0.1)' : 'rgba(239, 68, 68, 0.1)',
                            color: p1h >= 0 ? '#22c55e' : '#ef4444',
                            mt: 0.3
                        }}
                    />
                </Box>
            </TableCell>

            {/* Mini Chart Cell with Volume */}
            <TableCell sx={{ border: 'none' }} className="hide-mobile">
                <Box sx={{ width: 120, height: 40 }}>
                    <ResponsiveContainer width="100%" height="100%">
                        <ComposedChart data={token.ohlcv1h?.slice(-20) || []}>
                            <defs>
                                <linearGradient id={`gradient-${token.mint}`} x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor={isUp ? "#22c55e" : "#ef4444"} stopOpacity={0.3} />
                                    <stop offset="95%" stopColor={isUp ? "#22c55e" : "#ef4444"} stopOpacity={0} />
                                </linearGradient>
                            </defs>
                            <Bar
                                dataKey="volume"
                                yAxisId="vol"
                                fill="rgba(255,255,255,0.05)"
                                barSize={4}
                            />
                            <Area
                                type="monotone"
                                dataKey="close"
                                stroke={isUp ? "#22c55e" : "#ef4444"}
                                fillOpacity={1}
                                fill={`url(#gradient-${token.mint})`}
                                strokeWidth={2}
                            />
                            <YAxis hide domain={['auto', 'auto']} />
                            <YAxis yAxisId="vol" hide domain={[0, 'auto']} />
                        </ComposedChart>
                    </ResponsiveContainer>
                </Box>
            </TableCell>

            <TableCell sx={{ border: 'none' }} className="hide-mobile">
                <Box sx={{ width: 100 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                        <Typography variant="caption" sx={{ fontSize: '0.65rem', fontWeight: 600, color: token.curvePct > 80 ? '#ef4444' : '#22c55e' }}>
                            {token.curvePct?.toFixed(0)}%
                        </Typography>
                    </Box>
                    <Box sx={{ width: '100%', height: 4, bgcolor: 'rgba(255,255,255,0.05)', borderRadius: 2 }}>
                        <Box sx={{
                            width: `${token.curvePct}%`,
                            height: '100%',
                            bgcolor: token.curvePct > 80 ? '#ef4444' : '#22c55e',
                            transition: 'width 0.5s ease'
                        }} />
                    </Box>
                </Box>
            </TableCell>

            <TableCell sx={{ border: 'none' }} className="hide-mobile">
                <Box sx={{ display: 'flex', gap: 1 }}>
                    {token.hasTwitter && <TwitterIcon size={14} color="#1da1f2" />}
                    {token.hasTelegram && <TelegramIcon size={14} color="#0088cc" />}
                    {token.hasWebsite && <WebIcon size={14} color="#ffffff" />}
                </Box>
            </TableCell>

            <TableCell sx={{ border: 'none' }}>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.1 }}>
                    <Typography variant="caption" sx={{ fontWeight: 600, color: token.devPct > 5 ? '#ef4444' : 'rgba(161, 161, 170, 1)' }}>
                        DEV: {token.devPct.toFixed(1)}%
                    </Typography>
                    <Typography variant="caption" sx={{ fontWeight: 600, color: token.sniperPct > 15 ? '#f59e0b' : 'rgba(161, 161, 170, 1)' }}>
                        SNIPER: {token.sniperPct.toFixed(1)}%
                    </Typography>
                </Box>
            </TableCell>

            <TableCell sx={{ border: 'none', textAlign: 'right' }}>
                <Chip
                    label={token.launchpad.toUpperCase()}
                    size="small"
                    sx={{
                        height: 18, fontSize: '0.6rem', fontWeight: 800, mb: 0.5,
                        bgcolor: 'rgba(255,255,255,0.05)', color: 'rgba(161, 161, 170, 1)'
                    }}
                />
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5, color: 'rgba(161, 161, 170, 1)' }}>
                    <ClockIcon size={10} />
                    <Typography sx={{ fontSize: '0.7rem', fontWeight: 500 }}>{getTimeAge(token.createdTime)}</Typography>
                </Box>
            </TableCell>
        </TableRow>
    );
});

import { io, Socket } from 'socket.io-client';

const NewTable = () => {
    const [tokens, setTokens] = useState<TokenData[]>([]);
    const [connected, setConnected] = useState(false);
    const socketRef = useRef<Socket | null>(null);
    const tokensRef = useRef<TokenData[]>([]);
    const bufferRef = useRef<TokenData[]>([]);
    const [now, setNow] = useState(Date.now());
    const [selectedToken, setSelectedToken] = useState<TokenData | null>(null);
    const [platformFilter, setPlatformFilter] = useState<string>('all');

    useEffect(() => {
        let isActive = true;
        const BACKEND_URL = `http://${window.location.hostname}:3000`;
        const socket = io(BACKEND_URL);
        socketRef.current = socket;

        socket.on('connect', () => {
            if (isActive) {
                setConnected(true);
                socket.emit('join', 'latest-tokens');
                socket.emit('join', 'graduating');
                socket.emit('join', 'price-by-token');
                console.log('[SOCKET] Connected and joined rooms');
            }
        });

        socket.on('message', (payload) => {
            if (!isActive) return;
            try {
                // Handle different message types from the new multi-room system
                let data = null;

                if (payload.type === 'new-token') {
                    // Extract data from the nested structure if needed
                    data = payload.data.data ? payload.data.data : payload.data;
                } else if (payload.type === 'price-update') {
                    // We can handle price updates separately if we want to be more efficient
                    // For now, the 'new-token' (latest-tokens) room still sends full updates
                    return;
                } else {
                    return;
                }

                if (data && data.token) {
                    const token = data.token;
                    const pool = data.pools[0];
                    const risk = data.risk;

                    const tokenData: TokenData = {
                        mint: token.mint,
                        name: token.name,
                        symbol: token.symbol,
                        image: token.image,
                        mcapUsd: pool.marketCap.usd,
                        volumeSol: pool.txns.volume / (data.solPrice || 1),
                        curvePct: pool.curvePercentage,
                        launchpad: pool.market,
                        createdTime: token.creation.created_time * 1000,
                        hasTwitter: !!token.strictSocials.twitter,
                        hasTelegram: !!token.strictSocials.telegram,
                        hasWebsite: !!token.strictSocials.website,
                        devPct: risk.dev.percentage,
                        top10Pct: risk.top10,
                        sniperPct: risk.snipers.totalPercentage,
                        signature: token.creation.created_tx,
                        priceHistory: data.priceHistory,
                        events: data.events
                    };
                    tokenData.ohlcv1h = tokenData.priceHistory ? getCandleData(tokenData.priceHistory, '1m') : [];
                    bufferRef.current.push(tokenData);
                }
            } catch (e) {
                console.error('[SOCKET] Error processing message:', e);
            }
        });

        socket.on('disconnect', () => {
            if (isActive) setConnected(false);
        });

        const batchInterval = setInterval(() => {
            if (bufferRef.current.length > 0) {
                const newTokens = [...tokensRef.current];
                bufferRef.current.forEach(incoming => {
                    const idx = newTokens.findIndex(t => t.mint === incoming.mint);
                    if (idx !== -1) newTokens[idx] = { ...newTokens[idx], ...incoming };
                    else newTokens.push(incoming);
                });
                bufferRef.current = [];
                newTokens.sort((a, b) => b.createdTime - a.createdTime);
                if (newTokens.length > 50) newTokens.length = 50;
                tokensRef.current = newTokens;
                setTokens(newTokens);
            }
        }, 100);

        fetch(`http://${window.location.hostname}:3000/api/tokens?limit=20`)
            .then(res => res.json())
            .then(data => {
                if (!isActive) return;
                const formatted = data.map((p: any) => ({
                    mint: p.data.token.mint,
                    name: p.data.token.name,
                    symbol: p.data.token.symbol,
                    image: p.data.token.image,
                    mcapUsd: p.data.pools[0].marketCap.usd,
                    volumeSol: p.data.pools[0].txns.volume,
                    curvePct: p.data.pools[0].curvePercentage,
                    launchpad: p.data.pools[0].market,
                    createdTime: p.data.token.creation.created_time * 1000,
                    hasTwitter: !!p.data.token.strictSocials.twitter,
                    hasTelegram: !!p.data.token.strictSocials.telegram,
                    hasWebsite: !!p.data.token.strictSocials.website,
                    devPct: p.data.risk.dev.percentage,
                    top10Pct: p.data.risk.top10,
                    sniperPct: p.data.risk.snipers.totalPercentage,
                    signature: p.data.token.creation.created_tx,
                    priceHistory: p.data.priceHistory,
                    ohlcv1h: p.data.priceHistory ? getCandleData(p.data.priceHistory, '1m') : [],
                    events: p.data.events
                }));
                formatted.sort((a: any, b: any = {}) => (b.createdTime || 0) - (a.createdTime || 0));
                tokensRef.current = formatted;
                setTokens(formatted);
            }).catch(() => { });

        return () => {
            isActive = false;
            clearInterval(batchInterval);
            socket.disconnect();
        };
    }, []);

    useEffect(() => {
        const timer = setInterval(() => setNow(Date.now()), 1000);
        return () => clearInterval(timer);
    }, []);

    return (
        <Box sx={{ p: { xs: 1.5, md: 4 }, maxWidth: 1200, margin: '0 auto' }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', mb: 3 }}>
                <Box>
                    <Typography variant="h5" sx={{ fontWeight: 800, letterSpacing: '-0.03em', display: 'flex', alignItems: 'center', gap: 1.5 }}>
                        <Box sx={{ bgcolor: 'var(--accent-color)', p: 0.5, borderRadius: '8px', display: 'flex' }}>
                            <TrendingUp size={20} color="black" />
                        </Box>
                        Terminal Stream
                    </Typography>
                    <Box sx={{ display: 'flex', alignItems: 'center', mt: 1 }}>
                        <div className="live-indicator"></div>
                        <Typography variant="caption" sx={{ color: 'var(--text-secondary)', fontWeight: 600, fontSize: '0.65rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                            {connected ? 'Live - 100ms Sync' : 'Reconnecting...'}
                        </Typography>
                    </Box>
                </Box>
                <Box sx={{ display: 'flex', gap: 1.5, alignItems: 'center' }}>
                    <Box sx={{ position: 'relative' }}>
                        <Chip
                            icon={<FilterIcon size={12} />}
                            label={platformFilter.toUpperCase()}
                            onClick={() => { }} // Could add a menu here
                            variant="outlined"
                            size="small"
                            sx={{
                                borderColor: 'rgba(255,255,255,0.1)',
                                color: 'rgba(161, 161, 170, 1)',
                                fontWeight: 600,
                                fontSize: '0.65rem',
                                '&:hover': { bgcolor: 'rgba(255,255,255,0.05)' }
                            }}
                        />
                        {/* Quick toggle between all -> pump -> moonshot */}
                        <IconButton
                            size="small"
                            onClick={() => {
                                const transitions: Record<string, string> = { all: 'pump', pump: 'moonshot', moonshot: 'letsbonk', letsbonk: 'all' };
                                setPlatformFilter(transitions[platformFilter] || 'all');
                            }}
                            sx={{ color: 'var(--accent-color)', ml: 0.5 }}
                        >
                            <ChevronDownIcon size={14} />
                        </IconButton>
                    </Box>
                    <Chip icon={<RefreshCw size={14} />} label={`${tokens.length} Active`} variant="outlined" sx={{ borderColor: 'rgba(255,255,255,0.1)', color: 'rgba(161, 161, 170, 1)', fontWeight: 600 }} />
                </Box>
            </Box>

            <TableContainer component={Paper} className="glass-card" sx={{ background: 'transparent', boxShadow: 'none', overflow: 'hidden' }}>
                <Table size="small">
                    <TableHead sx={{ bgcolor: 'rgba(255,255,255,0.01)' }}>
                        <TableRow>
                            <TableCell sx={{ color: 'rgba(161, 161, 170, 1)', fontWeight: 700, fontSize: '0.65rem', border: 'none' }}>ASSET</TableCell>
                            <TableCell sx={{ color: 'rgba(161, 161, 170, 1)', fontWeight: 700, fontSize: '0.65rem', border: 'none' }}>VALUATION</TableCell>
                            <TableCell className="hide-mobile" sx={{ color: 'rgba(161, 161, 170, 1)', fontWeight: 700, fontSize: '0.65rem', border: 'none' }}>CHART</TableCell>
                            <TableCell className="hide-mobile" sx={{ color: 'rgba(161, 161, 170, 1)', fontWeight: 700, fontSize: '0.65rem', border: 'none' }}>CURVE</TableCell>
                            <TableCell className="hide-mobile" sx={{ color: 'rgba(161, 161, 170, 1)', fontWeight: 700, fontSize: '0.65rem', border: 'none' }}>SOCIALS</TableCell>
                            <TableCell sx={{ color: 'rgba(161, 161, 170, 1)', fontWeight: 700, fontSize: '0.65rem', border: 'none' }}>RISK</TableCell>
                            <TableCell sx={{ color: 'rgba(161, 161, 170, 1)', fontWeight: 700, fontSize: '0.65rem', border: 'none', textAlign: 'right' }}>AGE</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        <AnimatePresence initial={false}>
                            {tokens
                                .filter(t => platformFilter === 'all' || t.launchpad === platformFilter)
                                .slice(0, 50)
                                .map((token) => (
                                    <TokenRow key={token.mint} token={token} now={now} onSelect={setSelectedToken} />
                                ))
                            }
                        </AnimatePresence>
                    </TableBody>
                </Table>
                {tokens.length === 0 && (
                    <Box sx={{ textAlign: 'center', py: 10 }}>
                        <Typography variant="body2" sx={{ color: 'rgba(161, 161, 170, 1)', fontWeight: 600 }}>
                            <RefreshCw size={24} style={{ display: 'block', margin: '0 auto 16px', opacity: 0.3 }} className="spin-slow" />
                            Waiting for new launches...
                        </Typography>
                    </Box>
                )}
            </TableContainer>

            {/* Detail Modal */}
            <Modal
                open={!!selectedToken}
                onClose={() => setSelectedToken(null)}
                closeAfterTransition
                BackdropComponent={Backdrop}
                BackdropProps={{ timeout: 500, sx: { backdropFilter: 'blur(8px)', bgcolor: 'rgba(0,0,0,0.8)' } }}
            >
                <Fade in={!!selectedToken}>
                    <Box sx={{
                        position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%, -50%)',
                        width: { xs: '95%', md: 800 }, bgcolor: '#0a0a0a', border: '1px solid rgba(255,255,255,0.1)',
                        boxShadow: 24, p: { xs: 2, md: 4 }, borderRadius: '24px', outline: 'none'
                    }}>
                        {selectedToken && (
                            <>
                                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 3 }}>
                                    <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
                                        <Avatar src={selectedToken.image} variant="rounded" sx={{ width: 64, height: 64, borderRadius: '16px' }} />
                                        <Box>
                                            <Typography variant="h5" sx={{ fontWeight: 800 }}>{selectedToken.name} ({selectedToken.symbol})</Typography>
                                            <Typography variant="caption" sx={{ color: 'rgba(161, 161, 170, 1)', fontFamily: 'JetBrains Mono' }}>{selectedToken.mint}</Typography>
                                        </Box>
                                    </Box>
                                    <IconButton onClick={() => window.open(`https://solscan.io/token/${selectedToken.mint}`, '_blank')} sx={{ color: '#22c55e' }}>
                                        <MaximizeIcon size={20} />
                                    </IconButton>
                                </Box>

                                <Box sx={{ height: 400, width: '100%', mb: 4, bgcolor: 'rgba(0,0,0,0.2)', borderRadius: '16px', p: 2, border: '1px solid rgba(255,255,255,0.05)' }}>
                                    <ResponsiveContainer width="100%" height="100%">
                                        <AreaChart data={selectedToken.ohlcv1h || []}>
                                            <defs>
                                                <linearGradient id="modalGradient" x1="0" y1="0" x2="0" y2="1">
                                                    <stop offset="5%" stopColor="#22c55e" stopOpacity={0.3} />
                                                    <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
                                                </linearGradient>
                                            </defs>
                                            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" vertical={false} />
                                            <XAxis
                                                dataKey="timestamp"
                                                tickFormatter={(t: number) => new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                                                axisLine={false}
                                                tickLine={false}
                                                tick={{ fill: 'rgba(161, 161, 170, 1)', fontSize: 10 }}
                                            />
                                            <YAxis
                                                hide
                                                domain={['auto', 'auto']}
                                            />
                                            <Tooltip
                                                contentStyle={{ backgroundColor: '#141416', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '12px' }}
                                                itemStyle={{ color: '#22c55e' }}
                                                labelFormatter={(t: any) => new Date(t).toLocaleString()}
                                                formatter={(value: any) => [value ? `$${value.toFixed(8)}` : '$0', 'Price']}
                                            />
                                            <Area type="monotone" dataKey="close" stroke="#22c55e" fillOpacity={1} fill="url(#modalGradient)" strokeWidth={3} />
                                        </AreaChart>
                                    </ResponsiveContainer>
                                </Box>

                                <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 2 }}>
                                    <Box sx={{ p: 2, bgcolor: 'rgba(255,255,255,0.02)', borderRadius: '12px', border: '1px solid rgba(255,255,255,0.05)' }}>
                                        <Typography variant="caption" sx={{ color: 'rgba(161, 161, 170, 1)', display: 'block', mb: 0.5 }}>MARKET CAP</Typography>
                                        <Typography variant="h6" sx={{ fontWeight: 700 }}>${selectedToken.mcapUsd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</Typography>
                                    </Box>
                                    <Box sx={{ p: 2, bgcolor: 'rgba(255,255,255,0.02)', borderRadius: '12px', border: '1px solid rgba(255,255,255,0.05)' }}>
                                        <Typography variant="caption" sx={{ color: 'rgba(161, 161, 170, 1)', display: 'block', mb: 0.5 }}>VOLUME (SOL)</Typography>
                                        <Typography variant="h6" sx={{ fontWeight: 700 }}>{selectedToken.volumeSol.toFixed(2)}</Typography>
                                    </Box>
                                    <Box sx={{ p: 2, bgcolor: 'rgba(255,255,255,0.02)', borderRadius: '12px', border: '1px solid rgba(255,255,255,0.05)' }}>
                                        <Typography variant="caption" sx={{ color: 'rgba(161, 161, 170, 1)', display: 'block', mb: 0.5 }}>CURVE PROGRESS</Typography>
                                        <Typography variant="h6" sx={{ fontWeight: 700, color: '#22c55e' }}>{selectedToken.curvePct.toFixed(1)}%</Typography>
                                    </Box>
                                </Box>
                            </>
                        )}
                    </Box>
                </Fade>
            </Modal>
        </Box>
    );
};

export default NewTable;
