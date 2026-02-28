import { PublicKey } from '@solana/web3.js';

export const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');
export const MOONSHOT_PROGRAM_ID = new PublicKey('MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG');
export const RAYDIUM_LAUNCHLAB_PROGRAM = new PublicKey('LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj');
export const METDBC_PROGRAM = new PublicKey('dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN');
export const BAGS_FEE_PROGRAM = new PublicKey('FEE2tBhCKAt7shrod19QttSVREUYPiyMzoku1mL1gqVK');

export const PUMP_CREATE_EVENT_DISCRIMINATOR = Buffer.from('1b72a94ddeeb6376', 'hex');
export const PUMP_TRADE_EVENT_DISCRIMINATOR = Buffer.from('bddb7fd34ee661ee', 'hex');
export const MOONSHOT_CREATE_EVENT_DISCRIMINATOR = Buffer.from('bddbfff4ee6661ee', 'hex');
export const LETSBONK_CREATE_EVENT_DISCRIMINATOR = Buffer.from('97d7e20976a173ae', 'hex');
export const METDBC_CREATE_EVENT_DISCRIMINATOR = Buffer.from('e61ef53aeebf846a', 'hex'); // initialize_virtual_pool

export interface LaunchpadConfig {
    programId: PublicKey;
    createDiscriminator: Buffer;
    tradeDiscriminator?: Buffer;
    parseCreate: (data: Buffer) => any;
    parseTrade?: (data: Buffer) => any;
}

export const LAUNCHPADS: Record<string, LaunchpadConfig> = {
    pump: {
        programId: PUMP_PROGRAM_ID,
        createDiscriminator: PUMP_CREATE_EVENT_DISCRIMINATOR,
        tradeDiscriminator: PUMP_TRADE_EVENT_DISCRIMINATOR,
        parseCreate: parsePumpCreateEvent,
        parseTrade: parsePumpTradeEvent,
    },
    moonshot: {
        programId: MOONSHOT_PROGRAM_ID,
        createDiscriminator: MOONSHOT_CREATE_EVENT_DISCRIMINATOR,
        parseCreate: parseMoonshotCreateEvent,
    },
    letsbonk: {
        programId: RAYDIUM_LAUNCHLAB_PROGRAM,
        createDiscriminator: LETSBONK_CREATE_EVENT_DISCRIMINATOR,
        parseCreate: parseLetsBonkCreateEvent,
    },
    meteora: {
        programId: METDBC_PROGRAM,
        createDiscriminator: METDBC_CREATE_EVENT_DISCRIMINATOR,
        parseCreate: parseMeteoraCreateEvent,
    },
    daosfun: {
        programId: new PublicKey('4FqThZWv3QKWkSyXCDmATpWkpEiCHq5yhkdGWpSEDAZM'),
        createDiscriminator: Buffer.from('0621e6ca365c7930', 'hex'), // global:CreateDao
        parseCreate: parseDaosFunCreateEvent,
    }
};

export function parseDaosFunCreateEvent(data: Buffer) {
    // Preliminary guessing based on common Anchor layouts
    // Usually name, symbol, uri are first
    let offset = 8;
    const readStr = () => {
        if (offset + 4 > data.length) return "";
        const len = data.readUInt32LE(offset); offset += 4;
        if (offset + len > data.length) return "";
        const s = data.slice(offset, offset + len).toString(); offset += len;
        return s;
    };
    const name = readStr();
    const symbol = readStr();
    const uri = readStr();
    // Then often mint and bonding curve
    if (offset + 32 > data.length) return { name, symbol, uri };
    const mint = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    if (offset + 32 > data.length) return { name, symbol, uri, mint };
    const bonding_curve = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    const user = offset + 32 <= data.length ? new PublicKey(data.slice(offset, offset + 32)).toString() : "unknown";
    return { name, symbol, uri, mint, bonding_curve, user };
}

export function parseMeteoraCreateEvent(data: Buffer) {
    let offset = 8;
    const readStr = () => {
        if (offset + 4 > data.length) return "";
        const len = data.readUInt32LE(offset); offset += 4;
        if (offset + len > data.length) return "";
        const s = data.slice(offset, offset + len).toString(); offset += len;
        return s;
    };
    const name = readStr(); const symbol = readStr(); const uri = readStr();
    const mint = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    const bonding_curve = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    const creator = new PublicKey(data.slice(offset, offset + 32)).toString();
    return { name, symbol, uri, mint, bonding_curve, user: creator };
}

export function parseLetsBonkCreateEvent(data: Buffer) {
    let offset = 8; // Based on user's instruction log decode
    const readStr = () => {
        if (offset + 4 > data.length) return "";
        const len = data.readUInt32LE(offset); offset += 4;
        if (offset + len > data.length) return "";
        const s = data.slice(offset, offset + len).toString(); offset += len;
        return s;
    };
    const name = readStr(); const symbol = readStr(); const uri = readStr();
    const mint = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    const bonding_curve = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    const user = new PublicKey(data.slice(offset, offset + 32)).toString();
    return { name, symbol, uri, mint, bonding_curve, user };
}

export function parsePumpCreateEvent(data: Buffer) {
    let offset = 8;
    const readStr = () => {
        if (offset + 4 > data.length) return "";
        const len = data.readUInt32LE(offset); offset += 4;
        if (offset + len > data.length) return "";
        const s = data.slice(offset, offset + len).toString(); offset += len;
        return s;
    };
    const name = readStr(); const symbol = readStr(); const uri = readStr();
    const mint = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    const bonding_curve = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    const user = new PublicKey(data.slice(offset, offset + 32)).toString();
    return { name, symbol, uri, mint, bonding_curve, user };
}

export function parsePumpTradeEvent(data: Buffer) {
    let offset = 8;
    const mint = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    const solAmount = data.readBigUInt64LE(offset); offset += 8;
    const tokenAmount = data.readBigUInt64LE(offset); offset += 8;
    const isBuy = data.readUInt8(offset) === 1; offset += 1;
    const user = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    const timestamp = data.readBigInt64LE(offset); offset += 8;
    const vSol = data.readBigUInt64LE(offset); offset += 8;
    const vToken = data.readBigUInt64LE(offset); offset += 8;
    return { mint, solAmount, tokenAmount, isBuy, user, timestamp, vSol, vToken };
}

export function parseMoonshotCreateEvent(data: Buffer) {
    let offset = 8;
    const readStr = () => {
        const len = data.readUInt32LE(offset); offset += 4;
        const s = data.slice(offset, offset + len).toString(); offset += len;
        return s;
    };
    const name = readStr();
    const symbol = readStr();
    const uri = readStr();
    const mint = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    const bonding_curve = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
    const user = new PublicKey(data.slice(offset, offset + 32)).toString();
    return { name, symbol, uri, mint, bonding_curve, user };
}

export interface PumpCreateEvent {
    name: string;
    symbol: string;
    uri: string;
    mint: string;
    bonding_curve: string;
    user: string;
}

export interface PumpTradeEvent {
    mint: string;
    solAmount: bigint;
    tokenAmount: bigint;
    isBuy: boolean;
    user: string;
    timestamp: bigint;
    vSol: bigint;
    vToken: bigint;
}

export interface MoonshotCreateEvent {
    name: string;
    symbol: string;
    uri: string;
    mint: string;
    bonding_curve: string;
    user: string;
}

export type DetectedEvent =
    | { launchpad: string; type: 'create'; parsed: PumpCreateEvent | MoonshotCreateEvent }
    | { launchpad: string; type: 'trade'; parsed: PumpTradeEvent };

export function detectLaunchpad(programId: string, data: Buffer): DetectedEvent | null {
    for (const [name, config] of Object.entries(LAUNCHPADS)) {
        if (config.programId.toBase58() === programId) {
            const createDisc = config.createDiscriminator;
            if (data.length >= createDisc.length && data.slice(0, createDisc.length).equals(createDisc)) {
                return { launchpad: name, type: 'create', parsed: config.parseCreate(data) };
            }
            if (config.tradeDiscriminator && config.parseTrade) {
                const tradeDisc = config.tradeDiscriminator;
                if (data.length >= tradeDisc.length && data.slice(0, tradeDisc.length).equals(tradeDisc)) {
                    return { launchpad: name, type: 'trade', parsed: config.parseTrade(data) };
                }
            }
        }
    }
    return null;
}
