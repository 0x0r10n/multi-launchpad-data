import WebSocket from 'ws';

const ws = new WebSocket('ws://localhost:3000/subscribe-newmints');

ws.on('open', () => {
    console.log('✅ Connected to local newmints feed');
});

ws.on('message', (data) => {
    const json = JSON.parse(data.toString());
    console.log('🚀 New Mint Broadcast:', JSON.stringify(json, null, 2));
});

ws.on('error', (e) => console.error('❌ Error:', e));
