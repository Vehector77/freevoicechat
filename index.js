const http = require('http');
const { WebSocketServer } = require('ws');

const port = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('FreeVoiceChat Signaling Server Online ✓');
});

const wss = new WebSocketServer({ server });

// Map: serverAddress -> Set<ClientSocket>
const serverRooms = new Map();
// Map: serverAddress -> Map<groupId, GroupObject>
const serverGroups = new Map();

function broadcastGroupList(serverAddress) {
    if (!serverRooms.has(serverAddress)) return;
    const room = serverRooms.get(serverAddress);
    const groupsMap = serverGroups.get(serverAddress) || new Map();

    const groupsList = Array.from(groupsMap.values()).map(g => ({
        id: g.id,
        name: g.name,
        hasPassword: g.hasPassword,
        owner: g.owner,
        members: Array.from(g.members.values())
    }));

    const message = JSON.stringify({
        type: 'group_list',
        groups: groupsList
    });

    for (const client of room) {
        if (client.readyState === 1) {
            client.send(message);
        }
    }
}

function broadcastGroupUpdate(serverAddress, groupObj) {
    if (!serverRooms.has(serverAddress)) return;
    const room = serverRooms.get(serverAddress);
    const updateMsg = JSON.stringify({
        type: 'group_update',
        group: {
            id: groupObj.id,
            name: groupObj.name,
            hasPassword: groupObj.hasPassword,
            owner: groupObj.owner,
            members: Array.from(groupObj.members.values())
        }
    });

    for (const client of room) {
        if (client.readyState === 1 && groupObj.members.has(client.uuid)) {
            client.send(updateMsg);
        }
    }
}

function leaveCurrentGroup(ws) {
    if (!ws.room || !serverGroups.has(ws.room) || !ws.currentGroupId) return;
    const groups = serverGroups.get(ws.room);
    const groupObj = groups.get(ws.currentGroupId);
    if (groupObj) {
        groupObj.members.delete(ws.uuid);
        if (groupObj.members.size === 0) {
            groups.delete(ws.currentGroupId);
        } else {
            if (groupObj.owner === ws.uuid) {
                groupObj.owner = groupObj.members.keys().next().value;
            }
            broadcastGroupUpdate(ws.room, groupObj);
        }
    }
    ws.currentGroupId = null;
    broadcastGroupList(ws.room);
}

wss.on('connection', (ws, req) => {
    let clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '127.0.0.1';
    if (clientIp.includes(',')) {
        clientIp = clientIp.split(',')[0].trim();
    }
    if (clientIp.startsWith('::ffff:')) {
        clientIp = clientIp.substring(7);
    }

    ws.clientIp = clientIp;
    ws.room = null;
    ws.uuid = null;
    ws.currentGroupId = null;

    ws.on('message', (message, isBinary) => {
        if (isBinary) {
            // Real-time Audio Relay (Bypasses NAT / CGNAT between different countries)
            if (!ws.room || !serverRooms.has(ws.room)) return;
            const room = serverRooms.get(ws.room);
            for (const client of room) {
                if (client !== ws && client.readyState === ws.OPEN) {
                    client.send(message, { binary: true });
                }
            }
            return;
        }

        try {
            const data = JSON.parse(message.toString());

            if (data.type === 'join') {
                const { uuid, name, server, port: udpPort } = data;
                ws.uuid = uuid;
                ws.name = name;
                ws.room = server;
                ws.udpPort = udpPort;

                if (!serverRooms.has(server)) {
                    serverRooms.set(server, new Set());
                }
                const room = serverRooms.get(server);

                const newPeerNotice = JSON.stringify({
                    type: 'peer',
                    uuid: ws.uuid,
                    name: ws.name,
                    ip: ws.clientIp,
                    port: ws.udpPort,
                    muted: ws.muted || false,
                    deafened: ws.deafened || false
                });

                for (const client of room) {
                    if (client !== ws && client.readyState === ws.OPEN) {
                        client.send(newPeerNotice);
                        ws.send(JSON.stringify({
                            type: 'peer',
                            uuid: client.uuid,
                            name: client.name,
                            ip: client.clientIp,
                            port: client.udpPort,
                            muted: client.muted || false,
                            deafened: client.deafened || false
                        }));
                    }
                }

                room.add(ws);
                console.log(`[+] Player joined: ${name} (${uuid}) on ${server}`);

                // Send current groups to player
                broadcastGroupList(server);
            } else if (data.type === 'player_state') {
                const { uuid, muted, deafened } = data;
                ws.muted = !!muted;
                ws.deafened = !!deafened;
                if (!ws.room || !serverRooms.has(ws.room)) return;
                const room = serverRooms.get(ws.room);
                const stateNotice = JSON.stringify({
                    type: 'player_state',
                    uuid,
                    muted: ws.muted,
                    deafened: ws.deafened
                });
                for (const client of room) {
                    if (client !== ws && client.readyState === ws.OPEN) {
                        client.send(stateNotice);
                    }
                }
            } else if (data.type === 'group_create') {
                const { id, name, hasPassword, password } = data;
                if (!ws.room) return;
                leaveCurrentGroup(ws);

                if (!serverGroups.has(ws.room)) {
                    serverGroups.set(ws.room, new Map());
                }
                const groups = serverGroups.get(ws.room);

                const groupObj = {
                    id,
                    name,
                    hasPassword,
                    password: password || '',
                    owner: ws.uuid,
                    members: new Map([[ws.uuid, { uuid: ws.uuid, name: ws.name }]])
                };

                groups.set(id, groupObj);
                ws.currentGroupId = id;

                // Send group_joined confirmation
                ws.send(JSON.stringify({
                    type: 'group_joined',
                    group: {
                        id: groupObj.id,
                        name: groupObj.name,
                        hasPassword: groupObj.hasPassword,
                        owner: groupObj.owner,
                        members: Array.from(groupObj.members.values())
                    }
                }));

                broadcastGroupList(ws.room);
                console.log(`[Group] Created: ${name} (${id}) by ${ws.name}`);
            } else if (data.type === 'group_join') {
                const { id, password } = data;
                if (!ws.room || !serverGroups.has(ws.room)) return;
                const groups = serverGroups.get(ws.room);
                const groupObj = groups.get(id);

                if (!groupObj) {
                    ws.send(JSON.stringify({ type: 'group_error', error: 'El grupo no existe' }));
                    return;
                }

                if (groupObj.hasPassword && groupObj.password !== (password || '')) {
                    ws.send(JSON.stringify({ type: 'group_error', error: 'Contraseña incorrecta' }));
                    return;
                }

                leaveCurrentGroup(ws);

                groupObj.members.set(ws.uuid, { uuid: ws.uuid, name: ws.name });
                ws.currentGroupId = id;

                ws.send(JSON.stringify({
                    type: 'group_joined',
                    group: {
                        id: groupObj.id,
                        name: groupObj.name,
                        hasPassword: groupObj.hasPassword,
                        owner: groupObj.owner,
                        members: Array.from(groupObj.members.values())
                    }
                }));

                broadcastGroupList(ws.room);
                broadcastGroupUpdate(ws.room, groupObj);
                console.log(`[Group] ${ws.name} joined ${groupObj.name}`);
            } else if (data.type === 'group_leave') {
                leaveCurrentGroup(ws);
            }
        } catch (err) {
            console.error('Error handling message:', err);
        }
    });

    ws.on('close', () => {
        if (ws.room) {
            leaveCurrentGroup(ws);

            if (serverRooms.has(ws.room)) {
                const room = serverRooms.get(ws.room);
                room.delete(ws);
                if (room.size === 0) {
                    serverRooms.delete(ws.room);
                    serverGroups.delete(ws.room);
                } else if (ws.uuid) {
                    const leaveNotice = JSON.stringify({ type: 'peer_leave', uuid: ws.uuid });
                    for (const client of room) {
                        if (client.readyState === ws.OPEN) {
                            client.send(leaveNotice);
                        }
                    }
                }
            }
            console.log(`[-] Player left: ${ws.name || ws.uuid}`);
        }
    });
});

server.listen(port, () => {
    console.log(`[FreeVoiceChat Signaling Server] Listening on port ${port}...`);
});
