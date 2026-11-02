// =====================================================
// GoldenHour - emergency response server (v2)
// ONE source of truth: every emergency lives here with a stage and a timeline.
// Every change calls broadcastUpdate(), so all screens always agree.
// =====================================================
const express = require('express');
const http = require('http');
const fs = require('fs');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { pingInterval: 5000, pingTimeout: 5000 });   // heartbeat
const PORT = process.env.PORT || 3000;

// ---------- Settings ----------
const RADIUS_KM = 100;
const MERGE_DISTANCE_KM = 0.2;
const MERGE_WINDOW_MS = 10 * 60 * 1000;
const MAX_SOS_PER_MINUTE = 3;
const MAX_STRIKES = 2;
const HOSPITAL_RESPONSE_MS = 45000;             // no answer in 45 s -> next hospital
const VOLUNTEER_CODE = process.env.VOLUNTEER_CODE || 'GOLD123';
const BLOOD_GROUPS = ['A+', 'A-', 'B+', 'B-', 'O+', 'O-', 'AB+', 'AB-'];
const TYPES = ['accident', 'blood', 'ambulance', 'other'];

app.use(express.static('public'));

// ---------- In-memory notebooks ----------
const hospitals = {};
const volunteers = {};     // socket.id -> { name, lat, lng }
const incidents = {};
const seenReports = {};
const strikes = {};
const hits = {};
let nextIncident = 1;

// ---------- Helpers ----------
const isNum = (v) => typeof v === 'number' && isFinite(v);
const isLoc = (d) => d && isNum(d.lat) && isNum(d.lng) && Math.abs(d.lat) <= 90 && Math.abs(d.lng) <= 180;

// HAVERSINE distance in km
function distanceKm(lat1, lng1, lat2, lng2) {
  const R = 6371, rad = (x) => (x * Math.PI) / 180;
  const dLat = rad(lat2 - lat1), dLng = rad(lng2 - lng1);
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(rad(lat1)) * Math.cos(rad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// RATE LIMITING (sliding window)
function isRateLimited(key, max, windowMs) {
  const now = Date.now();
  hits[key] = (hits[key] || []).filter((t) => now - t < windowMs);
  if (hits[key].length >= max) return true;
  hits[key].push(now);
  return false;
}

function cleanStock(s) {
  const out = { beds: 5, ambulances: 2, blood: {} };
  BLOOD_GROUPS.forEach((g) => (out.blood[g] = 5));
  if (!s) return out;
  const n = (v) => Math.max(0, Math.min(999, parseInt(v, 10) || 0));
  if (s.beds !== undefined) out.beds = n(s.beds);
  if (s.ambulances !== undefined) out.ambulances = n(s.ambulances);
  if (s.blood) BLOOD_GROUPS.forEach((g) => { if (s.blood[g] !== undefined) out.blood[g] = n(s.blood[g]); });
  return out;
}

function makeHospital(id, name, lat, lng, stock, socketId) {
  return { id, name, lat, lng, ...cleanStock(stock), socketId: socketId || null };
}

function addEvent(inc, text) { inc.events.push({ t: Date.now(), text }); }

// What clients are allowed to see
function publicIncident(i) {
  return {
    id: i.id, type: i.type, bloodGroup: i.bloodGroup, lat: i.lat, lng: i.lng,
    createdAt: i.createdAt, status: i.status, stage: i.stage, reportCount: i.reportCount,
    hospitalId: i.hospitalId, hospitalName: i.hospitalName, hospitalKm: i.hospitalKm,
    volunteers: i.volunteers.map((v) => ({ name: v.name, state: v.state })),
    fakeVotes: i.fakeVotes.length, events: i.events.slice(-15)
  };
}
function statusFor(i) { return { ...publicIncident(i), incidentId: i.id, volunteersNotified: i.notified }; }

// NEAREST SUITABLE HOSPITAL (skips hospitals that lack capacity or already declined)
function findHospital(type, bloodGroup, lat, lng, exclude) {
  let best = null, bestKm = Infinity;
  for (const id in hospitals) {
    if (exclude && exclude.includes(id)) continue;
    const h = hospitals[id];
    let ok;
    if (type === 'blood') ok = bloodGroup ? h.blood[bloodGroup] > 0 : Object.values(h.blood).some((n) => n > 0);
    else if (type === 'ambulance') ok = h.ambulances > 0;
    else ok = h.beds > 0;
    if (!ok) continue;
    const km = distanceKm(lat, lng, h.lat, h.lng);
    if (km < bestKm) { best = h; bestKm = km; }
  }
  return best ? { hospital: best, km: bestKm } : null;
}

function setHospital(inc, r) {
  clearTimeout(inc.timer);
  inc.hospitalId = r.hospital.id;
  inc.hospitalName = r.hospital.name;
  inc.hospitalKm = r.km;
  inc.stage = 'reported';
  addEvent(inc, 'Hospital notified: ' + r.hospital.name + ' (' + r.km.toFixed(1) + ' km)');
  console.log(inc.id + ' -> ' + r.hospital.name);
  // No answer in time? Move to the next hospital.
  inc.timer = setTimeout(() => escalate(inc), HOSPITAL_RESPONSE_MS);
  // Demo hospitals have no screen, so they answer by themselves
  if (!r.hospital.socketId) {
    const hid = r.hospital.id;
    setTimeout(() => applyHospitalAction(inc, hid, 'accept'), 4000);
    setTimeout(() => applyHospitalAction(inc, hid, 'dispatch'), 9000);
  }
}

function clearHospital(inc) {
  clearTimeout(inc.timer);
  inc.hospitalId = null; inc.hospitalName = null; inc.hospitalKm = null; inc.stage = 'reported';
}

function assignHospital(inc) {
  const r = findHospital(inc.type, inc.bloodGroup, inc.lat, inc.lng, inc.declined);
  if (r) setHospital(inc, r);
  return !!r;
}

function escalate(inc) {
  if (inc.status !== 'active' || inc.stage !== 'reported') return;
  const old = inc.hospitalId;
  const r = findHospital(inc.type, inc.bloodGroup, inc.lat, inc.lng, [...inc.declined, old]);
  if (!r) {
    addEvent(inc, 'No other hospital available, still waiting for ' + inc.hospitalName);
    return broadcastUpdate(inc);
  }
  inc.declined.push(old);
  const oldH = hospitals[old];
  if (oldH && oldH.socketId) io.to(oldH.socketId).emit('incident-withdrawn', { id: inc.id });
  addEvent(inc, inc.hospitalName + ' did not respond. Moving to the next hospital');
  setHospital(inc, r);
  notifyHospital(inc, 'incoming-alert');
  notifyReporters(inc);
  notifyVolunteers(inc);
}

// All hospital actions go through ONE function, so rules are enforced in one place
function applyHospitalAction(inc, hid, action) {
  if (!inc || inc.status !== 'active' || inc.hospitalId !== hid) return;
  const name = inc.hospitalName;
  if (action === 'accept' && inc.stage === 'reported') {
    clearTimeout(inc.timer);
    inc.stage = 'accepted';
    addEvent(inc, name + ' accepted and is preparing');
  } else if (action === 'dispatch' && inc.stage === 'accepted') {
    inc.stage = 'dispatched';
    addEvent(inc, 'Ambulance dispatched by ' + name);
  } else if (action === 'resolve') {
    clearTimeout(inc.timer);
    inc.status = 'resolved'; inc.stage = 'resolved';
    addEvent(inc, 'Resolved by ' + name);
    saveLog();
  } else if (action === 'decline' && inc.stage === 'reported') {
    clearTimeout(inc.timer);
    inc.declined.push(hid);
    addEvent(inc, name + ' declined (no capacity)');
    const h = hospitals[hid];
    if (h && h.socketId) io.to(h.socketId).emit('incident-withdrawn', { id: inc.id });
    clearHospital(inc);
    if (assignHospital(inc)) notifyHospital(inc, 'incoming-alert');
    else addEvent(inc, 'No other hospital available right now');
  } else return;
  broadcastUpdate(inc);
}

function volunteersInRange(inc) {
  const list = [];
  for (const vid in volunteers) {
    const v = volunteers[vid];
    if (v.lat === null) continue;
    const km = distanceKm(inc.lat, inc.lng, v.lat, v.lng);
    if (km <= RADIUS_KM) list.push({ id: vid, km });
  }
  return list;
}

function notifyHospital(inc, event) {
  const h = inc.hospitalId && hospitals[inc.hospitalId];
  if (h && h.socketId) io.to(h.socketId).emit(event, publicIncident(inc));
}
function notifyReporters(inc) {
  inc.deviceIds.forEach((d) => io.to('dev:' + d).emit('incident-status', statusFor(inc)));
}
function notifyVolunteers(inc) {
  // everyone who was ever alerted + anyone currently in range, so nobody keeps a stale card
  const ids = new Set(inc.alerted);
  volunteersInRange(inc).forEach((v) => ids.add(v.id));
  ids.forEach((id) => io.to(id).emit('incident-update', publicIncident(inc)));
}
function broadcastUpdate(inc) {
  notifyHospital(inc, 'incident-update');
  notifyReporters(inc);
  notifyVolunteers(inc);
}

function reassignUnassigned() {
  for (const id in incidents) {
    const inc = incidents[id];
    if (inc.status === 'active' && !inc.hospitalId && assignHospital(inc)) {
      notifyHospital(inc, 'incoming-alert');
      notifyReporters(inc);
      notifyVolunteers(inc);
    }
  }
}

function saveLog() {
  try { fs.writeFileSync('data.json', JSON.stringify(Object.values(incidents).map(publicIncident), null, 2)); }
  catch (e) { console.log('Could not save data.json:', e.message); }
}

// =====================================================
io.on('connection', (socket) => {
  const ip = socket.handshake.address;
  console.log('Client connected: ' + socket.id);

  // ---------- REPORTER ----------
  socket.on('reporter-join', (deviceId) => {
    if (typeof deviceId === 'string' && deviceId.length < 60) socket.join('dev:' + deviceId);
  });

  socket.on('sos', (d, ack) => {
    ack = typeof ack === 'function' ? ack : () => {};

    if (!d || !TYPES.includes(d.type) || !isLoc(d) ||
        typeof d.reportId !== 'string' || typeof d.deviceId !== 'string' ||
        d.reportId.length > 60 || d.deviceId.length > 60) {
      return ack({ ok: false, error: 'Invalid report.' });
    }
    const bloodGroup = BLOOD_GROUPS.includes(d.bloodGroup) ? d.bloodGroup : null;

    // Idempotency: a retried report is never processed twice
    if (seenReports[d.reportId]) {
      return ack({ ok: true, duplicate: true, ...statusFor(incidents[seenReports[d.reportId]]) });
    }
    if ((strikes[d.deviceId] || 0) >= MAX_STRIKES) {
      return ack({ ok: false, error: 'This phone is blocked for sending fake reports.' });
    }
    if (isRateLimited('dev:' + d.deviceId, MAX_SOS_PER_MINUTE, 60000) || isRateLimited('ip:' + ip, 10, 60000)) {
      return ack({ ok: false, error: 'Too many reports. Please wait a minute.' });
    }

    socket.join('dev:' + d.deviceId);
    const now = Date.now();

    // DUPLICATE MERGING
    let inc = Object.values(incidents).find((i) =>
      i.status === 'active' && i.type === d.type && now - i.createdAt < MERGE_WINDOW_MS &&
      distanceKm(i.lat, i.lng, d.lat, d.lng) <= MERGE_DISTANCE_KM);

    if (inc) {
      inc.reportCount++;
      if (!inc.deviceIds.includes(d.deviceId)) inc.deviceIds.push(d.deviceId);
      seenReports[d.reportId] = inc.id;
      addEvent(inc, 'Another report received (' + inc.reportCount + ' reports)');
      broadcastUpdate(inc);
      return ack({ ok: true, merged: true, ...statusFor(inc) });
    }

    inc = {
      id: 'INC-' + nextIncident++, type: d.type, bloodGroup, lat: d.lat, lng: d.lng,
      createdAt: now, status: 'active', stage: 'reported', reportCount: 1,
      deviceIds: [d.deviceId], volunteers: [], fakeVotes: [], alerted: [], declined: [], events: [],
      hospitalId: null, hospitalName: null, hospitalKm: null, notified: 0, timer: null
    };
    incidents[inc.id] = inc;
    seenReports[d.reportId] = inc.id;
    addEvent(inc, 'SOS received: ' + d.type);
    console.log('NEW ' + inc.id + ': ' + inc.type);

    // GEO-RADIUS: who is within 3 km?
    const near = volunteersInRange(inc);
    inc.alerted = near.map((v) => v.id);
    inc.notified = near.length;
    addEvent(inc, near.length ? near.length + ' volunteer(s) alerted within ' + RADIUS_KM + ' km'
                              : 'No volunteers within ' + RADIUS_KM + ' km yet');

    if (assignHospital(inc)) notifyHospital(inc, 'incoming-alert');
    else addEvent(inc, 'No hospital with capacity found yet');

    near.forEach((v) => io.to(v.id).emit('new-alert', { ...publicIncident(inc), distanceKm: v.km }));
    saveLog();
    ack({ ok: true, ...statusFor(inc) });
  });

  socket.on('cancel-sos', (d) => {
    const inc = d && incidents[d.incidentId];
    if (!inc || inc.status !== 'active' || !inc.deviceIds.includes(d.deviceId)) return;
    clearTimeout(inc.timer);
    inc.status = 'cancelled';
    addEvent(inc, 'Cancelled by the reporter');
    saveLog();
    broadcastUpdate(inc);
  });

  // ---------- VOLUNTEER ----------
  socket.on('volunteer-join', (d, ack) => {
    ack = typeof ack === 'function' ? ack : () => {};
    if (!d || d.code !== VOLUNTEER_CODE) return ack({ ok: false, error: 'Wrong volunteer code.' });
    const name = String(d.name || 'Volunteer').trim().slice(0, 30) || 'Volunteer';
    volunteers[socket.id] = { name, lat: null, lng: null };
    ack({ ok: true });
  });

  socket.on('volunteer-location', (loc) => {
    const v = volunteers[socket.id];
    if (!v || !isLoc(loc)) return;
    const first = v.lat === null;
    v.lat = loc.lat; v.lng = loc.lng;
    if (first) {
      const activeIds = [];
      for (const id in incidents) {
        const inc = incidents[id];
        if (inc.status !== 'active') continue;
        const km = distanceKm(inc.lat, inc.lng, v.lat, v.lng);
        if (km > RADIUS_KM) continue;
        activeIds.push(id);
        if (!inc.alerted.includes(socket.id)) inc.alerted.push(socket.id);
        socket.emit('new-alert', { ...publicIncident(inc), distanceKm: km });
      }
      socket.emit('sync-active', activeIds);   // lets the page drop old, finished cards
    }
  });

  socket.on('coming', (d) => {
    const v = volunteers[socket.id];
    const inc = d && incidents[d.incidentId];
    if (!v || !inc || inc.status !== 'active') return;
    if (inc.volunteers.some((x) => x.id === socket.id)) return;
    inc.volunteers.push({ id: socket.id, name: v.name, state: 'coming' });
    addEvent(inc, v.name + ' is on the way');
    broadcastUpdate(inc);
  });

  socket.on('arrived', (d) => {
    const inc = d && incidents[d.incidentId];
    const mv = inc && inc.volunteers.find((x) => x.id === socket.id);
    if (!mv || inc.status !== 'active' || mv.state === 'arrived') return;
    mv.state = 'arrived';
    addEvent(inc, mv.name + ' arrived at the spot');
    broadcastUpdate(inc);
  });

  // FAKE-REPORT CONTROL: needs votes from 2 volunteers (or 1 if only 1 was alerted)
  socket.on('flag-fake', (d) => {
    if (!volunteers[socket.id]) return;
    const inc = d && incidents[d.incidentId];
    if (!inc || inc.status !== 'active' || inc.fakeVotes.includes(socket.id)) return;
    inc.fakeVotes.push(socket.id);
    const needed = Math.min(2, Math.max(1, inc.notified));
    if (inc.fakeVotes.length >= needed) {
      clearTimeout(inc.timer);
      inc.status = 'fake';
      addEvent(inc, 'Marked as a fake report by volunteers');
      inc.deviceIds.forEach((dev) => { strikes[dev] = (strikes[dev] || 0) + 1; });
      saveLog();
    }
    broadcastUpdate(inc);
  });

  // ---------- HOSPITAL ----------
  socket.on('hospital-register', (d, ack) => {
    ack = typeof ack === 'function' ? ack : () => {};
    if (!isLoc(d)) return ack({ ok: false, error: 'Location missing.' });
    const id = 'h-' + socket.id;
    hospitals[id] = makeHospital(id, String(d.name || 'Hospital').trim().slice(0, 40) || 'Hospital',
                                 d.lat, d.lng, d.stock, socket.id);
    socket.hospitalId = id;
    console.log('Hospital registered: ' + hospitals[id].name);
    reassignUnassigned();
    const mine = Object.values(incidents).filter((i) => i.hospitalId === id && i.status === 'active').map(publicIncident);
    ack({ ok: true, incidents: mine });
  });

  socket.on('hospital-update', (s) => {
    const h = hospitals[socket.hospitalId];
    if (!h) return;
    Object.assign(h, cleanStock(s));
    reassignUnassigned();
  });

  socket.on('hospital-action', (d) => {
    const inc = d && incidents[d.incidentId];
    if (!inc || !socket.hospitalId) return;
    applyHospitalAction(inc, socket.hospitalId, d.action);
  });

  socket.on('demo-hospitals', (d, ack) => {
    if (!isLoc(d)) return;
    hospitals['demo-1'] = makeHospital('demo-1', 'Demo City Hospital', d.lat + 0.010, d.lng + 0.005, { beds: 6, ambulances: 2 });
    hospitals['demo-2'] = makeHospital('demo-2', 'Demo Care Centre', d.lat - 0.030, d.lng + 0.020, { beds: 12, ambulances: 3 });
    reassignUnassigned();
    if (typeof ack === 'function') ack({ ok: true });
  });

  // ---------- DISCONNECT ----------
  socket.on('disconnect', () => {
    console.log('Client disconnected: ' + socket.id);
    delete volunteers[socket.id];
    if (socket.hospitalId) {
      const hid = socket.hospitalId;
      delete hospitals[hid];
      for (const id in incidents) {
        const i = incidents[id];
        if (i.hospitalId === hid && i.status === 'active') {
          addEvent(i, i.hospitalName + ' went offline');
          clearHospital(i);
        }
      }
      reassignUnassigned();
    }
  });
});

server.listen(PORT, () => console.log('GoldenHour running at http://localhost:' + PORT));
