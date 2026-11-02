// SERVICE WORKER: a small program the browser runs in the background.
// It sits between the page and the network, so pages still open with no internet.
const CACHE = 'goldenhour-v1';
const FILES = ['/', '/index.html', '/reporter.html', '/volunteer.html', '/hospital.html',
               '/style.css', '/manifest.json', '/icon.svg', '/socket.io/socket.io.js'];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(FILES)));
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  e.waitUntil(caches.keys().then((keys) =>
    Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))));
  self.clients.claim();
});

// NETWORK FIRST, cache as backup: you always see fresh files while online,
// and the saved copy when offline.
self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);
  if (e.request.method !== 'GET') return;
  if (url.pathname.startsWith('/socket.io/') && url.pathname !== '/socket.io/socket.io.js') return; // live connection
  if (url.hostname.endsWith('tile.openstreetmap.org')) return;                                       // map tiles

  e.respondWith(
    fetch(e.request)
      .then((res) => {
        const copy = res.clone();
        caches.open(CACHE).then((c) => c.put(e.request, copy));
        return res;
      })
      .catch(() => caches.match(e.request))
  );
});