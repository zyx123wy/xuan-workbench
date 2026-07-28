/* 旋旋的工作台 —— Service Worker：离线缓存 App Shell */
const CACHE = "xuan-wb-v1";
const ASSETS = [
  "./",
  "index.html",
  "manifest.webmanifest",
  "icon.svg",
  "assets/css/style.css",
  "assets/js/app.js"
];

self.addEventListener("install", function (e) {
  e.waitUntil((async function () {
    const c = await caches.open(CACHE);
    await Promise.all(ASSETS.map(function (u) {
      return fetch(u).then(function (r) { return r.ok ? c.put(u, r) : null; }).catch(function () { return null; });
    }));
    await self.skipWaiting();
  })());
});

self.addEventListener("activate", function (e) {
  e.waitUntil((async function () {
    const keys = await caches.keys();
    await Promise.all(keys.filter(function (k) { return k !== CACHE; }).map(function (k) { return caches.delete(k); }));
    await self.clients.claim();
  })());
});

self.addEventListener("fetch", function (e) {
  if (e.request.method !== "GET") return;
  e.respondWith((async function () {
    const cached = await caches.match(e.request);
    if (cached) return cached;
    try {
      const resp = await fetch(e.request);
      if (e.request.url.startsWith(self.location.origin)) {
        const c = await caches.open(CACHE);
        c.put(e.request, resp.clone()).catch(function () {});
      }
      return resp;
    } catch (err) {
      if (e.request.mode === "navigate") {
        const idx = await caches.match("index.html");
        if (idx) return idx;
      }
      throw err;
    }
  })());
});
