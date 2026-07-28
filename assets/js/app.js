/* ===================================================================
   旋旋的工作台  —  独立前端应用（无后端，localStorage 持久化）
   模块：每日计划 / 每日噗噗 / 恋爱手记 / 倒数日
=================================================================== */
(function () {
  "use strict";

  /* ===================================================================
     存储层：IndexedDB + JSON 分片
     - 同步内存缓存 cache（wb_key -> JSON 字符串），所有模块沿用 Store.get/set/remove
     - 写回 IndexedDB，按 50MB 自动拆分为多个 JSON 分片文件
     - IndexedDB 不可用时降级到 localStorage（仍按 key 存储）
     - 支持导出/导入真实 .json 分片文件，便于备份与迁移
  =================================================================== */
  var SHARD_MAX = 50 * 1024 * 1024; // 单分片上限 50MB
  var DB_NAME = "xuan_workbench";
  var DB_VER = 1;
  var cache = {};        // wb_key -> JSON 字符串（同步）
  var idb = null;
  var dirty = false;
  var saveTimer = null;

  function encBytes(str) { return new TextEncoder().encode(str).length; }

  function idbOpen() {
    return new Promise(function (res) {
      if (!("indexedDB" in window)) { res(null); return; }
      var r;
      try { r = indexedDB.open(DB_NAME, DB_VER); } catch (e) { res(null); return; }
      r.onupgradeneeded = function (e) {
        var db = e.target.result;
        if (!db.objectStoreNames.contains("kv")) db.createObjectStore("kv");
      };
      r.onsuccess = function (e) { res(e.target.result); };
      r.onerror = function () { res(null); };
    });
  }
  function idbPut(key, val) {
    return new Promise(function (res) {
      if (!idb) { res(); return; }
      try {
        var tx = idb.transaction("kv", "readwrite");
        tx.objectStore("kv").put(val, key);
        tx.oncomplete = function () { res(); };
        tx.onerror = function () { res(); };
      } catch (e) { res(); }
    });
  }
  function idbGet(key) {
    return new Promise(function (res) {
      if (!idb) { res(undefined); return; }
      try {
        var tx = idb.transaction("kv", "readonly");
        var rq = tx.objectStore("kv").get(key);
        rq.onsuccess = function () { res(rq.result); };
        rq.onerror = function () { res(undefined); };
      } catch (e) { res(undefined); }
    });
  }
  function idbDel(key) {
    return new Promise(function (res) {
      if (!idb) { res(); return; }
      try {
        var tx = idb.transaction("kv", "readwrite");
        tx.objectStore("kv").delete(key);
        tx.oncomplete = function () { res(); };
        tx.onerror = function () { res(); };
      } catch (e) { res(); }
    });
  }
  function idbKeys() {
    return new Promise(function (res) {
      if (!idb) { res([]); return; }
      try {
        var tx = idb.transaction("kv", "readonly");
        var rq = tx.objectStore("kv").getAllKeys();
        rq.onsuccess = function () { res(rq.result || []); };
        rq.onerror = function () { res([]); };
      } catch (e) { res([]); }
    });
  }

  // 把 cache 拆分为多个 ≤50MB 的分片（返回 pairs 数组的数组）
  function splitShards() {
    var pairs = Object.keys(cache).map(function (k) { return [k, cache[k]]; });
    var shards = [], cur = [], curBytes = 0;
    pairs.forEach(function (p) {
      var pb = encBytes(JSON.stringify(p));
      if (cur.length && curBytes + pb > SHARD_MAX) { shards.push(cur); cur = []; curBytes = 0; }
      cur.push(p); curBytes += pb;
    });
    if (cur.length) shards.push(cur);
    if (!shards.length) shards.push([]);
    return shards;
  }

  function persistShards() {
    var shards = splitShards();
    var ops = [idbPut("xw_meta", JSON.stringify({ v: 1, shards: shards.length, updated: Date.now() }))];
    shards.forEach(function (sh, i) { ops.push(idbPut("xw_shard_" + i, JSON.stringify({ n: i, pairs: sh }))); });
    return Promise.all(ops).then(function () {
      return idbKeys().then(function (keys) {
        keys.forEach(function (k) {
          if (k.indexOf("xw_shard_") === 0) {
            var idx = parseInt(k.slice(10), 10);
            if (idx >= shards.length) idbDel(k);
          }
        });
      });
    }).then(function () { return shards.length; });
  }

  function loadShards() {
    return idbGet("xw_meta").then(function (metaStr) {
      if (!metaStr) return false;
      var meta = JSON.parse(metaStr), count = meta.shards || 0, chain = Promise.resolve();
      for (var i = 0; i < count; i++) {
        (function (idx) {
          chain = chain.then(function () {
            return idbGet("xw_shard_" + idx).then(function (s) {
              if (s) { (JSON.parse(s).pairs || []).forEach(function (p) { cache[p[0]] = p[1]; }); }
            });
          });
        })(i);
      }
      return chain.then(function () { return true; });
    });
  }

  var Store = {
    get: function (k, fb) {
      var v = cache["wb_" + k];
      if (v === undefined) return fb;
      try { return JSON.parse(v); } catch (e) { return fb; }
    },
    set: function (k, v) {
      var s = JSON.stringify(v);
      cache["wb_" + k] = s;
      if (!idb) { try { localStorage.setItem("wb_" + k, s); } catch (e) {} }
      this._scheduleSave();
    },
    remove: function (k) {
      delete cache["wb_" + k];
      if (!idb) { try { localStorage.removeItem("wb_" + k); } catch (e) {} }
      this._scheduleSave();
    },
    _scheduleSave: function () {
      dirty = true;
      if (saveTimer) clearTimeout(saveTimer);
      saveTimer = setTimeout(function () { Store.flush(); }, 500);
    },
    flush: function () {
      if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
      if (!dirty) return Promise.resolve();
      dirty = false;
      return idb ? persistShards() : Promise.resolve();
    },
    init: function () {
      return idbOpen().then(function (db) {
        idb = db;
        return loadShards();
      }).then(function (had) {
        if (!had) {
          // 首次运行：从旧 localStorage 迁移（如有）
          try {
            for (var i = localStorage.length - 1; i >= 0; i--) {
              var lk = localStorage.key(i);
              if (lk && lk.indexOf("wb_") === 0) { cache[lk] = localStorage.getItem(lk); localStorage.removeItem(lk); }
            }
          } catch (e) {}
          if (Object.keys(cache).length) {
            if (!idb) { // 无 IndexedDB：迁回 localStorage，避免丢失
              Object.keys(cache).forEach(function (k) { try { localStorage.setItem(k, cache[k]); } catch (e) {} });
            } else { Store.flush(); }
          }
        }
        return true;
      });
    },
    // 导出为真实 .json 分片文件（返回 [{n, pairs}]）
    exportShards: function () {
      return splitShards().map(function (sh, i) { return { n: i, pairs: sh }; });
    },
    // 从导入的分片文件还原并合并
    importShards: function (shardObjs) {
      shardObjs.forEach(function (sh) { (sh.pairs || []).forEach(function (p) { cache[p[0]] = p[1]; }); });
      dirty = true;
      return this.flush();
    },
    shardInfo: function () {
      var shards = splitShards();
      var bytes = encBytes(JSON.stringify(shards.map(function (s) { return s; })));
      return { count: shards.length, bytes: bytes };
    }
  };

  /* ---------------- 工具函数 ---------------- */
  function pad(n) { return n < 10 ? "0" + n : "" + n; }
  function todayStr() { var d = new Date(); return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()); }
  function nowTime() { var d = new Date(); return pad(d.getHours()) + ":" + pad(d.getMinutes()); }
  function escapeHtml(s) {
    if (s == null) return "";
    return String(s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2, 7); }
  function daysBetween(aStr, bStr) {
    var a = new Date(aStr + "T00:00:00");
    var b = new Date(bStr + "T00:00:00");
    return Math.round((b - a) / 86400000);
  }
  function toast(msg) {
    var t = document.getElementById("toast");
    t.textContent = msg;
    t.classList.add("show");
    clearTimeout(toast._t);
    toast._t = setTimeout(function () { t.classList.remove("show"); }, 1800);
  }

  /* 图片压缩为 dataURL（限制尺寸，节省存储） */
  function compressImage(file, maxW, cb) {
    if (!file || !file.type || file.type.indexOf("image") !== 0) { cb(null); return; }
    var reader = new FileReader();
    reader.onload = function (e) {
      var img = new Image();
      img.onload = function () {
        var w = img.width, h = img.height;
        if (w > maxW) { h = Math.round(h * maxW / w); w = maxW; }
        var canvas = document.createElement("canvas");
        canvas.width = w; canvas.height = h;
        var ctx = canvas.getContext("2d");
        ctx.drawImage(img, 0, 0, w, h);
        try { cb(canvas.toDataURL("image/jpeg", 0.72)); }
        catch (err) { cb(null); }
      };
      img.onerror = function () { cb(null); };
      img.src = e.target.result;
    };
    reader.onerror = function () { cb(null); };
    reader.readAsDataURL(file);
  }

  /* 弹窗系统 */
  function showModal(html) {
    closeModal();
    var overlay = document.createElement("div");
    overlay.className = "modal-overlay";
    overlay.innerHTML = '<div class="modal">' + html + '<button class="modal-close" data-close>×</button></div>';
    document.body.appendChild(overlay);
    overlay.addEventListener("click", function (e) {
      if (e.target === overlay || e.target.hasAttribute("data-close")) closeModal();
    });
    return overlay;
  }
  function closeModal() {
    var m = document.querySelector(".modal-overlay");
    if (m) m.remove();
  }

  /* ===================================================================
     模块一：每日计划
  =================================================================== */
  var Plans = {
    key: "plans_days",
    tplKey: "plans_tpl",
    exGoalKey: "plans_exgoal",
    remindKey: "plans_remind",
    state: { view: "day", selDate: todayStr() },
    _cdTimer: null,
    _timer: null,

    getDays: function () { return Store.get(this.key, {}); },
    saveDays: function (d) { Store.set(this.key, d); },
    getTpl: function () {
      var t = Store.get(this.tplKey, null);
      if (!t) { t = [{ id: "tt1", name: "喝水 8 杯" }, { id: "tt2", name: "阅读 30 分钟" }]; Store.set(this.tplKey, t); }
      return t;
    },
    saveTpl: function (t) { Store.set(this.tplKey, t); },
    exGoal: function () { var g = Store.get(this.exGoalKey, 30); return g > 0 ? g : 30; },
    remind: function () { return Store.get(this.remindKey, { time: "23:30", enabled: false }); },

    ensureDay: function (date) {
      var d = this.getDays();
      if (!d[date]) {
        var tasks = this.getTpl().map(function (t) { return { id: t.id, name: t.name, done: false }; });
        d[date] = { tasks: tasks, exercises: [], sleep: { done: false, time: "" }, summary: "" };
        this.saveDays(d);
      }
      return d[date];
    },
    getDay: function (date) { return this.getDays()[date]; },
    saveDay: function (date, rec) { var d = this.getDays(); d[date] = rec; this.saveDays(d); },

    exSum: function (rec) { return (rec.exercises || []).reduce(function (s, x) { return s + (parseInt(x.duration, 10) || 0); }, 0); },
    exDone: function (rec) { return !!(rec && this.exSum(rec) > 0); },
    sleepOnTime: function (rec, target) { return !!(rec && rec.sleep && rec.sleep.done && rec.sleep.time && rec.sleep.time <= target); },

    sleepCountdown: function (target) {
      var now = new Date();
      var p = (target || "23:30").split(":");
      var t = new Date(now); t.setHours(+p[0], +p[1], 0, 0);
      var diff = t - now;
      if (diff > 0) {
        var m = Math.floor(diff / 60000), h = Math.floor(m / 60); m = m % 60;
        return { past: false, txt: "距 " + target + " 还有 " + (h > 0 ? h + " 时 " : "") + m + " 分" };
      }
      return { past: true, txt: "已过 " + target };
    },

    overviewHtml: function (date) {
      var rec = this.getDay(date) || { tasks: [], exercises: [], sleep: { done: false, time: "" }, summary: "" };
      var total = (rec.tasks || []).length, done = (rec.tasks || []).filter(function (t) { return t.done; }).length;
      var sum = this.exSum(rec), goal = this.exGoal();
      var pct = Math.min(100, goal ? Math.round(sum / goal * 100) : 0);
      var target = this.remind().time;
      var cd = this.sleepCountdown(target);
      var sl = rec.sleep;
      var sleepTxt = (sl && sl.done) ? (this.sleepOnTime(rec, target) ? "已打卡✓ 达标" : "已打卡 超时") : cd.txt;
      var isToday = date === todayStr();
      return '<div class="ov-bar">' +
        '<div class="ov-item"><div class="ov-k">待办</div><div class="ov-v">' + done + '/' + total + '</div></div>' +
        '<div class="ov-item"><div class="ov-k">运动 ' + sum + '/' + goal + '分</div>' +
        '<div class="ov-prog"><div class="ov-prog-bar" style="width:' + pct + '%"></div></div></div>' +
        '<div class="ov-item"><div class="ov-k">睡眠</div><div class="ov-v ' + ((cd.past && !(sl && sl.done)) ? "warn" : "") + '" id="sleep-cd">' + sleepTxt + '</div></div>' +
        '</div>' +
        (isToday ? "" : '<div class="ov-date-tag">正在查看 ' + date + ' · <span data-act="plan-backtoday" style="color:var(--primary);cursor:pointer">返回今日</span></div>');
    },

    render: function () {
      var self = this;
      clearInterval(self._cdTimer);
      var date = self.state.selDate;
      var html = '<div class="page-head"><div><div class="page-title">每日计划</div>' +
        '<div class="page-desc">每日高频打卡 · ' + (self.state.view === "day" ? "日视图" : self.state.view === "month" ? "月视图" : "年视图") + '</div></div>' +
        '<button class="btn-ghost" data-act="plan-remind">🔔 提醒</button></div>';
      html += '<div class="plan-tabs">' +
        '<button class="plan-tab ' + (self.state.view === "day" ? "active" : "") + '" data-act="plan-view" data-v="day">日</button>' +
        '<button class="plan-tab ' + (self.state.view === "month" ? "active" : "") + '" data-act="plan-view" data-v="month">月</button>' +
        '<button class="plan-tab ' + (self.state.view === "year" ? "active" : "") + '" data-act="plan-view" data-v="year">年</button>' +
        '</div>';
      html += '<div class="ov-sticky">' + self.overviewHtml(date) + '</div>';
      if (self.state.view === "day") html += self.dayHtml(date);
      else if (self.state.view === "month") html += self.monthHtml();
      else html += self.yearHtml();
      document.getElementById("page-plans").innerHTML = html;
      if (self.state.view === "day") {
        self._cdTimer = setInterval(function () {
          var elc = document.getElementById("sleep-cd");
          if (elc) {
            var c = self.sleepCountdown(self.remind().time);
            var r = self.getDay(date); var st = r && r.sleep;
            elc.textContent = (st && st.done) ? (self.sleepOnTime(r, self.remind().time) ? "已打卡✓ 达标" : "已打卡 超时") : c.txt;
            elc.classList.toggle("warn", c.past && !(st && st.done));
          }
        }, 30000);
      }
    },

    dayHtml: function (date) {
      var self = this, rec = this.ensureDay(date);
      var html = '<div class="card"><div class="plan-date" style="font-weight:800;margin-bottom:10px">📅 ' + date + '</div>';
      html += '<div class="qk-label">✅ 当日待办清单</div><div class="task-list">';
      if (!rec.tasks.length) html += '<div class="empty" style="padding:14px">暂无待办，下方添加</div>';
      rec.tasks.forEach(function (t) {
        html += '<div class="task ' + (t.done ? "done" : "") + '" data-id="' + t.id + '">' +
          '<button class="check" data-act="plan-toggle" data-id="' + t.id + '">' + (t.done ? "✓" : "") + '</button>' +
          '<span class="task-name">' + escapeHtml(t.name) + '</span>' +
          '<button class="task-del" data-act="plan-deltask" data-id="' + t.id + '">✕</button></div>';
      });
      html += '</div><div class="add-row"><input id="plan-task-input" placeholder="新增待办，如：背单词" maxlength="40" />' +
        '<button class="btn-primary" data-act="plan-addtask">添加</button></div>';
      html += '<div class="qk-label" style="margin-top:16px">🏃 运动记录 <small>项目 / 时长(分)</small></div><div class="today-list">';
      if (!rec.exercises.length) html += '<div class="empty" style="padding:12px">今天还没运动记录</div>';
      rec.exercises.forEach(function (x) {
        html += '<div class="poop-item"><span class="pi-dot dot-ok"></span><div class="pi-main"><div class="pi-time">' + escapeHtml(x.name) + '</div>' +
          '<div class="pi-meta">时长 ' + (parseInt(x.duration, 10) || 0) + ' 分钟</div></div>' +
          '<button class="pi-del" data-act="plan-delex" data-id="' + x.id + '">🗑</button></div>';
      });
      html += '</div><div class="add-row">' +
        '<input id="plan-ex-name" placeholder="项目，如：跑步">' +
        '<input id="plan-ex-dur" placeholder="分钟" inputmode="numeric" style="max-width:92px">' +
        '<button class="btn-primary" data-act="plan-addex">添加</button></div>';
      var target = this.remind().time, sl = rec.sleep;
      html += '<div class="qk-label" style="margin-top:16px">😴 睡眠打卡 <small>目标 ' + target + ' 前入睡</small></div>';
      html += '<div class="sleep-card ' + (sl.done ? "on" : "") + '">';
      html += '<div class="sleep-left"><div class="sleep-status">' + (sl.done ? (this.sleepOnTime(rec, target) ? "已达标 ✓" : "已打卡（超时）") : "未打卡") + '</div>' +
        '<div class="sleep-sub">实际入睡时间</div></div>';
      html += '<div class="sleep-right"><input type="time" id="plan-sleep-time" value="' + (sl.time || nowTime()) + '" class="sleep-time"></div></div>';
      html += '<button class="btn-primary qk-save" data-act="plan-sleep">' + (sl.done ? "更新打卡" : "睡眠打卡") + '</button></div>';
      html += '<div class="qk-label" style="margin-top:16px">📝 当日小结</div>' +
        '<textarea id="plan-summary" class="plan-summary" placeholder="今天过得怎么样？写点什么…">' + escapeHtml(rec.summary || "") + '</textarea>';
      html += '</div>';
      return html;
    },

    monthHtml: function () {
      var self = this;
      var base = new Date(self.state.selDate + "T00:00:00");
      var y = base.getFullYear(), m = base.getMonth();
      var first = new Date(y, m, 1), startDay = (first.getDay() + 6) % 7;
      var daysInMonth = new Date(y, m + 1, 0).getDate();
      var target = self.remind().time, tStr = todayStr();
      var html = '<div class="card"><div class="cal-head"><div class="cal-title">' + y + ' 年 ' + (m + 1) + ' 月 · 打卡率</div>' +
        '<div class="cal-nav"><button data-act="plan-mprev">‹</button><button data-act="plan-mnext">›</button></div></div>';
      var elapsed = (y === new Date().getFullYear() && m === new Date().getMonth()) ? new Date().getDate() : daysInMonth;
      var exCnt = 0, slCnt = 0;
      for (var i = 1; i <= elapsed; i++) {
        var ds = y + "-" + pad(m + 1) + "-" + pad(i);
        var r = self.getDay(ds);
        if (self.exDone(r)) exCnt++;
        if (self.sleepOnTime(r, target)) slCnt++;
      }
      html += '<div class="stat-grid" style="margin-bottom:12px">' +
        '<div class="stat-box"><div class="stat-num green">' + Math.round(exCnt / elapsed * 100) + '%</div><div class="stat-cap">运动完成率</div></div>' +
        '<div class="stat-box"><div class="stat-num orange">' + Math.round(slCnt / elapsed * 100) + '%</div><div class="stat-cap">睡眠达标率</div></div>' +
        '<div class="stat-box"><div class="stat-num">' + exCnt + '/' + slCnt + '</div><div class="stat-cap">运动/睡眠天</div></div></div>';
      html += '<div class="cal-week"><span>一</span><span>二</span><span>三</span><span>四</span><span>五</span><span>六</span><span>日</span></div>';
      html += '<div class="cal-grid">';
      for (var s = 0; s < startDay; s++) html += '<div class="cal-cell empty"></div>';
      for (var d = 1; d <= daysInMonth; d++) {
        var ds2 = y + "-" + pad(m + 1) + "-" + pad(d);
        var rr = self.getDay(ds2);
        var ex = self.exDone(rr), sl = self.sleepOnTime(rr, target);
        var cls = "cal-cell plan-cell";
        if (ds2 === tStr) cls += " today";
        if (!rr) cls += " empty-day";
        html += '<div class="' + cls + '" data-act="plan-day" data-date="' + ds2 + '">' + d +
          '<div class="pdots"><i class="pd ' + (ex ? "on" : "") + '"></i><i class="pd sleep ' + (sl ? "on" : "") + '"></i></div></div>';
      }
      html += '</div><div class="cal-legend"><span><i style="background:var(--green)"></i>运动</span><span><i style="background:var(--primary)"></i>睡眠达标</span><span>点日期看当日</span></div></div>';
      return html;
    },

    yearHtml: function () {
      var self = this, y = new Date().getFullYear(), target = self.remind().time, tStr = todayStr();
      var exDays = 0, slDays = 0, elapsed = 0;
      var html = '<div class="card"><div class="qk-label">' + y + ' 年打卡热力图</div><div class="heat-scroll">';
      for (var mo = 0; mo < 12; mo++) {
        var daysInMonth = new Date(y, mo + 1, 0).getDate();
        var first = new Date(y, mo, 1), startDay = (first.getDay() + 6) % 7;
        html += '<div class="heat-month"><div class="hm-title">' + (mo + 1) + '月</div><div class="cal-grid hm-grid">';
        for (var s = 0; s < startDay; s++) html += '<div class="heat-cell empty"></div>';
        for (var d = 1; d <= daysInMonth; d++) {
          var ds = y + "-" + pad(mo + 1) + "-" + pad(d);
          if (ds > tStr) { html += '<div class="heat-cell future"></div>'; continue; }
          elapsed++;
          var rr = self.getDay(ds);
          var ex = self.exDone(rr), sl = self.sleepOnTime(rr, target);
          if (ex) exDays++; if (sl) slDays++;
          var lvl = (ex ? 1 : 0) + (sl ? 1 : 0);
          html += '<div class="heat-cell lvl' + lvl + '" title="' + ds + '：运动' + (ex ? "✓" : "✗") + ' 睡眠' + (sl ? "✓" : "✗") + '"></div>';
        }
        html += '</div></div>';
      }
      html += '</div>';
      var rate = elapsed ? Math.round((exDays + slDays) / (elapsed * 2) * 100) : 0;
      html += '<div class="stat-grid" style="margin-top:12px">' +
        '<div class="stat-box"><div class="stat-num green">' + exDays + '</div><div class="stat-cap">全年运动天数</div></div>' +
        '<div class="stat-box"><div class="stat-num orange">' + slDays + '</div><div class="stat-cap">睡眠达标天数</div></div>' +
        '<div class="stat-box"><div class="stat-num">' + rate + '%</div><div class="stat-cap">整体打卡率</div></div></div>';
      html += '<div class="cal-legend"><span><i style="background:#eef0f3"></i>未打卡</span><span><i style="background:#bfe6d3"></i>完成1项</span><span><i style="background:var(--green)"></i>两项全完成</span></div></div>';
      return html;
    },

    openRemind: function () {
      var self = this, cfg = self.remind();
      var html = '<h3>🔔 睡眠提醒</h3>' +
        '<div class="field"><label>提醒时间（目标入睡点）</label><input type="time" id="rm-time" value="' + cfg.time + '"></div>' +
        '<div class="field"><label class="check-line"><input type="checkbox" id="rm-enable"' + (cfg.enabled ? " checked" : "") + '> 启用系统通知（到点提示入睡）</label></div>' +
        '<div class="field" style="font-size:12px;color:var(--muted);line-height:1.5">提示：系统通知需浏览器授权，且在页面/标签页打开时才会弹出（纯前端应用限制，关掉页面则不会提醒）。</div>' +
        '<div class="modal-actions"><button class="btn-cancel" data-close>取消</button>' +
        '<button class="btn-confirm" data-act="rm-save">保存</button></div>';
      var overlay = showModal(html);
      overlay.querySelector('[data-act="rm-save"]').addEventListener("click", function () {
        var time = overlay.querySelector("#rm-time").value || "23:30";
        var enabled = overlay.querySelector("#rm-enable").checked;
        if (enabled && "Notification" in window && Notification.permission === "default") {
          Notification.requestPermission().then(function (p) {
            var ok = (p === "granted");
            self.setRemind({ time: time, enabled: ok });
            if (!ok) toast("未授权通知，将仅用页内提示"); else toast("提醒已开启 💤");
            scheduleReminder(); self.render();
          });
        } else {
          self.setRemind({ time: time, enabled: enabled });
          scheduleReminder(); self.render();
          toast(enabled ? "提醒已开启 💤" : "提醒已关闭");
        }
        closeModal();
      });
    },

    handle: function (act, el) {
      var self = this, date = self.state.selDate;
      if (act === "plan-view") { self.state.view = el.dataset.v; self.render(); }
      else if (act === "plan-remind") { self.openRemind(); }
      else if (act === "plan-toggle") {
        var rec = self.ensureDay(date);
        var t = rec.tasks.filter(function (x) { return x.id === el.dataset.id; })[0];
        if (t) { t.done = !t.done; self.saveDay(date, rec); self.render(); }
      }
      else if (act === "plan-deltask") {
        var rec2 = self.ensureDay(date);
        rec2.tasks = rec2.tasks.filter(function (x) { return x.id !== el.dataset.id; });
        self.saveTpl(self.getTpl().filter(function (x) { return x.id !== el.dataset.id; }));
        self.saveDay(date, rec2); self.render(); toast("已删除");
      }
      else if (act === "plan-addtask") {
        var inp = document.getElementById("plan-task-input");
        var name = (inp.value || "").trim();
        if (!name) { toast("请输入待办内容"); return; }
        var rec3 = self.ensureDay(date);
        var id = "tt_" + uid();
        rec3.tasks.push({ id: id, name: name, done: false });
        var tpl = self.getTpl();
        if (!tpl.some(function (x) { return x.name === name; })) { tpl.push({ id: id, name: name }); self.saveTpl(tpl); }
        self.saveDay(date, rec3); self.render();
      }
      else if (act === "plan-addex") {
        var nm = (document.getElementById("plan-ex-name").value || "").trim();
        var du = parseInt(document.getElementById("plan-ex-dur").value, 10);
        if (!nm) { toast("请输入运动项目"); return; }
        if (!du || du <= 0) { toast("请输入时长(分钟)"); return; }
        var rec4 = self.ensureDay(date);
        rec4.exercises.push({ id: "ex_" + uid(), name: nm, duration: du });
        self.saveDay(date, rec4); self.render(); toast("已记录运动 🏃");
      }
      else if (act === "plan-delex") {
        var rec5 = self.ensureDay(date);
        rec5.exercises = rec5.exercises.filter(function (x) { return x.id !== el.dataset.id; });
        self.saveDay(date, rec5); self.render();
      }
      else if (act === "plan-sleep") {
        var rec6 = self.ensureDay(date);
        rec6.sleep = { done: true, time: document.getElementById("plan-sleep-time").value || nowTime() };
        self.saveDay(date, rec6); self.render(); toast("睡眠打卡 ✓");
      }
      else if (act === "plan-mprev") { var a = new Date(date + "T00:00:00"); a.setMonth(a.getMonth() - 1); self.state.selDate = a.getFullYear() + "-" + pad(a.getMonth() + 1) + "-" + pad(a.getDate()); self.render(); }
      else if (act === "plan-mnext") { var b = new Date(date + "T00:00:00"); b.setMonth(b.getMonth() + 1); self.state.selDate = b.getFullYear() + "-" + pad(b.getMonth() + 1) + "-" + pad(b.getDate()); self.render(); }
      else if (act === "plan-day") { self.state.selDate = el.dataset.date; self.state.view = "day"; self.render(); }
      else if (act === "plan-backtoday") { self.state.selDate = todayStr(); self.render(); }
    },
    handleKey: function (e) {
      if (e.key === "Enter") {
        if (e.target.id === "plan-task-input") Plans.handle("plan-addtask", e.target);
        else if (e.target.id === "plan-ex-name" || e.target.id === "plan-ex-dur") Plans.handle("plan-addex", e.target);
      }
    },
    handleChange: function (e) {
      if (e.target.id === "plan-summary") {
        var rec = Plans.ensureDay(Plans.state.selDate); rec.summary = e.target.value; Plans.saveDay(Plans.state.selDate, rec);
      }
    }
  };

  function scheduleReminder() {
    clearTimeout(Plans._timer);
    var cfg = Plans.remind();
    if (!cfg.enabled) return;
    if (!("Notification" in window)) return;
    var now = new Date();
    var p = cfg.time.split(":");
    var t = new Date(now); t.setHours(+p[0], +p[1], 0, 0);
    if (t <= now) t.setDate(t.getDate() + 1);
    Plans._timer = setTimeout(function () {
      var msg = "该睡觉啦 💤 目标 " + cfg.time + " 前入睡";
      if ("Notification" in window && Notification.permission === "granted") { try { new Notification("睡眠提醒", { body: msg }); } catch (e) {} }
      toast(msg);
      scheduleReminder();
    }, t - now);
  }

  /* ===================================================================
     模块二：每日噗噗
  =================================================================== */
  var BRISTOL = [
    { t: 1, d: "分散硬块（坚果状），很难排出", ok: false },
    { t: 2, d: "香肠状但表面成块", ok: false },
    { t: 3, d: "香肠状，表面有裂缝（理想）", ok: true },
    { t: 4, d: "光滑柔软香肠状（最理想）", ok: true },
    { t: 5, d: "柔软团块，边缘清晰", ok: true },
    { t: 6, d: "蓬松糊状，边缘不规则", ok: false },
    { t: 7, d: "完全液体，无固体块", ok: false }
  ];
  var SMOOTH = [
    { v: "smooth", label: "顺畅" },
    { v: "normal", label: "一般" },
    { v: "hard", label: "费力" }
  ];
  var Poop = {
    key: "poop_records",
    year: new Date().getFullYear(),
    month: new Date().getMonth(),
    draft: { bristol: 4, smooth: "smooth" },
    get: function () { return Store.get(this.key, []); },
    save: function (a) { Store.set(this.key, a); },
    add: function (rec) { var a = this.get(); a.push(rec); this.save(a); },
    remove: function (id) { this.save(this.get().filter(function (r) { return r.id !== id; })); },
    byDate: function (date) { return this.get().filter(function (r) { return r.date === date; }); },
    render: function () {
      var self = this;
      var html = '<div class="page-head"><div><div class="page-title">每日噗噗</div>' +
        '<div class="page-desc">3 步快速记录，降低坚持门槛</div></div></div>';

      /* 快速记录卡 */
      html += '<div class="card"><div class="qk-label">⚡ 快速记录 <small>日期·时间 → 分型 → 顺畅度</small></div>';
      html += '<div class="qk-row" style="margin-bottom:10px">' +
        '<input type="date" id="poop-date" value="' + todayStr() + '">' +
        '<input type="time" id="poop-time" value="' + nowTime() + '"></div>';
      html += '<div class="qk-label">布里斯托分型</div><div class="bristol-grid">';
      BRISTOL.forEach(function (b) {
        html += '<button class="bristol-btn ' + (self.draft.bristol === b.t ? "sel" : "") + '" data-act="bristol" data-v="' + b.t + '">' + b.t + '</button>';
      });
      html += '</div><div class="bristol-desc" id="bristol-desc">' + (BRISTOL[self.draft.bristol - 1].d) + '</div>';
      html += '<div class="qk-label" style="margin-top:12px">排便顺畅度</div><div class="smooth-row">';
      SMOOTH.forEach(function (s) {
        html += '<button class="smooth-btn ' + (self.draft.smooth === s.v ? "sel" : "") + '" data-act="smooth" data-v="' + s.v + '">' + s.label + '</button>';
      });
      html += '</div><button class="btn-primary qk-save" data-act="save-poop">保存记录</button></div>';

      /* 今日记录 */
      var todayRecs = this.byDate(todayStr());
      html += '<div class="card"><div class="qk-label">📅 今日记录（' + todayRecs.length + ' 次）</div>';
      if (!todayRecs.length) html += '<div class="empty" style="padding:18px">今天还没有记录</div>';
      else {
        html += '<div class="today-list">';
        todayRecs.slice().reverse().forEach(function (r) {
          var b = BRISTOL[r.bristol - 1];
          html += '<div class="poop-item"><span class="pi-dot ' + (b.ok ? "dot-ok" : "dot-bad") + '"></span>' +
            '<div class="pi-main"><div class="pi-time">' + escapeHtml(r.time) + '</div>' +
            '<div class="pi-meta">分型 ' + r.bristol + ' · ' + SMOOTH.filter(function (s) { return s.v === r.smooth; })[0].label + '</div></div>' +
            '<button class="pi-del" data-act="del-poop" data-id="' + r.id + '">🗑</button></div>';
        });
        html += '</div>';
      }
      html += '</div>';

      /* 周统计 */
      html += this.weekStatsHtml();

      /* 月历 */
      html += this.calendarHtml();

      document.getElementById("page-poop").innerHTML = html;
    },
    weekStatsHtml: function () {
      var now = new Date();
      var mon = new Date(now);
      mon.setDate(now.getDate() - ((now.getDay() + 6) % 7)); // 本周一
      var sun = new Date(mon); sun.setDate(mon.getDate() + 6);
      var recs = this.get().filter(function (r) {
        var d = new Date(r.date + "T00:00:00");
        return d >= new Date(mon.getFullYear() + "-" + pad(mon.getMonth() + 1) + "-" + pad(mon.getDate())) &&
          d <= new Date(sun.getFullYear() + "-" + pad(sun.getMonth() + 1) + "-" + pad(sun.getDate()));
      });
      var days = {}; recs.forEach(function (r) { days[r.date] = 1; });
      var dayCount = Object.keys(days).length;
      var avg = (recs.length / 7);
      var healthy = recs.filter(function (r) { return BRISTOL[r.bristol - 1].ok; }).length;
      var ratio = recs.length ? Math.round(healthy / recs.length * 100) : 0;
      return '<div class="card"><div class="qk-label">📊 本周统计</div><div class="stat-grid">' +
        '<div class="stat-box"><div class="stat-num green">' + dayCount + '</div><div class="stat-cap">排便天数</div></div>' +
        '<div class="stat-box"><div class="stat-num">' + avg.toFixed(1) + '</div><div class="stat-cap">平均次数/天</div></div>' +
        '<div class="stat-box"><div class="stat-num orange">' + ratio + '%</div><div class="stat-cap">健康便占比</div></div>' +
        '</div></div>';
    },
    calendarHtml: function () {
      var self = this, y = this.year, m = this.month;
      var first = new Date(y, m, 1);
      var startDay = (first.getDay() + 6) % 7; // 周一为第一列
      var daysInMonth = new Date(y, m + 1, 0).getDate();
      var html = '<div class="card"><div class="cal-head"><div class="cal-title">' + y + ' 年 ' + (m + 1) + ' 月</div>' +
        '<div class="cal-nav"><button data-act="cal-prev">‹</button><button data-act="cal-next">›</button></div></div>';
      html += '<div class="cal-week"><span>一</span><span>二</span><span>三</span><span>四</span><span>五</span><span>六</span><span>日</span></div>';
      html += '<div class="cal-grid">';
      for (var i = 0; i < startDay; i++) html += '<div class="cal-cell empty"></div>';
      var recs = this.get();
      var tStr = todayStr();
      for (var d = 1; d <= daysInMonth; d++) {
        var ds = y + "-" + pad(m + 1) + "-" + pad(d);
        var dr = recs.filter(function (r) { return r.date === ds; });
        var cls = "cal-cell";
        var cnt = "";
        if (dr.length) {
          var allOk = dr.every(function (r) { return BRISTOL[r.bristol - 1].ok; });
          cls += allOk ? " has" : " has-bad";
          cnt = '<span class="cnt">' + dr.length + '</span>';
        }
        if (ds === tStr) cls += " today";
        html += '<div class="' + cls + '" data-act="cal-day" data-date="' + ds + '">' + d + cnt + '</div>';
      }
      html += '</div>';
      html += '<div class="cal-legend"><span><i style="background:var(--green-soft)"></i>有记录</span>' +
        '<span><i style="background:var(--orange-soft)"></i>含异常便</span>' +
        '<span><i style="background:#fff;outline:2px solid var(--primary)"></i>今天</span></div>';
      html += '<div class="day-detail" id="day-detail"></div></div>';
      return html;
    },
    showDayDetail: function (date) {
      var dr = this.byDate(date);
      var box = document.getElementById("day-detail");
      if (!box) return;
      if (!dr.length) { box.innerHTML = '<h4>' + date + '</h4><div class="empty" style="padding:14px">当天无记录</div>'; return; }
      var h = '<h4>' + date + ' · ' + dr.length + ' 次</h4><div class="today-list">';
      dr.slice().reverse().forEach(function (r) {
        var b = BRISTOL[r.bristol - 1];
        h += '<div class="poop-item"><span class="pi-dot ' + (b.ok ? "dot-ok" : "dot-bad") + '"></span>' +
          '<div class="pi-main"><div class="pi-time">' + escapeHtml(r.time) + '</div>' +
          '<div class="pi-meta">分型 ' + r.bristol + '（' + escapeHtml(b.d.split("（")[0]) + '） · ' +
          SMOOTH.filter(function (s) { return s.v === r.smooth; })[0].label + '</div></div></div>';
      });
      h += '</div>';
      box.innerHTML = h;
    },
    handle: function (act, el) {
      var self = this;
      if (act === "bristol") {
        self.draft.bristol = parseInt(el.dataset.v, 10);
        document.querySelectorAll(".bristol-btn").forEach(function (b) { b.classList.toggle("sel", b.dataset.v === el.dataset.v); });
        document.getElementById("bristol-desc").textContent = BRISTOL[self.draft.bristol - 1].d;
      } else if (act === "smooth") {
        self.draft.smooth = el.dataset.v;
        document.querySelectorAll(".smooth-btn").forEach(function (b) { b.classList.toggle("sel", b.dataset.v === el.dataset.v); });
      } else if (act === "save-poop") {
        var date = document.getElementById("poop-date").value || todayStr();
        var time = document.getElementById("poop-time").value || nowTime();
        self.add({ id: uid(), date: date, time: time, bristol: self.draft.bristol, smooth: self.draft.smooth });
        toast("已记录 ✅"); self.render();
      } else if (act === "del-poop") {
        self.remove(el.dataset.id); self.render(); toast("已删除");
      } else if (act === "cal-prev") { self.month--; if (self.month < 0) { self.month = 11; self.year--; } self.render(); }
      else if (act === "cal-next") { self.month++; if (self.month > 11) { self.month = 0; self.year++; } self.render(); }
      else if (act === "cal-day") { self.showDayDetail(el.dataset.date); }
    }
  };

  /* ===================================================================
     模块三：恋爱手记
  =================================================================== */
  var LOVE_TYPES = {
    today: { label: "今日随手记", badge: "badge-today" },
    gift: { label: "花束 & 礼物", badge: "badge-gift" },
    date: { label: "约会出行", badge: "badge-date" },
    note: { label: "暖心碎碎念", badge: "badge-note" },
    anniversary: { label: "纪念日", badge: "badge-anni" }
  };
  var Love = {
    key: "love_records",
    startKey: "love_start",
    state: { tab: "overview", q: "", tag: "" },
    get: function () { return Store.get(this.key, []); },
    save: function (a) { Store.set(this.key, a); },
    getStart: function () { return Store.get(this.startKey, ""); },
    setStart: function (d) { Store.set(this.startKey, d); },
    tabs: [
      { id: "overview", label: "概览" },
      { id: "all", label: "全部时间线" },
      { id: "today", label: "今日随手记" },
      { id: "gift", label: "花束·礼物图鉴" },
      { id: "date", label: "约会足迹" },
      { id: "note", label: "暖心碎碎念" },
      { id: "anniversary", label: "纪念日清单" }
    ],
    relDuration: function (start) {
      if (!start) return "未设置";
      var s = new Date(start + "T00:00:00");
      var now = new Date();
      var today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      var days = Math.max(0, Math.floor((today - s) / 86400000));
      var y = Math.floor(days / 365), d = days % 365;
      return y > 0 ? (y + " 年 " + d + " 天") : (days + " 天");
    },
    render: function () {
      var self = this;
      var html = '<div class="page-head"><div><div class="page-title">恋爱手记</div>' +
        '<div class="page-desc">温柔记录每一份心动</div></div></div>';
      html += '<div class="love-tabs">';
      self.tabs.forEach(function (t) {
        html += '<button class="love-tab ' + (self.state.tab === t.id ? "active" : "") + '" data-act="love-tab" data-tab="' + t.id + '">' + t.label + '</button>';
      });
      html += '</div>';

      if (self.state.tab === "overview") html += self.overviewHtml();
      else if (self.state.tab === "gift") html += self.giftGridHtml();
      else html += self.listHtml();

      document.getElementById("page-love").innerHTML = html;
      // 浮窗新增按钮
      if (!document.getElementById("love-fab")) {
        var fab = document.createElement("button");
        fab.className = "fab"; fab.id = "love-fab"; fab.textContent = "＋";
        fab.addEventListener("click", function () { Love.openForm(); });
        document.body.appendChild(fab);
      }
      document.getElementById("love-fab").style.display = "block";
    },
    overviewHtml: function () {
      var recs = this.get(), start = this.getStart();
      var gifts = recs.filter(function (r) { return r.type === "gift"; }).length;
      var dates = recs.filter(function (r) { return r.type === "date"; }).length;
      var h = '<div class="overview-hero"><div class="ov-label">💞 我们已经相恋</div>' +
        '<div class="ov-dur">' + this.relDuration(start) + '</div>' +
        '<div class="ov-set">恋爱起始日：<input type="date" id="love-start" value="' + (start || "") + '"></div></div>';
      h += '<div class="ov-board">' +
        '<div class="ov-box"><div class="ov-num">' + recs.length + '</div><div class="ov-cap">记录总条数</div></div>' +
        '<div class="ov-box"><div class="ov-num">' + gifts + '</div><div class="ov-cap">收到礼物数</div></div>' +
        '<div class="ov-box"><div class="ov-num">' + dates + '</div><div class="ov-cap">约会次数</div></div>' +
        '</div>';
      // 最近记录预览
      var recent = recs.slice().sort(function (a, b) { return b.date < a.date ? -1 : 1; }).slice(0, 3);
      if (recent.length) {
        h += '<div class="card" style="margin-top:14px"><div class="qk-label">🕒 最近记录</div>';
        recent.forEach(function (r) {
          h += '<div style="padding:7px 0;border-top:1px solid var(--line);display:flex;justify-content:space-between;align-items:center">' +
            '<span style="font-weight:600">' + escapeHtml(r.title || LOVE_TYPES[r.type].label) + '</span>' +
            '<span style="font-size:12px;color:var(--muted)">' + r.date + '</span></div>';
        });
        h += '</div>';
      } else {
        h += '<div class="empty" style="margin-top:14px"><span class="em-ico">💌</span>还没有记录，点右下角 ＋ 写第一条吧</div>';
      }
      return h;
    },
    listHtml: function () {
      var self = this;
      var tab = self.state.tab;
      var recs = self.get();
      if (tab !== "all") recs = recs.filter(function (r) { return r.type === tab; });
      // 搜索 & 标签
      var q = self.state.q.trim().toLowerCase();
      var allTags = {};
      self.get().forEach(function (r) { (r.tags || []).forEach(function (t) { allTags[t] = 1; }); });
      if (q) recs = recs.filter(function (r) {
        return (r.title + " " + r.summary + " " + r.content + " " + (r.tags || []).join(" ")).toLowerCase().indexOf(q) >= 0;
      });
      if (self.state.tag) recs = recs.filter(function (r) { return (r.tags || []).indexOf(self.state.tag) >= 0; });

      var html = "";
      if (tab === "all" || tab === "today" || tab === "note" || tab === "anniversary" || tab === "date") {
        html += '<div class="search-bar"><input id="love-search" placeholder="🔍 搜索关键词…" value="' + escapeHtml(self.state.q) + '"></div>';
        var tagsArr = Object.keys(allTags);
        if (tagsArr.length) {
          html += '<div class="tag-filter">';
          html += '<button class="tag-chip ' + (!self.state.tag ? "active" : "") + '" data-act="love-tag" data-tag="">全部标签</button>';
          tagsArr.forEach(function (t) {
            html += '<button class="tag-chip ' + (self.state.tag === t ? "active" : "") + '" data-act="love-tag" data-tag="' + escapeHtml(t) + '">#' + escapeHtml(t) + '</button>';
          });
          html += '</div>';
        }
      }
      if (!recs.length) {
        html += '<div class="empty"><span class="em-ico">📭</span>这里还空空如也</div>';
        return html;
      }
      recs.sort(function (a, b) { return b.date < a.date ? -1 : (b.date > a.date ? 1 : (b.id < a.id ? -1 : 1)); });
      if (tab === "date") {
        // 按地点分组
        var groups = {};
        recs.forEach(function (r) { var k = r.location || "未标注地点"; (groups[k] = groups[k] || []).push(r); });
        Object.keys(groups).forEach(function (loc) {
          html += '<div class="qk-label">📍 ' + escapeHtml(loc) + '</div><div class="timeline">';
          groups[loc].forEach(function (r) { html += self.cardHtml(r); });
          html += '</div>';
        });
        return html;
      }
      html += '<div class="timeline">';
      recs.forEach(function (r) { html += self.cardHtml(r); });
      html += '</div>';
      return html;
    },
    cardHtml: function (r) {
      var t = LOVE_TYPES[r.type] || LOVE_TYPES.note;
      var cover = (r.photos && r.photos[0]) || r.cover || "";
      var extra = "";
      if (r.type === "gift") extra = (r.reason ? "🎁 原因：" + escapeHtml(r.reason) + "<br>" : "") + (r.story ? "💡 故事：" + escapeHtml(r.story) : "");
      else if (r.type === "date") extra = (r.dateType ? "类型：" + escapeHtml(r.dateType) : "");
      else if (r.type === "anniversary") extra = "📌 纪念日";
      var tags = (r.tags || []).map(function (x) { return "<span>#" + escapeHtml(x) + "</span>"; }).join("");
      var photosHtml = (r.photos && r.photos.length) ? '<div class="tl-photos">' + r.photos.map(function (p) { return '<img src="' + p + '" alt="">'; }).join("") + '</div>' : "";
      return '<div class="tl-card" data-act="love-expand" data-id="' + r.id + '">' +
        '<button class="tl-del" data-act="love-del" data-id="' + r.id + '">🗑</button>' +
        '<div class="tl-top"><img class="tl-cover" src="' + (cover || "") + '" onerror="this.style.display=\'none\'">' +
        '<div class="tl-meta"><div class="tl-title">' + escapeHtml(r.title || t.label) +
        ' <span class="badge ' + t.badge + '">' + t.label + '</span></div>' +
        '<div class="tl-date">' + escapeHtml(r.date) + (r.summary ? " · " + escapeHtml(r.summary) : "") + '</div></div></div>' +
        '<div class="tl-detail"><div class="tl-content">' + escapeHtml(r.content || "（无正文）") + '</div>' +
        photosHtml + (extra ? '<div class="tl-extra">' + extra + '</div>' : "") +
        (tags ? '<div class="tl-tags">' + tags + '</div>' : "") + '</div></div>';
    },
    giftGridHtml: function () {
      var self = this;
      var recs = self.get().filter(function (r) { return r.type === "gift"; });
      var q = self.state.q.trim().toLowerCase();
      if (q) recs = recs.filter(function (r) { return (r.title + " " + r.reason + " " + r.story).toLowerCase().indexOf(q) >= 0; });
      var html = '<div class="search-bar"><input id="love-search" placeholder="🔍 搜索礼物 / 故事…" value="' + escapeHtml(self.state.q) + '"></div>';
      if (!recs.length) { html += '<div class="empty"><span class="em-ico">💐</span>还没有收到礼物记录</div>'; return html; }
      html += '<div class="gift-grid">';
      recs.sort(function (a, b) { return b.date < a.date ? -1 : 1; }).forEach(function (r) {
        var img = (r.photos && r.photos[0]) || r.cover || "";
        html += '<div class="gift-card" data-act="love-expand" data-id="' + r.id + '">' +
          '<img src="' + img + '" onerror="this.style.background=\'#f3f4f6\'">' +
          '<div class="gift-cap"><div class="gc-title">' + escapeHtml(r.title || "礼物") + '</div>' +
          '<div class="gc-sub">' + escapeHtml(r.date) + (r.reason ? " · " + r.reason : "") + '</div></div></div>';
      });
      html += '</div>';
      return html;
    },
    openForm: function (presetType) {
      var self = this;
      var typeOpts = Object.keys(LOVE_TYPES).map(function (k) {
        return '<option value="' + k + '" ' + (presetType === k ? "selected" : "") + '>' + LOVE_TYPES[k].label + '</option>';
      }).join("");
      var html = '<h3>✍️ 写一条恋爱记录</h3>' +
        '<div class="field"><label>分类</label><select id="lf-type">' + typeOpts + '</select></div>' +
        '<div class="field"><label>标题</label><input id="lf-title" placeholder="给这条回忆起个名字"></div>' +
        '<div class="field"><label>日期</label><input type="date" id="lf-date" value="' + todayStr() + '"></div>' +
        '<div class="field" id="lf-cond"></div>' +
        '<div class="field"><label>简短摘要</label><input id="lf-summary" placeholder="一句话概括"></div>' +
        '<div class="field"><label>正文</label><textarea id="lf-content" placeholder="写下完整的故事、心情…"></textarea></div>' +
        '<div class="field" id="lf-extra"></div>' +
        '<div class="field"><label>标签（逗号分隔）</label><input id="lf-tags" placeholder="如：惊喜, 生日, 旅行"></div>' +
        '<div class="field"><label>照片</label><div class="img-previews" id="lf-previews"></div>' +
        '<label class="thumb-add" style="margin-top:8px">＋<input type="file" id="lf-photos" accept="image/*" multiple hidden></label></div>' +
        '<div class="modal-actions"><button class="btn-cancel" data-close>取消</button>' +
        '<button class="btn-confirm" data-act="lf-save">保存</button></div>';
      var overlay = showModal(html);
      var photos = [];
      function refreshCond() {
        var type = overlay.querySelector("#lf-type").value;
        var cond = overlay.querySelector("#lf-cond");
        var extra = overlay.querySelector("#lf-extra");
        cond.innerHTML = type === "gift"
          ? '<label>送礼原因</label><input id="lf-reason" placeholder="为什么送这份礼物？">' +
            '<label style="margin-top:10px">背后的小故事</label><textarea id="lf-story" placeholder="藏在这份礼物里的小故事…"></textarea>'
          : "";
        extra.innerHTML = type === "date"
          ? '<label>地点</label><input id="lf-location" placeholder="如：外滩、迪士尼">' +
            '<label style="margin-top:10px">约会类型</label><input id="lf-dateType" placeholder="如：吃饭、看电影、旅行">'
          : "";
      }
      refreshCond();
      overlay.querySelector("#lf-type").addEventListener("change", refreshCond);
      overlay.querySelector("#lf-photos").addEventListener("change", function (e) {
        var files = Array.prototype.slice.call(e.target.files);
        files.forEach(function (f) {
          compressImage(f, 900, function (d) { if (d) { photos.push(d); renderPrev(); } });
        });
        e.target.value = "";
      });
      function renderPrev() {
        var box = overlay.querySelector("#lf-previews");
        box.innerHTML = photos.map(function (p) { return '<img src="' + p + '">'; }).join("");
      }
      overlay.querySelector('[data-act="lf-save"]').addEventListener("click", function () {
        var type = overlay.querySelector("#lf-type").value;
        var rec = {
          id: uid(), type: type,
          title: overlay.querySelector("#lf-title").value.trim(),
          date: overlay.querySelector("#lf-date").value || todayStr(),
          summary: overlay.querySelector("#lf-summary").value.trim(),
          content: overlay.querySelector("#lf-content").value.trim(),
          tags: overlay.querySelector("#lf-tags").value.split(/[,，]/).map(function (s) { return s.trim(); }).filter(Boolean),
          photos: photos,
          cover: photos[0] || ""
        };
        if (type === "gift") { rec.reason = (overlay.querySelector("#lf-reason") || {}).value || ""; rec.story = (overlay.querySelector("#lf-story") || {}).value || ""; }
        if (type === "date") { rec.location = (overlay.querySelector("#lf-location") || {}).value || ""; rec.dateType = (overlay.querySelector("#lf-dateType") || {}).value || ""; }
        var arr = self.get(); arr.push(rec); self.save(arr);
        closeModal(); self.render(); toast("已保存 💕");
      });
    },
    handle: function (act, el) {
      var self = this;
      if (act === "love-tab") { self.state.tab = el.dataset.tab; self.state.q = ""; self.state.tag = ""; self.render(); }
      else if (act === "love-tag") { self.state.tag = el.dataset.tag; self.render(); }
      else if (act === "love-expand") {
        var card = el.closest(".tl-card") || el.closest(".gift-card");
        if (card && card.classList.contains("tl-card")) card.classList.toggle("open");
      }
      else if (act === "love-del") {
        var id = el.dataset.id;
        var arr = self.get().filter(function (r) { return r.id !== id; });
        self.save(arr); self.render(); toast("已删除");
      }
    },
    handleKey: function (e) {
      if (e.key === "Enter" && e.target.id === "love-search") {
        Love.state.q = e.target.value; Love.render();
      }
    },
    handleChange: function (e) {
      if (e.target.id === "love-start") { Love.setStart(e.target.value); Love.render(); toast("已记录起始日"); }
      if (e.target.id === "love-search") { Love.state.q = e.target.value; }
    },
    handleInput: function (e) {
      if (e.target.id === "love-search") { Love.state.q = e.target.value; }
    }
  };

  /* ===================================================================
     模块四：倒数日
  =================================================================== */
  var CD_CATS = ["生日", "纪念日", "节日", "其他"];
  var Countdown = {
    key: "cd_events",
    get: function () { return Store.get(this.key, []); },
    save: function (a) { Store.set(this.key, a); },
    add: function (ev) { var a = this.get(); a.push(ev); this.save(a); },
    remove: function (id) { this.save(this.get().filter(function (e) { return e.id !== id; })); },
    effectiveDate: function (ev) {
      var t = todayStr();
      if (!ev.repeat) {
        if (ev.targetDate < t) return { date: ev.targetDate, passed: true };
        return { date: ev.targetDate, passed: false };
      }
      // 年度重复：取今年或明年
      var y = new Date(t + "T00:00:00").getFullYear();
      var md = ev.targetDate.slice(5);
      var thisYear = y + "-" + md;
      if (thisYear >= t) return { date: thisYear, passed: false };
      return { date: (y + 1) + "-" + md, passed: false };
    },
    render: function () {
      var self = this, t = todayStr();
      var events = this.get().map(function (ev) {
        var eff = self.effectiveDate(ev);
        var days = daysBetween(t, eff.date);
        return { ev: ev, eff: eff, days: days };
      });
      var html = '<div class="page-head"><div><div class="page-title">倒数日</div>' +
        '<div class="page-desc">生日 · 纪念日 · 重要日期</div></div>' +
        '<button class="btn-primary" data-act="cd-add">＋ 新增</button></div>';

      // 最近 7 天
      var soon = events.filter(function (x) { return !x.eff.passed && x.days >= 0 && x.days <= 7; })
        .sort(function (a, b) { return a.days - b.days; });
      html += '<div class="soon-card"><div class="soon-head">🔥 最近 7 天即将到来</div>';
      if (!soon.length) html += '<div style="font-size:13px;color:var(--muted)">未来一周没有临近的日期 🎉</div>';
      else soon.forEach(function (x) {
        html += '<div class="soon-item"><div class="si-name">' + escapeHtml(x.ev.name) +
          ' <span class="cd-cat cat-' + x.ev.category + '">' + x.ev.category + '</span></div>' +
          '<div class="si-date">' + x.eff.date + (x.ev.repeat ? " 🔁" : "") + '</div>' +
          '<div class="si-days">' + (x.days === 0 ? "今天" : x.days + " 天") + '</div></div>';
      });
      html += '</div>';

      // 全部（按剩余天数近→远）
      var sorted = events.slice().sort(function (a, b) {
        if (a.eff.passed !== b.eff.passed) return a.eff.passed ? 1 : -1;
        return a.days - b.days;
      });
      html += '<div class="cd-list">';
      if (!sorted.length) html += '<div class="empty"><span class="em-ico">⏳</span>还没有事件，点右上角添加</div>';
      sorted.forEach(function (x) {
        var cls = "cd-days";
        var numTxt;
        if (x.eff.passed) { cls += " past"; numTxt = "已过"; }
        else if (x.days === 0) { cls += " urgent"; numTxt = "今天"; }
        else if (x.days <= 7) { cls += " urgent"; numTxt = x.days; }
        else numTxt = x.days;
        html += '<div class="cd-card"><button class="cd-del" data-act="cd-del" data-id="' + x.ev.id + '">🗑</button>' +
          '<div class="cd-left"><div class="cd-name">' + escapeHtml(x.ev.name) +
          ' <span class="cd-cat cat-' + x.ev.category + '">' + x.ev.category + '</span>' +
          (x.ev.repeat ? ' <span class="repeat-ico">🔁</span>' : '') + '</div>' +
          (x.ev.note ? '<div class="cd-note">' + escapeHtml(x.ev.note) + '</div>' : '') +
          '<div class="cd-date">目标日 ' + x.eff.date + '</div></div>' +
          '<div class="' + cls + '"><div class="d-num">' + numTxt + '</div><div class="d-cap">' + (x.eff.passed ? "" : "天后") + '</div></div></div>';
      });
      html += '</div>';
      document.getElementById("page-countdown").innerHTML = html;
    },
    openForm: function () {
      var self = this;
      var catOpts = CD_CATS.map(function (c) { return '<option value="' + c + '">' + c + '</option>'; }).join("");
      var html = '<h3>⏳ 新增倒数日</h3>' +
        '<div class="field"><label>事件名称 *</label><input id="cd-name" placeholder="如：妈妈生日"></div>' +
        '<div class="field-row"><div class="field"><label>目标日期 *</label><input type="date" id="cd-date"></div>' +
        '<div class="field"><label>分类 *</label><select id="cd-cat">' + catOpts + '</select></div></div>' +
        '<div class="field"><label>备注</label><input id="cd-note" placeholder="选填，补充说明"></div>' +
        '<div class="field"><label class="check-line"><input type="checkbox" id="cd-repeat"> 每年重复（生日/年度纪念日自动续到下一年）</label></div>' +
        '<div class="modal-actions"><button class="btn-cancel" data-close>取消</button>' +
        '<button class="btn-confirm" data-act="cd-save">保存</button></div>';
      var overlay = showModal(html);
      overlay.querySelector('[data-act="cd-save"]').addEventListener("click", function () {
        var name = overlay.querySelector("#cd-name").value.trim();
        var date = overlay.querySelector("#cd-date").value;
        var cat = overlay.querySelector("#cd-cat").value;
        var note = overlay.querySelector("#cd-note").value.trim();
        var repeat = overlay.querySelector("#cd-repeat").checked;
        if (!name) { toast("请填写事件名称"); return; }
        if (!date) { toast("请选择目标日期"); return; }
        self.add({ id: uid(), name: name, targetDate: date, category: cat, note: note, repeat: repeat });
        closeModal(); self.render(); toast("已添加 ⏳");
      });
    },
    handle: function (act, el) {
      if (act === "cd-add") Countdown.openForm();
      else if (act === "cd-del") { Countdown.remove(el.dataset.id); Countdown.render(); toast("已删除"); }
    }
  };

  /* ===================================================================
     路由 & 初始化
  =================================================================== */
  var current = "plans";
  function showPage(p) {
    current = p;
    document.querySelectorAll(".nav-item").forEach(function (b) { b.classList.toggle("active", b.dataset.page === p); });
    document.querySelectorAll(".page").forEach(function (s) { s.classList.toggle("active", s.id === "page-" + p); });
    if (p === "love") Love.render(); else if (document.getElementById("love-fab")) document.getElementById("love-fab").style.display = "none";
    if (p === "plans") Plans.render();
    else if (p === "poop") Poop.render();
    else if (p === "countdown") Countdown.render();
  }

  function dispatch(page, act, el, e) {
    if (page === "plans") { if (act) Plans.handle(act, el, e); }
    else if (page === "poop") { if (act) Poop.handle(act, el); }
    else if (page === "love") Love.handle(act, el);
    else if (page === "countdown") Countdown.handle(act, el);
  }

  /* ---------------- PWA / 离线 / 数据备份 ---------------- */
  function registerSW() {
    if (!("serviceWorker" in navigator)) return;
    window.addEventListener("load", function () {
      navigator.serviceWorker.register("sw.js").catch(function () {});
    });
  }

  function refreshBkInfo() {
    var info = document.getElementById("bk-info");
    if (!info) return;
    var si = Store.shardInfo();
    var mb = (si.bytes / 1024 / 1024).toFixed(2);
    info.textContent = "当前数据：" + si.count + " 个 JSON 分片，约 " + mb + " MB（单文件超过 50MB 自动拆下一个）";
  }

  function openBackup() {
    var html = "" +
      "<h3>💾 数据备份</h3>" +
      '<p id="bk-info" class="bk-info"></p>' +
      '<p class="bk-tip">数据存储在手机本地（IndexedDB），以 JSON 分片保存。可导出为 .json 文件备份或迁移到其它设备；导入会合并到现有数据。</p>' +
      '<div class="bk-actions">' +
      '<button class="btn-primary" id="bk-export">⬇️ 导出 JSON 备份</button>' +
      '<label class="btn-line">⬆️ 导入 JSON 备份<input id="bk-import" type="file" accept=".json,application/json" multiple hidden></label>' +
      "</div>";
    var ov = showModal(html);
    refreshBkInfo();
    ov.querySelector("#bk-export").addEventListener("click", doExport);
    ov.querySelector("#bk-import").addEventListener("change", doImport);
  }

  function doExport() {
    var shards = Store.exportShards();
    if (!shards.length) { toast("暂无数据"); return; }
    shards.forEach(function (sh, i) {
      var blob = new Blob([JSON.stringify(sh)], { type: "application/json" });
      var url = URL.createObjectURL(blob);
      var a = document.createElement("a");
      a.href = url; a.download = "xuan-data-" + (i + 1) + ".json";
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      setTimeout(function () { URL.revokeObjectURL(url); }, 1500);
    });
    toast("已导出 " + shards.length + " 个 JSON 文件");
  }

  function doImport(e) {
    var files = e.target.files;
    if (!files || !files.length) return;
    var readers = [];
    for (var i = 0; i < files.length; i++) {
      (function (f) {
        readers.push(new Promise(function (res) {
          var fr = new FileReader();
          fr.onload = function () { try { res(JSON.parse(fr.result)); } catch (err) { res(null); } };
          fr.onerror = function () { res(null); };
          fr.readAsText(f);
        }));
      })(files[i]);
    }
    Promise.all(readers).then(function (objs) {
      var valid = objs.filter(function (o) { return o && o.pairs; });
      if (!valid.length) { toast("文件格式不正确"); return; }
      Store.importShards(valid).then(function () {
        toast("导入成功，已合并 " + valid.length + " 个文件");
        closeModal();
        var active = document.querySelector(".nav-item.active");
        showPage(active ? active.dataset.page : "plans");
      });
    });
  }

  function setupBackupUI() {
    var fab = document.getElementById("backup-fab");
    if (fab) fab.addEventListener("click", openBackup);
    document.addEventListener("visibilitychange", function () { if (document.hidden) Store.flush(); });
  }

  function startApp() {
    document.querySelectorAll(".nav-item").forEach(function (b) {
      b.addEventListener("click", function () { showPage(b.dataset.page); });
    });
    ["plans", "poop", "love", "countdown"].forEach(function (p) {
      var sec = document.getElementById("page-" + p);
      sec.addEventListener("click", function (e) {
        var t = e.target.closest("[data-act]");
        if (t) dispatch(p, t.dataset.act, t, e);
      });
      sec.addEventListener("keydown", function (e) {
        if (p === "plans") Plans.handleKey(e);
        if (p === "love") Love.handleKey(e);
      });
      sec.addEventListener("change", function (e) {
        if (p === "love") Love.handleChange(e);
        if (p === "plans") Plans.handleChange(e);
      });
      sec.addEventListener("input", function (e) {
        if (p === "love") Love.handleInput(e);
      });
    });
    showPage("plans");
    scheduleReminder();
  }

  document.addEventListener("DOMContentLoaded", function () {
    registerSW();
    setupBackupUI();
    var p0 = document.getElementById("page-plans");
    if (p0) p0.innerHTML = '<div class="loading">正在加载本地数据…</div>';
    var ready = Store.init();
    var guard = new Promise(function (res) { setTimeout(res, 4000); });
    Promise.race([ready, guard]).then(function () { startApp(); });
  });
})();
