(function () {
  "use strict";

  var HOST = "relay.bitcraftsync.app";
  var SUBPROTOCOL = "v1.json.spacetimedb";
  var ROW_CAP = 75000;
  var ROW_HEIGHT = 28;
  var OVERSCAN = 12;

  var HUGE_TABLES = {
    location_state: true,
    footprint_tile_state: true,
    inventory_state: true,
    mobile_entity_state: true,
    building_state: true,
    terrain_chunk_state: true,
    claim_tile_state: true,
    player_lower_body_clothing_state: true,
  };

  var FALLBACK_REGIONS = [
    { name: "global", port: 3000, database: "relay-mirror-bc-global" },
    { name: "bitcraft-live-3", port: 3003, database: "relay-mirror-bc3" },
    { name: "bitcraft-live-7", port: 3007, database: "relay-mirror-bc7" },
    { name: "bitcraft-live-8", port: 3008, database: "relay-mirror-bc8" },
    { name: "bitcraft-live-9", port: 3009, database: "relay-mirror-bc9" },
    { name: "bitcraft-live-11", port: 3011, database: "relay-mirror-bc11" },
    { name: "bitcraft-live-12", port: 3012, database: "relay-mirror-bc12" },
    { name: "bitcraft-live-13", port: 3013, database: "relay-mirror-bc13" },
    { name: "bitcraft-live-14", port: 3014, database: "relay-mirror-bc14" },
    { name: "bitcraft-live-15", port: 3015, database: "relay-mirror-bc15" },
    { name: "bitcraft-live-17", port: 3017, database: "relay-mirror-bc17" },
    { name: "bitcraft-live-18", port: 3018, database: "relay-mirror-bc18" },
    { name: "bitcraft-live-19", port: 3019, database: "relay-mirror-bc19" },
    { name: "bitcraft-live-23", port: 3023, database: "relay-mirror-bc23" },
  ];

  // ---- DOM ----
  var elRegion = document.getElementById("region");
  var elTableFilter = document.getElementById("table-filter");
  var elTableList = document.getElementById("table-list");
  var elSql = document.getElementById("sql");
  var elBtnSub = document.getElementById("btn-subscribe");
  var elBtnDisc = document.getElementById("btn-disconnect");
  var elStatus = document.getElementById("status");
  var elHugeWarn = document.getElementById("huge-warn");
  var elError = document.getElementById("error-banner");
  var elMeta = document.getElementById("meta");
  var elMetaTable = document.getElementById("meta-table");
  var elMetaCols = document.getElementById("meta-cols");
  var elFilters = document.getElementById("filters");
  var elRowSearch = document.getElementById("row-search");
  var elRowCount = document.getElementById("row-count");
  var elPlaceholder = document.getElementById("placeholder");
  var elGridInner = document.getElementById("grid-inner");
  var elGridWrap = document.getElementById("grid-wrap");
  var elGridHead = document.getElementById("grid-head");
  var elGridBody = document.getElementById("grid-body");
  var elGrid = document.getElementById("grid");

  // ---- state ----
  var regions = [];
  var schema = null;
  var tables = []; // { name, columns:[{name,type,pk}], primaryKey: string[] }
  var selectedTable = null;
  var ws = null;
  var requestId = 1;
  var rows = []; // array of plain objects
  var rowByKey = new Map();
  var pkCols = [];
  var colNames = [];
  var sortCol = null;
  var sortDir = 1; // 1 asc, -1 desc
  var searchQ = "";
  var filteredIdx = []; // indices into rows
  var capped = false;
  var bytesIn = 0;
  var live = false;
  var scrollRaf = 0;
  var progressRaf = 0;

  // ---- helpers ----
  function regionNumber(name, port) {
    if (name === "global" || /global/.test(name)) return 0;
    var m = /(?:live-|bc)(\d+)/.exec(name);
    if (m) return parseInt(m[1], 10);
    return port - 3000;
  }

  function sortRegions(list) {
    return list.slice().sort(function (a, b) {
      return regionNumber(a.name, a.port) - regionNumber(b.name, b.port);
    });
  }

  function regionLabel(r) {
    var n = regionNumber(r.name, r.port);
    if (n === 0) return "global — " + r.database + " :" + r.port;
    return "region " + n + " — " + r.database + " :" + r.port;
  }

  function schemaUrl(port, database) {
    return "https://" + HOST + ":" + port +
      "/v1/database/" + encodeURIComponent(database) + "/schema?version=9";
  }

  function subscribeUrl(port, database) {
    return "wss://" + HOST + ":" + port +
      "/v1/database/" + encodeURIComponent(database) + "/subscribe?compression=None";
  }

  function setStatus(html) {
    elStatus.innerHTML = html;
  }

  function showError(msg) {
    if (!msg) {
      elError.classList.remove("show");
      elError.textContent = "";
      return;
    }
    elError.textContent = msg;
    elError.classList.add("show");
  }

  function updateHugeWarn() {
    if (!selectedTable || !HUGE_TABLES[selectedTable.name]) {
      elHugeWarn.classList.remove("show");
      elHugeWarn.textContent = "";
      return;
    }
    var sql = elSql.value || "";
    var hasWhere = /\bWHERE\b/i.test(sql);
    if (hasWhere) {
      elHugeWarn.classList.remove("show");
      return;
    }
    elHugeWarn.innerHTML =
      "<b>Large table.</b> <code>" + selectedTable.name + "</code> can be " +
      "hundreds of MB. Add a <code>WHERE</code> clause before subscribing, " +
      "or the browser may hang / run out of memory.";
    elHugeWarn.classList.add("show");
  }

  function cellText(v) {
    if (v === null || v === undefined) return "";
    if (typeof v === "object") {
      try { return JSON.stringify(v); } catch (e) { return String(v); }
    }
    return String(v);
  }

  function rowKey(row) {
    if (pkCols.length) {
      return pkCols.map(function (c) { return cellText(row[c]); }).join("\0");
    }
    try { return JSON.stringify(row); } catch (e) { return String(Math.random()); }
  }

  // ---- schema parse ----
  function resolveType(types, ty) {
    var guard = 0;
    while (ty && ty.Ref !== undefined && guard++ < 64) {
      ty = types[ty.Ref];
    }
    return ty;
  }

  function typeLabel(types, ty) {
    ty = resolveType(types, ty);
    if (!ty || typeof ty !== "object") return "?";
    var keys = Object.keys(ty);
    if (keys.length !== 1) return "?";
    var k = keys[0];
    if (k === "Array") return "Array<" + typeLabel(types, ty.Array) + ">";
    if (k === "Product") return "Product";
    if (k === "Sum") return "Sum";
    return k;
  }

  function fieldName(el, i) {
    if (!el || !el.name) return "col_" + i;
    if (typeof el.name === "string") return el.name;
    if (el.name.some !== undefined) return el.name.some;
    return "col_" + i;
  }

  function parseTables(raw) {
    var types = (raw.typespace && raw.typespace.types) || [];
    var out = [];
    var list = raw.tables || [];
    for (var i = 0; i < list.length; i++) {
      var t = list[i];
      if (!t || !t.name) continue;
      // Skip private / system if flagged
      var access = t.table_access;
      if (access && access.Private !== undefined) continue;
      var kind = t.table_type;
      if (kind && kind.System !== undefined) continue;

      var ty = resolveType(types, types[t.product_type_ref]);
      var elements = (ty && ty.Product && ty.Product.elements) || [];
      var pkIdx = t.primary_key || [];
      var columns = [];
      for (var j = 0; j < elements.length; j++) {
        var el = elements[j];
        columns.push({
          name: fieldName(el, j),
          type: typeLabel(types, el.algebraic_type),
          pk: pkIdx.indexOf(j) >= 0 || pkIdx.indexOf(String(j)) >= 0,
        });
      }
      out.push({
        name: t.name,
        columns: columns,
        primaryKey: columns.filter(function (c) { return c.pk; }).map(function (c) { return c.name; }),
        huge: !!HUGE_TABLES[t.name],
      });
    }
    out.sort(function (a, b) { return a.name.localeCompare(b.name); });
    return out;
  }

  // ---- region / schema UI ----
  function populateRegions(list) {
    regions = sortRegions(list);
    elRegion.innerHTML = "";
    for (var i = 0; i < regions.length; i++) {
      var opt = document.createElement("option");
      opt.value = String(i);
      opt.textContent = regionLabel(regions[i]);
      elRegion.appendChild(opt);
    }
    elRegion.disabled = false;
    elRegion.addEventListener("change", onRegionChange);
    if (regions.length) onRegionChange();
  }

  function loadHealth() {
    return fetch("https://" + HOST + "/health")
      .then(function (r) {
        if (!r.ok) throw new Error("health " + r.status);
        return r.json();
      })
      .then(function (data) {
        var src = data.sources || {};
        var list = [];
        Object.keys(src).forEach(function (name) {
          var s = src[name];
          if (!s || !s.port || !s.database) return;
          list.push({ name: name, port: s.port, database: s.database });
        });
        if (!list.length) throw new Error("no sources");
        populateRegions(list);
      })
      .catch(function () {
        populateRegions(FALLBACK_REGIONS);
        setStatus('<span class="warn">/health unavailable — using fallback region list</span>');
      });
  }

  function currentRegion() {
    var i = parseInt(elRegion.value, 10);
    return regions[i] || null;
  }

  function onRegionChange() {
    disconnect();
    selectedTable = null;
    tables = [];
    schema = null;
    elTableList.innerHTML = '<div class="loading">Loading schema&hellip;</div>';
    elTableFilter.disabled = true;
    elTableFilter.value = "";
    elSql.disabled = true;
    elBtnSub.disabled = true;
    elMeta.style.display = "none";
    elFilters.style.display = "none";
    elPlaceholder.style.display = "";
    elGridInner.style.display = "none";
    updateHugeWarn();
    showError("");

    var r = currentRegion();
    if (!r) return;
    setStatus("Fetching schema for " + r.database + "&hellip;");

    fetch(schemaUrl(r.port, r.database))
      .then(function (resp) {
        if (!resp.ok) throw new Error("schema HTTP " + resp.status);
        return resp.json();
      })
      .then(function (raw) {
        schema = raw;
        tables = parseTables(raw);
        elTableFilter.disabled = false;
        renderTableList();
        setStatus(
          '<span class="ok">' + tables.length + " tables</span> · " +
          r.database + " · port " + r.port
        );
      })
      .catch(function (e) {
        elTableList.innerHTML =
          '<div class="empty">Failed to load schema: ' + escapeHtml(String(e.message || e)) + "</div>";
        setStatus('<span class="err">schema fetch failed</span>');
        showError(
          "Could not fetch schema (CORS or network). Ensure the relay frontend " +
          "allows origin https://" + HOST + "."
        );
      });
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function renderTableList() {
    var q = (elTableFilter.value || "").trim().toLowerCase();
    var html = "";
    var n = 0;
    for (var i = 0; i < tables.length; i++) {
      var t = tables[i];
      if (q && t.name.toLowerCase().indexOf(q) < 0) continue;
      n++;
      var active = selectedTable && selectedTable.name === t.name ? " active" : "";
      var badge = t.huge ? '<span class="badge">large</span>' : "";
      html +=
        '<button type="button" class="table-item' + active + '" data-name="' +
        escapeHtml(t.name) + '">' + escapeHtml(t.name) + badge + "</button>";
    }
    if (!n) {
      html = '<div class="empty">No tables match.</div>';
    }
    elTableList.innerHTML = html;
  }

  elTableList.addEventListener("click", function (ev) {
    var btn = ev.target.closest(".table-item");
    if (!btn) return;
    var name = btn.getAttribute("data-name");
    var t = null;
    for (var i = 0; i < tables.length; i++) {
      if (tables[i].name === name) { t = tables[i]; break; }
    }
    if (!t) return;
    selectTable(t);
  });

  elTableFilter.addEventListener("input", renderTableList);

  function selectTable(t) {
    disconnect();
    selectedTable = t;
    renderTableList();
    elSql.value = "SELECT * FROM " + t.name;
    elSql.disabled = false;
    elBtnSub.disabled = false;
    elMeta.style.display = "";
    elMetaTable.textContent = t.name + " · ";
    elMetaCols.textContent = t.columns.map(function (c) {
      return c.name + (c.pk ? "*" : "") + ":" + c.type;
    }).join(", ");
    elPlaceholder.style.display = "none";
    elGridInner.style.display = "none";
    elFilters.style.display = "none";
    clearRows();
    updateHugeWarn();
    showError("");
    setStatus("Ready — edit SQL if needed, then Subscribe");
  }

  elSql.addEventListener("input", updateHugeWarn);

  // ---- row store / filter / sort / virtual grid ----
  function clearRows() {
    rows = [];
    rowByKey = new Map();
    filteredIdx = [];
    capped = false;
    bytesIn = 0;
    live = false;
    sortCol = null;
    sortDir = 1;
    searchQ = "";
    elRowSearch.value = "";
  }

  function addRow(row) {
    if (capped) return;
    if (rows.length >= ROW_CAP) {
      capped = true;
      setStatus(
        elStatus.innerHTML +
        ' · <span class="warn">row cap ' + ROW_CAP.toLocaleString() +
        " reached — stop ingesting; add a WHERE filter</span>"
      );
      return;
    }
    var k = rowKey(row);
    if (rowByKey.has(k)) {
      var idx = rowByKey.get(k);
      rows[idx] = row;
    } else {
      rowByKey.set(k, rows.length);
      rows.push(row);
    }
  }

  function deleteRow(row) {
    var k = rowKey(row);
    if (!rowByKey.has(k)) return;
    var idx = rowByKey.get(k);
    // swap-remove
    var last = rows.length - 1;
    if (idx !== last) {
      rows[idx] = rows[last];
      rowByKey.set(rowKey(rows[idx]), idx);
    }
    rows.pop();
    rowByKey.delete(k);
  }

  function recomputeFilter() {
    filteredIdx = [];
    var q = searchQ;
    for (var i = 0; i < rows.length; i++) {
      if (!q) {
        filteredIdx.push(i);
        continue;
      }
      var row = rows[i];
      var hit = false;
      for (var c = 0; c < colNames.length; c++) {
        if (cellText(row[colNames[c]]).toLowerCase().indexOf(q) >= 0) {
          hit = true;
          break;
        }
      }
      if (hit) filteredIdx.push(i);
    }
    if (sortCol) {
      var col = sortCol;
      var dir = sortDir;
      filteredIdx.sort(function (ai, bi) {
        var a = cellText(rows[ai][col]);
        var b = cellText(rows[bi][col]);
        var an = Number(a), bn = Number(b);
        var cmp;
        if (a !== "" && b !== "" && !isNaN(an) && !isNaN(bn)) {
          cmp = an < bn ? -1 : an > bn ? 1 : 0;
        } else {
          cmp = a < b ? -1 : a > b ? 1 : 0;
        }
        return cmp * dir;
      });
    }
    elRowCount.textContent =
      filteredIdx.length.toLocaleString() + " / " +
      rows.length.toLocaleString() + " rows" +
      (capped ? " (capped)" : "") +
      (live ? " · live" : "");
    renderVirtual();
  }

  function setupGrid() {
    colNames = selectedTable.columns.map(function (c) { return c.name; });
    pkCols = selectedTable.primaryKey.slice();
    var head = "<tr>";
    for (var i = 0; i < selectedTable.columns.length; i++) {
      var c = selectedTable.columns[i];
      var w = Math.min(320, Math.max(100, c.name.length * 9 + 40));
      head +=
        '<th data-col="' + escapeHtml(c.name) + '" style="width:' + w + 'px">' +
        escapeHtml(c.name) +
        (c.pk ? '<span class="pk">PK</span>' : "") +
        '<span class="sort" data-sort="' + escapeHtml(c.name) + '"></span></th>';
    }
    head += "</tr>";
    elGridHead.innerHTML = head;
    elPlaceholder.style.display = "none";
    elGridInner.style.display = "";
    elFilters.style.display = "flex";
    elGrid.style.width = (selectedTable.columns.length * 160) + "px";
  }

  elGridHead.addEventListener("click", function (ev) {
    var th = ev.target.closest("th");
    if (!th) return;
    var col = th.getAttribute("data-col");
    if (!col) return;
    if (sortCol === col) sortDir = -sortDir;
    else { sortCol = col; sortDir = 1; }
    // update sort indicators
    var spans = elGridHead.querySelectorAll(".sort");
    for (var i = 0; i < spans.length; i++) {
      var s = spans[i];
      if (s.getAttribute("data-sort") === sortCol) {
        s.textContent = sortDir > 0 ? "▲" : "▼";
      } else {
        s.textContent = "";
      }
    }
    recomputeFilter();
  });

  elRowSearch.addEventListener("input", function () {
    searchQ = (elRowSearch.value || "").trim().toLowerCase();
    recomputeFilter();
  });

  function renderVirtual() {
    var wrap = elGridWrap;
    var scrollTop = wrap.scrollTop;
    var viewH = wrap.clientHeight;
    var total = filteredIdx.length;
    var totalH = total * ROW_HEIGHT + 36; // + header approx handled by sticky
    elGridInner.style.height = Math.max(viewH, total * ROW_HEIGHT + 40) + "px";

    var start = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
    var end = Math.min(total, Math.ceil((scrollTop + viewH) / ROW_HEIGHT) + OVERSCAN);

    var html = "";
    // spacer top
    if (start > 0) {
      html += '<tr style="height:' + (start * ROW_HEIGHT) + 'px"><td colspan="' +
        colNames.length + '" style="padding:0;border:none;height:' +
        (start * ROW_HEIGHT) + 'px"></td></tr>';
    }
    for (var i = start; i < end; i++) {
      var row = rows[filteredIdx[i]];
      html += "<tr>";
      for (var c = 0; c < colNames.length; c++) {
        var txt = cellText(row[colNames[c]]);
        html += '<td title="' + escapeHtml(txt) + '">' + escapeHtml(txt) + "</td>";
      }
      html += "</tr>";
    }
    if (end < total) {
      html += '<tr style="height:' + ((total - end) * ROW_HEIGHT) + 'px"><td colspan="' +
        colNames.length + '" style="padding:0;border:none;height:' +
        ((total - end) * ROW_HEIGHT) + 'px"></td></tr>';
    }
    elGridBody.innerHTML = html;
    void totalH;
  }

  elGridWrap.addEventListener("scroll", function () {
    if (scrollRaf) return;
    scrollRaf = requestAnimationFrame(function () {
      scrollRaf = 0;
      renderVirtual();
    });
  });

  // ---- WebSocket v1.json ----
  function parseRow(raw) {
    if (typeof raw === "string") {
      try { return JSON.parse(raw); } catch (e) { return { _raw: raw }; }
    }
    return raw;
  }

  function ingestTableUpdate(tableUpdate) {
    if (!tableUpdate) return;
    var updates = tableUpdate.updates || [];
    for (var i = 0; i < updates.length; i++) {
      var u = updates[i];
      var deletes = u.deletes || [];
      var inserts = u.inserts || [];
      for (var d = 0; d < deletes.length; d++) {
        deleteRow(parseRow(deletes[d]));
      }
      for (var n = 0; n < inserts.length; n++) {
        addRow(parseRow(inserts[n]));
      }
    }
  }

  function handleServerMessage(msg) {
    if (msg.IdentityToken) {
      setStatus('<span class="ok">Connected</span> — sending Subscribe&hellip;');
      var sql = elSql.value.trim();
      ws.send(JSON.stringify({
        Subscribe: {
          request_id: requestId++,
          query_strings: [sql],
        },
      }));
      return;
    }
    if (msg.InitialSubscription) {
      var isub = msg.InitialSubscription;
      var db = isub.database_update || {};
      var tbls = db.tables || [];
      for (var i = 0; i < tbls.length; i++) {
        ingestTableUpdate(tbls[i]);
      }
      live = true;
      recomputeFilter();
      setStatus(
        '<span class="ok">Subscribed</span> · ' +
        rows.length.toLocaleString() + " rows · " +
        formatBytes(bytesIn) + " received · listening for updates"
      );
      return;
    }
    // v1 SubscribeMulti path (rare here) — same DatabaseUpdate payload
    if (msg.SubscribeApplied) {
      var sa = msg.SubscribeApplied;
      var upd = sa.update || sa.database_update || {};
      var tblsSa = upd.tables || [];
      for (var isi = 0; isi < tblsSa.length; isi++) {
        ingestTableUpdate(tblsSa[isi]);
      }
      live = true;
      recomputeFilter();
      setStatus(
        '<span class="ok">Subscribed</span> · ' +
        rows.length.toLocaleString() + " rows · " +
        formatBytes(bytesIn) + " received · listening for updates"
      );
      return;
    }
    if (msg.TransactionUpdate) {
      // v1 JSON: status.Committed is a DatabaseUpdate { tables: [...] }
      var tu = msg.TransactionUpdate;
      var status = tu.status || {};
      var committed = status.Committed;
      if (!committed) return;
      var tables2 = committed.tables || [];
      for (var j = 0; j < tables2.length; j++) {
        ingestTableUpdate(tables2[j]);
      }
      recomputeFilter();
      setStatus(
        '<span class="ok">Live</span> · ' +
        rows.length.toLocaleString() + " rows · " +
        formatBytes(bytesIn) + " received"
      );
      return;
    }
    // v2 stdb serving v1.json often broadcasts rows-only TUL (no rewrite
    // on the JSON path). Treat like a committed DatabaseUpdate.
    if (msg.TransactionUpdateLight) {
      var tul = msg.TransactionUpdateLight;
      var tulUpd = tul.update || {};
      var tablesTul = tulUpd.tables || [];
      for (var k = 0; k < tablesTul.length; k++) {
        ingestTableUpdate(tablesTul[k]);
      }
      if (live) {
        recomputeFilter();
        setStatus(
          '<span class="ok">Live</span> · ' +
          rows.length.toLocaleString() + " rows · " +
          formatBytes(bytesIn) + " received"
        );
      }
      return;
    }
    if (msg.SubscriptionError) {
      showError("SubscriptionError: " + JSON.stringify(msg.SubscriptionError));
      setStatus('<span class="err">SubscriptionError</span>');
      return;
    }
    // Ignore other message kinds (TransactionUpdateLight passthrough etc.)
  }

  function formatBytes(n) {
    if (n < 1024) return n + " B";
    if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
    return (n / (1024 * 1024)).toFixed(1) + " MB";
  }

  function disconnect() {
    if (ws) {
      try { ws.close(); } catch (e) { /* ignore */ }
      ws = null;
    }
    elBtnDisc.disabled = true;
    if (selectedTable) elBtnSub.disabled = false;
  }

  function subscribe() {
    var r = currentRegion();
    if (!r || !selectedTable) return;

    var sql = elSql.value.trim();
    if (!sql) {
      showError("SQL is empty.");
      return;
    }

    if (HUGE_TABLES[selectedTable.name] && !/\bWHERE\b/i.test(sql)) {
      var ok = window.confirm(
        selectedTable.name + " is a large table.\n\n" +
        "Subscribing without a WHERE clause may download hundreds of MB and " +
        "freeze this tab.\n\nContinue anyway?"
      );
      if (!ok) return;
    }

    disconnect();
    clearRows();
    setupGrid();
    recomputeFilter();
    showError("");
    bytesIn = 0;
    live = false;
    capped = false;
    elBtnSub.disabled = true;
    elBtnDisc.disabled = false;
    setStatus("Connecting&hellip;");

    var url = subscribeUrl(r.port, r.database);
    try {
      ws = new WebSocket(url, [SUBPROTOCOL]);
    } catch (e) {
      showError(String(e.message || e));
      elBtnSub.disabled = false;
      elBtnDisc.disabled = true;
      return;
    }

    ws.onopen = function () {
      setStatus('<span class="ok">WebSocket open</span> — waiting for IdentityToken&hellip;');
    };

    ws.onmessage = function (ev) {
      var data = ev.data;
      if (typeof data !== "string") return; // ignore binary/ping
      bytesIn += data.length;
      var msg;
      try {
        msg = JSON.parse(data);
      } catch (e) {
        showError("Bad JSON frame: " + String(e.message || e));
        return;
      }
      try {
        handleServerMessage(msg);
      } catch (e) {
        showError("Handler error: " + String(e.message || e));
      }
      // Progress while still receiving (rare for v1 — Applied is usually one frame)
      if (!live && rows.length && !progressRaf) {
        progressRaf = requestAnimationFrame(function () {
          progressRaf = 0;
          if (live) return;
          setStatus(
            "Receiving snapshot&hellip; " +
            rows.length.toLocaleString() + " rows · " +
            formatBytes(bytesIn)
          );
          recomputeFilter();
        });
      }
    };

    ws.onerror = function () {
      setStatus('<span class="err">WebSocket error</span>');
    };

    ws.onclose = function (ev) {
      ws = null;
      elBtnDisc.disabled = true;
      elBtnSub.disabled = !selectedTable;
      var code = ev.code || 0;
      if (!live && rows.length === 0) {
        setStatus('<span class="err">Disconnected</span> (code ' + code + ")");
      } else {
        setStatus(
          (live ? '<span class="warn">Disconnected</span>' : '<span class="ok">Done</span>') +
          " · " + rows.length.toLocaleString() + " rows cached · code " + code
        );
        live = false;
        recomputeFilter();
      }
    };
  }

  elBtnSub.addEventListener("click", subscribe);
  elBtnDisc.addEventListener("click", function () {
    disconnect();
    setStatus("Disconnected · " + rows.length.toLocaleString() + " rows still in grid");
    live = false;
    recomputeFilter();
  });

  window.addEventListener("beforeunload", disconnect);

  // boot
  loadHealth();
})();
