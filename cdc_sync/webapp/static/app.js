// CDC 监控前端：轮询只读端点，渲染状态卡与表格；操作按钮调用 POST 端点。
'use strict';

const $ = (sel) => document.querySelector(sel);
const esc = (s) => String(s == null ? '' : s)
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

async function getJSON(url) {
  const r = await fetch(url);
  if (r.status === 401) showLogin();
  return r.json();
}
async function postJSON(url) {
  const r = await fetch(url, { method: 'POST' });
  if (r.status === 401) showLogin();
  return r.json();
}
// 带鉴权的 fetch 封装：会话过期(401)时弹登录框
async function fetchAuth(url, opts) {
  const r = await fetch(url, opts);
  if (r.status === 401) showLogin();
  return r;
}

// ---------------- 登录 / 退出 ----------------
function clearPolling() {
  if (timer) { clearInterval(timer); timer = null; }
  syncing = false;
}

// 会话失效/未登录：跳到独立登录页（不再用遮罩）
function showLogin() {
  clearPolling();
  window.location.replace('/login');
}

async function doLogout() {
  clearPolling();
  try {
    await fetch('/api/logout', { method: 'POST' });
  } catch (e) { /* ignore */ }
  showLogin();
}

function statusClass(s) {
  if (s === 'ok') return 'ok';
  if (s === 'warn') return 'warn';
  return 'error';
}

// ---------------- 概览卡片 ----------------
function renderCards(ov) {
  const cfg = ov.config || {};
  setCard('config', cfg.status || 'error',
    cfg.status === 'ok' ? `${cfg.tables} 张表` : '配置错误',
    cfg.status === 'ok' ? `连接器 ${esc(cfg.connector)}` : esc(cfg.error));

  const c = (ov.components || {});
  const conn = c.connect || { status: 'error', error: '无数据' };
  setCard('connect', conn.status,
    conn.state || (conn.status === 'error' ? '不可达' : '—'),
    conn.status === 'error' ? esc(conn.error) : `任务 ${conn.tasks_running}/${conn.tasks_total} RUNNING`);

  const k = c.kafka || { status: 'error', error: '无数据' };
  setCard('kafka', k.status,
    k.status === 'error' ? '不可达' : `堆积 ${k.total_lag}`,
    k.status === 'error' ? esc(k.error) : `${k.topics} 个 topic`);

  const ck = c.clickhouse || { status: 'error', error: '无数据' };
  setCard('clickhouse', ck.status,
    ck.status === 'error' ? '不可达' : `${ck.consumers} 消费者`,
    ck.status === 'error' ? esc(ck.error) : `异常 ${ck.with_exception}`);
}

function setCard(key, status, big, detail) {
  const card = document.querySelector(`.card[data-key="${key}"]`);
  if (!card) return;
  card.className = 'card ' + statusClass(status);
  card.querySelector('.card-body').innerHTML =
    `<span class="dot ${statusClass(status)}"></span>${esc(big)}<div class="detail">${detail}</div>`;
}

// ---------------- 连接器任务 ----------------
async function loadConnector() {
  const box = $('#connectorBox');
  const res = await getJSON('/api/connectors');
  if (!res.ok) { box.innerHTML = `<div class="errline">✗ ${esc(res.error)}</div>`; return; }
  const st = res.data;
  const conn = st.connector || {};
  const tasks = st.tasks || [];
  let html = `<table><tr><th>对象</th><th>状态</th><th>worker</th></tr>`;
  html += `<tr><td>connector</td><td>${badge(conn.state)}</td><td>${esc(conn.worker_id || '')}</td></tr>`;
  for (const t of tasks) {
    html += `<tr><td>task #${t.id}</td><td>${badge(t.state)}</td><td>${esc(t.worker_id || '')}</td></tr>`;
    if (t.trace) html += `<tr><td colspan="3"><div class="errline">${esc(t.trace.split('\n')[0])}</div></td></tr>`;
  }
  html += `</table>`;
  box.innerHTML = html;
}

function badge(state) {
  const s = (state || '').toUpperCase();
  if (s === 'RUNNING') return `<span class="badge ok">${esc(s)}</span>`;
  if (s === 'PAUSED') return `<span class="badge warn">${esc(s)}</span>`;
  return `<span class="badge err">${esc(s || '?')}</span>`;
}

// ---------------- Kafka 堆积 ----------------
async function loadLag() {
  const box = $('#lagBox');
  const res = await getJSON('/api/kafka/lag');
  if (!res.ok) { box.innerHTML = `<div class="errline">✗ ${esc(res.error)}</div>`; return; }
  const rows = res.data;
  if (rows.length === 1 && rows[0].error) { box.innerHTML = `<div class="errline">✗ ${esc(rows[0].error)}</div>`; return; }
  let html = `<table><tr><th>表</th><th>topic</th><th class="num">分区</th><th class="num">末端</th><th class="num">已消费</th><th class="num">堆积</th></tr>`;
  for (const r of rows) {
    if (r.error) { html += `<tr><td>${esc(r.table)}</td><td colspan="5"><span class="errline">${esc(r.error)}</span></td></tr>`; continue; }
    const lagCls = (r.lag > 0) ? 'warn' : 'ok';
    html += `<tr><td>${esc(r.table)}</td><td>${esc(r.topic)}</td>`
      + `<td class="num">${r.partitions}</td><td class="num">${r.end_offset}</td>`
      + `<td class="num">${r.committed}</td><td class="num"><span class="badge ${lagCls}">${r.lag}</span></td></tr>`;
  }
  html += `</table>`;
  box.innerHTML = html;
}

// ---------------- CK 消费状态 ----------------
async function loadCK() {
  const box = $('#ckBox');
  const res = await getJSON('/api/clickhouse/consumers');
  if (!res.ok) { box.innerHTML = `<div class="errline">✗ ${esc(res.error)}</div>`; return; }
  const rows = res.data;
  if (!rows.length) { box.innerHTML = `<div class="muted">system.kafka_consumers 无记录（尚未消费）</div>`; return; }
  let html = `<table><tr><th>表</th><th class="num">已读</th><th>最近异常</th></tr>`;
  for (const r of rows) {
    const exc = r.last_exception || '';
    html += `<tr><td>${esc(r.database)}.${esc(r.table)}</td>`
      + `<td class="num">${esc(r.num_messages_read)}</td>`
      + `<td>${exc ? `<span class="errline">${esc(exc)}</span>` : '<span class="badge ok">正常</span>'}</td></tr>`;
  }
  html += `</table>`;
  box.innerHTML = html;
}

// ---------------- 对账 ----------------
async function loadReconcile(run) {
  const box = $('#reconcileBox');
  box.innerHTML = '<span class="muted">对账中…</span>';
  const res = await getJSON(run ? '/api/reconcile/run' : '/api/reconcile');
  if (!res.ok) { box.innerHTML = `<div class="errline">✗ ${esc(res.error)}</div>`; return; }
  const rows = res.data;
  let html = `<table><tr><th>表</th><th class="num">MySQL</th><th class="num">ClickHouse</th><th class="num">差异</th><th>状态</th></tr>`;
  for (const r of rows) {
    const cls = r.consistent ? 'ok' : 'warn';
    html += `<tr><td>${esc(r.table)}</td><td class="num">${r.mysql}</td>`
      + `<td class="num">${r.clickhouse}</td><td class="num">${r.diff}</td>`
      + `<td><span class="badge ${cls}">${r.consistent ? '一致' : '不一致'}</span></td></tr>`;
  }
  html += `</table>`;
  box.innerHTML = html;
}

// ---------------- 操作按钮 ----------------
async function doAction(action, btn) {
  const msg = $('#actionMsg');
  msg.textContent = '执行中…'; msg.className = 'action-msg';
  btn.disabled = true;
  try {
    let res;
    if (action === 'deploy-idle') res = await postJSON('/api/pipeline/deploy');
    else if (action === 'restart') res = await postJSON('/api/connectors/restart');
    else if (action === 'reconcile') { await loadReconcile(true); msg.textContent = '对账完成'; msg.className = 'action-msg ok'; btn.disabled = false; return; }
    if (res.ok) { msg.textContent = '✓ 成功：' + JSON.stringify(res.data); msg.className = 'action-msg ok'; }
    else { msg.textContent = '✗ ' + res.error; msg.className = 'action-msg err'; }
  } catch (e) {
    msg.textContent = '✗ ' + e; msg.className = 'action-msg err';
  }
  btn.disabled = false;
  refreshAll();
}

// ---------------- 登录告警 ----------------
const ALERT_TYPE_LABELS = {
  brute_force: '连续登录失败',
  risky_query: '高风险查询',
};
const ALERT_ACTION_LABELS = {
  login_fail: '密码错误',
  confirmed_execute: '确认执行',
};

async function loadAlerts() {
  const box = $('#alertsBox');
  if (!box) return;
  const res = await getJSON('/api/alerts');
  if (!res.ok) { box.innerHTML = `<div class="errline">✗ ${esc(res.error)}</div>`; return; }
  const rows = res.data;
  const tabBadge = $('#alertsTabBadge');
  if (!rows.length) {
    box.innerHTML = '<span class="muted">暂无告警</span>';
    if (tabBadge) { tabBadge.textContent = ''; tabBadge.style.display = 'none'; }
    return;
  }
  if (tabBadge) { tabBadge.textContent = rows.length; tabBadge.style.display = ''; }
  let html = `<table>
    <tr><th>时间</th><th>类型</th><th>行为</th><th>用户</th><th>IP</th><th>详情</th><th>语句</th></tr>`;
  for (const r of rows) {
    const typeLabel = ALERT_TYPE_LABELS[r.type] || r.type || '—';
    const actionLabel = ALERT_ACTION_LABELS[r.action] || r.action || '—';
    const sqlCell = r.sql
      ? `<button class="btn small js-toggle-sql">查看</button><pre class="sql-preview" style="display:none;margin-top:6px">${esc(r.sql)}</pre>`
      : '—';
    html += `<tr>
      <td style="white-space:nowrap">${esc(r.time)}</td>
      <td><span class="badge err">${esc(typeLabel)}</span></td>
      <td>${esc(actionLabel)}</td>
      <td>${esc(r.user || '—')}</td>
      <td>${esc(r.ip || '—')}</td>
      <td>${esc(r.detail || '')}</td>
      <td style="max-width:300px">${sqlCell}</td>
    </tr>`;
  }
  html += `</table>`;
  box.innerHTML = html;
  // 事件委托展开/收起 SQL（CSP 已禁用内联 onclick）
  box.querySelectorAll('.js-toggle-sql').forEach((btn) => {
    btn.addEventListener('click', () => {
      const pre = btn.nextElementSibling;
      if (!pre) return;
      const open = pre.style.display === 'none';
      pre.style.display = open ? '' : 'none';
      btn.textContent = open ? '收起' : '查看';
    });
  });
}

// ---------------- 刷新调度 ----------------
async function refreshAll() {
  try {
    const ov = await getJSON('/api/overview');
    renderCards(ov);
  } catch (e) { /* ignore */ }
  loadConnector();
  loadLag();
  loadCK();
  loadAlerts();
  $('#lastUpdate').textContent = '更新于 ' + new Date().toLocaleTimeString();
}

let timer = null;
let syncing = false;
let busy = false;

// 更新按钮/状态显示 + 轮询定时器（不触发后端）
function applyState(on) {
  syncing = on;
  const b = $('#pipelineToggle');
  if (on) {
    b.textContent = '⏸ 停止同步';
    b.classList.remove('primary'); b.classList.add('warn');
    $('#pipelineState').textContent = '同步进行中（每 5s 刷新）';
    if (timer) clearInterval(timer);
    timer = setInterval(refreshAll, 5000);
  } else {
    if (timer) { clearInterval(timer); timer = null; }
    b.textContent = '▶ 开启同步';
    b.classList.add('primary'); b.classList.remove('warn');
    $('#pipelineState').textContent = '同步已停止';
  }
}

// 点击主开关：真正开启/停止同步（调后端管道）
async function togglePipeline() {
  if (busy) return;
  const wantStart = !syncing;
  if (wantStart && !confirm('开启同步？将挂上 CK 消费并恢复连接器，开始抓取数据。')) return;
  if (!wantStart && !confirm('停止同步？将暂停连接器并摘除 CK 消费。')) return;
  busy = true;
  const b = $('#pipelineToggle'); b.disabled = true;
  $('#pipelineState').textContent = wantStart ? '正在开启…' : '正在停止…';
  try {
    const r = await postJSON(wantStart ? '/api/pipeline/start' : '/api/pipeline/stop');
    if (r.ok) {
      applyState(wantStart);
      if (wantStart) refreshAll();
    } else {
      $('#pipelineState').textContent = '✗ ' + r.error;
    }
  } catch (e) {
    $('#pipelineState').textContent = '✗ ' + e;
  }
  b.disabled = false; busy = false;
}

// ================= 配置页 =================

// 表单结构：分组 → 字段 [key, 标签, 类型]
const SETTINGS_SCHEMA = [
  ['mysql', 'MySQL (源库 / A 服)', [
    ['host', '主机', 'text'], ['port', '端口', 'number'], ['user', '用户', 'text'],
    ['password', '密码', 'password'], ['charset', '字符集', 'text'],
  ]],
  ['clickhouse', 'ClickHouse (目标 / B 服宿主)', [
    ['host', '主机', 'text'], ['port', 'HTTP端口', 'number'], ['user', '用户', 'text'],
    ['password', '密码', 'password'], ['database', '库', 'text'], ['secure', 'HTTPS', 'bool'],
  ]],
  ['kafka', 'Kafka', [
    ['broker_list', 'CK消费用 broker(宿主机地址)', 'text'],
    ['internal_broker_list', '容器内 broker(kafka:9092)', 'text'],
    ['sasl.enabled', '启用 SASL 认证', 'bool'],
    ['sasl.security_protocol', '协议 (SASL_PLAINTEXT / SASL_SSL)', 'text'],
    ['sasl.mechanism', '机制 (SCRAM-SHA-512 / SCRAM-SHA-256)', 'text'],
    ['sasl.username', '用户名', 'text'],
    ['sasl.password', '密码', 'password'],
  ]],
  ['schema_registry', 'Schema Registry', [['url', 'URL(容器内)', 'text'], ['url_for_ck', 'URL(CK宿主机用)', 'text']]],
  ['connect', 'Kafka Connect', [['url', 'REST URL', 'text']]],
  ['debezium', 'Debezium 连接器', [
    ['connector_name', '连接器名', 'text'], ['server_name', 'server.name', 'text'],
    ['server_id', 'server.id', 'number'], ['tasks_max', 'tasks.max', 'number'],
    ['snapshot_mode', 'snapshot.mode', 'text'], ['history_topic', 'history topic', 'text'],
  ]],
  ['panel', '监控面板登录', [
    ['user', '用户名', 'text'], ['password', '密码', 'password'],
    ['allow_default_password', '允许平台默认口令(仅本地调试)', 'bool'],
  ]],
];

// 支持 'sasl.enabled' 这类点号嵌套键
function getPath(obj, path) {
  return path.split('.').reduce((o, k) => (o == null ? undefined : o[k]), obj);
}
function setPath(obj, path, value) {
  const parts = path.split('.');
  let cur = obj;
  for (const k of parts.slice(0, -1)) {
    if (typeof cur[k] !== 'object' || cur[k] === null) cur[k] = {};
    cur = cur[k];
  }
  cur[parts[parts.length - 1]] = value;
}

// 可测试连通性的分组 → 测试端点组件名
const TESTABLE = new Set(['mysql', 'clickhouse', 'kafka', 'schema_registry', 'connect']);

async function loadSettings() {
  const box = $('#settingsForm');
  const res = await getJSON('/api/config/settings');
  if (!res.ok) { box.innerHTML = `<div class="errline">✗ ${esc(res.error)}</div>`; return; }
  const d = res.data;
  let html = '';
  for (const [group, title, fields] of SETTINGS_SCHEMA) {
    const testBtn = TESTABLE.has(group)
      ? `<button class="btn small" data-test="${group}">测试连接</button><span class="test-result" id="test-${group}"></span>`
      : '';
    html += `<fieldset><legend>${esc(title)} ${testBtn}</legend><div class="cfgrow">`;
    for (const [key, label, type] of fields) {
      const val = getPath(d[group] || {}, key);
      const id = `cfg-${group}-${key}`;
      if (type === 'bool') {
        html += `<label>${esc(label)} <input type="checkbox" id="${id}" ${val ? 'checked' : ''}></label>`;
      } else {
        html += `<label>${esc(label)} <input type="${type}" id="${id}" value="${esc(val == null ? '' : val)}"></label>`;
      }
    }
    html += `</div></fieldset>`;
  }
  box.innerHTML = html;
  // 绑定测试按钮（用当前表单值测试，无需先保存）
  box.querySelectorAll('[data-test]').forEach((b) =>
    b.addEventListener('click', () => testConn(b.dataset.test, b)));
}

async function testConn(component, btn) {
  const out = $(`#test-${component}`);
  out.textContent = ' 测试中…'; out.className = 'test-result';
  if (btn) btn.disabled = true;
  try {
    const r = await fetchAuth(`/api/test/${component}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(collectSettings()),
    }).then((x) => x.json());
    if (!r.ok) { out.textContent = ' ✗ ' + r.error; out.className = 'test-result err'; }
    else {
      const d = r.data;
      out.textContent = (d.ok ? ' ✓ ' : ' ✗ ') + d.message;
      out.className = 'test-result ' + (d.ok ? 'ok' : 'err');
    }
  } catch (e) {
    out.textContent = ' ✗ ' + e; out.className = 'test-result err';
  }
  if (btn) btn.disabled = false;
}

async function testAll() {
  for (const group of TESTABLE) {
    const btn = document.querySelector(`[data-test="${group}"]`);
    await testConn(group, btn);
  }
}

function collectSettings() {
  const out = {};
  for (const [group, , fields] of SETTINGS_SCHEMA) {
    out[group] = {};
    for (const [key, , type] of fields) {
      const el = document.getElementById(`cfg-${group}-${key}`);
      if (!el) continue;
      let v;
      if (type === 'bool') v = el.checked;
      else if (type === 'number') v = el.value === '' ? null : Number(el.value);
      else v = el.value;
      setPath(out[group], key, v);
    }
  }
  return out;
}

async function saveSettings() {
  const msg = $('#settingsMsg'); msg.textContent = '保存中…'; msg.className = 'action-msg';
  const r = await fetchAuth('/api/config/settings', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(collectSettings()),
  }).then((x) => x.json());
  if (r.ok) { msg.textContent = '✓ 已保存'; msg.className = 'action-msg ok'; refreshAll(); }
  else { msg.textContent = '✗ ' + r.error; msg.className = 'action-msg err'; }
}

async function loadTables() {
  const res = await getJSON('/api/config/tables');
  if (!res.ok) { $('#tablesMsg').textContent = '✗ ' + res.error; $('#tablesMsg').className = 'action-msg err'; return; }
  $('#tablesEditor').value = res.data.text;
  $('#tablesCount').textContent = res.data.count >= 0 ? `${res.data.count} 张表` : '（当前不合法）';
}

async function saveTables() {
  const msg = $('#tablesMsg'); msg.textContent = '保存中…'; msg.className = 'action-msg';
  const r = await fetchAuth('/api/config/tables', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text: $('#tablesEditor').value }),
  }).then((x) => x.json());
  if (r.ok) { msg.textContent = `✓ 已保存 ${r.data.count} 张表`; msg.className = 'action-msg ok'; $('#tablesCount').textContent = `${r.data.count} 张表`; }
  else { msg.textContent = '✗ ' + r.error; msg.className = 'action-msg err'; }
}

async function importSql() {
  const msg = $('#importMsg'); const file = $('#sqlFile').files[0];
  if (!file) { msg.textContent = '请先选择 .sql 文件'; msg.className = 'action-msg err'; return; }
  const database = $('#impDatabase').value.trim();
  if (!database) { msg.textContent = '请填写源库名'; msg.className = 'action-msg err'; return; }
  msg.textContent = '解析中…'; msg.className = 'action-msg';
  const sql = await file.text();
  const r = await fetchAuth('/api/config/import-sql', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, database, target_database: $('#impTargetDb').value.trim(), prefix: $('#impPrefix').value.trim() }),
  }).then((x) => x.json());
  if (r.ok) {
    msg.textContent = `✓ 生成 ${r.data.tables} 张表` + (r.data.no_pk.length ? `（无主键需补 order_by: ${r.data.no_pk.join(', ')}）` : '');
    msg.className = 'action-msg ok';
    loadTables();
  } else { msg.textContent = '✗ ' + r.error; msg.className = 'action-msg err'; }
}

function switchTab(name) {
  document.querySelectorAll('.tab').forEach((t) => t.classList.toggle('active', t.dataset.tab === name));
  $('#tab-monitor').style.display = name === 'monitor' ? '' : 'none';
  $('#tab-query').style.display = name === 'query' ? '' : 'none';
  $('#tab-config').style.display = name === 'config' ? '' : 'none';
  $('#tab-alerts').style.display = name === 'alerts' ? '' : 'none';
  if (name === 'config') { loadSettings(); loadTables(); }
  if (name === 'query') loadSavedQueries();
  if (name === 'alerts') loadAlerts();
}

// ================= 查询页 =================

let savedQueriesMap = {};

async function runQuery() {
  const msg = $('#queryMsg'); const box = $('#queryResult');
  const sql = $('#queryInput').value.trim();
  if (!sql) { msg.textContent = '请输入查询语句'; msg.className = 'action-msg err'; return; }
  localStorage.setItem('cdc_last_query', sql);

  msg.textContent = '执行中…'; msg.className = 'action-msg';
  box.innerHTML = '<span class="muted">查询中…</span>';
  const r = await fetchAuth('/api/query', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql }),
  }).then((x) => x.json());
  if (!r.ok) { msg.textContent = ''; box.innerHTML = `<div class="errline">✗ ${esc(r.error)}</div>`; return; }
  const d = r.data;
  msg.textContent = `${d.row_count} 行` + (d.truncated ? '（已截断到 1000 行）' : ''); msg.className = 'action-msg ok';
  if (!d.columns.length) { box.innerHTML = '<span class="muted">执行成功，无结果集</span>'; return; }
  let html = '<table><tr>' + d.columns.map((c) => `<th>${esc(c)}</th>`).join('') + '</tr>';
  for (const row of d.rows) {
    html += '<tr>' + row.map((v) => `<td>${esc(v)}</td>`).join('') + '</tr>';
  }
  html += '</table>';
  box.innerHTML = html;
}

// 风险确认弹窗（返回 Promise<boolean>）
async function loadSavedQueries() {
  const box = $('#savedQueries');
  const res = await getJSON('/api/queries');
  if (!res.ok) { box.innerHTML = `<div class="errline">✗ ${esc(res.error)}</div>`; return; }
  const items = res.data;
  if (!items.length) { box.innerHTML = '<span class="muted">还没有保存的查询</span>'; return; }
  let html = '<table><tr><th>名称</th><th>语句</th><th></th></tr>';
  for (const q of items) {
    const sqlPreview = (q.sql || '').replace(/\s+/g, ' ').slice(0, 80);
    html += `<tr><td><b>${esc(q.name)}</b></td><td class="muted">${esc(sqlPreview)}</td>`
      + `<td><button class="btn small" data-load="${esc(q.name)}">载入</button>`
      + `<button class="btn small" data-del="${esc(q.name)}">删除</button></td></tr>`;
  }
  html += '</table>';
  box.innerHTML = html;
  // 保存完整 sql 供载入
  savedQueriesMap = {};
  items.forEach((q) => { savedQueriesMap[q.name] = q.sql; });
  box.querySelectorAll('[data-load]').forEach((b) =>
    b.addEventListener('click', () => { $('#queryInput').value = savedQueriesMap[b.dataset.load] || ''; $('#queryName').value = b.dataset.load; switchTabToQueryInput(); }));
  box.querySelectorAll('[data-del]').forEach((b) =>
    b.addEventListener('click', () => deleteSavedQuery(b.dataset.del)));
}

function switchTabToQueryInput() {
  $('#queryInput').scrollIntoView({ behavior: 'smooth' });
}

async function exportQuery() {
  const msg = $('#queryMsg');
  const sql = $('#queryInput').value.trim();
  const fmt = $('#exportFormat').value;
  if (!sql) { msg.textContent = '请输入查询语句'; msg.className = 'action-msg err'; return; }
  msg.textContent = '导出中…'; msg.className = 'action-msg';
  try {
    const resp = await fetchAuth('/api/query/export', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql, format: fmt }),
    });
    const ct = resp.headers.get('content-type') || '';
    if (ct.includes('application/json')) {          // 出错返回 JSON
      const j = await resp.json();
      msg.textContent = '✗ ' + (j.error || '导出失败'); msg.className = 'action-msg err';
      return;
    }
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'query_export.' + fmt;
    document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
    msg.textContent = `✓ 已导出 ${fmt.toUpperCase()}`; msg.className = 'action-msg ok';
  } catch (e) {
    msg.textContent = '✗ ' + e; msg.className = 'action-msg err';
  }
}

async function saveQuery() {
  const msg = $('#queryMsg');
  const name = $('#queryName').value.trim();
  const sql = $('#queryInput').value.trim();
  if (!name) { msg.textContent = '请填写查询名字'; msg.className = 'action-msg err'; return; }
  if (!sql) { msg.textContent = '查询语句为空'; msg.className = 'action-msg err'; return; }
  const r = await fetchAuth('/api/queries', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, sql }),
  }).then((x) => x.json());
  if (r.ok) { msg.textContent = `✓ 已保存「${name}」`; msg.className = 'action-msg ok'; loadSavedQueries(); }
  else { msg.textContent = '✗ ' + r.error; msg.className = 'action-msg err'; }
}

async function deleteSavedQuery(name) {
  const r = await fetchAuth('/api/queries/' + encodeURIComponent(name), { method: 'DELETE' }).then((x) => x.json());
  if (r.ok) loadSavedQueries();
}

document.addEventListener('DOMContentLoaded', () => {
  $('#refreshBtn').addEventListener('click', refreshAll);
  $('#pipelineToggle').addEventListener('click', togglePipeline);
  document.querySelectorAll('[data-action]').forEach((b) =>
    b.addEventListener('click', () => doAction(b.dataset.action, b)));
  document.querySelectorAll('.tab').forEach((t) =>
    t.addEventListener('click', () => switchTab(t.dataset.tab)));
  $('#saveSettings').addEventListener('click', saveSettings);
  $('#reloadSettings').addEventListener('click', loadSettings);
  $('#testAll').addEventListener('click', testAll);
  $('#saveTables').addEventListener('click', saveTables);
  $('#reloadTables').addEventListener('click', loadTables);
  $('#importSql').addEventListener('click', importSql);
  // 查询页
  $('#runQuery').addEventListener('click', runQuery);
  $('#exportQuery').addEventListener('click', exportQuery);
  $('#saveQuery').addEventListener('click', saveQuery);
  $('#queryInput').addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runQuery(); }
  });
  const last = localStorage.getItem('cdc_last_query');
  if (last) $('#queryInput').value = last;
  // 退出登录
  $('#logoutBtn').addEventListener('click', doLogout);
  // 先探 /api/me：已登录才初始化界面，否则跳独立登录页
  getJSON('/api/me').then((r) => {
    if (r.ok) initMain();
    else showLogin();
  }).catch(() => showLogin());
});
// 登录成功后才初始化轮询与状态
function initMain() {
  // 无论开/停，先拉一次当前状态显示快照
  refreshAll();
  // 从后端读同步开关状态并恢复（换浏览器/刷新一致）；running 才自动轮询
  getJSON('/api/pipeline').then((r) => {
    if (r.ok && r.data && r.data.desired === 'running') applyState(true);
  }).catch(() => {});
  initTsConverter();
}

// ================= 时间戳转换器 =================

const TS_ZONES = [
  { label: 'UTC+8 · Beijing / Shanghai', offset: 8 },
  { label: 'UTC+0 · UTC',                offset: 0 },
  { label: 'UTC+9 · Tokyo / Seoul',       offset: 9 },
  { label: 'UTC-5 · New York (EST)',      offset: -5 },
  { label: 'UTC-8 · Los Angeles (PST)',   offset: -8 },
];

function buildTzOptions(selectEl, defaultOffset = 8) {
  selectEl.innerHTML = TS_ZONES.map((z) =>
    `<option value="${z.offset}" ${z.offset === defaultOffset ? 'selected' : ''}>${esc(z.label)}</option>`
  ).join('');
}

/** 把 epoch 秒换算成指定 UTC 偏移的本地字符串 */
function epochToLocal(sec, offsetHours) {
  const ms = sec * 1000 + offsetHours * 3600000;
  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}/${pad(d.getUTCMonth()+1)}/${pad(d.getUTCDate())} `
       + `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

/** 把指定 UTC 偏移的本地时间字符串转回 epoch 秒 */
function localToEpoch(str, offsetHours) {
  const m = str.match(/(\d{4})[^\d](\d{1,2})[^\d](\d{1,2})[^\d](\d{1,2})[^\d](\d{1,2})[^\d](\d{1,2})/);
  if (!m) return NaN;
  const [, Y, Mo, D, H, Mi, S] = m.map(Number);
  return Date.UTC(Y, Mo-1, D, H, Mi, S) / 1000 - offsetHours * 3600;
}

/** 相对时间描述 */
function relTime(sec) {
  const diff = Math.round(Date.now() / 1000) - sec;
  const abs = Math.abs(diff);
  const future = diff < 0;
  if (abs < 60) return future ? `${abs} 秒后` : `${abs} 秒前`;
  if (abs < 3600) return future ? `${Math.round(abs/60)} 分钟后` : `${Math.round(abs/60)} 分钟前`;
  if (abs < 86400) return future ? `${Math.round(abs/3600)} 小时后` : `${Math.round(abs/3600)} 小时前`;
  return future ? `${Math.round(abs/86400)} 天后` : `${Math.round(abs/86400)} 天前`;
}

function tsMoreFmts(sec, offsetHours) {
  const ms = sec * 1000 + offsetHours * 3600000;
  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, '0');
  const iso = new Date(sec * 1000).toISOString();
  const ymd = `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}`;
  const hms = `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
  return [
    ['毫秒时间戳', String(sec * 1000)],
    ['ISO 8601 (UTC)', iso],
    ['日期', ymd],
    ['时间', hms],
  ];
}

function renderMoreFmts(containerId, rows) {
  const el = document.getElementById(containerId);
  if (!el) return;
  el.innerHTML = rows.map(([label, val]) =>
    `<div class="ts-fmt-row"><span class="ts-fmt-label">${esc(label)}</span><span class="ts-fmt-val">${esc(val)}</span></div>`
  ).join('');
}

function copyText(text, btnEl) {
  navigator.clipboard.writeText(text).then(() => {
    const orig = btnEl.textContent;
    btnEl.textContent = '✓';
    setTimeout(() => { btnEl.textContent = orig; }, 1200);
  }).catch(() => {});
}

function nowIsoLocal(offsetHours) {
  const sec = Math.floor(Date.now() / 1000);
  return epochToLocal(sec, offsetHours).replace('/', '-').replace('/', '-');
}

function initTsConverter() {
  const tsTzSel = document.getElementById('tsTzSelect');
  const dtTzSel = document.getElementById('dtTzSelect');
  if (!tsTzSel || !dtTzSel) return;
  buildTzOptions(tsTzSel, 8);
  buildTzOptions(dtTzSel, 8);

  function runTs() {
    const raw = (document.getElementById('tsInput').value || '').trim();
    const offset = Number(tsTzSel.value);
    if (!raw) { document.getElementById('tsResultValue').textContent = '—'; document.getElementById('tsResultRel').textContent = ''; return; }
    let sec = Number(raw);
    if (isNaN(sec)) { document.getElementById('tsResultValue').textContent = '格式错误'; return; }
    if (sec > 1e12) sec = Math.floor(sec / 1000);  // 毫秒自动转秒
    const local = epochToLocal(sec, offset);
    document.getElementById('tsResultValue').textContent = local;
    document.getElementById('tsResultRel').textContent = relTime(sec);
    renderMoreFmts('tsMoreFmts', tsMoreFmts(sec, offset));
    document.getElementById('tsCopyBtn').onclick = () => copyText(local, document.getElementById('tsCopyBtn'));
  }

  function runDt() {
    const raw = (document.getElementById('dtInput').value || '').trim();
    const offset = Number(dtTzSel.value);
    if (!raw) { document.getElementById('dtResultValue').textContent = '—'; document.getElementById('dtResultRel').textContent = ''; return; }
    const sec = localToEpoch(raw, offset);
    if (isNaN(sec)) { document.getElementById('dtResultValue').textContent = '格式错误'; return; }
    document.getElementById('dtResultValue').textContent = String(Math.floor(sec));
    document.getElementById('dtResultRel').textContent = relTime(sec);
    renderMoreFmts('dtMoreFmts', tsMoreFmts(Math.floor(sec), offset));
    document.getElementById('dtCopyBtn').onclick = () => copyText(String(Math.floor(sec)), document.getElementById('dtCopyBtn'));
  }

  document.getElementById('tsInput').addEventListener('input', runTs);
  tsTzSel.addEventListener('change', runTs);
  document.getElementById('tsNowBtn').addEventListener('click', () => {
    document.getElementById('tsInput').value = String(Math.floor(Date.now() / 1000));
    runTs();
  });

  document.getElementById('dtInput').addEventListener('input', runDt);
  dtTzSel.addEventListener('change', runDt);
  document.getElementById('dtNowBtn').addEventListener('click', () => {
    document.getElementById('dtInput').value = nowIsoLocal(Number(dtTzSel.value));
    runDt();
  });

  // 默认填入当前时间戳
  document.getElementById('tsInput').value = String(Math.floor(Date.now() / 1000));
  runTs();
}
