import os, json, uuid
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pg8000.native

app = FastAPI(title='AI Corporate OS', version='1.0.0')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'], allow_credentials=True)

DATABASE_URL = os.environ.get('DATABASE_URL', '')

def parse_db_url(url):
    from urllib.parse import urlparse
    p = urlparse(url)
    return {'host': p.hostname, 'port': p.port or 5432, 'user': p.username, 'password': p.password, 'database': p.path.lstrip('/')}

def get_db():
    cfg = parse_db_url(DATABASE_URL)
    return pg8000.native.Connection(host=cfg['host'], port=cfg['port'], user=cfg['user'], password=cfg['password'], database=cfg['database'], ssl_context=None)

def rows_to_dicts(rows, cols):
    return [dict(zip(cols, row)) for row in rows]

def setup_tables():
    if not DATABASE_URL: return False
    try:
        c = get_db()
        c.run("CREATE TABLE IF NOT EXISTS clients (id UUID PRIMARY KEY, name VARCHAR(255), industry VARCHAR(100), city VARCHAR(100), status VARCHAR(50) DEFAULT 'setup', employees_total INTEGER DEFAULT 0, contract_value VARCHAR(100), tasks_completed INTEGER DEFAULT 0, roi_realized DECIMAL(12,2) DEFAULT 0, created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())")
        c.run("CREATE TABLE IF NOT EXISTS employees (id UUID PRIMARY KEY, client_id UUID, name VARCHAR(255), role VARCHAR(255), department VARCHAR(255), level INTEGER DEFAULT 3, created_at TIMESTAMP DEFAULT NOW())")
        c.run("CREATE TABLE IF NOT EXISTS tasks (id UUID PRIMARY KEY, client_id UUID, employee_id UUID, title VARCHAR(500), description TEXT, expert_name VARCHAR(255), priority VARCHAR(50) DEFAULT 'normal', roi_estimate VARCHAR(100), source VARCHAR(255), status VARCHAR(50) DEFAULT 'pending', created_at TIMESTAMP DEFAULT NOW(), completed_at TIMESTAMP)")
        c.run("CREATE TABLE IF NOT EXISTS task_executions (id UUID PRIMARY KEY, task_id UUID, client_id UUID, employee_id UUID, result_summary TEXT, result_data TEXT DEFAULT '{}', time_saved_minutes INTEGER DEFAULT 0, roi_realized DECIMAL(12,2) DEFAULT 0, executed_at TIMESTAMP DEFAULT NOW())")
        c.run("CREATE TABLE IF NOT EXISTS agents (id UUID PRIMARY KEY, client_id UUID, name VARCHAR(255), description TEXT, category VARCHAR(100), status VARCHAR(50) DEFAULT 'active', runs_total INTEGER DEFAULT 0, success_rate DECIMAL(5,2) DEFAULT 100.0, last_run TIMESTAMP, created_at TIMESTAMP DEFAULT NOW())")
        c.close(); return True
    except Exception as e:
        print(f'DB setup error: {e}'); return False

DB_READY = setup_tables()

FRONTEND = '''<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>AI Corporate OS</title><script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script><style>*{margin:0;padding:0;box-sizing:border-box}:root{--bg:#07080f;--bg2:#0d0f1a;--bg3:#12152a;--card:#141828;--b:rgba(99,120,255,.15);--b2:rgba(99,120,255,.28);--ac:#6378ff;--ac2:#a855f7;--gr:#22d3a3;--yl:#f59e0b;--rd:#f43f5e;--tx:#e2e8f0;--t2:#94a3b8;--mt:#475569}body{background:var(--bg);color:var(--tx);font-family:-apple-system,Inter,sans-serif;min-height:100vh}.bar{background:var(--bg2);border-bottom:1px solid var(--b);height:54px;display:flex;align-items:center;justify-content:space-between;padding:0 24px;position:sticky;top:0;z-index:100}.lo{display:flex;align-items:center;gap:10px}.lic{width:32px;height:32px;background:linear-gradient(135deg,var(--ac),var(--ac2));border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:16px}.ln{font-size:14px;font-weight:700}.ls2{font-size:10px;color:var(--mt);text-transform:uppercase;letter-spacing:.8px}.top-r{display:flex;align-items:center;gap:10px}.live{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--t2);background:var(--bg3);border:1px solid var(--b);padding:5px 12px;border-radius:20px}.dot{width:6px;height:6px;border-radius:50%;background:var(--gr);box-shadow:0 0 6px var(--gr);animation:blink 2s infinite}@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}.chip{display:flex;align-items:center;gap:7px;background:var(--bg3);border:1px solid var(--b);border-radius:20px;padding:4px 12px 4px 4px;cursor:pointer}.av{width:26px;height:26px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#fff}.un{font-size:12px;font-weight:600}.ur{font-size:10px;color:var(--mt)}.btn{padding:7px 16px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;border:none;font-family:inherit;transition:all .18s}.bp{background:linear-gradient(135deg,var(--ac),var(--ac2));color:#fff}.bp:hover{opacity:.85}.bg2{background:transparent;border:1.5px solid var(--b);color:var(--t2)}.bg2:hover{border-color:var(--ac);color:var(--tx)}.layout{display:flex;min-height:calc(100vh - 54px)}.sb{width:210px;min-width:210px;background:var(--bg2);border-right:1px solid var(--b);padding:16px 0;display:flex;flex-direction:column}.main2{flex:1;overflow-y:auto}.cn{padding:24px}.ns{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--mt);padding:0 14px;margin:14px 0 5px}.ni{display:flex;align-items:center;gap:9px;padding:9px 14px;font-size:12px;color:var(--t2);cursor:pointer;border-left:2px solid transparent;transition:all .15s}.ni:hover{background:rgba(99,120,255,.06);color:var(--tx)}.ni.on{background:rgba(99,120,255,.1);color:var(--ac);border-left-color:var(--ac)}.nbadge{margin-left:auto;font-size:9px;font-weight:700;padding:1px 6px;border-radius:8px;background:rgba(99,120,255,.2);color:var(--ac)}.sbf{margin-top:auto;padding:14px;border-top:1px solid var(--b)}.pg{display:none}.pg.on{display:block;animation:fu .22s ease}@keyframes fu{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}.kr{display:grid;gap:12px;margin-bottom:22px}.k{background:var(--card);border:1px solid var(--b);border-radius:12px;padding:14px 16px;position:relative;overflow:hidden}.k::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;border-radius:2px 2px 0 0}.k.bl::before{background:var(--ac)}.k.gr::before{background:var(--gr)}.k.yl::before{background:var(--yl)}.k.pu::before{background:var(--ac2)}.k.rd::before{background:var(--rd)}.kv{font-size:26px;font-weight:800;letter-spacing:-.5px;line-height:1.1}.kl{font-size:10px;color:var(--mt);text-transform:uppercase;letter-spacing:.5px;margin-top:5px}.card{background:var(--card);border:1px solid var(--b);border-radius:14px;overflow:hidden;margin-bottom:18px}.ch{padding:14px 18px;border-bottom:1px solid var(--b);display:flex;align-items:center;justify-content:space-between}.ct{font-size:13px;font-weight:700}.cb{padding:16px 18px}.ph{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:22px;gap:14px}.ph h1{font-size:20px;font-weight:800;letter-spacing:-.4px;margin-bottom:3px}.ph p{font-size:12px;color:var(--t2)}.tc{background:var(--card);border:1px solid var(--b);border-radius:12px;overflow:hidden;transition:all .2s;cursor:pointer;margin-bottom:10px}.tc:hover{border-color:var(--b2);transform:translateY(-1px)}.tc.urg{border-left:3px solid var(--rd)}.tc.td2{border-left:3px solid var(--yl)}.tc.nm{border-left:3px solid var(--ac)}.tc.dn{border-left:3px solid var(--gr);opacity:.7}.ti{padding:15px 17px;display:grid;grid-template-columns:1fr auto;gap:12px;align-items:start}.tt{font-size:13px;font-weight:700;margin-bottom:4px}.td3{font-size:11px;color:var(--t2);line-height:1.5;margin-bottom:7px}.tm3{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.mi{font-size:10px;color:var(--mt)}.rv{font-size:13px;font-weight:700;color:var(--gr);text-align:right}.rl2{font-size:9px;color:var(--mt);text-align:right}.bdg{font-size:9px;font-weight:700;padding:2px 7px;border-radius:4px;white-space:nowrap;display:inline-block}.bu{background:rgba(244,63,94,.12);color:var(--rd)}.bt2{background:rgba(245,158,11,.12);color:var(--yl)}.bn{background:rgba(99,120,255,.12);color:var(--ac)}.bdn{background:rgba(34,211,163,.12);color:var(--gr)}.bai{background:linear-gradient(135deg,rgba(99,120,255,.15),rgba(168,85,247,.15));color:#c084fc;border:1px solid rgba(168,85,247,.2)}.bxt{display:inline-flex;align-items:center;gap:7px;background:linear-gradient(135deg,var(--ac),var(--ac2));color:#fff;border:none;padding:8px 14px;border-radius:8px;font-size:11px;font-weight:700;cursor:pointer;transition:all .2s;white-space:nowrap;font-family:inherit}.bxt:hover{opacity:.85;transform:translateY(-1px)}.bxt.dn{background:linear-gradient(135deg,#22d3a3,#0ea5e9)}.fi{display:flex;align-items:flex-start;gap:10px;padding:10px 0;border-bottom:1px solid var(--b)}.fi:last-child{border-bottom:none}.fdot{width:7px;height:7px;border-radius:50%;flex-shrink:0;margin-top:4px}.fdot.gr{background:var(--gr);box-shadow:0 0 5px var(--gr)}.ftxt{font-size:12px;line-height:1.5;flex:1}.ftxt strong{font-weight:700;color:var(--tx)}.ftime{font-size:10px;color:var(--mt);margin-top:2px}.ov{position:fixed;inset:0;background:rgba(0,0,0,.65);backdrop-filter:blur(5px);z-index:200;display:flex;align-items:center;justify-content:center;padding:20px}.ov.h{display:none}.md{background:var(--card);border:1px solid var(--b2);border-radius:16px;width:100%;max-width:520px;max-height:88vh;overflow-y:auto;animation:fu .2s ease}.mh{padding:18px 22px;border-bottom:1px solid var(--b);display:flex;align-items:flex-start;justify-content:space-between;gap:12px;position:sticky;top:0;background:var(--card);z-index:5}.mtit{font-size:17px;font-weight:800;margin-bottom:3px}.msub{font-size:12px;color:var(--t2)}.mc{width:26px;height:26px;background:var(--bg3);border:1px solid var(--b);border-radius:6px;display:flex;align-items:center;justify-content:center;cursor:pointer;font-size:12px;color:var(--t2);flex-shrink:0}.mb4{padding:18px 22px}.mf3{padding:13px 22px;border-top:1px solid var(--b);display:flex;justify-content:space-between;align-items:center}.ig{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px}.ic{background:var(--bg3);border-radius:8px;padding:10px 13px}.il{font-size:10px;color:var(--mt);text-transform:uppercase;letter-spacing:.5px;margin-bottom:3px}.iv{font-size:14px;font-weight:700}.term{background:#000;border-radius:10px;padding:14px;font-family:monospace;font-size:11px;line-height:1.7;margin-bottom:14px;min-height:100px;border:1px solid rgba(99,120,255,.2);overflow-y:auto;max-height:180px}.tl{color:#4ade80}.tl.i{color:#94a3b8}.tl.w{color:#fbbf24}.tl.s{color:#22d3a3}.tl.h{color:#818cf8}.rb{background:rgba(34,211,163,.06);border:1px solid rgba(34,211,163,.2);border-radius:12px;padding:14px;margin-bottom:14px}.rt{font-size:13px;font-weight:700;color:var(--gr);margin-bottom:8px}.ri{display:flex;align-items:center;gap:7px;font-size:12px;margin-bottom:4px}.em{text-align:center;padding:30px;color:var(--mt)}.emico{font-size:32px;margin-bottom:8px}.em p{font-size:13px}.sp{width:22px;height:22px;border:2px solid var(--b);border-top-color:var(--ac);border-radius:50%;animation:spin .7s linear infinite;margin:0 auto 8px}@keyframes spin{to{transform:rotate(360deg)}}.lw{min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}.lc{background:var(--card);border:1px solid var(--b2);border-radius:16px;padding:32px;width:100%;max-width:460px}.rg{display:grid;gap:8px;margin-bottom:18px}.rc{background:var(--bg3);border:1.5px solid var(--b);border-radius:10px;padding:12px 14px;cursor:pointer;transition:all .2s;display:flex;align-items:center;gap:12px}.rc:hover{border-color:rgba(99,120,255,.4)}.rc.sel{border-color:var(--ac);background:rgba(99,120,255,.1)}.rca{width:34px;height:34px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:14px;flex-shrink:0}.rn{font-size:13px;font-weight:700;margin-bottom:2px}.rd{font-size:11px;color:var(--t2)}.rr{margin-left:auto;text-align:right;font-size:11px;color:var(--mt)}.rr strong{display:block;font-size:15px;font-weight:800;color:var(--ac)}.two{display:grid;grid-template-columns:1fr 300px;gap:16px;margin-bottom:18px}.cwr{height:200px;position:relative}.mb3b{height:4px;background:var(--bg3);border-radius:3px;overflow:hidden;margin-top:3px}.mf3b{height:100%;border-radius:3px;transition:width 1s}.cl{background:var(--card);border:1px solid var(--b);border-radius:14px;overflow:hidden;margin-bottom:14px}.cr{display:grid;align-items:center;gap:10px;padding:11px 16px;border-bottom:1px solid var(--b)}.cr:last-child{border-bottom:none}.cr:hover{background:rgba(99,120,255,.04);cursor:pointer}input,select{background:var(--bg3);border:1.5px solid var(--b);border-radius:8px;padding:10px 13px;color:var(--tx);font-size:13px;outline:none;font-family:inherit;width:100%}input:focus,select:focus{border-color:var(--ac)}select option{background:var(--bg3)}::-webkit-scrollbar{width:4px}::-webkit-scrollbar-thumb{background:var(--b);border-radius:4px}</style></head><body><div id="lw" class="lw"><div class="lc"><div style="display:flex;align-items:center;justify-content:center;gap:10px;margin-bottom:20px"><div class="lic" style="width:40px;height:40px;font-size:20px">&#9889;</div><div><div style="font-size:16px;font-weight:700">AI Corporate OS</div><div style="font-size:10px;color:var(--mt);text-transform:uppercase;letter-spacing:.8px">Powered by Extella</div></div></div><div id="ls" style="text-align:center;padding:16px;font-size:13px;color:var(--t2)"><div class="sp"></div>Connecting...</div><div class="rg" id="rg" style="display:none"></div><button class="btn bp" style="width:100%;padding:13px;display:none;font-size:14px" id="lb" onclick="doLogin()">&#9889; Enter AI OS &#x2192;</button><div style="text-align:center;font-size:10px;color:var(--mt);margin-top:10px" id="au"></div></div></div><div id="aw" style="display:none"><div class="bar"><div class="lo"><div class="lic">&#9889;</div><div><div class="ln">AI Corporate OS</div><div class="ls2" id="tc3">&#8212;</div></div></div><div class="top-r"><div class="live"><div class="dot"></div><span id="lc2">&#8212;</span> agents</div><div style="font-size:10px;color:var(--gr);font-family:monospace">&#9679; Live</div><div class="chip" onclick="lo()"><div class="av" id="ta" style="background:linear-gradient(135deg,#6378ff,#a855f7)">&#128100;</div><div><div class="un" id="tn">&#8212;</div><div class="ur" id="tr">&#8212;</div></div></div></div></div><div class="layout"><div class="sb"><div class="ns">Main</div><div class="ni on" id="nh" onclick="sp('home',this)"><span style="width:18px;text-align:center">&#127968;</span>Dashboard</div><div class="ni" id="nt" onclick="sp('tasks',this)"><span style="width:18px;text-align:center">&#9989;</span>Tasks<span class="nbadge" id="tb">0</span></div><div class="ni" id="nhi" onclick="sp('hist',this)"><span style="width:18px;text-align:center">&#128203;</span>History</div><div class="ns" id="ds2" style="display:none">Portfolio</div><div class="ni" id="nd" onclick="sp('dash',this)" style="display:none"><span style="width:18px;text-align:center">&#9638;</span>Analytics</div><div class="ni" id="nc2" onclick="sp('clients',this)" style="display:none"><span style="width:18px;text-align:center">&#127970;</span>Clients</div><div class="ni" id="nag" onclick="sp('agents',this)" style="display:none"><span style="width:18px;text-align:center">&#129302;</span>Agents</div><div class="sbf"><div style="background:var(--bg3);border-radius:10px;padding:11px"><div style="font-size:10px;color:var(--mt);margin-bottom:4px">Backend</div><div style="font-size:9px;font-family:monospace;color:var(--gr)">api-production-788d<br>.up.railway.app</div></div></div></div><div class="main2"><div class="cn"><div class="pg on" id="p-home"><div class="ph"><div><h1 id="hg">Welcome</h1><p id="hs">&#8212;</p></div></div><div class="kr" id="hk" style="grid-template-columns:repeat(4,1fr)"></div><div id="hc"></div></div><div class="pg" id="p-tasks"><div class="ph"><div><h1>My Tasks</h1><p id="tsub">&#8212;</p></div><div style="display:flex;gap:6px"><button class="btn bp" style="font-size:11px;padding:5px 12px" onclick="ft('all',this)">All</button><button class="btn bg2" style="font-size:11px;padding:5px 12px" onclick="ft('pending',this)">Active</button><button class="btn bg2" style="font-size:11px;padding:5px 12px" onclick="ft('done',this)">Done</button></div></div><div id="ta2"></div></div><div class="pg" id="p-hist"><div class="ph"><div><h1>History</h1><p>Completed tasks</p></div></div><div id="hl"></div></div><div class="pg" id="p-dash"><div class="ph"><div><h1>Dashboard</h1><p>Live portfolio</p></div><button class="btn bg2" onclick="ld()">&#8634; Refresh</button></div><div class="kr" id="dk" style="grid-template-columns:repeat(4,1fr)"></div><div class="two"><div class="card"><div class="ch"><div class="ct">Clients</div><span style="font-size:9px;font-family:monospace;color:var(--gr)">live</span></div><div id="dc"></div></div><div class="card"><div class="ch"><div class="ct">Activity</div></div><div class="cb" id="df" style="padding:12px 16px"></div></div></div><div class="card"><div class="ch"><div class="ct">ROI</div></div><div class="cb"><div class="cwr"><canvas id="rc2"></canvas></div></div></div></div><div class="pg" id="p-clients"><div class="ph"><div><h1>Clients</h1><p id="csub">&#8212;</p></div><button class="btn bp" onclick="snc()">+ New Client</button></div><div id="cl2"></div></div><div class="pg" id="p-agents"><div class="ph"><div><h1>AI Agents</h1><p id="asub">&#8212;</p></div></div><div id="al2"></div></div></div></div></div></div><div class="ov h" id="to2"><div class="md" id="tm2"></div></div><div class="ov h" id="eo2"><div class="md" id="em2" style="max-width:500px"></div></div><div class="ov h" id="nco"><div class="md" style="max-width:420px"><div class="mh"><div class="mtit">New Client</div><div class="mc" onclick="cm('nco')">x</div></div><div class="mb4"><div style="display:grid;gap:12px"><div><div style="font-size:11px;color:var(--t2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:5px">Name</div><input id="ncn" placeholder="Company"></div><div><div style="font-size:11px;color:var(--t2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:5px">Industry</div><select id="nci"><option value="mining">Mining</option><option value="finance">Finance</option><option value="construction">Construction</option><option value="gov">Government</option><option value="telecom">Telecom</option></select></div><div><div style="font-size:11px;color:var(--t2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:5px">City</div><input id="ncc2" placeholder="City"></div></div><div style="display:flex;justify-content:flex-end;gap:8px;margin-top:16px"><button class="btn bg2" onclick="cm('nco')">Cancel</button><button class="btn bp" onclick="cc()">Create &#x2192;</button></div></div></div></div><script>
const API=window.location.origin;
const CL=['linear-gradient(135deg,#6378ff,#a855f7)','linear-gradient(135deg,#22d3a3,#0ea5e9)','linear-gradient(135deg,#f59e0b,#ef4444)','linear-gradient(135deg,#8b5cf6,#ec4899)','linear-gradient(135deg,#0ea5e9,#6378ff)'];
const IC={predictive_maintenance_analysis:'&#128295;',dispatch_optimizer:'&#128667;',shift_data_entry:'&#128202;',ml_credit_scoring:'&#127919;',aml_transaction_monitor:'&#128270;',retrain_production_forecast:'&#129302;',generate_strategy_doc:'&#128203;',default:'&#9889;'};
let role=null,tasks=[],clients=[],dash=null,eids={},rch=null;
const ROLES=[
  {k:'director',n:'Seitkali N.',t:'CEO',d:'Management',a:'&#128084;',c:CL[0],dir:true},
  {k:'head',n:'Zhaksybekov A.',t:'Head of Production',d:'QazMunayGas',a:'&#127981;',c:CL[1]},
  {k:'analyst',n:'Bekova M.',t:'Data Analyst',d:'QazMunayGas',a:'&#128202;',c:CL[4]},
  {k:'dispatcher',n:'Omarov D.',t:'Shift Dispatcher',d:'QazMunayGas',a:'&#128667;',c:'linear-gradient(135deg,#f59e0b,#ef4444)'},
  {k:'operator',n:'Satybaldi Y.',t:'Operator',d:'QazMunayGas',a:'&#9881;',c:'linear-gradient(135deg,#475569,#334155)'},
];
async function api2(m,p,d){try{const r=await fetch(API+'/api'+p,m==='POST'?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)}:{});return r.ok?r.json():null}catch{return null}}
async function init(){
  document.getElementById('au').textContent=window.location.host;
  const h=await api2('GET','/health');
  if(!h){document.getElementById('ls').innerHTML='<div style="color:var(--rd)">API unavailable</div>';return}
  document.getElementById('ls').innerHTML='<div style="color:var(--gr)">Connected &#10003; DB:'+h.db+'</div>';
  const emps=await api2('GET','/employees');
  if(emps?.employees)emps.employees.forEach(e=>{
    if(e.name.includes('Сейткали')||e.name.includes('Seitkali'))eids.director=e.id;
    else if(e.name.includes('Жаксыбеков')||e.name.includes('Zhaksybekov'))eids.head=e.id;
    else if(e.name.includes('Бекова')||e.name.includes('Bekova'))eids.analyst=e.id;
    else if(e.name.includes('Омаров')||e.name.includes('Omarov'))eids.dispatcher=e.id;
    else if(e.name.includes('Сатыбалды')||e.name.includes('Satybaldi'))eids.operator=e.id;
  });
  document.getElementById('rg').innerHTML=ROLES.map(r=>`<div class="rc" onclick="sr('${r.k}',this)"><div class="rca" style="background:${r.c}">${r.a}</div><div><div class="rn">${r.n}</div><div class="rd">${r.t} &middot; ${r.d}</div></div><div class="rr"><strong id="cnt-${r.k}">&#8212;</strong>tasks</div></div>`).join('');
  document.getElementById('rg').style.display='grid';
  document.getElementById('lb').style.display='block';
  ROLES.forEach(async r=>{
    const el=document.getElementById('cnt-'+r.k);if(!el)return;
    if(eids[r.k]){const t=await api2('GET','/tasks?employee_id='+eids[r.k]+'&status=pending');if(t)el.textContent=t.tasks?.length||0;}
    else if(r.dir){const d=await api2('GET','/dashboard');if(d&&el)el.textContent=(d.kpis?.tasks_done||0)+'&#10003;';}
  });
}
let sk=null;
function sr(k,el){sk=k;document.querySelectorAll('.rc').forEach(c=>c.classList.remove('sel'));el.classList.add('sel')}
async function doLogin(){
  if(!sk){alert('Select a role');return}
  role=ROLES.find(r=>r.k===sk);
  document.getElementById('lw').style.display='none';
  document.getElementById('aw').style.display='block';
  await setup();
}
function lo(){sk=null;role=null;tasks=[];document.getElementById('lw').style.display='flex';document.getElementById('aw').style.display='none';document.querySelectorAll('.rc').forEach(c=>c.classList.remove('sel'))}
async function setup(){
  document.getElementById('ta').innerHTML=role.a;document.getElementById('ta').style.background=role.c;
  document.getElementById('tn').textContent=role.n;
  document.getElementById('tr').textContent=role.t;document.getElementById('tc3').textContent=role.d;
  if(role.dir){document.getElementById('ds2').style.display='block';['nd','nc2','nag'].forEach(i=>document.getElementById(i).style.display='flex')}
  dash=await api2('GET','/dashboard');document.getElementById('lc2').textContent=dash?.kpis?.agents||'&#8212;';
  await lt();rh();
}
async function lt(){
  if(role.dir){const d=await api2('GET','/tasks');tasks=d?.tasks||[];}
  else{const eid=eids[role.k];if(eid){const d=await api2('GET','/tasks?employee_id='+eid);tasks=d?.tasks||[];}else tasks=[];}
  document.getElementById('tb').textContent=tasks.filter(t=>t.status!=='completed').length||0;
}
function gi(t){return IC[t.expert_name]||IC.default}
function mkCard(t){
  const dn=t.status==='completed';
  const cl2=dn?'dn':t.priority==='urgent'?'urg':t.priority==='today'?'td2':'nm';
  const bc=dn?'bdn':t.priority==='urgent'?'bu':t.priority==='today'?'bt2':'bn';
  const bt=dn?'&#9989; Done':t.priority==='urgent'?'&#128308; URGENT':t.priority==='today'?'&#128993; Today':'&#128203; Normal';
  return`<div class="tc ${cl2}" onclick="ot('${t.id}')">`
    +`<div class="ti"><div><div style="display:flex;align-items:center;gap:7px;margin-bottom:5px;flex-wrap:wrap">`
    +`<span style="font-size:18px">${gi(t)}</span><span class="bdg ${bc}">${bt}</span>`
    +`<span class="bdg bai">&#9889; Extella</span>`
    +`${t.employee_name?'<span style="font-size:10px;color:var(--mt)">'+t.employee_name+'</span>':''}`
    +`</div><div class="tt">${t.title}</div>`
    +`<div class="td3">${(t.description||'').substring(0,90)}</div>`
    +`<div class="tm3"><div class="mi">&#128204; ${t.source||'manual'}</div><div class="mi">&#129302; ${t.expert_name||'&#8212;'}</div></div>`
    +`</div><div><div class="rv">${t.roi_estimate||'&#8212;'}</div><div class="rl2">ROI</div>`
    +`<div style="margin-top:8px">${!dn?'<button class="bxt" onclick="event.stopPropagation();ot(\''+t.id+'\')" >&#9889; Run</button>':'<button class="bxt dn">&#10003; Done</button>'}</div>`
    +`</div></div></div>`;
}
function rh(){
  const h=new Date().getHours();document.getElementById('hg').textContent=(h<12?'Good morning':h<18?'Good afternoon':'Good evening')+', '+role.n.split(' ')[0]+'!';
  const pend=tasks.filter(t=>t.status!=='completed');document.getElementById('hs').textContent=role.d+' &#183; '+pend.length+' tasks pending';
  const kpis=role.dir&&dash?[{v:dash.kpis?.clients||0,l:'Clients',c:'bl'},{v:dash.kpis?.agents||0,l:'AI Agents',c:'gr'},{v:'$'+Number(dash.kpis?.roi||0).toLocaleString(),l:'ROI',c:'pu'},{v:dash.kpis?.tasks_done||0,l:'Done',c:'yl'}]
    :[{v:tasks.length,l:'Tasks',c:'bl'},{v:pend.length,l:'Pending',c:'yl'},{v:tasks.filter(t=>t.status==='completed').length,l:'Done',c:'gr'},{v:pend.filter(t=>t.priority==='urgent').length,l:'Urgent',c:'rd'}];
  document.getElementById('hk').innerHTML=kpis.map(k=>'<div class="k '+k.c+'"><div class="kv">'+k.v+'</div><div class="kl">'+k.l+'</div></div>').join('');
  const urg=pend.filter(t=>t.priority==='urgent'),tod=pend.filter(t=>t.priority!=='urgent');
  let html='';
  if(urg.length)html+='<div style="font-size:14px;font-weight:700;margin-bottom:12px">&#128308; Urgent</div>'+urg.slice(0,3).map(mkCard).join('');
  if(tod.length)html+='<div style="font-size:14px;font-weight:700;margin-bottom:12px;margin-top:20px">&#128993; Today</div>'+tod.slice(0,3).map(mkCard).join('');
  if(!html)html='<div class="em"><div class="emico">&#9989;</div><p>No active tasks</p></div>';
  document.getElementById('hc').innerHTML=html;
  ra('all');
  const dn2=tasks.filter(t=>t.status==='completed');
  document.getElementById('hl').innerHTML=dn2.length?dn2.map(t=>'<div style="display:flex;align-items:center;gap:12px;padding:12px 16px;background:var(--card);border:1px solid var(--b);border-radius:10px;margin-bottom:8px"><span style="font-size:18px">&#9989;</span><div style="flex:1"><div style="font-size:13px;font-weight:600">'+t.title+'</div><div style="font-size:11px;color:var(--t2)">'+(t.expert_name||'&#8212;')+'</div></div><div style="font-size:11px;color:var(--gr)">&#10003;</div></div>').join(''):'<div class="em"><div class="emico">&#128203;</div><p>No history</p></div>';
}
function ra(f){
  document.getElementById('tsub').textContent=tasks.length+' tasks &#183; '+tasks.filter(t=>t.status!=='completed').length+' active';
  const list=f==='pending'?tasks.filter(t=>t.status!=='completed'):f==='done'?tasks.filter(t=>t.status==='completed'):tasks;
  document.getElementById('ta2').innerHTML=list.length?list.map(mkCard).join(''):'<div class="em"><div class="emico">&#9989;</div><p>No tasks</p></div>';
}
function ft(f,btn){document.querySelectorAll('.ph .btn').forEach(b=>{b.className='btn bg2';b.style.fontSize='11px';b.style.padding='5px 12px'});btn.className='btn bp';btn.style.fontSize='11px';btn.style.padding='5px 12px';ra(f)}
function ot(id){
  const t=tasks.find(x=>x.id===id);if(!t)return;
  const dn=t.status==='completed';
  document.getElementById('tm2').innerHTML='<div class="mh"><div><div class="mtit">'+t.title+'</div><div class="msub">'+(t.expert_name||'&#8212;')+'</div></div><div class="mc" onclick="cm(\'to2\')">x</div></div>'
    +'<div class="mb4"><div class="ig"><div class="ic"><div class="il">ROI</div><div class="iv" style="color:var(--gr)">'+(t.roi_estimate||'&#8212;')+'</div></div>'
    +'<div class="ic"><div class="il">Expert</div><div class="iv" style="font-family:monospace;font-size:11px">'+(t.expert_name||'&#8212;')+'</div></div></div>'
    +'<div style="font-size:13px;color:var(--t2);line-height:1.65;margin-bottom:14px">'+(t.description||'No description')+'</div></div>'
    +'<div class="mf3"><button class="btn bg2" onclick="cm(\'to2\')">Close</button>'
    +((!dn)?'<button class="bxt" onclick="cm(\'to2\');la(\''+id+'\')" >&#9889; Launch in Extella &#x2192;</button>':'<button class="bxt dn">&#10003; Completed</button>')+'</div>';
  document.getElementById('to2').classList.remove('h');
}
const SC={predictive_maintenance_analysis:[{t:500,c:'i',l:'Loading sensor CSV...'},{t:1200,c:'i',l:'FFT analysis of bearings'},{t:2100,c:'w',l:'&#9888; Anomaly: bearing #3 critical trend'},{t:3000,c:'i',l:'Failure prediction: 8-12 days'},{t:3800,c:'s',l:'&#10003; Recording to AI OS...'}],
ml_credit_scoring:[{t:400,c:'i',l:'Loading 5,000 applications...'},{t:1200,c:'i',l:'XGBoost scoring (47 features)...'},{t:2200,c:'i',l:'Accuracy: 94.2%'},{t:3000,c:'s',l:'&#10003; ROI $82,000 saved to API'}],
dispatch_optimizer:[{t:300,c:'i',l:'BelAZ #04 stopped...'},{t:900,c:'i',l:'Recalculating routes...'},{t:1800,c:'s',l:'&#10003; Routes ready. Loss: 4.2% vs 18%'}],
shift_data_entry:[{t:400,c:'i',l:'OCR reading meters...'},{t:1300,c:'i',l:'24 instruments validated...'},{t:2200,c:'s',l:'&#10003; Report sent. 40 min saved'}],
default:[{t:400,c:'i',l:'Initializing...'},{t:1200,c:'i',l:'Processing...'},{t:2000,c:'s',l:'&#10003; Done!'}]};
const RS={predictive_maintenance_analysis:{t:'Predictive Maintenance',i:['Bearing #3 - critical trend','Recommended maintenance: Apr 25-27','Prevented loss: ~$83,000'],r:83000,m:240,s:'Predictive analysis EKP-12. Bearing #3 critical.'},
ml_credit_scoring:{t:'ML Credit Scoring',i:['5,000 apps. Accuracy 94.2%','Auto-decision 78%','Time: 5 days &#x2192; 4 hours'],r:82000,m:240,s:'ML scoring 5,000 apps. ROI $82,000.'},
dispatch_optimizer:{t:'Route Optimization',i:['Loss: 18% &#x2192; 4.2%','Plan completion: 96%'],r:0,m:60,s:'Routes redistributed. Loss 4.2%.'},
shift_data_entry:{t:'Meter Data Entry',i:['24 instruments entered','Report sent to supervisor'],r:0,m:40,s:'Shift data entered automatically.'},
default:{t:'Task Completed',i:['Processed','In system'],r:0,m:30,s:'Task completed.'}};
async function la(id){
  const t=tasks.find(x=>x.id===id);if(!t)return;
  document.getElementById('em2').innerHTML='<div class="mh"><div><div class="mtit">&#9889; Extella Expert</div><div class="msub" style="font-family:monospace">'+(t.expert_name||'default')+'</div></div><div class="mc" onclick="cm(\'eo2\')">x</div></div>'
    +'<div class="mb4"><div class="term" id="to3"><div class="tl h">&#9658; Extella AI OS &#183; Live</div></div><div id="er3"></div>'
    +'<div style="text-align:center" id="eb3"><button class="btn bg2" style="font-size:11px" onclick="cm(\'eo2\')">Cancel</button></div></div>';
  document.getElementById('eo2').classList.remove('h');
  const sc=SC[t.expert_name]||SC.default,term=document.getElementById('to3');
  sc.forEach(({t:d,c,l})=>setTimeout(()=>{const el=document.createElement('div');el.className='tl '+c;el.innerHTML=l;term.appendChild(el);term.scrollTop=term.scrollHeight;if(c==='s')setTimeout(()=>sr3(id,t),500);},d));
}
async function sr3(id,t){
  const res=RS[t.expert_name]||RS.default;
  const ar=await api2('POST','/tasks/'+id+'/complete',{result_summary:res.s,time_saved_minutes:res.m,roi_realized:res.r});
  const ok=ar?.status==='completed';
  document.getElementById('er3').innerHTML='<div class="rb"><div class="rt">&#9989; '+res.t+'</div>'
    +res.i.map(i=>'<div class="ri"><span style="color:var(--gr)">&#10003;</span>'+i+'</div>').join('')
    +'<div style="margin-top:8px;font-size:11px;color:var(--t2)">&#9201; '+res.m+' min'+(res.r>0?' &#183; &#128176; $'+res.r.toLocaleString():'')+'</div>'
    +'<div style="margin-top:4px;font-size:10px;'+(ok?'color:var(--gr)':'color:var(--yl)')+'">'+(ok?'&#10003; Saved to PostgreSQL':'&#9888; Completed locally')+'</div></div>';
  document.getElementById('eb3').innerHTML='<div style="display:flex;gap:10px;justify-content:center"><button class="btn bg2" onclick="cm(\'eo2\')">Close</button><button class="bxt dn" onclick="md3(\''+id+'\')" >&#10003; Mark Done</button></div>';
  await lt();
}
async function md3(id){
  const i=tasks.findIndex(t=>t.id===id);if(i>=0)tasks[i].status='completed';
  document.getElementById('tb').textContent=tasks.filter(t=>t.status!=='completed').length||0;
  cm('eo2');ra('all');rh();sp('tasks',document.getElementById('nt'));
}
async function ld(){
  document.getElementById('dk').innerHTML='<div style="padding:20px;text-align:center"><div class="sp"></div></div>';
  dash=await api2('GET','/dashboard');const cl=await api2('GET','/clients');clients=cl?.clients||[];
  if(!dash)return;
  const kpis=[{v:dash.kpis?.clients||0,l:'Clients',c:'bl'},{v:dash.kpis?.agents||0,l:'AI Agents',c:'gr'},{v:'$'+Number(dash.kpis?.roi||0).toLocaleString(),l:'ROI',c:'pu'},{v:dash.kpis?.tasks_done||0,l:'Done',c:'yl'}];
  document.getElementById('dk').innerHTML=kpis.map(k=>'<div class="k '+k.c+'"><div class="kv">'+k.v+'</div><div class="kl">'+k.l+'</div></div>').join('');
  document.getElementById('dc').innerHTML=clients.length?clients.map((c,i)=>{
    const pct=c.progress_pct||(c.tasks_total>0?Math.round(c.tasks_done/c.tasks_total*100):0);
    return'<div class="cr" style="grid-template-columns:26px 1fr 50px 55px"><div style="width:24px;height:24px;border-radius:5px;background:'+CL[i%CL.length]+';display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:800;color:#fff">'+c.name.charAt(0)+'</div>'
      +'<div><div style="font-size:12px;font-weight:700">'+c.name+'</div><div class="mb3b"><div class="mf3b" style="width:'+pct+'%;background:'+(pct>70?'var(--gr)':pct>40?'var(--ac)':'var(--rd)')+'"></div></div></div>'
      +'<div style="font-size:12px;font-weight:700;color:'+(pct>70?'var(--gr)':pct>40?'var(--yl)':'var(--rd)')+'">'+pct+'%</div>'
      +'<div style="font-size:12px;font-weight:700;color:var(--gr)">$'+Number(c.roi_realized||c.roi_sum||0).toLocaleString()+'</div></div>';
  }).join(''):'<div class="em" style="padding:20px"><p>No clients</p></div>';
  const act=dash.activity||[];
  document.getElementById('df').innerHTML=act.length?act.slice(0,7).map(a=>'<div class="fi"><div class="fdot gr"></div><div><div class="ftxt"><strong>'+(a.client_name||'&#8212;')+'</strong> &#183; '+(a.employee_name||'&#8212;')+': '+(a.title||'&#8212;')+'</div><div class="ftime">'+(a.roi_realized>0?'&#128176; $'+Number(a.roi_realized).toLocaleString():'Completed')+'</div></div></div>').join('')
    :'<div class="em" style="padding:20px"><p>No activity yet</p></div>';
  if(rch)rch.destroy();
  const ctx=document.getElementById('rc2');
  if(ctx&&clients.length)rch=new Chart(ctx,{type:'bar',data:{labels:clients.map(c=>c.name.split(' ')[0]),datasets:[{label:'ROI',data:clients.map(c=>Number(c.roi_realized||c.roi_sum||0)),backgroundColor:'rgba(99,120,255,.7)',borderRadius:6,borderSkipped:false}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>'$'+c.raw.toLocaleString()}}},scales:{x:{grid:{color:'rgba(99,120,255,.08)'},ticks:{color:'var(--mt)',font:{size:10}}},y:{grid:{color:'rgba(99,120,255,.08)'},ticks:{color:'var(--mt)',font:{size:10},callback:v=>'$'+v.toLocaleString()}}}}});
}
async function lcl(){
  const d=await api2('GET','/clients');clients=d?.clients||[];
  document.getElementById('csub').textContent=clients.length+' companies &#183; Live';
  document.getElementById('cl2').innerHTML=clients.length?'<div class="cl">'+clients.map((c,i)=>{
    const pct=c.progress_pct||(c.tasks_total>0?Math.round(c.tasks_done/c.tasks_total*100):0);
    const sc=pct>70?'bdn':pct>20?'bt2':'bn';const st=pct>70?'&#9679; Active':pct>20?'&#9680; Pilot':'&#9675; Setup';
    return'<div class="cr" style="grid-template-columns:26px 1fr 70px 60px 60px 65px">'
      +'<div style="width:24px;height:24px;border-radius:5px;background:'+CL[i%CL.length]+';display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:800;color:#fff">'+c.name.charAt(0)+'</div>'
      +'<div><div style="font-size:12px;font-weight:700">'+c.name+'</div><div style="font-size:10px;color:var(--t2)">'+(c.industry||'&#8212;')+' &#183; '+(c.city||'&#8212;')+'</div></div>'
      +'<span class="bdg '+sc+'">'+st+'</span>'
      +'<div style="font-size:12px;font-weight:700">'+(c.agents_count||0)+'</div>'
      +'<div><div style="font-size:11px;font-weight:700;color:'+(pct>70?'var(--gr)':pct>40?'var(--yl)':'var(--rd)')+'">'+pct+'%</div>'
      +'<div class="mb3b"><div class="mf3b" style="width:'+pct+'%;background:'+(pct>70?'var(--gr)':pct>40?'var(--ac)':'var(--rd)')+'"></div></div></div>'
      +'<div style="font-size:13px;font-weight:700;color:'+((c.roi_realized||c.roi_sum)>0?'var(--gr)':'var(--mt)')+'">$'+Number(c.roi_realized||c.roi_sum||0).toLocaleString()+'</div></div>';
  }).join('')+'</div>':
  '<div class="em"><div class="emico">&#127970;</div><p>No clients</p></div>';
}
async function lag(){
  const d=await api2('GET','/agents');const ags=d?.agents||[];
  document.getElementById('asub').textContent=ags.length+' agents';
  document.getElementById('al2').innerHTML=ags.length?'<div class="cl">'+ags.map(a=>'<div class="cr" style="grid-template-columns:26px 1fr 80px 60px"><div style="width:24px;height:24px;border-radius:5px;background:var(--bg3);display:flex;align-items:center;justify-content:center;font-size:12px">&#9889;</div><div><div style="font-size:12px;font-weight:600;font-family:monospace">'+a.name+'</div><div style="font-size:10px;color:var(--t2)">'+(a.description||'&#8212;')+'</div></div><span class="bdg bdn">'+(a.category||'&#8212;')+'</span><div style="font-size:13px;font-weight:700">'+(a.runs_total||0)+'</div></div>').join('')+'</div>':
  '<div class="em"><div class="emico">&#129302;</div><p>No agents</p></div>';
}
async function cc(){
  const name=document.getElementById('ncn').value.trim();const ind=document.getElementById('nci').value;const city=document.getElementById('ncc2').value.trim();
  if(!name){alert('Enter name');return}
  const r=await api2('POST','/clients',{name,industry:ind,city,employees_total:0,contract_value:''});
  if(r?.id){cm('nco');await lcl();alert('Client created! ID: '+r.id);}else alert('Error');
}
function sp(id,el){
  document.querySelectorAll('.pg').forEach(p=>p.classList.remove('on'));
  document.querySelectorAll('.ni').forEach(n=>n.classList.remove('on'));
  const p=document.getElementById('p-'+id);if(p)p.classList.add('on');
  if(el)el.classList.add('on');
  document.querySelector('.main2').scrollTop=0;
  if(id==='dash')ld();if(id==='clients')lcl();if(id==='agents')lag();if(id==='tasks')ra('all');
}
function cm(id){document.getElementById(id).classList.add('h')}
function snc(){document.getElementById('nco').classList.remove('h')}
window.onload=init;
</script></body></html>'''

@app.get('/', response_class=HTMLResponse)
def frontend(): return FRONTEND

@app.get('/app', response_class=HTMLResponse)
def frontend_app(): return FRONTEND

@app.get('/api/health')
def health(): return {'status': 'ok', 'db': DB_READY, 'ts': datetime.utcnow().isoformat()}

@app.post('/api/setup')
def setup_route(): ok=setup_tables(); return {'status': 'ok' if ok else 'error', 'db_ready': ok}

@app.get('/api/dashboard')
def dashboard():
    c=get_db(); kpi={}
    kpi['clients']=c.run('SELECT COUNT(*) FROM clients')[0][0]
    kpi['agents']=c.run('SELECT COUNT(*) FROM agents')[0][0]
    kpi['roi']=float(c.run('SELECT COALESCE(SUM(roi_realized),0) FROM clients')[0][0])
    kpi['tasks_done']=c.run("SELECT COUNT(*) FROM tasks WHERE status='completed'")[0][0]
    rows=c.run("SELECT c.id::text,c.name,c.industry,c.city,c.status,c.contract_value,COALESCE(c.roi_realized,0)::float,COUNT(DISTINCT t.id),COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END),COUNT(DISTINCT e.id),CASE WHEN COUNT(DISTINCT t.id)>0 THEN ROUND(COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END)*100.0/COUNT(DISTINCT t.id)) ELSE 0 END FROM clients c LEFT JOIN tasks t ON t.client_id=c.id LEFT JOIN employees e ON e.client_id=c.id GROUP BY c.id ORDER BY c.roi_realized DESC")
    clients2=[dict(zip(['id','name','industry','city','status','contract_value','roi_realized','tasks_total','tasks_done','employees_total','progress_pct'],r)) for r in rows]
    rows2=c.run("SELECT te.executed_at::text,t.title,t.expert_name,cl.name,e.name,te.roi_realized::float FROM task_executions te JOIN tasks t ON t.id=te.task_id JOIN clients cl ON cl.id=te.client_id JOIN employees e ON e.id=te.employee_id ORDER BY te.executed_at DESC LIMIT 20")
    activity=[dict(zip(['executed_at','title','expert_name','client_name','employee_name','roi_realized'],r)) for r in rows2]
    c.close(); return {'kpis':kpi,'clients':clients2,'activity':activity}

@app.get('/api/clients')
def list_clients():
    c=get_db()
    rows=c.run("SELECT c.id::text,c.name,c.industry,c.city,c.status,c.contract_value,COALESCE(c.roi_realized,0)::float,c.tasks_completed,COUNT(DISTINCT e.id),COUNT(DISTINCT t.id),COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END),COUNT(DISTINCT a.id) FROM clients c LEFT JOIN employees e ON e.client_id=c.id LEFT JOIN tasks t ON t.client_id=c.id LEFT JOIN agents a ON a.client_id=c.id LEFT JOIN task_executions te ON te.client_id=c.id GROUP BY c.id ORDER BY c.created_at DESC")
    data=[dict(zip(['id','name','industry','city','status','contract_value','roi_realized','tasks_completed','employees_active','tasks_total','tasks_done','agents_count'],r)) for r in rows]
    c.close(); return {'clients':data}

@app.post('/api/clients')
def create_client(payload:dict):
    cid=str(uuid.uuid4()); c=get_db()
    c.run("INSERT INTO clients(id,name,industry,city,employees_total,contract_value,status,created_at,roi_realized,tasks_completed) VALUES(:id,:name,:ind,:city,:emp,:cv,'setup',NOW(),0,0)",id=cid,name=payload['name'],ind=payload.get('industry',''),city=payload.get('city',''),emp=payload.get('employees_total',0),cv=payload.get('contract_value',''))
    c.close(); return {'id':cid,'status':'created'}

@app.get('/api/clients/{cid}')
def get_client(cid:str):
    c=get_db()
    rows=c.run('SELECT id::text,name,industry,city,status,contract_value,roi_realized::float FROM clients WHERE id=:id',id=cid)
    if not rows: raise HTTPException(404,'Not found')
    cl=dict(zip(['id','name','industry','city','status','contract_value','roi_realized'],rows[0]))
    e_rows=c.run('SELECT id::text,name,role,department,level FROM employees WHERE client_id=:cid',cid=cid)
    emps=[dict(zip(['id','name','role','department','level'],r)) for r in e_rows]
    c.close(); return {'client':cl,'employees':emps}

@app.post('/api/employees')
def create_employee(payload:dict):
    eid=str(uuid.uuid4()); c=get_db()
    c.run("INSERT INTO employees(id,client_id,name,role,department,level,created_at) VALUES(:id,:cid,:name,:role,:dept,:level,NOW())",id=eid,cid=payload['client_id'],name=payload['name'],role=payload.get('role',''),dept=payload.get('department',''),level=payload.get('level',3))
    c.close(); return {'id':eid}

@app.get('/api/employees')
def list_employees(client_id:Optional[str]=None):
    c=get_db()
    if client_id: rows=c.run("SELECT e.id::text,e.name,e.role,e.department,e.level FROM employees e WHERE e.client_id=:cid",cid=client_id)
    else: rows=c.run('SELECT id::text,name,role,department,level FROM employees')
    data=[dict(zip(['id','name','role','department','level'],r)) for r in rows]
    c.close(); return {'employees':data}

@app.post('/api/tasks')
def create_task(payload:dict):
    tid=str(uuid.uuid4()); c=get_db()
    c.run("INSERT INTO tasks(id,client_id,employee_id,title,description,expert_name,priority,roi_estimate,source,status,created_at) VALUES(:id,:cid,:eid,:title,:desc,:exp,:pri,:roi,:src,'pending',NOW())",id=tid,cid=payload['client_id'],eid=payload['employee_id'],title=payload['title'],desc=payload.get('description',''),exp=payload.get('expert_name',''),pri=payload.get('priority','normal'),roi=payload.get('roi_estimate',''),src=payload.get('source','manual'))
    c.close(); return {'id':tid,'status':'created'}

@app.get('/api/tasks')
def get_tasks(employee_id:Optional[str]=None,client_id:Optional[str]=None,status:Optional[str]=None):
    c=get_db(); where,params=[],[]
    q="SELECT t.id::text,t.client_id::text,t.employee_id::text,t.title,t.description,t.expert_name,t.priority,t.roi_estimate,t.source,t.status,e.name,e.role FROM tasks t LEFT JOIN employees e ON e.id=t.employee_id"
    kw={}
    if employee_id: where.append('t.employee_id=:eid'); kw['eid']=employee_id
    if client_id: where.append('t.client_id=:cid'); kw['cid']=client_id
    if status: where.append('t.status=:status'); kw['status']=status
    if where: q+=' WHERE '+' AND '.join(where)
    q+=' ORDER BY t.created_at DESC'
    rows=c.run(q,**kw)
    data=[dict(zip(['id','client_id','employee_id','title','description','expert_name','priority','roi_estimate','source','status','employee_name','employee_role'],r)) for r in rows]
    c.close(); return {'tasks':data}

@app.post('/api/tasks/{task_id}/complete')
def complete_task(task_id:str,payload:dict):
    c=get_db()
    rows=c.run('SELECT id::text,client_id::text,employee_id::text FROM tasks WHERE id=:id',id=task_id)
    if not rows: raise HTTPException(404,'Not found')
    task=rows[0]
    c.run("UPDATE tasks SET status='completed',completed_at=NOW() WHERE id=:id",id=task_id)
    eid2=str(uuid.uuid4()); roi=float(payload.get('roi_realized',0))
    c.run('INSERT INTO task_executions(id,task_id,client_id,employee_id,result_summary,result_data,time_saved_minutes,roi_realized,executed_at) VALUES(:id,:tid,:cid,:empid,:rs,:rd,:ts,:roi,NOW())',id=eid2,tid=task_id,cid=task[1],empid=task[2],rs=payload.get('result_summary',''),rd=json.dumps(payload.get('result_data',{})),ts=payload.get('time_saved_minutes',0),roi=roi)
    c.run('UPDATE clients SET tasks_completed=tasks_completed+1,roi_realized=roi_realized+:roi,updated_at=NOW() WHERE id=:cid',roi=roi,cid=task[1])
    c.close(); return {'status':'completed','execution_id':eid2}

@app.post('/api/agents')
def register_agent(payload:dict):
    aid=str(uuid.uuid4()); c=get_db()
    c.run("INSERT INTO agents(id,client_id,name,description,category,status,runs_total,success_rate,created_at) VALUES(:id,:cid,:name,:desc,:cat,'active',0,100.0,NOW())",id=aid,cid=payload.get('client_id'),name=payload['name'],desc=payload.get('description',''),cat=payload.get('category','general'))
    c.close(); return {'id':aid}

@app.get('/api/agents')
def list_agents(client_id:Optional[str]=None):
    c=get_db()
    if client_id: rows=c.run('SELECT id::text,client_id::text,name,description,category,status,runs_total,success_rate::float FROM agents WHERE client_id=:cid',cid=client_id)
    else: rows=c.run('SELECT id::text,client_id::text,name,description,category,status,runs_total,success_rate::float FROM agents ORDER BY created_at DESC')
    data=[dict(zip(['id','client_id','name','description','category','status','runs_total','success_rate'],r)) for r in rows]
    c.close(); return {'agents':data}

@app.post('/api/agents/{agent_id}/ping')
def ping_agent(agent_id:str,payload:dict={}):
    c=get_db(); s=100 if payload.get('success',True) else 0
    c.run('UPDATE agents SET runs_total=runs_total+1,last_run=NOW(),success_rate=(success_rate*runs_total+:s)/(runs_total+1) WHERE id=:id',s=s,id=agent_id)
    c.close(); return {'status':'ok'}
