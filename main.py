import os, json, uuid
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pg8000.native

app = FastAPI(title="AI Corporate OS", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

def get_db():
    from urllib.parse import urlparse
    p = urlparse(DATABASE_URL)
    return pg8000.native.Connection(host=p.hostname, port=p.port or 5432,
        user=p.username, password=p.password, database=p.path.lstrip("/"), ssl_context=None)

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
        print(f"DB setup error: {e}"); return False

DB_READY = setup_tables()

def rows_to_dicts(rows, cols):
    return [dict(zip(cols, r)) for r in rows]

@app.get("/", response_class=HTMLResponse)
def frontend():
    return HTMLResponse(content="""<!DOCTYPE html>
<html lang="ru"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AI Corporate OS — Extella</title>
<meta http-equiv="refresh" content="0;url=https://api-production-788d.up.railway.app/api/health">
<style>body{background:#07080f;color:#e2e8f0;font-family:-apple-system,Inter,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;flex-direction:column;gap:20px;text-align:center}
.logo{width:60px;height:60px;background:linear-gradient(135deg,#6378ff,#a855f7);border-radius:14px;display:flex;align-items:center;justify-content:center;font-size:28px;margin:0 auto}
h1{font-size:24px;font-weight:800}.sub{color:#94a3b8;font-size:14px}
.dot{width:8px;height:8px;border-radius:50%;background:#22d3a3;display:inline-block;animation:blink 1.5s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
a{color:#6378ff;text-decoration:none;font-size:13px;padding:10px 24px;background:rgba(99,120,255,.1);border:1px solid rgba(99,120,255,.3);border-radius:8px;transition:all .2s}
a:hover{background:rgba(99,120,255,.2)}</style>
</head><body>
<div class="logo">⚡</div>
<h1>AI Corporate OS</h1>
<p class="sub"><span class="dot"></span> Backend API Live · PostgreSQL Connected</p>
<p style="color:#475569;font-size:13px;font-family:monospace">api-production-788d.up.railway.app</p>
<a href="/api/dashboard">📊 API Dashboard →</a>
<a href="/docs">📖 API Docs →</a>
</body></html>""")

@app.get("/api/health")
def health():
    return {"status": "ok", "db": DB_READY, "ts": datetime.utcnow().isoformat()}

@app.post("/api/setup")
def setup():
    return {"status": "ok" if setup_tables() else "error", "db_ready": setup_tables()}

@app.get("/api/dashboard")
def dashboard():
    c = get_db()
    kpi = {}
    rows = c.run("SELECT COUNT(*) FROM clients"); kpi["clients"] = rows[0][0]
    rows = c.run("SELECT COUNT(*) FROM agents"); kpi["agents"] = rows[0][0]
    rows = c.run("SELECT COALESCE(SUM(roi_realized), 0) FROM clients"); kpi["roi"] = float(rows[0][0])
    rows = c.run("SELECT COUNT(*) FROM tasks WHERE status='completed'"); kpi["tasks_done"] = rows[0][0]
    rows = c.run("SELECT c.id::text, c.name, c.industry, c.city, c.status, c.contract_value, COALESCE(c.roi_realized,0)::float, COUNT(DISTINCT t.id), COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END), COUNT(DISTINCT e.id), CASE WHEN COUNT(DISTINCT t.id)>0 THEN ROUND(COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END)*100.0/COUNT(DISTINCT t.id)) ELSE 0 END FROM clients c LEFT JOIN tasks t ON t.client_id=c.id LEFT JOIN employees e ON e.client_id=c.id GROUP BY c.id ORDER BY c.roi_realized DESC")
    clients = rows_to_dicts(rows, ["id","name","industry","city","status","contract_value","roi_realized","tasks_total","tasks_done","employees_total","progress_pct"])
    rows = c.run("SELECT te.executed_at::text, t.title, t.expert_name, cl.name, e.name, te.roi_realized::float FROM task_executions te JOIN tasks t ON t.id=te.task_id JOIN clients cl ON cl.id=te.client_id JOIN employees e ON e.id=te.employee_id ORDER BY te.executed_at DESC LIMIT 20")
    activity = rows_to_dicts(rows, ["executed_at","title","expert_name","client_name","employee_name","roi_realized"])
    c.close()
    return {"kpis": kpi, "clients": clients, "activity": activity}

@app.get("/api/clients")
def list_clients():
    c = get_db()
    rows = c.run("SELECT c.id::text, c.name, c.industry, c.city, c.status, c.contract_value, COALESCE(c.roi_realized,0)::float, c.tasks_completed, COUNT(DISTINCT e.id), COUNT(DISTINCT t.id), COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END), COUNT(DISTINCT a.id) FROM clients c LEFT JOIN employees e ON e.client_id=c.id LEFT JOIN tasks t ON t.client_id=c.id LEFT JOIN agents a ON a.client_id=c.id GROUP BY c.id ORDER BY c.created_at DESC")
    c.close()
    return {"clients": rows_to_dicts(rows, ["id","name","industry","city","status","contract_value","roi_realized","tasks_completed","employees_active","tasks_total","tasks_done","agents_count"])}

@app.post("/api/clients")
def create_client(payload: dict):
    cid = str(uuid.uuid4()); c = get_db()
    c.run("INSERT INTO clients(id,name,industry,city,employees_total,contract_value,status,created_at,roi_realized,tasks_completed) VALUES(:id,:n,:ind,:city,:emp,:cv,'setup',NOW(),0,0)", id=cid, n=payload["name"], ind=payload.get("industry",""), city=payload.get("city",""), emp=payload.get("employees_total",0), cv=payload.get("contract_value",""))
    c.close(); return {"id": cid, "status": "created"}

@app.get("/api/clients/{cid}")
def get_client(cid: str):
    c = get_db()
    rows = c.run("SELECT id::text,name,industry,city,status,contract_value,roi_realized::float FROM clients WHERE id=:id", id=cid)
    if not rows: raise HTTPException(404, "Not found")
    emps = c.run("SELECT id::text,name,role,department,level FROM employees WHERE client_id=:cid", cid=cid)
    c.close()
    return {"client": rows_to_dicts(rows, ["id","name","industry","city","status","contract_value","roi_realized"])[0], "employees": rows_to_dicts(emps, ["id","name","role","department","level"])}

@app.post("/api/employees")
def create_employee(payload: dict):
    eid = str(uuid.uuid4()); c = get_db()
    c.run("INSERT INTO employees(id,client_id,name,role,department,level,created_at) VALUES(:id,:cid,:n,:r,:d,:l,NOW())", id=eid, cid=payload["client_id"], n=payload["name"], r=payload.get("role",""), d=payload.get("department",""), l=payload.get("level",3))
    c.close(); return {"id": eid}

@app.get("/api/employees")
def list_employees(client_id: Optional[str]=None):
    c = get_db()
    if client_id:
        rows = c.run("SELECT e.id::text,e.name,e.role,e.department,e.level FROM employees e WHERE e.client_id=:cid", cid=client_id)
    else:
        rows = c.run("SELECT id::text,name,role,department,level FROM employees")
    c.close(); return {"employees": rows_to_dicts(rows, ["id","name","role","department","level"])}

@app.post("/api/tasks")
def create_task(payload: dict):
    tid = str(uuid.uuid4()); c = get_db()
    c.run("INSERT INTO tasks(id,client_id,employee_id,title,description,expert_name,priority,roi_estimate,source,status,created_at) VALUES(:id,:cid,:eid,:t,:desc,:en,:pr,:roi,:src,'pending',NOW())", id=tid, cid=payload["client_id"], eid=payload["employee_id"], t=payload["title"], desc=payload.get("description",""), en=payload.get("expert_name",""), pr=payload.get("priority","normal"), roi=payload.get("roi_estimate",""), src=payload.get("source","manual"))
    c.close(); return {"id": tid, "status": "created"}

@app.get("/api/tasks")
def get_tasks(employee_id: Optional[str]=None, client_id: Optional[str]=None, status: Optional[str]=None):
    c = get_db()
    where, params = [], {}
    if employee_id: where.append("t.employee_id=:eid"); params["eid"] = employee_id
    if client_id: where.append("t.client_id=:cid"); params["cid"] = client_id
    if status: where.append("t.status=:st"); params["st"] = status
    w = "WHERE " + " AND ".join(where) if where else ""
    rows = c.run(f"SELECT t.id::text,t.client_id::text,t.employee_id::text,t.title,t.description,t.expert_name,t.priority,t.roi_estimate,t.source,t.status,e.name,e.role FROM tasks t LEFT JOIN employees e ON e.id=t.employee_id {w} ORDER BY t.created_at DESC", **params)
    c.close(); return {"tasks": rows_to_dicts(rows, ["id","client_id","employee_id","title","description","expert_name","priority","roi_estimate","source","status","employee_name","employee_role"])}

@app.post("/api/tasks/{task_id}/complete")
def complete_task(task_id: str, payload: dict):
    c = get_db()
    rows = c.run("SELECT id::text,client_id::text,employee_id::text FROM tasks WHERE id=:id", id=task_id)
    if not rows: raise HTTPException(404, "Task not found")
    task = rows[0]
    c.run("UPDATE tasks SET status='completed',completed_at=NOW() WHERE id=:id", id=task_id)
    eid = str(uuid.uuid4()); roi = float(payload.get("roi_realized", 0))
    c.run("INSERT INTO task_executions(id,task_id,client_id,employee_id,result_summary,result_data,time_saved_minutes,roi_realized,executed_at) VALUES(:id,:tid,:cid,:empid,:rs,:rd,:ts,:roi,NOW())", id=eid, tid=task_id, cid=task[1], empid=task[2], rs=payload.get("result_summary",""), rd=json.dumps(payload.get("result_data",{})), ts=payload.get("time_saved_minutes",0), roi=roi)
    c.run("UPDATE clients SET tasks_completed=tasks_completed+1,roi_realized=roi_realized+:roi,updated_at=NOW() WHERE id=:cid", roi=roi, cid=task[1])
    c.close(); return {"status": "completed", "execution_id": eid}

@app.post("/api/agents")
def register_agent(payload: dict):
    aid = str(uuid.uuid4()); c = get_db()
    c.run("INSERT INTO agents(id,client_id,name,description,category,status,runs_total,success_rate,created_at) VALUES(:id,:cid,:n,:d,:cat,'active',0,100.0,NOW())", id=aid, cid=payload.get("client_id"), n=payload["name"], d=payload.get("description",""), cat=payload.get("category","general"))
    c.close(); return {"id": aid}

@app.get("/api/agents")
def list_agents(client_id: Optional[str]=None):
    c = get_db()
    if client_id:
        rows = c.run("SELECT id::text,client_id::text,name,description,category,status,runs_total,success_rate::float FROM agents WHERE client_id=:cid", cid=client_id)
    else:
        rows = c.run("SELECT id::text,client_id::text,name,description,category,status,runs_total,success_rate::float FROM agents")
    c.close(); return {"agents": rows_to_dicts(rows, ["id","client_id","name","description","category","status","runs_total","success_rate"])}

@app.post("/api/agents/{agent_id}/ping")
def ping_agent(agent_id: str, payload: dict = {}):
    c = get_db(); s = 100 if payload.get("success", True) else 0
    c.run("UPDATE agents SET runs_total=runs_total+1,last_run=NOW(),success_rate=(success_rate*runs_total+:s)/(runs_total+1) WHERE id=:id", s=s, id=agent_id)
    c.close(); return {"status": "ok"}
