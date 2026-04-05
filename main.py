import os
import json
import uuid
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pg8000.native

app = FastAPI(title="AI Corporate OS", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

def parse_db_url(url):
    from urllib.parse import urlparse
    p = urlparse(url)
    return {"host": p.hostname, "port": p.port or 5432, "user": p.username, "password": p.password, "database": p.path.lstrip("/")}

def get_db():
    cfg = parse_db_url(DATABASE_URL)
    return pg8000.native.Connection(
        host=cfg["host"], port=cfg["port"],
        user=cfg["user"], password=cfg["password"],
        database=cfg["database"], ssl_context=None
    )

def setup_tables():
    if not DATABASE_URL:
        return False
    try:
        c = get_db()
        c.run("CREATE TABLE IF NOT EXISTS clients (id UUID PRIMARY KEY, name VARCHAR(255), industry VARCHAR(100), city VARCHAR(100), status VARCHAR(50) DEFAULT 'setup', employees_total INTEGER DEFAULT 0, contract_value VARCHAR(100), tasks_completed INTEGER DEFAULT 0, roi_realized DECIMAL(12,2) DEFAULT 0, created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())")
        c.run("CREATE TABLE IF NOT EXISTS employees (id UUID PRIMARY KEY, client_id UUID, name VARCHAR(255), role VARCHAR(255), department VARCHAR(255), level INTEGER DEFAULT 3, created_at TIMESTAMP DEFAULT NOW())")
        c.run("CREATE TABLE IF NOT EXISTS tasks (id UUID PRIMARY KEY, client_id UUID, employee_id UUID, title VARCHAR(500), description TEXT, expert_name VARCHAR(255), priority VARCHAR(50) DEFAULT 'normal', roi_estimate VARCHAR(100), source VARCHAR(255), status VARCHAR(50) DEFAULT 'pending', created_at TIMESTAMP DEFAULT NOW(), completed_at TIMESTAMP)")
        c.run("CREATE TABLE IF NOT EXISTS task_executions (id UUID PRIMARY KEY, task_id UUID, client_id UUID, employee_id UUID, result_summary TEXT, result_data TEXT DEFAULT '{}', time_saved_minutes INTEGER DEFAULT 0, roi_realized DECIMAL(12,2) DEFAULT 0, executed_at TIMESTAMP DEFAULT NOW())")
        c.run("CREATE TABLE IF NOT EXISTS agents (id UUID PRIMARY KEY, client_id UUID, name VARCHAR(255), description TEXT, category VARCHAR(100), status VARCHAR(50) DEFAULT 'active', runs_total INTEGER DEFAULT 0, success_rate DECIMAL(5,2) DEFAULT 100.0, last_run TIMESTAMP, created_at TIMESTAMP DEFAULT NOW())")
        c.close()
        return True
    except Exception as e:
        print(f"DB setup error: {e}")
        return False

DB_READY = setup_tables()

def rows_to_dicts(rows, columns):
    return [dict(zip(columns, row)) for row in rows]

@app.get("/api/health")
def health():
    return {"status": "ok", "db": DB_READY, "ts": datetime.utcnow().isoformat()}

@app.post("/api/setup")
def setup():
    ok = setup_tables()
    return {"status": "ok" if ok else "error", "db_ready": ok}

@app.get("/api/dashboard")
def dashboard():
    c = get_db()
    kpi = {}
    rows = c.run("SELECT COUNT(*) FROM clients"); kpi["clients"] = rows[0][0]
    rows = c.run("SELECT COUNT(*) FROM agents"); kpi["agents"] = rows[0][0]
    rows = c.run("SELECT COALESCE(SUM(roi_realized), 0) FROM clients"); kpi["roi"] = float(rows[0][0])
    rows = c.run("SELECT COUNT(*) FROM tasks WHERE status='completed'"); kpi["tasks_done"] = rows[0][0]
    rows = c.run("SELECT c.id::text, c.name, c.industry, c.city, c.status, c.contract_value, COALESCE(c.roi_realized,0)::float, COUNT(DISTINCT t.id), COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END), COUNT(DISTINCT e.id), CASE WHEN COUNT(DISTINCT t.id)>0 THEN ROUND(COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END)*100.0/COUNT(DISTINCT t.id)) ELSE 0 END FROM clients c LEFT JOIN tasks t ON t.client_id=c.id LEFT JOIN employees e ON e.client_id=c.id GROUP BY c.id ORDER BY c.roi_realized DESC")
    cols = ["id","name","industry","city","status","contract_value","roi_realized","tasks_total","tasks_done","employees_total","progress_pct"]
    clients = rows_to_dicts(rows, cols)
    rows = c.run("SELECT te.executed_at::text, t.title, t.expert_name, cl.name, e.name, te.roi_realized::float FROM task_executions te JOIN tasks t ON t.id=te.task_id JOIN clients cl ON cl.id=te.client_id JOIN employees e ON e.id=te.employee_id ORDER BY te.executed_at DESC LIMIT 20")
    activity = rows_to_dicts(rows, ["executed_at","title","expert_name","client_name","employee_name","roi_realized"])
    c.close()
    return {"kpis": kpi, "clients": clients, "activity": activity}

@app.get("/api/clients")
def list_clients():
    c = get_db()
    rows = c.run("SELECT c.id::text, c.name, c.industry, c.city, c.status, c.contract_value, COALESCE(c.roi_realized,0)::float, c.tasks_completed, COUNT(DISTINCT e.id), COUNT(DISTINCT t.id), COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END), COUNT(DISTINCT a.id) FROM clients c LEFT JOIN employees e ON e.client_id=c.id LEFT JOIN tasks t ON t.client_id=c.id LEFT JOIN agents a ON a.client_id=c.id LEFT JOIN task_executions te ON te.client_id=c.id GROUP BY c.id ORDER BY c.created_at DESC")
    cols = ["id","name","industry","city","status","contract_value","roi_realized","tasks_completed","employees_active","tasks_total","tasks_done","agents_count"]
    c.close()
    return {"clients": rows_to_dicts(rows, cols)}

@app.post("/api/clients")
def create_client(payload: dict):
    cid = str(uuid.uuid4())
    c = get_db()
    c.run("INSERT INTO clients(id,name,industry,city,employees_total,contract_value,status,created_at,roi_realized,tasks_completed) VALUES(:id,:name,:industry,:city,:employees_total,:contract_value,'setup',NOW(),0,0)", id=cid, name=payload["name"], industry=payload.get("industry",""), city=payload.get("city",""), employees_total=payload.get("employees_total",0), contract_value=payload.get("contract_value",""))
    c.close()
    return {"id": cid, "status": "created"}

@app.get("/api/clients/{cid}")
def get_client(cid: str):
    c = get_db()
    rows = c.run("SELECT id::text,name,industry,city,status,contract_value,roi_realized::float,employees_total FROM clients WHERE id=:id", id=cid)
    if not rows: raise HTTPException(404, "Not found")
    cols = ["id","name","industry","city","status","contract_value","roi_realized","employees_total"]
    cl = rows_to_dicts(rows, cols)[0]
    emp_rows = c.run("SELECT id::text,name,role,department,level FROM employees WHERE client_id=:cid", cid=cid)
    emps = rows_to_dicts(emp_rows, ["id","name","role","department","level"])
    c.close()
    return {"client": cl, "employees": emps}

@app.post("/api/employees")
def create_employee(payload: dict):
    eid = str(uuid.uuid4())
    c = get_db()
    c.run("INSERT INTO employees(id,client_id,name,role,department,level,created_at) VALUES(:id,:client_id,:name,:role,:department,:level,NOW())", id=eid, client_id=payload["client_id"], name=payload["name"], role=payload.get("role",""), department=payload.get("department",""), level=payload.get("level",3))
    c.close()
    return {"id": eid}

@app.get("/api/employees")
def list_employees(client_id: Optional[str]=None):
    c = get_db()
    if client_id:
        rows = c.run("SELECT e.id::text, e.name, e.role, e.department, e.level FROM employees e WHERE e.client_id=:cid", cid=client_id)
    else:
        rows = c.run("SELECT id::text, name, role, department, level FROM employees")
    data = rows_to_dicts(rows, ["id","name","role","department","level"])
    c.close()
    return {"employees": data}

@app.post("/api/tasks")
def create_task(payload: dict):
    tid = str(uuid.uuid4())
    c = get_db()
    c.run("INSERT INTO tasks(id,client_id,employee_id,title,description,expert_name,priority,roi_estimate,source,status,created_at) VALUES(:id,:client_id,:employee_id,:title,:description,:expert_name,:priority,:roi_estimate,:source,'pending',NOW())", id=tid, client_id=payload["client_id"], employee_id=payload["employee_id"], title=payload["title"], description=payload.get("description",""), expert_name=payload.get("expert_name",""), priority=payload.get("priority","normal"), roi_estimate=payload.get("roi_estimate",""), source=payload.get("source","manual"))
    c.close()
    return {"id": tid, "status": "created"}

@app.get("/api/tasks")
def get_tasks(employee_id: Optional[str]=None, client_id: Optional[str]=None, status: Optional[str]=None):
    c = get_db()
    where, params = [], {}
    if employee_id: where.append("t.employee_id=:employee_id"); params["employee_id"] = employee_id
    if client_id: where.append("t.client_id=:client_id"); params["client_id"] = client_id
    if status: where.append("t.status=:status"); params["status"] = status
    w = "WHERE " + " AND ".join(where) if where else ""
    rows = c.run(f"SELECT t.id::text, t.client_id::text, t.employee_id::text, t.title, t.description, t.expert_name, t.priority, t.roi_estimate, t.source, t.status, e.name, e.role FROM tasks t LEFT JOIN employees e ON e.id=t.employee_id {w} ORDER BY t.created_at DESC", **params)
    cols = ["id","client_id","employee_id","title","description","expert_name","priority","roi_estimate","source","status","employee_name","employee_role"]
    c.close()
    return {"tasks": rows_to_dicts(rows, cols)}

@app.post("/api/tasks/{task_id}/complete")
def complete_task(task_id: str, payload: dict):
    c = get_db()
    rows = c.run("SELECT id::text, client_id::text, employee_id::text FROM tasks WHERE id=:id", id=task_id)
    if not rows: raise HTTPException(404, "Task not found")
    task = rows[0]
    c.run("UPDATE tasks SET status='completed', completed_at=NOW() WHERE id=:id", id=task_id)
    eid = str(uuid.uuid4()); roi = float(payload.get("roi_realized", 0))
    c.run("INSERT INTO task_executions(id,task_id,client_id,employee_id,result_summary,result_data,time_saved_minutes,roi_realized,executed_at) VALUES(:id,:task_id,:client_id,:employee_id,:result_summary,:result_data,:time_saved,:roi,NOW())", id=eid, task_id=task_id, client_id=task[1], employee_id=task[2], result_summary=payload.get("result_summary",""), result_data=json.dumps(payload.get("result_data",{})), time_saved=payload.get("time_saved_minutes",0), roi=roi)
    c.run("UPDATE clients SET tasks_completed=tasks_completed+1, roi_realized=roi_realized+:roi, updated_at=NOW() WHERE id=:cid", roi=roi, cid=task[1])
    c.close()
    return {"status": "completed", "execution_id": eid}

@app.post("/api/agents")
def register_agent(payload: dict):
    aid = str(uuid.uuid4())
    c = get_db()
    c.run("INSERT INTO agents(id,client_id,name,description,category,status,runs_total,success_rate,created_at) VALUES(:id,:client_id,:name,:description,:category,'active',0,100.0,NOW())", id=aid, client_id=payload.get("client_id"), name=payload["name"], description=payload.get("description",""), category=payload.get("category","general"))
    c.close()
    return {"id": aid}

@app.post("/api/agents/{agent_id}/ping")
def ping_agent(agent_id: str, payload: dict = {}):
    c = get_db(); s = 100 if payload.get("success", True) else 0
    c.run("UPDATE agents SET runs_total=runs_total+1, last_run=NOW(), success_rate=(success_rate*runs_total+:s)/(runs_total+1) WHERE id=:id", s=s, id=agent_id)
    c.close()
    return {"status": "ok"}
