import os
import json
import uuid
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="AI Corporate OS", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

def get_db():
    import psycopg2
    return psycopg2.connect(DATABASE_URL)

def dict_cursor(c):
    import psycopg2.extras
    return c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def setup_tables():
    if not DATABASE_URL:
        return False
    try:
        c = get_db()
        cur = c.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS clients (id UUID PRIMARY KEY, name VARCHAR(255), industry VARCHAR(100), city VARCHAR(100), status VARCHAR(50) DEFAULT 'setup', employees_total INTEGER DEFAULT 0, contract_value VARCHAR(100), tasks_completed INTEGER DEFAULT 0, roi_realized DECIMAL(12,2) DEFAULT 0, created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())")
        cur.execute("CREATE TABLE IF NOT EXISTS employees (id UUID PRIMARY KEY, client_id UUID, name VARCHAR(255), role VARCHAR(255), department VARCHAR(255), level INTEGER DEFAULT 3, created_at TIMESTAMP DEFAULT NOW())")
        cur.execute("CREATE TABLE IF NOT EXISTS tasks (id UUID PRIMARY KEY, client_id UUID, employee_id UUID, title VARCHAR(500), description TEXT, expert_name VARCHAR(255), priority VARCHAR(50) DEFAULT 'normal', roi_estimate VARCHAR(100), source VARCHAR(255), status VARCHAR(50) DEFAULT 'pending', created_at TIMESTAMP DEFAULT NOW(), completed_at TIMESTAMP)")
        cur.execute("CREATE TABLE IF NOT EXISTS task_executions (id UUID PRIMARY KEY, task_id UUID, client_id UUID, employee_id UUID, result_summary TEXT, result_data JSONB DEFAULT '{}', time_saved_minutes INTEGER DEFAULT 0, roi_realized DECIMAL(12,2) DEFAULT 0, executed_at TIMESTAMP DEFAULT NOW())")
        cur.execute("CREATE TABLE IF NOT EXISTS agents (id UUID PRIMARY KEY, client_id UUID, name VARCHAR(255), description TEXT, category VARCHAR(100), status VARCHAR(50) DEFAULT 'active', runs_total INTEGER DEFAULT 0, success_rate DECIMAL(5,2) DEFAULT 100.0, last_run TIMESTAMP, created_at TIMESTAMP DEFAULT NOW())")
        c.commit()
        cur.close()
        c.close()
        return True
    except Exception as e:
        print(f"DB setup error: {e}")
        return False

DB_READY = setup_tables()

@app.get("/api/health")
def health():
    return {"status": "ok", "db": DB_READY, "ts": datetime.utcnow().isoformat()}

@app.post("/api/setup")
def setup():
    ok = setup_tables()
    return {"status": "ok" if ok else "error", "db_ready": ok}

@app.get("/api/dashboard")
def dashboard():
    c = get_db(); cur = dict_cursor(c)
    cur.execute("SELECT COUNT(*) as clients FROM clients"); kpi = dict(cur.fetchone())
    cur.execute("SELECT COUNT(*) as agents FROM agents"); kpi.update(cur.fetchone())
    cur.execute("SELECT COALESCE(SUM(roi_realized), 0) as roi FROM clients"); kpi.update(cur.fetchone())
    cur.execute("SELECT COUNT(*) as tasks_done FROM tasks WHERE status='completed'"); kpi.update(cur.fetchone())
    cur.execute("SELECT c.id, c.name, c.industry, c.city, c.status, c.contract_value, c.roi_realized, COUNT(DISTINCT t.id) tasks_total, COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END) tasks_done, COUNT(DISTINCT e.id) employees_total, CASE WHEN COUNT(DISTINCT t.id)>0 THEN ROUND(COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END)*100.0/COUNT(DISTINCT t.id)) ELSE 0 END progress_pct FROM clients c LEFT JOIN tasks t ON t.client_id=c.id LEFT JOIN employees e ON e.client_id=c.id GROUP BY c.id ORDER BY c.roi_realized DESC")
    clients = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT te.executed_at, t.title, t.expert_name, cl.name client_name, e.name employee_name, te.roi_realized FROM task_executions te JOIN tasks t ON t.id=te.task_id JOIN clients cl ON cl.id=te.client_id JOIN employees e ON e.id=te.employee_id ORDER BY te.executed_at DESC LIMIT 20")
    activity = [dict(r) for r in cur.fetchall()]
    cur.close(); c.close()
    return {"kpis": kpi, "clients": clients, "activity": activity}

@app.get("/api/clients")
def list_clients():
    c = get_db(); cur = dict_cursor(c)
    cur.execute("SELECT c.*, COUNT(DISTINCT e.id) employees_active, COUNT(DISTINCT t.id) tasks_total, COUNT(DISTINCT CASE WHEN t.status='completed' THEN t.id END) tasks_done, COUNT(DISTINCT a.id) agents_count, COALESCE(SUM(te.roi_realized),0) roi_sum FROM clients c LEFT JOIN employees e ON e.client_id=c.id LEFT JOIN tasks t ON t.client_id=c.id LEFT JOIN agents a ON a.client_id=c.id LEFT JOIN task_executions te ON te.client_id=c.id GROUP BY c.id ORDER BY c.created_at DESC")
    data = [dict(r) for r in cur.fetchall()]
    cur.close(); c.close()
    return {"clients": data}

@app.post("/api/clients")
def create_client(payload: dict):
    c = get_db(); cur = c.cursor(); cid = str(uuid.uuid4())
    cur.execute("INSERT INTO clients(id,name,industry,city,employees_total,contract_value,status,created_at,roi_realized,tasks_completed) VALUES(%s,%s,%s,%s,%s,%s,'setup',NOW(),0,0)", (cid, payload["name"], payload.get("industry",""), payload.get("city",""), payload.get("employees_total",0), payload.get("contract_value","")))
    c.commit(); cur.close(); c.close()
    return {"id": cid, "status": "created"}

@app.get("/api/clients/{cid}")
def get_client(cid: str):
    c = get_db(); cur = dict_cursor(c)
    cur.execute("SELECT * FROM clients WHERE id=%s", (cid,))
    cl = cur.fetchone()
    if not cl: raise HTTPException(404, "Not found")
    cur.execute("SELECT * FROM employees WHERE client_id=%s", (cid,))
    emps = [dict(r) for r in cur.fetchall()]
    cur.close(); c.close()
    return {"client": dict(cl), "employees": emps}

@app.post("/api/employees")
def create_employee(payload: dict):
    c = get_db(); cur = c.cursor(); eid = str(uuid.uuid4())
    cur.execute("INSERT INTO employees(id,client_id,name,role,department,level,created_at) VALUES(%s,%s,%s,%s,%s,%s,NOW())", (eid, payload["client_id"], payload["name"], payload.get("role",""), payload.get("department",""), payload.get("level",3)))
    c.commit(); cur.close(); c.close()
    return {"id": eid}

@app.get("/api/employees")
def list_employees(client_id: Optional[str]=None):
    c = get_db(); cur = dict_cursor(c)
    if client_id:
        cur.execute("SELECT e.*, COUNT(t.id) tasks_total, COUNT(CASE WHEN t.status='completed' THEN 1 END) tasks_done FROM employees e LEFT JOIN tasks t ON t.employee_id=e.id WHERE e.client_id=%s GROUP BY e.id", (client_id,))
    else:
        cur.execute("SELECT * FROM employees")
    data = [dict(r) for r in cur.fetchall()]
    cur.close(); c.close()
    return {"employees": data}

@app.post("/api/tasks")
def create_task(payload: dict):
    c = get_db(); cur = c.cursor(); tid = str(uuid.uuid4())
    cur.execute("INSERT INTO tasks(id,client_id,employee_id,title,description,expert_name,priority,roi_estimate,source,status,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',NOW())", (tid, payload["client_id"], payload["employee_id"], payload["title"], payload.get("description",""), payload.get("expert_name",""), payload.get("priority","normal"), payload.get("roi_estimate",""), payload.get("source","manual")))
    c.commit(); cur.close(); c.close()
    return {"id": tid, "status": "created"}

@app.get("/api/tasks")
def get_tasks(employee_id: Optional[str]=None, client_id: Optional[str]=None, status: Optional[str]=None):
    c = get_db(); cur = dict_cursor(c)
    where, params = [], []
    if employee_id: where.append("t.employee_id=%s"); params.append(employee_id)
    if client_id: where.append("t.client_id=%s"); params.append(client_id)
    if status: where.append("t.status=%s"); params.append(status)
    w = "WHERE " + " AND ".join(where) if where else ""
    cur.execute(f"SELECT t.*, e.name employee_name, e.role employee_role FROM tasks t LEFT JOIN employees e ON e.id=t.employee_id {w} ORDER BY t.created_at DESC", params)
    data = [dict(r) for r in cur.fetchall()]
    cur.close(); c.close()
    return {"tasks": data}

@app.post("/api/tasks/{task_id}/complete")
def complete_task(task_id: str, payload: dict):
    c = get_db(); cur = dict_cursor(c)
    cur.execute("SELECT * FROM tasks WHERE id=%s", (task_id,))
    task = cur.fetchone()
    if not task: raise HTTPException(404, "Task not found")
    cur.execute("UPDATE tasks SET status='completed', completed_at=NOW() WHERE id=%s", (task_id,))
    eid = str(uuid.uuid4()); roi = float(payload.get("roi_realized", 0))
    cur.execute("INSERT INTO task_executions(id,task_id,client_id,employee_id,result_summary,result_data,time_saved_minutes,roi_realized,executed_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,NOW())", (eid, task_id, task["client_id"], task["employee_id"], payload.get("result_summary",""), json.dumps(payload.get("result_data",{})), payload.get("time_saved_minutes",0), roi))
    cur.execute("UPDATE clients SET tasks_completed=tasks_completed+1, roi_realized=roi_realized+%s, updated_at=NOW() WHERE id=%s", (roi, task["client_id"]))
    c.commit(); cur.close(); c.close()
    return {"status": "completed", "execution_id": eid}

@app.post("/api/agents")
def register_agent(payload: dict):
    c = get_db(); cur = c.cursor(); aid = str(uuid.uuid4())
    cur.execute("INSERT INTO agents(id,client_id,name,description,category,status,runs_total,success_rate,created_at) VALUES(%s,%s,%s,%s,%s,'active',0,100.0,NOW())", (aid, payload.get("client_id"), payload["name"], payload.get("description",""), payload.get("category","general")))
    c.commit(); cur.close(); c.close()
    return {"id": aid}

@app.post("/api/agents/{agent_id}/ping")
def ping_agent(agent_id: str, payload: dict = {}):
    c = get_db(); cur = c.cursor()
    s = 100 if payload.get("success", True) else 0
    cur.execute("UPDATE agents SET runs_total=runs_total+1, last_run=NOW(), success_rate=(success_rate*runs_total+%s)/(runs_total+1) WHERE id=%s", (s, agent_id))
    c.commit(); cur.close(); c.close()
    return {"status": "ok"}
