"""
Network Signal Monitoring and Analysis System
Run:   pip install flask
       python network_monitor.py
Open:  http://127.0.0.1:5000/dashboard
"""
import os
import socket
import sqlite3
import statistics
import threading
import time
import urllib.request
from datetime import datetime

from flask import Flask, jsonify

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'network_monitor.db')
LOCK = threading.Lock()


# ---------------- DATABASE ----------------
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    c = db()
    c.execute('''CREATE TABLE IF NOT EXISTS measurements(
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
        latency_ms REAL, packet_loss_pct REAL, jitter_ms REAL,
        download_mbps REAL, upload_mbps REAL, score INTEGER,
        classification TEXT, stability_status TEXT,
        detected_problem TEXT, recommendation TEXT)''')
    c.commit()
    c.close()


def history(limit=100):
    c = db()
    rows = c.execute('SELECT * FROM measurements ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
    c.close()
    return [dict(x) for x in rows]


def save(m, r):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    c = db()
    cur = c.execute('''INSERT INTO measurements
        (timestamp,latency_ms,packet_loss_pct,jitter_ms,download_mbps,upload_mbps,score,classification)
        VALUES(?,?,?,?,?,?,?,?)''',
                    (ts, m['latency_ms'], m['packet_loss_pct'], m['jitter_ms'],
                     m['download_mbps'], m['upload_mbps'], r['score'], r['classification']))
    c.commit()
    mid = cur.lastrowid
    c.close()
    return mid, ts


def update_analysis(mid, stability_status, probs, recs):
    p = '; '.join(x['name'] for x in probs) or 'None detected'
    r = '; '.join(x['action'] for x in recs) or 'None'
    c = db()
    c.execute('UPDATE measurements SET stability_status=?,detected_problem=?,recommendation=? WHERE id=?',
              (stability_status, p, r, mid))
    c.commit()
    c.close()


init_db()

# ---------------- REAL MEASUREMENT ----------------
PING_HOST = '1.1.1.1'
PING_PORT = 443
PING_COUNT = 6
PING_TIMEOUT = 2
DOWNLOAD_URL = 'https://speed.cloudflare.com/__down?bytes=10000000'   # 10 MB
UPLOAD_URL = 'https://speed.cloudflare.com/__up'
UPLOAD_BYTES = 2000000                                                # 2 MB
HEADERS = {'User-Agent': 'Mozilla/5.0 NetworkMonitorMiniProject'}


def latency_test():
    """TCP connect time to PING_HOST. 'Loss' = failed connection attempts."""
    vals = []
    lost = 0
    for _ in range(PING_COUNT):
        s = None
        start = time.perf_counter()
        try:
            s = socket.create_connection((PING_HOST, PING_PORT), timeout=PING_TIMEOUT)
            vals.append((time.perf_counter() - start) * 1000)
        except OSError:
            lost += 1
        finally:
            if s:
                try:
                    s.close()
                except OSError:
                    pass
        time.sleep(.15)
    loss = lost / PING_COUNT * 100
    if not vals:
        return {'latency_ms': None, 'packet_loss_pct': 100.0, 'jitter_ms': None}
    jitter = (statistics.mean([abs(vals[i] - vals[i - 1]) for i in range(1, len(vals))])
              if len(vals) > 1 else None)
    return {'latency_ms': round(statistics.mean(vals), 2),
            'packet_loss_pct': round(loss, 1),
            'jitter_ms': round(jitter, 2) if jitter is not None else None}


def download_test():
    try:
        req = urllib.request.Request(DOWNLOAD_URL, headers=HEADERS)
        total = 0
        start = time.perf_counter()
        with urllib.request.urlopen(req, timeout=20) as r:
            while True:
                b = r.read(65536)
                if not b:
                    break
                total += len(b)
        elapsed = time.perf_counter() - start
        return round(total * 8 / elapsed / 1e6, 2) if total and elapsed > 0 else None
    except Exception:
        return None


def upload_test():
    try:
        data = os.urandom(UPLOAD_BYTES)
        h = dict(HEADERS)
        h['Content-Type'] = 'application/octet-stream'
        req = urllib.request.Request(UPLOAD_URL, data=data, method='POST', headers=h)
        start = time.perf_counter()
        with urllib.request.urlopen(req, timeout=20) as r:
            r.read()
        elapsed = time.perf_counter() - start
        return round(len(data) * 8 / elapsed / 1e6, 2) if elapsed > 0 else None
    except Exception:
        return None


def measure():
    m = {'latency_ms': None, 'packet_loss_pct': None, 'jitter_ms': None,
         'download_mbps': None, 'upload_mbps': None}
    try:
        m.update(latency_test())
    except Exception:
        pass
    m['download_mbps'] = download_test()
    m['upload_mbps'] = upload_test()
    return m


# ---------------- QUALITY SCORE ----------------
WEIGHTS = {'latency': 25, 'packet_loss': 30, 'download': 20, 'upload': 10, 'stability': 15}
CURVES = {
    'latency': [(20, 100), (50, 80), (100, 60), (200, 30), (400, 0)],
    'packet_loss': [(0, 100), (1, 80), (2.5, 60), (5, 30), (10, 0)],
    'download': [(0, 0), (1, 10), (5, 40), (10, 60), (25, 80), (50, 100)],
    'upload': [(0, 0), (.5, 10), (2, 40), (5, 60), (10, 80), (20, 100)],
    'stability': [(5, 100), (10, 80), (20, 60), (50, 30), (100, 0)],
}
PARAMS = {
    'latency': ('Latency', 'ms', 'latency_ms'),
    'packet_loss': ('Connection Failure Rate', '%', 'packet_loss_pct'),
    'download': ('Download', 'Mbps', 'download_mbps'),
    'upload': ('Upload', 'Mbps', 'upload_mbps'),
    'stability': ('Jitter', 'ms', 'jitter_ms'),
}


def score_class(s):
    return ('Excellent' if s >= 85 else 'Good' if s >= 70 else 'Fair' if s >= 50
            else 'Poor' if s >= 30 else 'Critical')


def interp(v, p):
    if v <= p[0][0]:
        return p[0][1]
    if v >= p[-1][0]:
        return p[-1][1]
    for (x1, y1), (x2, y2) in zip(p, p[1:]):
        if x1 <= v <= x2:
            return y1 + (y2 - y1) * (v - x1) / (x2 - x1)
    return 0


def quality(m):
    a = {}
    for n, (_, _, k) in PARAMS.items():
        try:
            v = float(m[k])
        except (TypeError, ValueError):
            continue
        if v >= 0:
            a[n] = v
    if len(a) < 2:
        return None
    tw = sum(WEIGHTS[n] for n in a)
    ps = []
    total = 0
    for n, (label, unit, _) in PARAMS.items():
        if n not in a:
            ps.append({'name': label, 'value': None, 'unit': unit,
                       'sub_score': None, 'status': 'Not available'})
            continue
        sub = interp(a[n], CURVES[n])
        ew = WEIGHTS[n] / tw * 100
        total += sub * ew / 100
        ps.append({'name': label, 'value': round(a[n], 2), 'unit': unit,
                   'sub_score': round(sub), 'status': score_class(sub)})
    s = round(total)
    return {'score': s, 'classification': score_class(s), 'raw_score': round(total, 1), 'parameters': ps}


# ---------------- ANALYSIS ----------------
def f(v):
    try:
        x = float(v)
        return x if x >= 0 else None
    except (TypeError, ValueError):
        return None


LEVEL = {'Stable': 0, 'Fluctuating': 1, 'Unstable': 2}


def stability(rows):
    if len(rows) < 3:
        return {'status': 'Not enough data', 'message': 'At least 3 measurements are needed.', 'parameters': []}
    out = []
    levels = []
    for key, label, unit in [('latency_ms', 'Latency', 'ms'),
                             ('download_mbps', 'Download', 'Mbps'),
                             ('upload_mbps', 'Upload', 'Mbps')]:
        vals = [f(x.get(key)) for x in rows]
        vals = [x for x in vals if x is not None]
        if len(vals) < 3:
            continue
        avg = statistics.mean(vals)
        var = statistics.pstdev(vals) / avg * 100 if avg else 0
        st = 'Stable' if var <= 25 else 'Fluctuating' if var <= 50 else 'Unstable'
        levels.append(LEVEL[st])
        out.append({'name': label, 'status': st, 'average': round(avg, 2), 'minimum': round(min(vals), 2),
                    'maximum': round(max(vals), 2), 'variation': round(var, 1), 'unit': unit})
    loss = [f(x.get('packet_loss_pct')) for x in rows]
    loss = [x for x in loss if x is not None]
    if loss:
        if max(loss) >= 20 or sum(x > 0 for x in loss) / len(loss) >= .4:
            st = 'Unstable'
        elif any(x > 0 for x in loss):
            st = 'Fluctuating'
        else:
            st = 'Stable'
        levels.append(LEVEL[st])
        out.append({'name': 'Connection Failure Rate', 'status': st, 'average': round(statistics.mean(loss), 2),
                    'minimum': round(min(loss), 2), 'maximum': round(max(loss), 2),
                    'variation': None, 'unit': '%'})
    lev = max(levels) if levels else 1
    return {'status': ['Stable', 'Fluctuating', 'Unstable'][lev],
            'message': 'Based on variation in recent saved measurements.', 'parameters': out}


def problems(m, r):
    p = []
    lat, loss, jit = f(m['latency_ms']), f(m['packet_loss_pct']), f(m['jitter_ms'])
    dl, ul = f(m['download_mbps']), f(m['upload_mbps'])
    if lat is not None and lat > 200:
        p.append({'name': 'High latency', 'result': f"{lat:.1f} ms"})
    if loss is not None and loss >= 5:
        p.append({'name': 'Packet loss', 'result': f"{loss:.1f}%"})
    if jit is not None and jit > 50:
        p.append({'name': 'High jitter', 'result': f"{jit:.1f} ms"})
    if dl is not None and dl < 5:
        p.append({'name': 'Low download speed', 'result': f"{dl:.2f} Mbps"})
    if ul is not None and ul < 2:
        p.append({'name': 'Low upload speed', 'result': f"{ul:.2f} Mbps"})
    if r and r['score'] < 50:
        p.append({'name': 'Low overall network quality', 'result': f"Score {r['score']}/100"})
    return p


def recommendations(ps):
    if not ps:
        return [{'title': 'Network looks normal', 'action': 'Continue monitoring to collect history.'}]
    rec = []
    for p in ps:
        n = p['name']
        if n == 'High latency':
            a = 'Move closer to the router and reduce background traffic.'
        elif n == 'Packet loss':
            a = 'Check Wi-Fi signal, router load, cables and other devices.'
        elif n == 'High jitter':
            a = 'Run several measurements and check whether variation continues.'
        elif n == 'Low download speed':
            a = 'Pause large downloads and test the connection again.'
        elif n == 'Low upload speed':
            a = 'Stop background uploads and test again.'
        else:
            a = 'Continue monitoring and inspect the measurements.'
        rec.append({'title': n, 'action': a})
    return rec


def degradation():
    rows = list(reversed(history(6)))
    if len(rows) < 2:
        return {'degraded': False, 'message': 'Need at least two saved measurements.'}
    latest = rows[-1]
    old = rows[:-1]
    warnings = []

    def avg(k):
        v = [f(x.get(k)) for x in old]
        v = [x for x in v if x is not None]
        return statistics.mean(v) if v else None

    la, oa = f(latest.get('latency_ms')), avg('latency_ms')
    ld, od = f(latest.get('download_mbps')), avg('download_mbps')
    ll, ol = f(latest.get('packet_loss_pct')), avg('packet_loss_pct')
    if la is not None and oa and la > oa * 1.5:
        warnings.append('Latency increased significantly.')
    if ld is not None and od and ld < od * .7:
        warnings.append('Download speed decreased significantly.')
    if ll is not None and ol is not None and ll > ol + 5:
        warnings.append('Packet loss increased.')
    return {'degraded': bool(warnings),
            'message': ' '.join(warnings) if warnings else 'No significant degradation detected.'}


# ---------------- HTML ----------------
HTML = '''<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Network Signal Monitoring and Analysis System</title>
<style>
body{margin:0;font-family:Arial,sans-serif;background:#eef2f7;color:#172033}
nav{background:#111827;padding:18px 24px}
nav b{color:#fff;font-size:21px;display:block;margin-bottom:12px}
nav a{color:#cbd5e1;text-decoration:none;margin-right:16px}
nav a:hover{color:#fff}
.wrap{max-width:1150px;margin:24px auto;padding:0 15px}
.card{background:#fff;border-radius:12px;padding:20px;margin-bottom:16px;box-shadow:0 2px 10px #0001}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:16px}
.metric{background:#fff;padding:16px;border-radius:10px;box-shadow:0 2px 10px #0001}
.metric small{display:block;color:#64748b}
.metric strong{font-size:23px}
button,select{padding:10px 14px;border-radius:8px;border:0;font-size:14px}
button{background:#111827;color:#fff;cursor:pointer;margin-right:8px}
button:disabled{opacity:.5;cursor:not-allowed}
table{width:100%;border-collapse:collapse}
th,td{padding:9px;border-bottom:1px solid #ddd;text-align:left}
.score{font-size:46px;font-weight:bold}
.small{color:#64748b}
canvas{width:100%;max-width:1000px;height:auto;background:#f8fafc;border-radius:8px;margin-bottom:12px}
.tag{display:inline-block;padding:2px 10px;border-radius:12px;background:#e2e8f0;font-size:13px}
</style></head><body>
<nav><b>Network Signal Monitoring and Analysis System</b>
<a href="/dashboard">Dashboard</a><a href="/monitoring">Live Monitoring</a><a href="/graphs">Performance Graphs</a>
<a href="/history">History</a><a href="/analysis">Analysis</a><a href="/reports">Reports</a></nav>
<div class="wrap">@@BODY@@</div>
<script>
function $(id){return document.getElementById(id)}
function v(x,u){return (x===null||x===undefined)?'--':Number(x).toFixed(2)+' '+u}

async function runMeasure(){
  let r=await fetch('/api/measure',{method:'POST',cache:'no-store'});
  let d=await r.json();
  if(!r.ok||!d.ok) throw new Error(d.error||'Measurement failed');
  return d;
}

async function measureNow(){
  let b=$('measure'); if(!b) return;
  b.disabled=true;
  $('msg').textContent='Measuring... please wait about 10-30 seconds.';
  try{
    let d=await runMeasure(); show(d);
    $('msg').textContent='Measurement completed successfully.';
  }catch(e){ $('msg').textContent='ERROR: '+e.message; }
  finally{ b.disabled=false; }
}

function show(d){
  let m=d.metrics, r=d.score;
  $('status').textContent=m.latency_ms!==null?'Connected':'Measurement unavailable';
  $('lat').textContent=v(m.latency_ms,'ms');
  $('loss').textContent=v(m.packet_loss_pct,'%');
  $('down').textContent=v(m.download_mbps,'Mbps');
  $('up').textContent=v(m.upload_mbps,'Mbps');
  $('jit').textContent=v(m.jitter_ms,'ms');
  $('score').textContent=r.score+'/100';
  $('class').textContent=r.classification;
  $('params').innerHTML='<table><tr><th>Parameter</th><th>Value</th><th>Sub-score</th><th>Status</th></tr>'+
    r.parameters.map(p=>'<tr><td>'+p.name+'</td><td>'+(p.value==null?'N/A':p.value+' '+p.unit)+'</td><td>'+
    (p.sub_score==null?'N/A':p.sub_score+'/100')+'</td><td>'+p.status+'</td></tr>').join('')+'</table>';
  $('problems').innerHTML=d.problems.length?d.problems.map(p=>'<p><b>'+p.name+'</b>: '+p.result+'</p>').join(''):'No major problem detected.';
  $('recs').innerHTML=d.recommendations.map(x=>'<p><b>'+x.title+'</b>: '+x.action+'</p>').join('');
  $('deg').textContent=d.degradation.message;
}

async function loadHistory(){
  let d=await (await fetch('/api/history')).json();
  $('history').innerHTML=d.history.length?
    '<table><tr><th>Time</th><th>Latency</th><th>Jitter</th><th>Failure rate</th><th>Download</th><th>Upload</th><th>Score</th><th>Class</th></tr>'+
    d.history.map(x=>'<tr><td>'+x.timestamp+'</td><td>'+v(x.latency_ms,'ms')+'</td><td>'+v(x.jitter_ms,'ms')+'</td><td>'+
    v(x.packet_loss_pct,'%')+'</td><td>'+v(x.download_mbps,'Mbps')+'</td><td>'+v(x.upload_mbps,'Mbps')+'</td><td>'+
    (x.score??'--')+'</td><td>'+(x.classification||'--')+'</td></tr>').join('')+'</table>'
    :'No measurements saved yet.';
}

async function loadAnalysis(){
  let d=await (await fetch('/api/analysis')).json();
  let deg=await (await fetch('/api/degradation')).json();
  $('analysis').innerHTML='<h3>Overall stability: '+d.status+'</h3><p>'+d.message+'</p>'+
    (d.parameters.length?'<table><tr><th>Parameter</th><th>Status</th><th>Average</th><th>Min</th><th>Max</th><th>Variation</th></tr>'+
    d.parameters.map(p=>'<tr><td>'+p.name+'</td><td>'+p.status+'</td><td>'+p.average+' '+p.unit+'</td><td>'+p.minimum+'</td><td>'+
    p.maximum+'</td><td>'+(p.variation==null?'--':p.variation+'%')+'</td></tr>').join('')+'</table>':'')+
    '<h3>Degradation check</h3><p>'+deg.message+'</p>';
}

async function loadLive(){
  let d=await (await fetch('/api/latest')).json();
  $('live').innerHTML=d.latest?
    '<h3>'+d.latest.timestamp+'</h3><p>Latency: '+v(d.latest.latency_ms,'ms')+'</p><p>Jitter: '+v(d.latest.jitter_ms,'ms')+
    '</p><p>Connection failure rate: '+v(d.latest.packet_loss_pct,'%')+'</p><p>Download: '+v(d.latest.download_mbps,'Mbps')+
    '</p><p>Upload: '+v(d.latest.upload_mbps,'Mbps')+'</p><p>Score: '+(d.latest.score??'--')+' ('+(d.latest.classification||'--')+')</p>'
    :'No measurement yet. Click "Start Auto Monitoring" or go to the Dashboard and click Measure Now.';
}

let autoTimer=null, autoBusy=false;
async function autoTick(){
  if(autoBusy) return; autoBusy=true;
  $('automsg').textContent='Measuring...';
  try{ await runMeasure(); $('automsg').textContent='Last run: '+new Date().toLocaleTimeString(); }
  catch(e){ $('automsg').textContent='ERROR: '+e.message; }
  autoBusy=false; loadLive();
}
function toggleAuto(){
  let b=$('autobtn');
  if(autoTimer){ clearInterval(autoTimer); autoTimer=null; b.textContent='Start Auto Monitoring (every 30 s)'; $('automsg').textContent='Stopped.'; }
  else{ autoTick(); autoTimer=setInterval(autoTick,30000); b.textContent='Stop Auto Monitoring'; }
}

async function loadReport(){
  let d=await (await fetch('/api/report')).json();
  $('report').innerHTML='<h3>Network Monitoring Report</h3><p>Total measurements: '+d.total+'</p><p>Average latency: '+(d.latency??'--')+
    ' ms</p><p>Average download: '+(d.download??'--')+' Mbps</p><p>Average upload: '+(d.upload??'--')+
    ' Mbps</p><p>Average connection failure rate: '+(d.loss??'--')+' %</p><p>Average score: '+(d.score??'--')+'</p>';
}

function drawChart(id,label,unit,data,color){
  let c=$(id); if(!c) return;
  let x=c.getContext('2d'), W=c.width, H=c.height, L=60, R=20, T=35, B=30;
  x.clearRect(0,0,W,H); x.font='14px Arial'; x.fillStyle='#172033'; x.fillText(label+' ('+unit+')',L,22);
  let a=data.filter(n=>n!==null&&Number.isFinite(n));
  if(!a.length){ x.fillStyle='#64748b'; x.fillText('No data yet.',L,H/2); return; }
  let mx=Math.max(...a,1)*1.1;
  x.strokeStyle='#cbd5e1'; x.fillStyle='#64748b'; x.font='12px Arial'; x.lineWidth=1;
  for(let i=0;i<=4;i++){
    let y=H-B-i*(H-T-B)/4; x.beginPath(); x.moveTo(L,y); x.lineTo(W-R,y); x.stroke();
    x.fillText((mx*i/4).toFixed(1),8,y+4);
  }
  x.strokeStyle=color; x.lineWidth=2; x.beginPath(); let started=false;
  data.forEach((n,i)=>{
    if(n===null||!Number.isFinite(n)) return;
    let X=L+i*(W-L-R)/Math.max(data.length-1,1), Y=H-B-n/mx*(H-T-B);
    if(!started){x.moveTo(X,Y);started=true}else x.lineTo(X,Y);
  });
  x.stroke(); x.fillStyle=color;
  data.forEach((n,i)=>{
    if(n===null||!Number.isFinite(n)) return;
    let X=L+i*(W-L-R)/Math.max(data.length-1,1), Y=H-B-n/mx*(H-T-B);
    x.beginPath(); x.arc(X,Y,3,0,6.283); x.fill();
  });
}

async function loadGraphs(){
  let d=await (await fetch('/api/history')).json();
  let h=d.history.slice().reverse();
  let col=k=>h.map(r=>(r[k]===null||r[k]===undefined)?null:Number(r[k]));
  drawChart('g_lat','Latency','ms',col('latency_ms'),'#2563eb');
  drawChart('g_down','Download','Mbps',col('download_mbps'),'#16a34a');
  drawChart('g_up','Upload','Mbps',col('upload_mbps'),'#d97706');
  drawChart('g_score','Quality score','/100',col('score'),'#7c3aed');
}

document.addEventListener('DOMContentLoaded',()=>{
  if($('history')) loadHistory();
  if($('analysis')) loadAnalysis();
  if($('live')){ loadLive(); setInterval(loadLive,5000); }
  if($('report')) loadReport();
  if($('g_lat')) loadGraphs();
});
</script></body></html>'''


def page(body):
    # str.replace (not %-formatting) so literal '%' characters in the template are safe
    return HTML.replace('@@BODY@@', body)


# ---------------- ROUTES ----------------
@app.route('/')
@app.route('/dashboard')
def dashboard():
    return page('''<div class="card"><h2>Dashboard</h2>
<p>Connection status: <b id="status">Not measured yet</b></p>
<button id="measure" onclick="measureNow()">Measure Now</button>
<p id="msg" class="small">Press Measure Now to run a real test.</p></div>
<div class="grid">
<div class="metric"><small>Latency</small><strong id="lat">--</strong></div>
<div class="metric"><small>Connection Failure Rate</small><strong id="loss">--</strong></div>
<div class="metric"><small>Download</small><strong id="down">--</strong></div>
<div class="metric"><small>Upload</small><strong id="up">--</strong></div>
<div class="metric"><small>Jitter</small><strong id="jit">--</strong></div></div>
<div class="card"><h2>Network Quality Score</h2><div id="score" class="score">--</div>
<p id="class">No data yet</p><div id="params">Run a measurement to see details.</div></div>
<div class="card"><h2>Problems Detected</h2><div id="problems">No measurement yet.</div></div>
<div class="card"><h2>Recommendations</h2><div id="recs">No measurement yet.</div></div>
<div class="card"><h2>Network Degradation</h2><div id="deg">No measurement yet.</div></div>''')


@app.route('/monitoring')
def monitoring():
    return page('''<div class="card"><h2>Live Monitoring</h2>
<button id="autobtn" onclick="toggleAuto()">Start Auto Monitoring (every 30 s)</button>
<span id="automsg" class="small">Not running. Keep this tab open while monitoring.</span></div>
<div class="card"><h3>Latest saved measurement (refreshes every 5 s)</h3><div id="live">Loading...</div></div>''')


@app.route('/history')
def history_page():
    return page('<div class="card"><h2>Measurement History</h2><div id="history">Loading...</div></div>')


@app.route('/analysis')
def analysis_page():
    return page('<div class="card"><h2>Network Analysis</h2><div id="analysis">Loading...</div></div>')


@app.route('/reports')
def reports():
    return page('<div class="card"><h2>Network Report</h2><div id="report">Loading...</div></div>')


@app.route('/graphs')
def graphs():
    return page('''<div class="card"><h2>Performance Graphs</h2>
<canvas id="g_lat" width="1000" height="260"></canvas>
<canvas id="g_down" width="1000" height="260"></canvas>
<canvas id="g_up" width="1000" height="260"></canvas>
<canvas id="g_score" width="1000" height="260"></canvas></div>''')


# ---------------- API ----------------
@app.route('/api/measure', methods=['POST'])
def api_measure():
    if not LOCK.acquire(False):
        return jsonify(ok=False, error='A measurement is already running. Please wait.'), 409
    try:
        m = measure()
        r = quality(m)
        if r is None:
            return jsonify(ok=False, metrics=m,
                           error='Not enough network data was measured. Check your internet connection and try again.'), 503
        mid, ts = save(m, r)
        st = stability(list(reversed(history(10))))
        ps = problems(m, r)
        rec = recommendations(ps)
        update_analysis(mid, st['status'], ps, rec)
        deg = degradation()
        return jsonify(ok=True, measurement_id=mid, timestamp=ts, metrics=m, score=r,
                       stability=st, problems=ps, recommendations=rec, degradation=deg)
    except Exception as e:
        app.logger.exception('Measurement error')
        return jsonify(ok=False, error='Measurement failed: ' + str(e)), 500
    finally:
        LOCK.release()


@app.route('/api/history')
def api_history():
    return jsonify(history=history())


@app.route('/api/latest')
def api_latest():
    h = history(1)
    return jsonify(latest=h[0] if h else None)


@app.route('/api/analysis')
def api_analysis():
    return jsonify(stability(list(reversed(history(10)))))


@app.route('/api/degradation')
def api_degradation():
    return jsonify(degradation())


@app.route('/api/report')
def api_report():
    h = history()

    def avg(k):
        a = [f(x.get(k)) for x in h]
        a = [x for x in a if x is not None]
        return round(statistics.mean(a), 2) if a else None

    return jsonify(total=len(h), latency=avg('latency_ms'), download=avg('download_mbps'),
                   upload=avg('upload_mbps'), loss=avg('packet_loss_pct'), score=avg('score'))


if __name__ == '__main__':
    print('\nNetwork Signal Monitoring and Analysis System')
    print('Open: http://127.0.0.1:5000/dashboard')
    print('Keep this terminal open. Press CTRL+C to stop.\n')
    app.run(host='127.0.0.1', port=5000, debug=False, threaded=True)
