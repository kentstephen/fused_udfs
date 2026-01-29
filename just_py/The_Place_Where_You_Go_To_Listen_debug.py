@fused.udf(cache_max_age="0s")
def udf(location: str = "New York", format: str = "page"):
    """
    Returns HTML page OR JSON data for continuous updates.
    Lightweight audio version - mobile browser compatible.
    """
    import pandas as pd
    import requests
    from datetime import datetime, timezone
    import math
    import json
    
    locations = {
        "New York": (40.7128, -74.0060),
        "California": (36.7783, -119.4179),
        "Alaska": (64.2008, -149.4937),
        "Texas": (31.9686, -99.9018),
        "Hawaii": (19.8968, -155.5828),
        "Colorado": (39.5501, -105.7821),
        "British Columbia": (53.7267, -127.6476),
        "Mexico City": (19.4326, -99.1332),
        "Iceland": (64.9631, -19.0208),
        "Norway": (60.4720, 8.4689),
        "Scotland": (56.4907, -4.2026),
        "Italy": (41.8719, 12.5674),
        "Greece": (39.0742, 21.8243),
        "Japan": (36.2048, 138.2529),
        "New Zealand": (-40.9006, 174.8860),
        "Indonesia": (-0.7893, 113.9213),
        "Philippines": (12.8797, 121.7740),
        "Chile": (-35.6751, -71.5430),
        "Peru": (-9.1900, -75.0152),
        "Antarctica": (-77.8463, 166.6686),
    }
    
    LAT, LON = locations.get(location, (40.7128, -74.0060))
    
    data = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'location': location,
        'lat': LAT,
        'lon': LON,
        'seismic_local': 0.0,
        'seismic_regional': 0.0,
        'seismic_global': 0.0,
        'aurora_intensity': 0.0,
        'aurora_kp': 0.0,
        'solar_brightness': 0.0,
        'solar_position': 0.0,
        'seasonal_position': 0.0
    }
    
    # SEISMIC
    try:
        r = requests.get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson", timeout=10)
        if r.status_code == 200:
            quakes = r.json()['features']
            local_quakes = []
            regional_quakes = []
            global_max = 0
            
            for q in quakes:
                coords = q['geometry']['coordinates']
                q_lon, q_lat = coords[0], coords[1]
                mag = q['properties']['mag']
                
                if mag and mag > global_max:
                    global_max = mag
                
                dist = math.sqrt((LAT - q_lat)**2 + (LON - q_lon)**2) * 111
                
                if dist <= 500 and mag:
                    local_quakes.append(mag)
                elif dist <= 2000 and mag:
                    regional_quakes.append(mag)
            
            if local_quakes:
                data['seismic_local'] = min(sum(local_quakes) / len(local_quakes) / 5.0, 1.0)
            if regional_quakes:
                data['seismic_regional'] = min(sum(regional_quakes) / len(regional_quakes) / 6.0, 1.0)
            data['seismic_global'] = min(global_max / 8.0, 1.0)
    except Exception as e:
        print(f"Seismic error: {e}")
    
    # AURORA
    try:
        r = requests.get("https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json", timeout=5)
        if r.status_code == 200 and len(r.json()) > 1:
            kp = float(r.json()[-1][1])
            data['aurora_kp'] = kp
            aurora_potential = 0
            if abs(LAT) > 50:
                aurora_potential = min((abs(LAT) - 50) / 40, 1.0)
            data['aurora_intensity'] = (kp / 9.0) * max(aurora_potential, 0.2)
    except:
        pass
    
    # SOLAR
    now = datetime.now(timezone.utc)
    hour = now.hour + now.minute / 60.0
    day_of_year = now.timetuple().tm_yday
    data['solar_position'] = hour / 24.0
    data['seasonal_position'] = day_of_year / 365.0
    
    hour_angle = (hour - 12) * 15
    decl = 23.45 * math.sin(math.radians((360/365) * (day_of_year - 81)))
    try:
        elev = math.asin(
            math.sin(math.radians(LAT)) * math.sin(math.radians(decl)) +
            math.cos(math.radians(LAT)) * math.cos(math.radians(decl)) * math.cos(math.radians(hour_angle))
        )
        data['solar_brightness'] = max(0, min(math.degrees(elev) / 90.0, 1.0))
    except:
        data['solar_brightness'] = 0
    
    # Calculate Turrell-style gradient colors server-side
    # Inner glow (warm, glowing center)
    innerR = min(255, int(180 + data['solar_brightness'] * 75 + data['seismic_local'] * 60))
    innerG = min(255, int(80 + data['solar_brightness'] * 100 + data['seismic_regional'] * 80))
    innerB = min(255, int(100 + data['aurora_intensity'] * 80 + data['solar_brightness'] * 30))
    
    # Outer color (deeper warm tones)
    outerR = min(255, int(120 + data['seismic_local'] * 100 + data['seismic_global'] * 80))
    outerG = min(255, int(40 + data['seismic_regional'] * 60 + data['aurora_intensity'] * 50))
    outerB = min(255, int(80 + data['aurora_intensity'] * 100 + data['seismic_global'] * 60))
    
    # Pre-compute the entire CSS gradient string
    data['backgroundGradient'] = f'radial-gradient(circle at center, rgb({innerR}, {innerG}, {innerB}) 0%, rgb({outerR}, {outerG}, {outerB}) 70%)'
    
    if format == "data":
        return pd.DataFrame([data])
    
    location_options = "\\n".join([f'<option value="{loc}" {"selected" if loc == location else ""}>{loc}</option>' for loc in sorted(locations.keys())])
    
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>The Place Where You Go to Listen</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, sans-serif; background: #000; color: rgba(255,255,255,0.7); min-height: 100vh; overflow: hidden; }}
#visual-field {{ position: fixed; top: 0; left: 0; width: 100%; height: 100%; z-index: 1; background: {data['backgroundGradient']}; }}
#ui {{ position: fixed; top: 20px; left: 20px; right: 20px; z-index: 10; display: flex; justify-content: space-between; align-items: flex-start; }}
.title-block {{ text-align: left; }}
h1 {{ font-size: 1.2em; font-weight: 400; color: rgba(255,255,255,0.8); margin-bottom: 5px; letter-spacing: 0.5px; }}
.credit {{ font-size: 0.7em; color: rgba(255,255,255,0.4); font-style: italic; }}
.credit a {{ color: rgba(255,255,255,0.5); text-decoration: none; border-bottom: 1px solid transparent; transition: all 0.3s; }}
.credit a:hover {{ color: rgba(255,255,255,0.8); border-bottom-color: rgba(255,255,255,0.8); }}
.controls {{ display: flex; gap: 15px; align-items: center; }}
select {{ padding: 8px 25px 8px 12px; font-size: 0.85em; background: rgba(0,0,0,0.5); border: 1px solid rgba(255,255,255,0.2); border-radius: 3px; color: rgba(255,255,255,0.8); cursor: pointer; outline: none; appearance: none; background-image: url('data:image/svg+xml;charset=UTF-8,<svg width="10" height="6" viewBox="0 0 10 6" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M1 1L5 5L9 1" stroke="rgba(255,255,255,0.5)" stroke-width="1.5" stroke-linecap="round"/></svg>'); background-repeat: no-repeat; background-position: right 8px center; }}
select:hover {{ background: rgba(0,0,0,0.7); border-color: rgba(255,255,255,0.4); }}
select:disabled {{ opacity: 0.4; cursor: not-allowed; border-color: rgba(255,255,255,0.1); }}
select:disabled:hover {{ background: rgba(0,0,0,0.5); border-color: rgba(255,255,255,0.1); }}
select option {{ background: #000; }}
button {{ padding: 8px 20px; font-size: 0.85em; background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.3); border-radius: 3px; color: rgba(255,255,255,0.8); cursor: pointer; transition: all 0.3s; }}
button:hover {{ background: rgba(255,255,255,0.2); border-color: rgba(255,255,255,0.5); }}
button:disabled {{ opacity: 0.3; cursor: not-allowed; background: rgba(255,255,255,0.05); }}
button:disabled:hover {{ background: rgba(255,255,255,0.05); border-color: rgba(255,255,255,0.3); }}
#data-panel {{ position: fixed; bottom: 20px; left: 20px; background: rgba(0,0,0,0.6); border: 1px solid rgba(255,255,255,0.15); border-radius: 3px; padding: 12px 15px; z-index: 10; min-width: 240px; }}
#data-toggle {{ cursor: pointer; font-size: 0.75em; color: rgba(255,255,255,0.6); user-select: none; display: flex; align-items: center; gap: 5px; }}
#data-toggle:hover {{ color: rgba(255,255,255,0.9); }}
#data-content {{ margin-top: 10px; font-size: 0.75em; }}
#data-content.collapsed {{ display: none; }}
.data-row {{ display: grid; grid-template-columns: auto 1fr auto; gap: 8px; align-items: baseline; margin: 8px 0; color: rgba(255,255,255,0.5); }}
.data-label {{ color: rgba(255,255,255,0.4); white-space: nowrap; }}
.data-value {{ color: rgba(255,255,255,0.7); font-variant-numeric: tabular-nums; text-align: right; }}
.data-unit {{ color: rgba(255,255,255,0.3); font-size: 0.9em; }}
#info-toggle {{ cursor: pointer; font-size: 0.7em; color: rgba(255,255,255,0.5); user-select: none; margin-top: 10px; padding-top: 8px; border-top: 1px solid rgba(255,255,255,0.1); }}
#info-toggle:hover {{ color: rgba(255,255,255,0.9); }}
#info-content {{ margin-top: 8px; font-size: 0.68em; line-height: 1.5; color: rgba(255,255,255,0.4); }}
#info-content.collapsed {{ display: none; }}
#status {{ position: fixed; bottom: 20px; right: 20px; font-size: 0.7em; color: rgba(255,255,255,0.3); z-index: 10; text-align: right; }}
#fused-credit {{ position: fixed; bottom: 20px; right: 20px; font-size: 0.7em; color: rgba(255,255,255,0.3); z-index: 10; }}
#fused-credit a {{ color: rgba(255,255,255,0.4); text-decoration: none; }}
#fused-credit a:hover {{ color: rgba(255,255,255,0.6); }}
</style>
</head>
<body>
<div id="visual-field"></div>
<div id="ui">
<div class="title-block">
<h1>The Place Where You Go to Listen</h1>
<p class="credit" style="font-style: normal; margin-bottom: 3px;">Real-time sonification of seismic, aurora, and solar data</p>
<p class="credit">Inspired by <a href="https://www.johnlutheradams.net/" target="_blank">John Luther Adams</a> • <a href="https://www.uaf.edu/museum/exhibits/galleries/the-place-where-you-go-to/" target="_blank">Original Installation</a></p>
</div>
<div class="controls">
<select id="location-select">{location_options}</select>
<button id="startBtn">Listen</button>
</div>
</div>
<div id="data-panel">
<div id="data-toggle">▼ Data</div>
<div id="data-content">
<div class="data-row">
<span class="data-label">Local Seismic</span>
<span class="data-value" id="local-val">0.00</span>
<span class="data-unit">norm</span>
</div>
<div class="data-row">
<span class="data-label">Regional Seismic</span>
<span class="data-value" id="regional-val">0.00</span>
<span class="data-unit">norm</span>
</div>
<div class="data-row">
<span class="data-label">Global Seismic</span>
<span class="data-value" id="global-val">0.00</span>
<span class="data-unit">norm</span>
</div>
<div class="data-row">
<span class="data-label">Aurora</span>
<span class="data-value" id="aurora-val">0.00</span>
<span class="data-unit">norm</span>
</div>
<div class="data-row">
<span class="data-label">Solar</span>
<span class="data-value" id="solar-val">0.00</span>
<span class="data-unit">norm</span>
</div>
<div id="info-toggle">▶ About these values</div>
<div id="info-content" class="collapsed">
<strong>All values normalized 0-1:</strong><br>
<strong>Local:</strong> Avg quake magnitude within 500km ÷ 5<br>
<strong>Regional:</strong> Avg magnitude within 2000km ÷ 6<br>
<strong>Global:</strong> Largest quake today ÷ 8<br>
<strong>Aurora:</strong> Kp index (0-9) × latitude factor<br>
<strong>Solar:</strong> Sun elevation angle ÷ 90°<br><br>
<strong>Data sources:</strong><br>
Seismic: USGS Earthquake Hazards Program<br>
Aurora: NOAA Space Weather Prediction Center
</div>
</div>
</div>
<div id="status"></div>
<div id="fused-credit">Made with <a href="https://www.fused.io" target="_blank">Fused.io</a></div>
<script>
console.log('Script starting...');
let data = {json.dumps(data)};
let audioCtx, playing = false;
let mainOsc, auroraOsc, solarOsc;
let mainGain, auroraGain, solarGain, master;
let audioInitialized = false;
let silentAudio = null;

const locationSelect = document.getElementById('location-select');
const startBtn = document.getElementById('startBtn');

locationSelect.addEventListener('change', function() {{
console.log('Location change triggered, playing:', playing);
const newLocation = this.value;
const baseUrl = window.location.href.split('?')[0];
window.location.href = baseUrl + '?dtype_out_vector=html&location=' + encodeURIComponent(newLocation);
}});

const canvas = document.getElementById('visualizer');
const ctx = canvas.getContext('2d');

function resizeCanvas() {{
canvas.width = window.innerWidth;
canvas.height = window.innerHeight;
drawColorFields();
}}
resizeCanvas();
window.addEventListener('resize', resizeCanvas);

function drawColorFields() {{
const w = canvas.width;
const h = canvas.height;

// Colors pre-calculated server-side
const gradient = ctx.createRadialGradient(w/2, h/2, 0, w/2, h/2, Math.max(w, h) * 0.7);
gradient.addColorStop(0, data.innerColor);
gradient.addColorStop(1, data.outerColor);

ctx.fillStyle = gradient;
ctx.fillRect(0, 0, w, h);
}}

document.addEventListener('visibilitychange', async () => {{
if (audioCtx && playing && !document.hidden && audioCtx.state === 'suspended') {{
await audioCtx.resume();
if (silentAudio && silentAudio.paused) {{
silentAudio.play().catch(() => {{}});
}}
}}
}});

startBtn.onclick = async () => {{
console.log('Start button clicked, playing:', playing);
if (!playing) {{
if (!audioInitialized) {{
await initAudio();
audioInitialized = true;
}}
if (audioCtx && audioCtx.state === 'suspended') {{
await audioCtx.resume();
}}
if (silentAudio) {{
silentAudio.play().catch(() => {{}});
}}
if (master) {{
master.gain.setValueAtTime(0.15, audioCtx.currentTime);
}}
updateSound(data);
drawColorFields();
playing = true;
startBtn.textContent = 'Stop';
locationSelect.disabled = true;
console.log('Location select disabled:', locationSelect.disabled);
}} else {{
// Fade out each gain node smoothly
if (audioCtx) {{
const t = audioCtx.currentTime;
if (mainGain) mainGain.gain.linearRampToValueAtTime(0, t + 0.5);
if (auroraGain) auroraGain.gain.linearRampToValueAtTime(0, t + 0.5);
if (solarGain) solarGain.gain.linearRampToValueAtTime(0, t + 0.5);
if (master) master.gain.linearRampToValueAtTime(0, t + 0.5);
await new Promise(resolve => setTimeout(resolve, 500));
}}
if (silentAudio && !silentAudio.paused) {{
silentAudio.pause();
}}
if (audioCtx && audioCtx.state === 'running') {{
await audioCtx.suspend();
}}
playing = false;
startBtn.textContent = 'Listen';
locationSelect.disabled = false;
console.log('Location select enabled:', locationSelect.disabled);
document.getElementById('status').textContent = '';
}}
}};

document.getElementById('data-toggle').onclick = () => {{
const content = document.getElementById('data-content');
const toggle = document.getElementById('data-toggle');
content.classList.toggle('collapsed');
toggle.textContent = content.classList.contains('collapsed') ? '▶ Data' : '▼ Data';
}};

document.getElementById('info-toggle').onclick = () => {{
const content = document.getElementById('info-content');
const toggle = document.getElementById('info-toggle');
content.classList.toggle('collapsed');
toggle.textContent = content.classList.contains('collapsed') ? '▶ About these values' : '▼ About these values';
}};

function updateUI(d) {{
document.getElementById('local-val').textContent = d.seismic_local.toFixed(2);
document.getElementById('regional-val').textContent = d.seismic_regional.toFixed(2);
document.getElementById('global-val').textContent = d.seismic_global.toFixed(2);
document.getElementById('aurora-val').textContent = d.aurora_intensity.toFixed(2);
document.getElementById('solar-val').textContent = d.solar_brightness.toFixed(2);
}}

updateUI(data);

async function initAudio() {{
audioCtx = new (window.AudioContext || window.webkitAudioContext)();
master = audioCtx.createGain();
master.gain.value = 0.35; // Boosted from 0.15
master.connect(audioCtx.destination);

silentAudio = document.createElement('audio');
silentAudio.src = 'data:audio/wav;base64,UklGRiQAAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQAAAAA=';
silentAudio.loop = true;
silentAudio.volume = 0.01;
silentAudio.setAttribute('playsinline', '');
silentAudio.setAttribute('webkit-playsinline', '');
document.body.appendChild(silentAudio);

// MAIN SEISMIC DRONE
mainOsc = audioCtx.createOscillator();
mainOsc.type = 'sine';
mainOsc.frequency.value = 80;
mainGain = audioCtx.createGain();
mainGain.gain.value = 0;
mainOsc.connect(mainGain).connect(master);
mainOsc.start();

// AURORA
auroraOsc = audioCtx.createOscillator();
auroraOsc.type = 'sine';
auroraOsc.frequency.value = 523.25;
auroraGain = audioCtx.createGain();
auroraGain.gain.value = 0;
auroraOsc.connect(auroraGain).connect(master);
auroraOsc.start();

// SOLAR NOISE with filter
const noiseFilter = audioCtx.createBiquadFilter();
noiseFilter.type = 'bandpass';
noiseFilter.frequency.value = 800;
noiseFilter.Q.value = 1;
solarGain = audioCtx.createGain();
solarGain.gain.value = 0;

const bufferSize = audioCtx.sampleRate * 0.5;
const buffer = audioCtx.createBuffer(1, bufferSize, audioCtx.sampleRate);
const bufferData = buffer.getChannelData(0);
for (let i = 0; i < bufferSize; i++) {{
bufferData[i] = Math.random() * 2 - 1;
}}

solarOsc = audioCtx.createBufferSource();
solarOsc.buffer = buffer;
solarOsc.loop = true;
solarOsc.connect(noiseFilter).connect(solarGain).connect(master);
solarOsc.start();

// Store filter for updates
window.solarFilter = noiseFilter;

if (audioCtx.state === 'suspended') {{
await audioCtx.resume();
}}
}}

function updateSound(d) {{
if (!audioCtx) return;

// Use pre-calculated values directly, minimal browser-side calculation
mainOsc.frequency.value = 60 + (d.seismic_local + d.seismic_regional * 0.5 + d.seismic_global * 0.3) * 80;
mainGain.gain.value = 0.1 + (d.seismic_local + d.seismic_regional * 0.5 + d.seismic_global * 0.3) * 0.15;

auroraGain.gain.value = d.aurora_intensity * 0.1;

window.solarFilter.frequency.value = 500 + d.solar_brightness * 1500;
solarGain.gain.value = d.solar_brightness * 0.03;
}}
</script>
</body>
</html>"""
    
    common = fused.load("https://github.com/fusedio/udfs/tree/abf9c87/public/common/")
    return common.html_to_obj(html)