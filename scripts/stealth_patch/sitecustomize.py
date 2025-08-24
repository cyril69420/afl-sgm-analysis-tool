import os, random
import undetected_chromedriver as uc
from selenium import webdriver

HEADFUL = os.getenv("ODDSPORTAL_HEADFUL", "1") == "1"
LANG = os.getenv("ODDSPORTAL_LANG", "en-AU,en")
TZ = os.getenv("ODDSPORTAL_TZ", "Australia/Perth")
UA = os.getenv("ODDSPORTAL_UA", "")    # optional
PROXY = os.getenv("ODDSPORTAL_PROXY", "")  # optional, e.g. http://user:pass@host:port

def _patched_chrome(*_, **__):
    opts = uc.ChromeOptions()
    if not HEADFUL:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"--lang={LANG}")
    if UA: opts.add_argument(f"--user-agent={UA}")
    if PROXY: opts.add_argument(f"--proxy-server={PROXY}")
    prof = os.path.expanduser("~/.op_stealth_profile"); os.makedirs(prof, exist_ok=True)
    opts.add_argument(f"--user-data-dir={prof}")

    d = uc.Chrome(options=opts, headless=(not HEADFUL))
    try:
        d.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
                          {"source":"Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"})
    except Exception: pass
    try:
        d.execute_cdp_cmd("Emulation.setTimezoneOverride", {"timezoneId": TZ})
    except Exception: pass
    try:
        d.set_window_size(random.randrange(1280,1600), random.randrange(800,1000))
    except Exception: pass
    return d

webdriver.Chrome = _patched_chrome
