from rapidfuzz import fuzz
from urllib.parse import urljoin

BASE = "https://www.ladbrokes.com.au"

class LadbrokesResolver:
    name = "ladbrokes"
    start_url = f"{BASE}/sports/australian-rules/afl"

    @classmethod
    async def resolve(cls, game, alias_map, ctx):
        page = await ctx.new_page()
        try:
            await page.goto(cls.start_url, timeout=15000)
            anchors = await page.locator("a").all()
            teams = [alias_map.get(game["home"], game["home"]), alias_map.get(game["away"], game["away"])]
            tcanon = [t.lower() for t in teams]
            best = (None, -1, "browse")
            for a in anchors:
                href = await a.get_attribute("href")
                text = (await a.inner_text()).strip().lower()
                if not href or "/afl" not in href:
                    continue
                score = 0
                if all(t in text for t in tcanon):
                    score = 100
                else:
                    score = max(
                        min(fuzz.partial_ratio(tcanon[0], text), fuzz.partial_ratio(tcanon[1], text)),
                        min(fuzz.WRatio(tcanon[0], text), fuzz.WRatio(tcanon[1], text))
                    )
                if score > best[1]:
                    best = (urljoin(BASE, href), score, "browse")
            if best[0]:
                return {"event_url": best[0], "match_quality": best[1], "found_at": best[2], "notes": ""}
        finally:
            await page.close()
        return None
