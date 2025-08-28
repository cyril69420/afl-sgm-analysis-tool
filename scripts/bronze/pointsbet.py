import re
from urllib.parse import urljoin
from rapidfuzz import fuzz

BASE = "https://pointsbet.com.au"
STARTS = [
    f"{BASE}/sports/aussie-rules/AFL",
    f"{BASE}/sports/australian-rules/AFL",  # PB sometimes flips this segment
]

class PointsbetResolver:
    name = "pointsbet"

    @classmethod
    async def resolve(cls, game, alias_map, ctx):
        page = await ctx.new_page()
        try:
            # Open (tolerate either URL form)
            for start in STARTS:
                try:
                    await page.goto(start, wait_until="domcontentloaded", timeout=25000)
                    break
                except Exception:
                    continue
            await page.wait_for_timeout(800)

            # Dismiss common banners (best-effort)
            for sel in [
                "button:has-text('Accept')",
                "button:has-text('I Agree')",
                "#onetrust-accept-btn-handler",
                "button[aria-label='Close']",
            ]:
                try:
                    await page.locator(sel).first.click(timeout=800)
                except Exception:
                    pass

            # Try to reveal full list
            for label in ["View all", "Show more", "All matches", "AFL", "Upcoming"]:
                try:
                    await page.get_by_role("button", name=label).click(timeout=800)
                except Exception:
                    pass

            # Infinite scroll until candidate count plateaus
            last = -1
            same = 0
            for _ in range(20):
                cand = page.locator("a[href*='/AFL']")
                n = await cand.count()
                same = same + 1 if n == last else 0
                last = n
                if same >= 3:  # 3 consecutive no-increase cycles
                    break
                await page.mouse.wheel(0, 2200)
                await page.wait_for_timeout(350)

            # Score candidates
            anchors = await page.locator("a[href*='/AFL']").all()
            def norm(s): return re.sub(r"[^a-z0-9 ]+", " ", (s or "").lower()).strip()
            targets = [norm(game["home"]), norm(game["away"])]
            best = ("", -1, "")

            for a in anchors:
                href = await a.get_attribute("href") or ""
                text = norm(await a.inner_text())
                # Some tiles keep names outside the link; try parent text if link text is tiny
                if len(text) < 6:
                    try:
                        parent = a.locator("xpath=..")
                        text = norm(await parent.inner_text())
                    except Exception:
                        pass

                tokens_ok = all(t in text for t in targets) or (" v " in f" {text} ") or (" vs " in f" {text} ")
                score = max(
                    fuzz.WRatio(targets[0], text),
                    fuzz.WRatio(targets[1], text),
                    min(fuzz.partial_ratio(targets[0], text), fuzz.partial_ratio(targets[1], text)),
                )
                if tokens_ok or score >= 70:
                    if score > best[1]:
                        best = (urljoin(BASE, href), score, text[:120])

            if best[0]:
                return {
                    "event_url": best[0],
                    "match_quality": int(best[1]),
                    "found_at": "browse",
                    "notes": f"{len(anchors)} candidates; sample='{best[2]}'"
                }

            # Fallback: quick on-page search for the home team, then match tiles
            try:
                search = page.get_by_placeholder(re.compile("Search", re.I))
                await search.fill(game["home"])
                await page.wait_for_timeout(600)
                for a in await page.locator("a:has-text(' v '), a:has-text(' vs ')").all():
                    href = await a.get_attribute("href") or ""
                    text = norm(await a.inner_text())
                    if all(t in text for t in targets):
                        return {
                            "event_url": urljoin(BASE, href),
                            "match_quality": 85,
                            "found_at": "search",
                            "notes": "fallback search"
                        }
            except Exception:
                pass

            return None
        finally:
            await page.close()
