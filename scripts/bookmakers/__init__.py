# scripts/bookmakers/__init__.py
from .sportsbet import SportsbetResolver
from .pointsbet import PointsbetResolver
from .tab import TABResolver            # ← note the exact class name: TABResolver
from .ladbrokes import LadbrokesResolver

RESOLVERS = {
    "sportsbet": SportsbetResolver,
    "pointsbet": PointsbetResolver,
    "tab": TABResolver,
    "ladbrokes": LadbrokesResolver,
}
