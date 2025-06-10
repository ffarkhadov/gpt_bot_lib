from aiogram import Router

from .start import router as start_router
from .add_store import router as add_router
from .store import router as store_router   # ← меню магазина

router = Router(name="handlers")
router.include_router(start_router)
router.include_router(add_router)
router.include_router(store_router)
