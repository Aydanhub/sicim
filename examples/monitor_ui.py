"""İzleme arayüzü demosu: çeşitli durumlarda run'lar, bir zamanlama ve web UI.

Bir SQLite veritabanını doldurur — tamamlanan bir sipariş akışı, telafiyle
başarısız olan bir sipariş, child'lı bir araştırma agent'ı, insan onayı
bekleyen bir taslak, 5 saniyede bir tetiklenen bir nabız kontrolü — ve
izleme arayüzünü bu worker'ın içinde açar. Tarayıcıda deneyin:

  * Runs: durum çipleri, etiket süzgeçleri (bir etikete tıklayın),
  * onay bekleyen run'a "review" sinyali: payload {"approved": true, "by": "siz"},
  * bir run'a etiket ekleyip silmek, journal satırlarına tıklayıp payload'ı açmak,
  * bir run'ı journal satırındaki ⟲ ile bir op'a geri sarmak (reset),
  * Schedules: nabız zamanlamasını duraklatmak / sürdürmek.

Arayüz bu worker'ın içinde koştuğu için nabız run'larının journal olayları
canlı akışla (başlıktaki yeşil "live" noktası) anında görünür.

Çalıştır:  .venv/bin/python examples/monitor_ui.py      (Ctrl-C ile durdur)
"""

import asyncio
import logging
import os
import tempfile

import sicim
from sicim.ui import start_ui

logging.basicConfig(level=logging.INFO, format="%(message)s")


# --- sahte araçlar -----------------------------------------------------------

async def charge(order_id, key):
    await asyncio.sleep(0.05)
    return {"charge_id": f"ch_{key[:8]}"}


async def refund(order_id):
    await asyncio.sleep(0.05)


async def reserve_stock(order_id):
    if order_id == "order-13":
        raise sicim.NonRetryable("stok yok: SKU-13")
    return {"reservation": f"rsv_{order_id}"}


async def release_stock(order_id):
    pass


async def ship(order_id):
    return {"tracking": f"TR{order_id[-2:]}"}


async def llm(task, prompt):
    await asyncio.sleep(0.1)
    return f"[{task}] {prompt[:40]}"


async def web_search(query):
    await asyncio.sleep(0.05)
    return {"query": query, "url": f"https://ornek.dev/{query.replace(' ', '-')}"}


async def probe():
    return {"latency_ms": 42}


# --- workflow'lar ------------------------------------------------------------

@sicim.workflow
async def order_flow(ctx: sicim.WorkflowContext, order_id: str):
    key = await ctx.uuid4()
    await ctx.step(charge, order_id, key, compensate=refund, compensate_args=(order_id,))
    await ctx.tag({"stage": "reserve"})
    await ctx.step(reserve_stock, order_id, compensate=release_stock, compensate_args=(order_id,))
    await ctx.tag({"stage": "ship"})
    return await ctx.step(ship, order_id)


@sicim.workflow
async def search_agent(ctx: sicim.WorkflowContext, query: str):
    hit = await ctx.step(web_search, query)
    return await ctx.step(llm, f"ozet:{query}", hit["url"], name="ozet")


@sicim.workflow
async def research_agent(ctx: sicim.WorkflowContext, question: str):
    await ctx.tag({"stage": "plan"})
    plan = await ctx.step(llm, "plan", question, name="plan")
    await ctx.tag({"stage": "research"})
    sources = await ctx.gather(*(ctx.child(search_agent, q) for q in ("sicim nedir", "durable execution", "saga")))
    await ctx.tag({"stage": "review"})
    draft = await ctx.step(llm, "draft", f"{plan} / {len(sources)} kaynak", name="taslak")
    ctx.log("taslak hazır; arayüzden 'review' sinyali bekleniyor")
    review = await ctx.wait_event("review", timeout=24 * 3600)
    await ctx.tag({"stage": "done", "reviewed_by": str(review.get("by", "?"))})
    return {"answer": draft, "approved": bool(review.get("approved")), "sources": sources}


@sicim.workflow
async def heartbeat(ctx: sicim.WorkflowContext):
    return await ctx.step(probe)


async def main():
    db = os.path.join(tempfile.mkdtemp(prefix="sicim-"), "monitor.db")
    store = sicim.SQLiteStore(db)
    rt = sicim.Runtime(store, worker_id="demo-worker", schedule_poll_interval=0.5)

    await (await rt.start(order_flow, "order-42", run_id="order-42", tags={"customer": "42", "env": "prod"})).result()
    try:
        await (await rt.start(order_flow, "order-13", run_id="order-13", tags={"customer": "13", "env": "prod"})).result()
    except sicim.WorkflowFailed:
        pass  # telafiler koştu: refund -> journal'da görünür
    await rt.start(research_agent, "Sicim projesi nedir?", run_id="arge-1", tags={"customer": "42", "team": "arge"})
    await rt.schedule(heartbeat, schedule_id="nabiz", cron="@every 5s", tags={"team": "ops"})

    server = await start_ui(rt, port=8787)
    print(f"\nİzleme arayüzü: {server.url}   (veritabanı: {db})")
    print("  'arge-1' onay bekliyor: Runs > arge-1 > Signal: name=review, payload={\"approved\": true, \"by\": \"ben\"}")
    print("  Ctrl-C ile durdur.\n")
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await server.aclose()
        await rt.shutdown()
        store.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
