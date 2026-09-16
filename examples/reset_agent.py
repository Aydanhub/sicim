"""Reset: bozuk bir LLM adımını, baştan başlamadan yeniden oynatmak.

Akış: kaynakları topla (pahalı) -> LLM'den JSON çıkar -> raporu yaz.

Demo'da LLM ikinci adımda bozuk JSON döndürür ve run FAILED olur. Operatör
prompt'u düzeltir ve `rt.reset(run_id)` çağırır: journal ilk başarısız op'a
geri sarılır, ondan öncesi (pahalı toplama adımı) journal'dan replay edilir ve
yalnızca bozuk adım yeniden çalışır.

Çalıştır:  .venv/bin/python examples/reset_agent.py
"""

import asyncio
import json
import os
import tempfile

import sicim

calls: dict[str, int] = {}

# Operatörün düzelttiği "prompt": önce modelden serbest metin ister (bozuk
# JSON gelir), reset'ten önce katı JSON talimatına çevrilir.
PROMPT = {"strict": False}


async def gather_sources(topic: str):
    """Pahalı toplama adımı: bir kez koşar, reset sonrası journal'dan gelir."""
    calls["gather"] = calls.get("gather", 0) + 1
    await asyncio.sleep(0.3)
    return [f"https://ornek.dev/{topic}-{i}" for i in range(3)]


async def extract_json(sources: list[str]):
    """LLM çağrısı: prompt düzeltilene kadar ayrıştırılamayan metin döndürür."""
    calls["extract"] = calls.get("extract", 0) + 1
    await asyncio.sleep(0.1)
    raw = (
        json.dumps({"kaynak_sayisi": len(sources), "guven": 0.82})
        if PROMPT["strict"]
        else "Tabii! İşte istediğiniz JSON:\n```\n{kaynak_sayisi: 3,}\n```"
    )
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise sicim.NonRetryable(f"model JSON döndürmedi: {exc}") from exc


async def write_report(topic: str, facts: dict):
    calls["report"] = calls.get("report", 0) + 1
    return f"{topic}: {facts['kaynak_sayisi']} kaynak, güven {facts['guven']}"


@sicim.workflow(name="reset_demo_agent")
async def research(ctx: sicim.WorkflowContext, topic: str):
    sources = await ctx.step(gather_sources, topic)
    facts = await ctx.step(extract_json, sources)
    return await ctx.step(write_report, topic, facts)


async def main():
    db = os.path.join(tempfile.mkdtemp(prefix="sicim-reset-"), "reset.db")
    store = sicim.SQLiteStore(db)
    rt = sicim.Runtime(store, default_retry=sicim.NO_RETRY)

    print("=== 1) agent bozuk LLM cevabıyla düşüyor ===")
    handle = await rt.start(research, "durable-execution", run_id="arge-1")
    try:
        await handle.result()
    except sicim.WorkflowFailed as exc:
        print(f"  {exc}")
    print(f"  çağrılar: {calls}")

    print("\n=== 2) prompt düzeltildi, run başarısız op'tan yeniden oynatılıyor ===")
    PROMPT["strict"] = True
    handle = await rt.reset("arge-1")          # to_op verilmedi -> ilk başarısız op
    print(f"  sonuç: {await handle.result()}")
    print(f"  çağrılar: {calls}")
    print("  -> gather bir kez koştu (journal'dan replay), yalnız bozuk adım yinelendi.")

    print("\n=== journal ===")
    for event in await rt.events("arge-1"):
        op = f"op={event.op_id}" if event.op_id >= 0 else "run  "
        print(f"  [{event.seq:>2}] {event.kind:<18} {op:<7} {str(event.payload)[:70]}")

    print(f"\nİncelemek için: .venv/bin/python -m sicim --db {db} show arge-1")
    await rt.shutdown()
    store.close()


if __name__ == "__main__":
    asyncio.run(main())
