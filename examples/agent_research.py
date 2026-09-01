"""LLM agent workflow'u: paralel sub-agent'lar, insan onayı, çökmeden devam.

Akış: plan (LLM) -> 3 paralel SUB-AGENT (her biri kendi journal'ına sahip
child workflow: arama + özet) -> taslak (LLM) -> insan onayı bekle -> yanıt.

Demo, agent insan onayı beklerken süreci "çökertir". Yeni süreç ebeveyn ve
çocuk run'ları devralır, onay sinyali gelince tamamlar — ve her benzersiz LLM
çağrısı yalnızca BİR kez yapılmıştır: tamamlanmış çağrılar journal'dan replay
edilir, asla tekrarlanmaz.

Çalıştır:  .venv/bin/python examples/agent_research.py
"""

import asyncio
import os
import tempfile

import sicim

# --- Sahte LLM ve araçlar ----------------------------------------------------

llm_calls: dict[str, int] = {}


async def llm(task: str, prompt: str):
    """Pahalı LLM çağrısını temsil eder; kaç kez çağrıldığını sayar."""
    llm_calls[task] = llm_calls.get(task, 0) + 1
    await asyncio.sleep(0.15)
    if task == "plan":
        return {"queries": ["sicim nedir", "durable execution", "saga pattern"]}
    return f"[{task}] {prompt[:60]}"


async def web_search(query: str):
    await asyncio.sleep(0.1)
    return {"query": query, "top_result": f"https://ornek.dev/{query.replace(' ', '-')}"}


@sicim.workflow
async def search_agent(ctx: sicim.WorkflowContext, query: str):
    """Sub-agent: kendi journal'ına sahip bağımsız bir run olarak koşar."""
    hit = await ctx.step(web_search, query)
    summary = await ctx.step(llm, f"ozet:{query}", f"Özetle: {hit['top_result']}", name="ozet")
    return {"query": query, "url": hit["top_result"], "summary": summary}


@sicim.workflow
async def research_agent(ctx: sicim.WorkflowContext, question: str):
    plan = await ctx.step(llm, "plan", f"Soruyu araştır: {question}", name="plan")

    # Paralel sub-agent'lar: her biri child workflow, run id'leri deterministik
    # (arge-1.c1, .c2, .c3) — çökme sonrası replay çocuklara yeniden bağlanır.
    sources = await ctx.gather(*(ctx.child(search_agent, q) for q in plan["queries"]))

    draft = await ctx.step(
        llm, "draft", f"{question} için {len(sources)} kaynaktan taslak yaz", name="taslak"
    )

    ctx.log("taslak hazır, insan onayı bekleniyor")
    review = await ctx.wait_event("review", timeout=3600)
    if not review.get("approved"):
        draft = await ctx.step(llm, "revise", f"Şu notla düzelt: {review.get('note')}", name="revize")

    return {"question": question, "answer": draft, "sources": sources, "reviewed_by": review.get("by")}


async def wait_for(store, run_id, kind):
    while not any(e.kind == kind for e in await store.load_events(run_id)):
        await asyncio.sleep(0.02)


async def main():
    db = os.path.join(tempfile.mkdtemp(prefix="sicim-"), "agent.db")
    store = sicim.SQLiteStore(db)

    print("=== 1. süreç: agent çalışıyor ===")
    rt1 = sicim.Runtime(store)
    await rt1.start(research_agent, "Sicim projesi nedir?", run_id="arge-1")
    await wait_for(store, "arge-1", sicim.Kind.WAIT_CREATED)  # onay beklemeye geçti
    await rt1.shutdown()
    print(f"  plan + 3 sub-agent + taslak bitti, onay beklerken süreç ÇÖKTÜ.")
    print(f"  LLM çağrı sayıları: {llm_calls}")

    print("\n=== 2. süreç: devral, onayla, bitir ===")
    rt2 = sicim.Runtime(store)
    await rt2.recover()
    await rt2.signal("arge-1", "review", {"approved": True, "by": "aydan"})
    handle = await rt2.resume("arge-1")
    result = await handle.result()

    print(f"  sonuç: {result['answer']}")
    print(f"  kaynaklar: {[s['url'] for s in result['sources']]}")
    print(f"  LLM çağrı sayıları: {llm_calls}")
    print("  -> çökmeye rağmen her LLM çağrısı tam 1 kez yapıldı (replay journal'dan okudu).")

    print("\n=== run hiyerarşisi ===")
    for record in await store.list_runs():
        print(f"  {record.run_id:<12} {record.workflow:<16} {record.status.value}")

    print(f"\nİncelemek için: .venv/bin/python -m sicim --db {db} show arge-1")
    store.close()


if __name__ == "__main__":
    asyncio.run(main())
