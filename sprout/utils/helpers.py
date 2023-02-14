import asyncio

def merge_async_iters(*aiters):
    queue = asyncio.Queue(1)
    run_count = len(aiters)
    cancelling = False
    async def drain(aiter):
        nonlocal run_count
        try:
            async for item in aiter:
                await queue.put((False, item))
        except Exception as e:
            if not cancelling:
                await queue.put((True, e))
            else:
                raise
        finally:
            run_count -= 1
    async def merged():
        try:
            while run_count:
                raised, next_item = await queue.get()
                if raised:
                    cancel_tasks()
                    raise next_item
                yield next_item
        finally:
            cancel_tasks()
    tasks = [asyncio.create_task(drain(aiter)) for aiter in aiters]
    def cancel_tasks():
        nonlocal cancelling
        cancelling = True
        for t in tasks:
            t.cancel()
    return merged()