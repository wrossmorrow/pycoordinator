
import aiohttp
import asyncio
import requests

from typing import Optional

from coordinator import Source, Step, Coordinator

def test_s(msg: str) -> int:
    #print( f"evaluating test_s: {msg}" )
    return int(msg)

def test_i(msg: int) -> int:
    #print( f"evaluating test_i: {msg}" )
    return msg

def test_add(i: int, a: Optional[int]=1) -> int:
    #print( f"evaluating test_add ({a}): {i}" )
    return i + a

def test_http(msg: int) -> None:
    #print( f"evaluating test_http" )
    return requests.get("http://google.com")

async def test_aiohttp(msg: int) -> None:
    #print( f"evaluating test_aiohttp" )
    async with aiohttp.ClientSession() as session:
        response = await session.get("http://google.com")

async def atest(msg: str, **kwargs) -> int:
    return int(msg)


async def test_sum(params={"a":1}):

    C = Coordinator()
    for i in range(5):
        dp = {}
        if i == 0: 
            dp['_source'] = "i"
        else:
            dp[f"t{i-1}"] = "i"
        C += {
            'name': f"t{i}", 
            'func': test_add,
            'depends_on': dp
        }
    results = await C.run(0, params=params)
    print(results)

async def test_types():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=test_s, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "ti", 
        'func': test_i,
        'depends_on': {
            "ts": "msg"
        }
    }

    results = await C.run("0")
    print(results)


async def test_wait_sync():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=test_s, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "rq", 
        'func': test_http,
        'depends_on': {
            "ts": "msg",
        }
    }

    C += {
        'name': "ti", 
        'func': test_i,
        'depends_on': {
            "ts": "msg",
            "rq": None,
        }
    }

    results = await C.run("0")
    print(results)

async def test_wait_async():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=test_s, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "aq", 
        'func': test_aiohttp,
        'depends_on': {
            "ts": "msg",
        }
    }

    C += {
        'name': "ti", 
        'func': test_i,
        'depends_on': {
            "ts": "msg",
            "aq": None,
        }
    }

    results = await C.run("0")
    print(results)


async def test_wait_sync_call():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=test_s, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "rq", 
        'func': test_http,
        'depends_on': {
            "ts": "msg",
        }
    }

    C += {
        'name': "ti", 
        'func': test_i,
        'depends_on': {
            "ts": "msg",
            "rq": None,
        }
    }

    results = await C("0")
    print(results)

async def test_wait_async_call():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=test_s, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "aq", 
        'func': test_aiohttp,
        'depends_on': {
            "ts": "msg",
        }
    }

    C += {
        'name': "ti", 
        'func': test_i,
        'depends_on': {
            "ts": "msg",
            "aq": None,
        }
    }

    results = await C("0")
    print(results)


async def test_fail_incomplete():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=test_s, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "ti", 
        'func': test_i,
        'depends_on': {
            "ts": "msg",
            "rq": None
        }
    }

    results = await C.run("0")

async def test_params():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=test_s, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "ti", 
        'func': test_i,
        'depends_on': {
            "ts": "msg"
        }
    }

    results = await C.run("0", params={'a': 0})
    print(results)

async def test_fail():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=test_s, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "ti", 
        'func': test_i,
        'depends_on': {
            "ts": "msg"
        }
    }

    results = await C.run("0", params={'ts': 0})
    print(results)


class Yielder(Source):

    async def __call__(self):
        for i in range(10):
            yield 1
            await asyncio.sleep(1)

async def test_gen():

    C = Coordinator()

    C += Step(
        name="ta", 
        func=test_add, 
        depends_on={"_source": "i"}
    )
    
    async for results in C.poll(Yielder()):
        print(await results)


if __name__ == "__main__":

    asyncio.run(test_types())

    asyncio.run(test_gen())

    asyncio.run(test_sum())

    asyncio.run(test_sum(params={"a":2}))

    try:
        asyncio.run(test_sum(params={"a":"1"}))
    except ValueError: 
        print("failed successfully")

    asyncio.run(test_wait_sync())
    asyncio.run(test_wait_async())
    asyncio.run(test_wait_sync_call())
    asyncio.run(test_wait_async_call())

    try:
        asyncio.run(test_fail_incomplete())
    except ValueError: 
        print("failed successfully")

    asyncio.run(test_params())

    try:
        asyncio.run(test_fail())
    except ValueError: 
        print("failed successfully")
