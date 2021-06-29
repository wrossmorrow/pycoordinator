
import aiohttp
import asyncio
import requests
import pytest

from typing import Optional

from coordinator import Source, Step, Coordinator

def f_str_to_int(msg: str) -> int:
    return int(msg)

def f_int_to_int(msg: int) -> int:
    return msg

def f_add(i: int, a: Optional[int]=1) -> int:
    return i + a

def f_sync_http(msg: int) -> None:
    return requests.get("http://google.com")

async def f_aio_http(msg: int) -> None:
    async with aiohttp.ClientSession() as session:
        response = await session.get("http://google.com")

async def atest(msg: str, **kwargs) -> int:
    return int(msg)


@pytest.mark.asyncio
async def test_sum_ok(params={"a":1}):

    C = Coordinator()
    for i in range(5):
        dp = {}
        if i == 0: 
            dp['_source'] = "i"
        else:
            dp[f"t{i-1}"] = "i"
        C += {
            'name': f"t{i}", 
            'func': f_add,
            'depends_on': dp
        }
    results = await C.run(0, params=params)
    print(results)

@pytest.mark.asyncio
async def test_sum_not_ok(params={"a":"1"}):

    with pytest.raises(ValueError) as err:

        C = Coordinator()
        for i in range(5):
            dp = {}
            if i == 0: 
                dp['_source'] = "i"
            else:
                dp[f"t{i-1}"] = "i"
            C += {
                'name': f"t{i}", 
                'func': f_add,
                'depends_on': dp
            }
        results = await C.run(0, params=params)
        print(results)

@pytest.mark.asyncio
async def test_types():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=f_str_to_int, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "ti", 
        'func': f_int_to_int,
        'depends_on': {
            "ts": "msg"
        }
    }

    results = await C.run("0")
    print(results)

@pytest.mark.asyncio
async def test_reject_cyclic():

    with pytest.raises(ValueError) as err:

        C = Coordinator()

        C += Step(
            name="t0", 
            func=f_str_to_int, 
            depends_on={
                "_source": "msg"
            }
        )

        C += Step(
            name="t1", 
            func=f_int_to_int, 
            depends_on={
                "t0": "msg",
                "t2": "msg"
            }
        )

        C += Step(
            name="t2", 
            func=f_int_to_int, 
            depends_on={
                "t1": "msg"
            }
        )

        results = await C.run("0")
        print(results)

@pytest.mark.asyncio
async def test_wait_sync():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=f_str_to_int, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "rq", 
        'func': f_sync_http,
        'depends_on': {
            "ts": "msg",
        }
    }

    C += {
        'name': "ti", 
        'func': f_int_to_int,
        'depends_on': {
            "ts": "msg",
            "rq": None,
        }
    }

    results = await C.run("0")
    print(results)

@pytest.mark.asyncio
async def test_wait_async():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=f_str_to_int, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "aq", 
        'func': f_aio_http,
        'depends_on': {
            "ts": "msg",
        }
    }

    C += {
        'name': "ti", 
        'func': f_int_to_int,
        'depends_on': {
            "ts": "msg",
            "aq": None,
        }
    }

    results = await C.run("0")
    print(results)

@pytest.mark.asyncio
async def test_wait_sync_call():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=f_str_to_int, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "rq", 
        'func': f_sync_http,
        'depends_on': {
            "ts": "msg",
        }
    }

    C += {
        'name': "ti", 
        'func': f_int_to_int,
        'depends_on': {
            "ts": "msg",
            "rq": None,
        }
    }

    results = await C("0")
    print(results)

@pytest.mark.asyncio
async def test_wait_async_call():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=f_str_to_int, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "aq", 
        'func': f_aio_http,
        'depends_on': {
            "ts": "msg",
        }
    }

    C += {
        'name': "ti", 
        'func': f_int_to_int,
        'depends_on': {
            "ts": "msg",
            "aq": None,
        }
    }

    results = await C("0")
    print(results)

@pytest.mark.asyncio
async def test_fail_incomplete():

    with pytest.raises(ValueError) as err:

        C = Coordinator()

        C += Step(
            name="ts", 
            func=f_str_to_int, 
            depends_on={"_source": "msg"}
        )

        C += {
            'name': "ti", 
            'func': f_int_to_int,
            'depends_on': {
                "ts": "msg",
                "rq": None
            }
        }

        results = await C.run("0")

@pytest.mark.asyncio
async def test_params():

    C = Coordinator()

    C += Step(
        name="ts", 
        func=f_str_to_int, 
        depends_on={"_source": "msg"}
    )

    C += {
        'name': "ti", 
        'func': f_int_to_int,
        'depends_on': {
            "ts": "msg"
        }
    }

    results = await C.run("0", params={'a': 0})
    print(results)

@pytest.mark.asyncio
async def test_fail():

    with pytest.raises(ValueError) as err:
        C = Coordinator()

        C += Step(
            name="ts", 
            func=f_str_to_int, 
            depends_on={"_source": "msg"}
        )

        C += {
            'name': "ti", 
            'func': f_int_to_int,
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

@pytest.mark.asyncio
async def test_gen():

    C = Coordinator()

    C += Step(
        name="ta", 
        func=f_add, 
        depends_on={
            "_source": "i"
        }
    )

    async for results in C.poll(Yielder()):
        print(await results)
