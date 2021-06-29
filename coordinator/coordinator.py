
import logging

from typing import Any, List, Dict, Optional, Union, Callable
from types import SimpleNamespace

from pydantic import BaseModel

from asyncio import Task, create_task, sleep as asleep

from inspect import iscoroutinefunction

from coordinator.steps import Step
# from coordinator.graphs import Graph
# from coordinator.plans import Plan
from coordinator.sources import Source

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# class Source:
#     pass

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# class Step(BaseModel):

#     name: str # step name
#     func: Callable[..., Any] # executable
#     depends_on: Dict[str,Optional[str]] = {} # list of dependencies, step => func arg

#     description: Optional[str] = "" # optional description for prints/logs etc
#     params: Optional[Dict[Any,Any]] = {} # optional execution parameters

#     def __repr__(self):
#         return f"{self.__class__.__name__}.{self.name}[{self.func}]({','.join(self.depends_on.keys())})"

#     def consumes(self):
#         return {k:v for k,v in self.func.__annotations__.items() if k != 'return' }

#     def produces(self):
#         try:
#             return self.func.__annotations__['return']
#         except KeyError:
#             return None

#     def is_async(self):
#         return iscoroutinefunction(self.func)

#     async def execute(self, **kwargs) -> Any:

#         # how do we get argument ordering correct?
#         # evaluate dynamic strict typing on arguments

#         # get arguments and their types (we will modify this dict)
#         intypes = self.consumes()

#         # unpack params... but treat carefully, filtering out undeclared params
#         _params = kwargs.pop('_params')
#         kwargs.update({p: v for p, v in _params.items() if p in intypes})

#         # now check kwargs sent against function declaration
#         for k in kwargs:

#             # don't accept unknown kwargs (requires caller to be specific)
#             if k not in intypes:
#                 raise ValueError(f"Step {self.name}'s executor does not have a keyword argument {k}")

#             # k in both intypes and kwargs, check types for this argument
#             # Note: intypes could be Union/Optional (is Optional ok?)
#             if hasattr(intypes[k], '__args__'):
#                 # Union or Optional typing?
#                 if type(kwargs[k]) not in intypes[k].__args__:
#                     raise ValueError(f"Step {self.name}'s executor requires {intypes[k]}, not {type(kwargs[k])}, for argument {k}")
#             else:
#                 # strict
#                 if type(kwargs[k]) is not intypes[k]:
#                     raise ValueError(f"Step {self.name}'s executor requires {intypes[k]}, not {type(kwargs[k])}, for argument {k}")

#             # argument is ok: exists and is typed correctly/consistently
#             # downsize intypes (for marginal efficiency)?
#             intypes.pop(k)

#         # ok, now check remaining input types. We've checked all kwargs matches, 
#         # so we need to validate that any **missing** kwargs are Optional.
#         for k in intypes:

#             # validate that we are not missing **required** arguments
#             if k not in kwargs: 
#                 optional = hasattr(intypes[k], "__args__") and (type(None) in intypes[k].__args__)
#                 if not optional:
#                     raise ValueError(f"Required argument \"{k}\" for {self.__class__.__name__}[\"{self.name}\"] executor missing")

#         if self.is_async():
#             return await self.func(**kwargs)
#         return self.func(**kwargs)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

class Coordinator: 

    def __init__(self, config: Union[dict,str]=None):

        # load config from YAML/JSON if str, dict o/w
        
        self.sleep_time = 0.0

        # DAG always has a "_source" vertex for root level input
        self._dag = Graph(nodes=['_source'])

        self._steps = {}
        self._verified = False

    def _configure(self, config: dict={}):
        pass

    def __repr__(self):
        return "Coordinator[" + ', '.join([s for s in self._steps]) + "]"

    def __getattr__(self, attr):
        if attr == "step": 
            return SimpleNamespace(**self._steps)
        raise AttributeError(f"{self.__class__.__name__} has no attribute \"{attr}\"")

    def add(self, step: Union[Step,Dict[str,Any]]):
        """Add a step, by Step obj or by dict to initialize Step with. Chainable. """

        # assert call with proper Step type
        if isinstance(step, dict):
            return self.__add__(Step(**step))

        # validate input type
        if not isinstance(step, Step):
            raise ValueError(f"Cannot add step of type {type(step)} to {self.__class__.__name__}")

        # validate uniqueness of names
        if step.name in self._steps:
            raise ValueError(f"Steps must have unique names, and there is already a step {step.name}")

        # add step after validations
        self._steps[step.name] = step

        # add edges to DAG we use for cycle analysis
        for dp in step.depends_on:
            self._dag.add(dp, step.name)

        # return self for chaining
        return self

    def remove(self, step: Union[Step,str]):
        """ Remove a step, by Step obj or by name. Chainable. """

        # assert call with step name (str), not step itself
        if isinstance(step, Step):
            return self.__sub__(step.name)

        # if this name is a step name, remove the step
        if step in self:
            self._steps.pop(step)

        # remove from the DAG
        self._dag.remove(step)

        # return self for chaining
        return self

    def intermediates(self, verify=False): 
        """ Get "intermediaries" of the step graph
        
        Intermediaries are steps whose output is used by other steps. 
        We get these by taking the set union of all depends_on lists
        over steps. We remove the special _source "step". 

        If verify == True, then we also check that there are no such keys
        that are _not_ defined in the step list. This would mean a step
        has been added that depends on another step that has _not_ been 
        added (yet). 
        """
        v = set({})
        for s in self._steps:
            v = v | set(list(self._steps[s].depends_on.keys()))
        v.remove('_source')
        if verify: 
            defined_keys = v & set(list(self._steps.keys()))
            if len(defined_keys) < len(v):
                undefined_keys = '", "'.join([s for s in v if s not in self._steps])
                raise ValueError(f"There are undefined dependency steps: \"{undefined_keys}\"")
        return v

    def leaves(self):
        """ Get "leaves" of the step graph
        
        Leaves are steps that don't feed any other steps, ie not intermediates.
        In other words, terminal nodes (if steps are nodes). 
        """
        return set([s for s in self._steps if s not in self.intermediates()])

    def verified(self):
        return self._verified

    def verify(self, params: Dict[str,Any]) -> bool:

        # check for undefined dependencies
        self.intermediates(verify=True)

        # check for cycles


        # check parameters if passed
        if params: 
            ambiguous_keys = set(list(params.keys())) & set(list(self._steps.keys()))
            if len(ambiguous_keys) > 0:
                ambiguous_keys = '\", \"'.join(ambiguous_keys)
                raise ValueError(f"There are ambiguous data/parameter keys: \"{ambiguous_keys}\"")

        # if no failures, return 
        self._verified = True

    def launch(self, step: Union[Step,str], source: Any, 
                    data: Dict[str,Any], params: Dict[str,Any]) -> Task:
        """ "Launch" a step, meaning run the step if possible
        
        First, we check if data has fields for each prerequisite for the 
        step. These fields should be populated if those other steps are
        complete (or we know their output). 


        """

        # assert call is with a step, not a step name
        if isinstance(step, str):
            if step not in self._steps:
                raise ValueError(f"Cannot launch step {step} here, no definition")
            return self.launch(self._steps[step], source=source, data=results, params=params)

        if not isinstance(step, Step):
            raise ValueError(f"Cannot launch non-Step type {type(step)}")

        # evaluate if the Step is, in fact, launchable from given data
        for p in [s for s in step.depends_on if s != "_source"]:
            if p not in data: # note data[p] _can_ be None, that may be valid output
                logging.debug(f"step {step.name} not executable, \"{p}\" has not completed")
                return None

        logging.debug(f"step {step.name} executable")

        # create keyword argument dict for execution; include 
        # params as a special field which gets unpacked in the Step
        # execution check
        kwargs = {
            step.depends_on[p]: source if p == "_source" else data[p] 
            for p in step.depends_on if step.depends_on[p] is not None
        }
        kwargs.update({'_params': params})

        return create_task(step.execute(**kwargs), name=step.name)

    async def sleep(self) -> None:
        """ Wrap sleep to get more flexibility; default is 0 for fast context switch """
        await asleep(self.sleep_time)

    async def run(self, data: Any=None, params: Dict[str,Any]={}):
        """ 

        """

        if not self._verified: 
            self.verify(params=params)

        # no running tasks, completed steps, or results (yet)
        # Note: a None result is possible, so we need results and flags
        # (but we could also use key existence as the flag)
        tasks, finished = [], 0
        started = {self._steps[s].name: False for s in self._steps}
        done    = {self._steps[s].name: False for s in self._steps}
        results = {} # {self._steps[s].name: None  for s in self._steps}

        # iterate until tasks are completed; inner loop should add tasks 
        # when possible
        while finished < len(self._steps):

            while True: # less than timeout?

                # attempt to launch unstarted tasks
                for s in [self._steps[s] for s in done if not started[s]]:
                    logging.debug(f"evaluating viability of step {s.name}")
                    task = self.launch(s, source=data, data=results, params=params)
                    if task: 
                        logging.debug(f"started task for step {s.name}")
                        tasks.append(task)
                        started[s.name] = True

                # see if any of the tasks have completed
                for t in tasks: 
                    if t.done():
                        name = t.get_name()
                        logging.debug(f"step {name} completed")
                        err = t.exception()
                        if err: 
                            logging.error(f"step {name} had an error: {err}")
                            raise err
                        done[name], results[name] = True, t.result()
                        finished += 1

                # remove completed tasks
                tasks = [t for t in tasks if not done[t.get_name()]]

                # if there are no tasks, we are finished
                if finished == len(self._steps):
                    break

                await self.sleep()

        return {s: results[s] for s in self.leaves()}

    async def poll(self, source: Source=None, params: Dict[str,Any]={}):
        async for data in source():
            yield self.run(data=data, params=params)

    async def __call__(self, data: Any=None, params: Dict[str,Any]={}):
        """ call wraps run """
        return await self.run(data=data, params=params)

    def __add__(self, step: Union[Step,Dict[str,Any]]):
        return self.add(step)

    def __sub__(self, step: Union[Step,str]):
        return self.remove(step)

    def __contains__(self, step: Union[Step,str]) -> bool:

        if isinstance(step, str):
            return step in self._steps

        if not isinstance(step, Step):
            raise ValueError(f"Cannot evaluate containment of {type(step)} in {self.__class__.__name__}")

        return step.name in self._steps

class PollingCoordinator:

    def __init__(self, coord: Coordinator, source: Source=None, params: Dict[str,Any]={}):

        self.crd = coord
        self.src = source
        self.prm = params

    async def __aiter__(self):
        if not self.crd.verified():
            self.crd.verify(params=params)
            # will raise on a problem
        return self

    async def __anext__(self):
        msg = await self.src.__anext__()
        raise StopAsyncIteration

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
