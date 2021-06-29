
import logging

from typing import Any, List, Dict, Optional, Union, Callable
from types import SimpleNamespace

from asyncio import Task, create_task, sleep as asleep

from coordinator.steps import Step
from coordinator.graphs import Graph
from coordinator.sources import Source

class Coordinator: 

    def __init__(self, config: Union[dict,str]=None):

        # load config from YAML/JSON if str, dict o/w
        
        self.sleep_time = 0.0

        # DAG always has a "_source" vertex for run-level input
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

    def dag(self):
        return self._dag

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

        Basically, 

            intermediates = nodes - roots - leaves

        but that's not how this is explicitly computed. 

        If verify == True, then we also check that there are no such keys
        that are _not_ defined in the step list. This would mean a step
        has been added that depends on another step that has _not_ been 
        added (yet). 
        """

        # take the union of all dependencies 
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

    def roots(self):
        """ Get "roots" of the step graph
        
        Roots are steps that don't get fed by any other steps, ie have no
        egress. In other words, "source" nodes (if steps are nodes). 
        """
        return self._dag.roots()

    def leaves(self):
        """ Get "leaves" of the step graph
        
        Leaves are steps that don't feed any other steps, ie not intermediates.
        In other words, terminal or "sink" nodes (if steps are nodes). 
        """
        return self._dag.leaves()

    def verified(self):
        return self._verified

    def verify(self, params: Dict[str,Any]) -> bool:
        """ Verify step graph is runnable

        Check that there are no missing dependencies; eg 

            N=[0,1], E=[(0->1),(2->1)]

        would not be runnable as step "2" is not defined. Our add/remove
        syntax should eliminate this possibility though. 

        Check that the step graph is not cyclic, which would
        represent a loop not a tree. eg

            N=[0,1], E=[(0->1),(1->0)]

        would not be runnable because it would just cycle (and our
        dependency logic would probably fail). 

        If we have parameters, check that there are no naming conflicts
        between parameters and arguments from dependencies. 
        """

        # check for undefined dependencies
        self.intermediates(verify=True)

        # check for cycles
        if self._dag.cyclic():
            raise ValueError("The implied step graph is cyclic, not executable")

        # check parameters if passed
        if params: 
            ambiguous_keys = set(list(params.keys())) & set(list(self._steps.keys()))
            if len(ambiguous_keys) > 0:
                ambiguous_keys = '\", \"'.join(ambiguous_keys)
                raise ValueError(f"There are ambiguous data/parameter keys: \"{ambiguous_keys}\"")

        # if no failures, return 
        self._verified = True

        # return True, as verification passed, but really raise if not
        return True

    def launch(self, step: Union[Step,str], source: Any, 
                    data: Dict[str,Any], params: Dict[str,Any]) -> Task:
        """ "Launch" a step, meaning run the step if possible
        
        First, we check if data has fields for each prerequisite for the 
        step. These fields should be populated if those other steps are
        complete (or we know their output). 

        We then construct kwargs to pass to the Step executor. 

        Finally an asyncio.Task is created for this execution. 
        """

        # assert call is with a step, not a step name
        if isinstance(step, str):
            if step not in self._steps:
                raise ValueError(f"Cannot launch step {step} here, no definition")
            return self.launch(self._steps[step], source=source, data=results, params=params)

        # now that we've asserted a call with a Step type, enforce that
        if not isinstance(step, Step):
            raise ValueError(f"Cannot launch non-Step type {type(step)}")

        # evaluate if the Step is, in fact, launchable from given data
        # We use existence of data fields for dependencies in depends_on
        # as a sentinel; thus these (currently) can't be optional
        for p in [s for s in step.depends_on if s != "_source"]:
            if p not in data: # note data[p] _can_ be None, that may be valid output
                logging.debug(f"step {step.name} not executable, \"{p}\" has not completed")
                return None

        logging.debug(f"step {step.name} executable")

        # create keyword argument dict for execution; include 
        # params as a special field which gets unpacked in the Step
        # execution check. depends_on provides the keys which will 
        # (cause kwargs) get mapped into argument names. So steps 
        # have to be configured with this in mind. 
        kwargs = {
            step.depends_on[p]: source if p == "_source" else data[p] 
            for p in step.depends_on if step.depends_on[p] is not None
        }
        kwargs.update({'_params': params})

        # create and start the asyncio.Task for this step. Using an 
        # async sleep of 0 to facilitate context switching this 
        # probably isn't so awful; but perhaps wrapping Tasks might
        # be more performant for explicitly non-async executors.  
        return create_task(step.execute(**kwargs), name=step.name)

    async def sleep(self) -> None:
        """ Wrap sleep to get more flexibility; default is 0 for fast context switch """
        await asleep(self.sleep_time)

    async def run(self, data: Any=None, params: Dict[str,Any]={}):
        """ Run a step graph with specific data and params

        First verify the current step graph state _can_ be run. 

        Then loop, starting tasks for Steps that can be started 
        (all their dependencies are complete), removing tasks and
        storing their results when they complete, incrementing a 
        counter for how many tasks have finished and stopping when
        this counter equals the number of steps. 

        By default we return only step results from leaves of the 
        step graph. 

        .run() can be called concurrently for different data/params
        with tools like asyncio.gather() or asyncio.Tasks. 
        """

        if not self._verified: 
            self.verify(params=params)

        # no running tasks, completed steps, or results (yet)
        # Note: a None result is possible, so we need results and flags
        # (but we could also use key existence as the flag)
        # 
        # Note these run tracking datastructures are local, not class
        # variables. In principle we should be able to kick off 
        # multiple separate (async) runs at the same time. 
        # 
        # TODO expand with instrumentation: 
        # 
        #   start time, done time, duration
        #   errors if loose exception handling
        # 
        tasks, finished = [], 0
        started = {self._steps[s].name: False for s in self._steps}
        done    = {self._steps[s].name: False for s in self._steps}
        results = {} # use result existence as a completed signal

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
                        # tasks.remove(t) here would change the iterator

                # remove completed tasks with a "not done" filter
                tasks = [t for t in tasks if not done[t.get_name()]]

                # are we finished? Note that tasks can be empty here
                # and we can not be finished, because we need another 
                # loop execution to know if there are more tasks to 
                # create. 
                if finished == len(self._steps):
                    break

                await self.sleep()

        # return terminal/sink results only by default
        return {s: results[s] for s in self.leaves()}

    async def poll(self, source: Source=None, params: Dict[str,Any]={}):
        """ Repeatedly run a step graph for given params
        
        Data is drawn from some Source which we expect to yield data
        from an (async) generator. Results should also be yielded back
        so we can use output as an async generator. 
        """
        async for data in source():
            yield self.run(data=data, params=params)

    async def __call__(self, data: Any=None, params: Dict[str,Any]={}):
        """ wraps run """
        return await self.run(data=data, params=params)

    def __add__(self, step: Union[Step,Dict[str,Any]]):
        """ wraps add """
        return self.add(step)

    def __sub__(self, step: Union[Step,str]):
        """ wraps remove """
        return self.remove(step)

    def __contains__(self, step: Union[Step,str]) -> bool:

        if isinstance(step, str):
            return step in self._steps

        if not isinstance(step, Step):
            raise ValueError(f"Cannot evaluate containment of {type(step)} in {self.__class__.__name__}")

        return step.name in self._steps

