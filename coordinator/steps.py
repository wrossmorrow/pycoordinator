
from typing import Any, List, Dict, Optional, Union, Callable

from pydantic import BaseModel

from inspect import iscoroutinefunction

class Step(BaseModel):

    name: str # step name
    func: Callable[..., Any] # executable
    depends_on: Dict[str,Optional[str]] = {} # list of dependencies, step => func arg

    description: Optional[str] = "" # optional description for prints/logs etc
    params: Optional[Dict[Any,Any]] = {} # optional execution parameters

    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name}[{self.func}]({','.join(self.depends_on.keys())})"

    def __hash__(self):
        return self.name

    def consumes(self):
        return {k:v for k,v in self.func.__annotations__.items() if k != 'return'}

    def produces(self):
        try:
            return self.func.__annotations__['return']
        except KeyError:
            return None

    def is_async(self):
        return iscoroutinefunction(self.func)

    async def execute(self, **kwargs) -> Any:

        # how do we get argument ordering correct?
        # evaluate dynamic strict typing on arguments

        # get arguments and their types (we will modify this dict)
        intypes = self.consumes()

        # unpack params... but treat carefully, filtering out undeclared params
        # (we should already have checked for arg/param naming conflicts)
        _params = kwargs.pop('_params')
        kwargs.update({p: v for p, v in _params.items() if p in intypes})

        # now check kwargs sent against function declaration
        for k in kwargs:

            # don't accept unknown kwargs (requires caller to be specific)
            if k not in intypes:
                raise ValueError(f"Step {self.name}'s executor does not have a keyword argument {k}")

            # k in both intypes and kwargs, check types for this argument
            # Note: intypes could be Union/Optional (is Optional ok?)
            if hasattr(intypes[k], '__args__'):
                # Union or Optional typing?
                if type(kwargs[k]) not in intypes[k].__args__:
                    raise ValueError(f"Step {self.name}'s executor requires {intypes[k]}, not {type(kwargs[k])}, for argument {k}")
            else:
                # strict
                if type(kwargs[k]) is not intypes[k]:
                    raise ValueError(f"Step {self.name}'s executor requires {intypes[k]}, not {type(kwargs[k])}, for argument {k}")

            # argument is ok: exists and is typed correctly/consistently
            # downsize intypes (for marginal efficiency)?
            intypes.pop(k)

        # ok, now check remaining input types. We've checked all kwargs matches, 
        # so we need to validate that any **missing** kwargs are Optional.
        for k in intypes:

            # validate that we are not missing **required** arguments
            if k not in kwargs: 
                optional = hasattr(intypes[k], "__args__") and (type(None) in intypes[k].__args__)
                if not optional:
                    raise ValueError(f"Required argument \"{k}\" for {self.__class__.__name__}[\"{self.name}\"] executor missing")

        if self.is_async():
            return await self.func(**kwargs)
        return self.func(**kwargs)

