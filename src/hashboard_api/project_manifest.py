from typing import *

from .api import HashboardAPI
from .project_importer import (
    fetch_all_connections,
    fetch_all_models,
    fetch_all_project_metrics,
)
from ..utils.env import guess_execution_environment

if TYPE_CHECKING:
    from ..model.model import Model
    from ..utils.resource import LinkedResource


class ProjectManifest:
    """
    Dynamic collection of all the Hashboard resources available for the project.

    For each resource type in this collection, you can access properties on them
    using standard `.` syntax, using the alias of the project resource to load
    them::

        model = project.models.sales
        conn = project.connections.uploads
        print(project) # to list all available aliases

    Attributes:
        models: Collection of project `Model` instances.
        connections: Collection of project connections, as `LinkedResource` instances.
    """

    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.models = ProjectModels(project_id)
        self.metrics = ProjectMetrics(project_id)
        self.connections = ProjectConnections(project_id)

        # pre-cache the names of imported resources so they can be
        # auto-completed in notebooks
        try:
            if guess_execution_environment() == "ipython":
                self.models.get_all()
                self.connections.get_all()
                self.metrics.get_all()
        except:
            # this is just a pre-cache attempt which is triggered on `import`
            # so swallow the error and it will resurface naturally if the user
            # attempts something later on where this is needed
            pass

    def _refresh(self) -> "ProjectManifest":
        self.models._refresh()
        self.metrics._refresh()
        self.connections._refresh()
        return self

    def __repr__(self) -> str:
        api = HashboardAPI.get_for_project(self.project_id)
        return "\n".join(
            [
                f"Project: {self.project_id} on {api.base_uri}",
                str(self.models),
                str(self.metrics),
                str(self.connections),
            ]
        )


T = TypeVar("T")


class ProjectManifestTypedCollection(Generic[T]):
    def __init__(self, project_id: str) -> None:
        self._project_id = project_id
        self._cache: Optional[List[T]] = None

    # --- subclass contract ---

    def _get_all_impl(self) -> List[T]:
        raise NotImplementedError()

    @property
    def _item_type_name(self) -> str:
        return "resource"

    def _item_alias(self, item: T) -> str:
        raise NotImplementedError()

    # --- derived ---

    def get_all(self) -> List[T]:
        if self._cache is not None:
            return self._cache
        self._cache = self._get_all_impl()
        return self._cache

    def _refresh(self):
        self._cache = None
        return self

    def _accessor(self, key: Union[str, int], *, dot_access: bool) -> T:
        if type(key) == int:
            return self.get_all()[key]
        else:
            if found := next(
                (i for i in self.get_all() if self._item_alias(i) == key),
                None,
            ):
                return found
            if dot_access and key in self.__dict__ or key.startswith("__"):
                # signal the attribute doesn't exist, not that the resource
                # doesn't -- this helps with code introspection tools (ie.
                # debuggers) which may scan through this object
                raise AttributeError()
            raise ResourceNotFoundError(key, self._item_type_name)

    def __getitem__(self, key: Union[str, int]) -> T:
        return self._accessor(key, dot_access=False)

    def __getattr__(self, key: str) -> T:
        return self._accessor(key, dot_access=True)

    def __len__(self):
        return len(self.get_all())

    def __iter__(self):
        return iter(self.get_all())

    def __repr__(self) -> str:
        aliases = ", ".join(self._item_alias(i) for i in self.get_all())
        return f"{self._item_type_name}s: {aliases}"

    def __dir__(self):
        return [
            *[self._item_alias(i) for i in self.get_all()],
            *super().__dir__(),
        ]


class ProjectModels(ProjectManifestTypedCollection["Model"]):
    """
    Collection of all the Hashboard models available for the project.
    """

    def _get_all_impl(self) -> List["Model"]:
        return fetch_all_models(self._project_id)

    @property
    def _item_type_name(self) -> str:
        return "Model"

    def _item_alias(self, item: "Model") -> str:
        return item._linked_resource.alias or "__unnamed_model__"


class ProjectMetrics(ProjectManifestTypedCollection["Model"]):
    """
    Collection of all the available Hashboard project metrics.
    """

    def _get_all_impl(self) -> List["Model"]:
        return fetch_all_project_metrics(self._project_id)

    @property
    def _item_type_name(self) -> str:
        return "Metric"

    def _item_alias(self, item: "Model") -> str:
        return item._linked_resource.alias or "__unnamed_project_metric__"


class ProjectConnections(ProjectManifestTypedCollection["LinkedResource"]):
    """
    Collection of all the Hashboard connections available for the project.
    """

    def _get_all_impl(self) -> List["LinkedResource"]:
        return fetch_all_connections(self._project_id)

    @property
    def _item_type_name(self) -> str:
        return "Connection"

    def _item_alias(self, item: "LinkedResource") -> str:
        return item.alias


class ResourceNotFoundError(Exception):
    """
    Indicates that no resource with the provided alias was found in
    the project.
    """

    def __init__(
        self,
        alias: str,
        type_name: str,
        found_options: Optional[List[str]] = None,
    ) -> None:
        if found_options is None:
            super().__init__(f"Failed to fetch {type_name}s for the project.")

        err = f"No {type_name} with alias '{alias}' was found in the project."
        hint = (
            f"Available {type_name}s: {','.join(found_options)}"
            if found_options
            else f"Project has no accessible {type_name}s."
        )
        super().__init__("\n".join([err, hint]))
