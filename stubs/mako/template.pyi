from typing import Any


class Template:
    def __init__(
        self,
        template: str,
        default_filters: list[str],
        imports: list[str],
    ) -> None:
        ...
        
    def render(self, **context: Any) -> str:
        ...
