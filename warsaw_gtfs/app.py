from argparse import Namespace

from impuls import App, LocalResource, Pipeline, PipelineOptions
from impuls.model import Agency
from impuls.tasks import AddEntity

from .load_json import LoadJSON


class WarsawGTFS(App):
    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        return Pipeline(
            tasks=[
                AddEntity(
                    task_name="AddAgency",
                    entity=Agency(
                        id="0",
                        name="Warszawski Transport Publiczny",
                        url="https://wtp.waw.pl",
                        timezone="Europe/Warsaw",
                        lang="pl",
                        phone="+48 22 19 115",
                    ),
                ),
                LoadJSON(),
            ],
            resources={
                "rozklady.json": LocalResource("ignore_rozklady.json"),
                "slowniki.json": LocalResource("ignore_slowniki.json"),
            },
            options=options,
        )
