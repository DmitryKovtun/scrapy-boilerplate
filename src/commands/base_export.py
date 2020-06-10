# -*- coding: utf-8 -*-
from enum import Enum
from typing import List

import pandas
from pandas import DataFrame
from sqlalchemy import text
from sqlalchemy.exc import DataError, IntegrityError, InvalidRequestError

from database.models.mixins.mysql_status import MysqlStatusMixin

from .base_command import BaseCommand
import petl as etl
import json
from collections import OrderedDict


class BaseExport(BaseCommand):
    """[summary]"""

    class FileTypes(Enum):
        csv = 0
        xlsx = 1

    def __init__(self):
        super().__init__()
        self.export_rows_count = 0
        # - возможность выбор количества записей для экспорта

        # - возможность выбора количества записей в одном файле. По умолчанию 100000.
        # Т.е. для команды можно задать параметрами экспортировать 10000 записей при разделении по 1000 записей в одном файле.
        # Таким образом команда должна сгенерировать 10 файлов
        self.max_file_length = 100000

        # - возможность выбора формата файла экспорта: CSV или XLSX. По умолчанию - CSV
        self.file_type = self.FileTypes.xlsx
        #
        self.field_mapping = {}

        #
        self.allow_ = True
        #
        # how much fetches to do for each file (self.max_file_length)
        self.fetching_times = 100

    def init(self) -> None:
        """Fields initialization before self.run()"""
        pass

    def add_options(self, parser) -> None:
        super().add_options(parser)

    def run(self, args: list, opts: list) -> None:
        self.set_logger("EXPORT", self.settings.get("LOG_LEVEL"))

        # Задача: добавить в scrapy-boilerplate общую команду для унифицированного
        # экспорта плоской таблицы/результата SELECT запроса к БД.

        # Требования к команде:

        # - возможность выбор варианта экспорта:
        #     - c  строками в одной ячейке
        #     - с вертикальным развертыванием  массивов. Как минимум одно поле, которое будет разворачиваться
        #           - в случае выбора опции вертикального развертывания также доступна опция выбора заполненности таблицы:
        #               - дублирование - для каждой развернутой из  записи дублируется вся остальная информация
        #               - облегченная - информация о присутствует только для первой из записей, для остальных должны быть пустые ячейки
        # your code here

        # - возможность выбор количества записей для экспорта

        # - возможность выбора количества записей в одном файле. По умолчанию 100000.
        # Т.е. для команды можно задать параметрами экспортировать 10000 записей при разделении по 1000 записей в одном файле.
        # Таким образом команда должна сгенерировать 10 файлов

        # - возможность выбора формата файла экспорта: CSV или XLSX. По умолчанию - CSV

        fetched_rows = 0
        previous_id = 0
        while (fetched_data := self.fetch_data(previous_id=previous_id)) and not self.stopped:
            fetched_rows += len(fetched_data)
            self.logger.info(len(fetched_data))
            # for temp in fetched_data:
            #    self.logger.info(f"\n{temp}\n")

            # TODO filename generation
            filename = "temp"
            if fetched_rows > 5:
                # 'yield' file with data
                self.export_data(fetched_data, filename)
                break

            # TODO more safety with indexes
            previous_id = etl.cut(fetched_data, "id")[1][0]
            self.logger.info(f"previous_id: {previous_id}")

        # TODO stats here
        self.logger.info("Export done")

    def fetch_data(self, **kwargs):
        query = text(
            """
            select * from members
            where status in :statuses and id > :previous_id
            limit :take
            """
        )
        return self._fetch_data(query, **kwargs)

    def _fetch_data(self, query, **kwargs) -> List[dict]:
        take = self.max_file_length // self.fetching_times

        #  TODO remove
        take = 1
        # connection = sqlite3.connect('example.db')

        table = etl.fromdb(
            self.engine, query, **kwargs, take=take, statuses=[MysqlStatusMixin.STATUS_SUCCESS],
        )
        return self.after_fetch(table)

        #
        query = text(
            """
            select * from members
            where status in :statuses
            limit :take
            order by id
            """
        )
        # return_list = []
        # for value in self.engine.execute(query, **kwargs):
        #    return_list.append(value)

        return self.engine.execute(
            query, **kwargs, statuses=[MysqlStatusMixin.STATUS_SUCCESS]
        ).fetchall()

        query = text("""""")
        return_list = []
        for value, next_value in self.engine.execute(query, **kwargs):
            return_list.append({"value": value, "next_value": next_value})
        return return_list

    def after_fetch(self, fetched_data: List[tuple]) -> List[tuple]:
        """[summary]

        Args:
            fetched_data (List[tuple]): [description]

        Returns:
            List[tuple]: [description]
        """

        #
        # rename a field
        # mappings["subject_id"] = "id"
        # translate a field
        # mappings["gender"] = "sex", {"male": "M", "female": "F"}
        # apply a calculation to a field
        # mappings["age_months"] = "age", lambda v: v * 12
        # apply a calculation to a combination of fields
        # mappings["bmi"] = lambda rec: rec["weight"] / rec["height"] ** 2
        # transform and inspect the output
        self.mappings = OrderedDict()
        self.columns = {
            "id": "id",
            "search_id": "search_id",
            "company_id": "company_id",
            "url": "url",
            "ln_id": "ln_id",
            "first_name": "first_name",
            "last_name": "last_name",
            "title": "title",
            "location": "location",
            "connections_count": "connections_count",
            "summary": "summary",
            "industry_id": "industry_id",
            "current_positions": "current_positions",
            "positions": "positions",
            "educations": "educations",
            "skills": "skills",
            "interests": "interests",
            # "error": "error",
            # "status": "status",
            # "sent_to_customer": "sent_to_customer",
            # "created_at": "created_at",
            # "updated_at": "updated_at",
        }
        for col in self.columns:
            self.mappings[self.columns[col]] = col

        # list some json columns (database names)
        json_columns = [
            "current_positions",
            "positions",
            "educations",
            "skills",
            "interests",
        ]
        for col in json_columns:
            self.mappings[self.columns[col]] = col, lambda value: json.loads(value)
        # TODO custom overrides

        return etl.fieldmap(fetched_data, self.mappings)

    def prepare_data(self, data_dict) -> DataFrame:
        pass

    def process_data(self, data_dict) -> DataFrame:

        return DataFrame(data_dict)

    def before_export(self, output_data):
        # TODO remove unused columns here

        # TODO custom overrides
        # list some json columns (database names)
        json_columns = [
            "current_positions",
            "positions",
            "educations",
            "skills",
            "interests",
        ]
        for col in self.columns:
            self.mappings[self.columns[col]] = col
        # for col in json_columns:
        #    self.mappings[self.columns[col]] = col, lambda value: json.dumps(value)
        # TODO custom overrides

        skip = ["id", "error", "status", "created_at", "updated_at", "sent_to_customer"]
        for s in skip:
            if self.mappings.get(s):
                self.mappings.pop(s)

        return etl.fieldmap(output_data, self.mappings)

    def export_data(self, output_data, filename: str):
        output_data = self.before_export(output_data)

        # Дополнительно:

        # - Продумать и реализовать удобный маппинг полей из БД в поля файла экспорта.
        # - Подготовить предложения или аргументацию почему так лучше не делать для случая развертываний нескольких полей
        # - После завершения выводить статистику о созданных файлах/количестве записей.
        # - Сделать пример использования на основании scrapy команды.
        # - Результат отправить в отдельную ветку на github scrapy-boilerplate. Сделать PR в ветку разработки

        #

        # - В общем и целом команда для шаблона должна быть абстрактной.
        # Что-то похожее на rmq.Producer/rmq.Consumer, чтобы при конечной реализации достаточно было определить
        # запрос для получения данных из БД/или метод предварительной подготовки данных,
        # метод маппинга полей в названия колонок файла,
        # возможно метод обновления записи в БД (поддержка поля sent_to_customer)
        # self.logger.info(previous_id)
        if self.file_type == self.FileTypes.csv:
            output_data.tocsv(f"{filename}.csv", "utf-8", write_header=True)
        elif self.file_type == self.FileTypes.xlsx:
            output_data.toxlsx(f"{filename}.xlsx", write_header=True)

        else:
            raise RuntimeError("Unknown file_type!")
        pass
