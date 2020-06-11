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
        # TODO remove
        self.max_file_length = 10

        # - возможность выбора формата файла экспорта: CSV или XLSX. По умолчанию - CSV
        self.file_type = self.FileTypes.xlsx
        #
        self.field_mapping = {}

        #
        self.allow_json = False
        #
        # how much fetches to do for each file (self.max_file_length)
        self.fetching_times = 100
        # place, where mapping will be
        self.columns = {}
        #
        # list of json columns in file
        # will be used for extending rows
        self.json_columns = []
        #
        # defines if json-extended rows will be empty
        self.is_new_row_empty = True

    def init(self) -> None:
        """Fields initialization before self.run()"""
        self.columns = {
            "id": "id",
            "search_id": "search_id",
            # "company_id": "company_id",
            "url": "url",
            "ln_id": "ln_id",
            "first_name": "first_name",
            "last_name": "last_name",
            "title": "title",
            "location": "location",
            "connections_count": "connections_count",
            "summary": "summary",
            "industry_id": "industry_id",
            # "current_positions": "current_positions",
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
        # list some json columns (database names)
        self.json_columns = [
            # "current_positions",
            "positions",
            "educations",
            # "skills",
            # "interests",
        ]

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
        # - возможность выбор количества записей для экспорта

        # - возможность выбора количества записей в одном файле. По умолчанию 100000.
        # Т.е. для команды можно задать параметрами экспортировать 10000 записей при разделении по 1000 записей в одном файле.
        # Таким образом команда должна сгенерировать 10 файлов

        # - возможность выбора формата файла экспорта: CSV или XLSX. По умолчанию - CSV

        fetched_rows = 0
        previous_id = 0
        output_data = []
        fetched_data = []
        # will contain filename: length
        stats = {}
        while (
            (fetched_data := self.fetch_data(previous_id=previous_id))
            and len(fetched_data) > 1
            and not self.stopped
        ):
            # -1 because of header
            fetched_rows += len(fetched_data) - 1
            if not output_data:
                output_data.append(fetched_data[0])

            for row in fetched_data[1:]:
                # self.logger.info(row)
                output_data.append(row)

            if fetched_rows % self.max_file_length == 0:
                # TODO filename generation
                filename = f"temp_{fetched_rows}"
                # 'yield' file with data
                stats[filename] = len(output_data) - 1
                self.export_data(output_data, filename)
                output_data = []
                # TODO remove this
                break

            # TODO more safety with indexes
            try:
                previous_id = etl.cut(fetched_data, "id")[1][0]
            except etl.errors.FieldSelectionError:
                self.logger.error("ID not found in fetched_data")
            except IndexError:
                # OK
                pass
            self.logger.info("previous_id: %s", previous_id)

        # TODO duplicate code fix
        # TODO filename generation
        filename = f"temp_{fetched_rows}"
        # 'yield' file with data
        stats[filename] = len(output_data) - 1
        self.export_data(output_data, filename)

        # TODO stats here
        for filename in stats:
            self.logger.info("File: '%s', %s items", filename, stats[filename])
        self.logger.info("Exported %s rows!", fetched_rows)
        self.logger.info("Export done")

    def fetch_data(self, **kwargs):
        query = text(
            """
            select * from members
            where status in :statuses and id > :previous_id and id = 147
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
            order by id
            limit :take
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

        self.mappings = OrderedDict()
        for col in self.columns:
            self.mappings[self.columns[col]] = col

        for col in self.json_columns:
            self.mappings[self.columns[col]] = col, lambda value: json.loads(value)
        # TODO custom overrides

        return etl.fieldmap(fetched_data, self.mappings)

    def prepare_data(self, data_dict) -> DataFrame:
        pass

    def expand_json(self, output_data):
        expanding_map = {
            "positions": [
                "title",
                "company",
                # "company": {
                #    "url": "https://www.linkedin.com/company/banco-bai/",
                #    "name": "BAI - Banco Angolano de Investimentos, s.a.",
                #    "size": "1001-5000",
                #    "ln_id": 976135,
                #    "industry_id": 40,
                # },
                "end_date",
                "start_date",
            ],
            "educations": [],
        }

        # new_columns = self.columns.copy()
        # header
        new_output = [output_data[0]]
        #
        # get some memory for new columns
        # expanded_lists = {}
        # for expanded in expanding_map:
        #    if expanded in new_columns:
        #        new_columns.pop(expanded)
        #
        #    for inner_json in expanding_map[expanded]:
        #        col_name = f"{expanded}_{inner_json}"
        #        new_columns[col_name] = col_name
        #        expanded_lists[col_name] = []

        empty_row = ["" for _ in self.columns]
        for data_row in output_data[1:]:
            new_data_rows = []

            data_row = list(data_row)
            data_row_no_json = data_row.copy()
            for expanded in self.json_columns:
                column_index = list(self.columns).index(expanded)
                data_row_no_json[column_index] = ""
            new_data_rows.append(data_row_no_json)

            # expand rows with json list items
            for expanded in self.json_columns:
                column_index = list(self.columns).index(expanded)

                for list_item in data_row[column_index]:
                    needed_row_index = None
                    # find empty row
                    for index, filled_row in enumerate(new_data_rows):
                        if not filled_row[column_index]:
                            needed_row_index = index
                            break

                    current_row_index = None
                    if needed_row_index is not None:
                        current_row_index = needed_row_index
                    else:
                        # no empty row found, create new one
                        current_row = []
                        if self.is_new_row_empty:
                            current_row = empty_row.copy()
                        else:
                            current_row = data_row_no_json.copy()
                        new_data_rows.append(current_row)
                        current_row_index = -1

                    new_data_rows[current_row_index][column_index] = list_item
            for new_data_row in new_data_rows:
                new_output.append(new_data_row)

        # self.columns = new_columns
        return new_output

    def before_export(self, output_data):
        # TODO remove unused columns here

        # TODO custom overrides
        # list some json columns (database names)
        for col in self.columns:
            self.mappings[self.columns[col]] = col

        skip = ["id", "error", "status", "created_at", "updated_at", "sent_to_customer"]
        for s in skip:
            if self.mappings.get(s):
                self.mappings.pop(s)

        if self.allow_json:
            for col in self.json_columns:
                self.mappings[self.columns[col]] = col, lambda value: json.dumps(value)
        else:
            output_data = self.expand_json(output_data)

        return etl.fieldmap(output_data, self.mappings)

    def export_data(self, output_data, filename: str):
        output_data = self.before_export(output_data)

        # Дополнительно:

        # - Продумать и реализовать удобный маппинг полей из БД в поля файла экспорта.
        # - Подготовить предложения или аргументацию почему так лучше не делать для случая развертываний нескольких полей
        # - После завершения выводить статистику о созданных файлах/количестве записей.
        # - Сделать пример использования на основании scrapy команды.
        # - Результат отправить в отдельную ветку на github scrapy-boilerplate. Сделать PR в ветку разработки

        # - В общем и целом команда для шаблона должна быть абстрактной.
        # Что-то похожее на rmq.Producer/rmq.Consumer, чтобы при конечной реализации достаточно было определить
        # запрос для получения данных из БД/или метод предварительной подготовки данных,
        # метод маппинга полей в названия колонок файла,
        # возможно метод обновления записи в БД (поддержка поля sent_to_customer)

        self.file_type = self.FileTypes.csv

        if self.file_type == self.FileTypes.csv:
            output_data.tocsv(f"{filename}.csv", "utf-8", write_header=True)
        elif self.file_type == self.FileTypes.xlsx:
            output_data.toxlsx(f"{filename}.xlsx", write_header=True)

        else:
            raise RuntimeError("Unknown file_type!")
