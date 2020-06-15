# -*- coding: utf-8 -*-
import json
import logging
import math
from collections import OrderedDict
from enum import Enum
from typing import Dict, List, Union

import pandas
import petl as etl
from pandas import DataFrame
from petl.transform.maps import FieldMapView
from scrapy.exceptions import UsageError
from sqlalchemy import text
from sqlalchemy.exc import DataError, IntegrityError, InvalidRequestError

from database.models.mixins.mysql_status import MysqlStatusMixin

from .base_command import BaseCommand


class BaseExport(BaseCommand):
    """Base command for csv & xlsx exporting.
    Usage:
        poetry run scrapy base_export --records 50 --length 10
                --file some_custom_name -json allow --type xlsx
    """

    XLSX_MAX_ROW_COUNT = 1048576
    # length of table with header
    TABLE_HEADER_LENGTH = 1

    class FileTypes(Enum):
        """Possible exported file types"""

        csv = 0
        xlsx = 1

    def __init__(self):
        super().__init__()

        # defaults
        self.fetched_rows = 0
        self.field_mapping = {}
        self.stats_per_file: Dict[Dict[str, int]] = {}

        # overriden by options default values
        # ! WARNING: DO NOT TWEAK THESE, they will be overriden
        # how much fetches to do for each file (self.max_records_count)
        self.fetching_times = 100
        self.max_records_count = 0
        self.items_per_file = 100000
        self.file_type = self.FileTypes.xlsx
        self.allow_json = True
        self.filename = "exported"

        # defines if json-extended rows will be empty
        self.is_new_row_empty = True
        # place, where mapping will be
        self.column_mapping = {}
        # petl-specific mapping
        self._mappings = OrderedDict()
        # list of json columns in file
        # will be used for extending rows
        self.json_columns = []

        logging.getLogger("petl.io.db").setLevel("INFO")

        self.__decorate_run()

    def init(self) -> None:
        """Init mappings, database, etc."""
        # Example implementation:
        # # Must-have mappings:
        # self.column_mapping = {
        #    "column_in_db_and_python": "Column In File",
        # }
        # # List some json columns (database names)
        # self.json_columns = [
        #    "column_in_db_and_python_but_with_JSON_data_type",
        # ]
        raise NotImplementedError("init(self) should contain column mappings!")

    def add_options(self, parser) -> None:
        super().add_options(parser)
        parser.add_option(
            "-f",
            "--filename",
            dest="filename",
            default="exported",
            help="Prefix of exported file",
        )
        parser.add_option(
            "-r",
            "--records",
            dest="max_records_count",
            default=0,
            help="How much records should be outputted",
        )
        parser.add_option(
            "-l",
            "--length",
            dest="items_per_file",
            default=100000,
            help="Max items count per file",
        )
        parser.add_option(
            "-t",
            "--type",
            dest="file_type",
            default="csv",
            help="Type of exported file. Can be 'csv' or 'xlsx'",
        )
        parser.add_option(
            "-j",
            "--json",
            dest="allow_json",
            default="true",
            help="Allow json columns in exported file. Can be 'true', 'allow', 'false', 'deny'",
        )

    def _parse_opts(self, opts: list):
        """Parses `opts` parameter and sets instances` fields.

        Args:
            opts (list): opts from run(args, opts)

        Raises:
            UsageError: Wrong value for max_records_count
            UsageError: Wrong value for items_per_file
            UsageError: Wrong value for file_type! Only 'csv' and 'xlsx' are accepted
        """
        self.filename = opts.filename

        if int(opts.max_records_count) >= 0:
            self.max_records_count = int(opts.max_records_count)
        else:
            raise UsageError("Wrong value for max_records_count!")

        if int(opts.items_per_file) >= 0:
            self.items_per_file = int(opts.items_per_file)
        else:
            raise UsageError("Wrong value for items_per_file!")

        allow_json = str(opts.allow_json).lower()
        self.allow_json = allow_json not in ("false", "deny") or allow_json in ("true", "allow")

        file_type = str(opts.file_type)
        if file_type == "csv":
            self.file_type = self.FileTypes.csv
        elif file_type == "xlsx":
            self.file_type = self.FileTypes.xlsx
        else:
            raise UsageError("Wrong value for file_type! Only 'csv' and 'xlsx' are accepted!")

    def __decorate_run(self):
        def decorator(function):
            def wrapper(*args, **kwargs):
                temp_result = function(*args, **kwargs)
                self._run_export(*args, **kwargs)
                return temp_result

            return wrapper

        self.run = decorator(self.run)

    def _run_export(self, args: list, opts: list) -> None:
        """Runs file exporting with predefined methods and opts from command line.

        Args:
            args (list): command line arguments
            opts (list): command line options
        """
        self._parse_opts(opts)

        previous_id = 0
        output_data = []
        fetched_data = []

        while (
            (fetched_data := self._fetch_data(previous_id=previous_id))
            and len(fetched_data) > self.TABLE_HEADER_LENGTH
            and not self.stopped
        ):
            self.fetched_rows += len(fetched_data) - self.TABLE_HEADER_LENGTH
            if not output_data:
                output_data.append(fetched_data[0])

            for row in fetched_data[self.TABLE_HEADER_LENGTH :]:
                output_data.append(row)

            if self.fetched_rows % self.items_per_file == 0:
                filename = f"{self.filename}_{self.fetched_rows}"
                # 'yield' file with data
                self.stats_per_file[filename] = {
                    "fetched_rows": len(output_data) - self.TABLE_HEADER_LENGTH
                }
                self._export_data(output_data, filename)
                output_data = []
            try:
                previous_id = etl.cut(fetched_data, "id")[self.TABLE_HEADER_LENGTH][0]
            except etl.errors.FieldSelectionError:
                self.logger.error("ID not found in fetched_data")
            except IndexError:
                # OK
                pass

        if output_data:
            filename = f"{self.filename}_{self.fetched_rows}"
            # 'yield' file with data
            self.stats_per_file[filename] = {
                "fetched_rows": len(output_data) - self.TABLE_HEADER_LENGTH
            }
            self._export_data(output_data, filename)

        # output stats
        for filename in self.stats_per_file:
            self.logger.info(
                "File: '%s', %s fetched, %s rows in file",
                filename,
                self.stats_per_file[filename]["fetched_rows"],
                self.stats_per_file[filename]["modified_rows"],
            )
        self.logger.info("Exported %s rows!", self.fetched_rows)
        self.logger.info("Export ended successfully!")

    # data fetching part

    @property
    def fetch_query(self) -> text:
        # Query for fetching data
        # Example implementation:
        # return text("""select 'your_query' as some_column""")
        raise NotImplementedError("fetch_query is not implemented!")

    def _fetch_data(self, **kwargs) -> FieldMapView:
        """Fetches data from database. Is aware of fetch limits, calculates take per query.
        Feel free to use kwargs for your query.
    
        Returns:
            FieldMapView: Table rows, formatted by 'self.after_fetch()'
        """
        query = self.fetch_query
        minimal_take = 1
        take = math.ceil(self.items_per_file // self.fetching_times) or minimal_take
        if self.max_records_count != 0:
            if self.fetched_rows + take > self.max_records_count:
                take = self.max_records_count - self.fetched_rows
                self.logger.info("Reached records count of %s!", self.max_records_count)
        fetched_data = etl.fromdb(
            self.engine, query, **kwargs, take=take, statuses=[MysqlStatusMixin.STATUS_SUCCESS],
        )

        # map fields
        for col in self.column_mapping:
            self._mappings[self.column_mapping[col]] = col

        # deserialize json columns
        for col in self.json_columns:
            self._mappings[self.column_mapping[col]] = col, lambda value: json.loads(value)

        return self.after_fetch(etl.fieldmap(fetched_data, self._mappings))

    def after_fetch(self, fetched_data: FieldMapView) -> FieldMapView:
        """Formats rows after they are fetched from database.

        Args:
            fetched_data (FieldMapView): data, fetched from database.
            Caution: JSON columns from 'self.json_columns' are already python objects

        Returns:
            FieldMapView: [description]
        """
        # Example implementation:
        # return fetched_data
        raise NotImplementedError("after_fetch is not implemented!")

    # rows & columns expanding part

    def expand_json_vertical(
        self, input_data: List[Union[list, tuple]]
    ) -> List[Union[list, tuple]]:
        """Expands json lists to rows vertically.

        Args:
            input_data (List[Union[list, tuple]]): list of rows to be expanded.

        Returns:
            List[Union[list, tuple]]: list of rows with json lists expanded to rows
        """

        # Требования к команде:
        # - возможность выбор варианта экспорта:
        #     - c  строками в одной ячейке
        #     - с вертикальным развертыванием  массивов. Как минимум одно поле, которое будет разворачиваться
        #           - в случае выбора опции вертикального развертывания также доступна опция выбора заполненности таблицы:
        #               - дублирование - для каждой развернутой из  записи дублируется вся остальная информация
        #               - облегченная - информация о присутствует только для первой из записей, для остальных должны быть пустые ячейки
        # - возможность выбор количества записей для экспорта

        # new_columns = self.column_mapping.copy()
        # header

        new_output = [input_data[0]]

        empty_row = ["" for _ in self.column_mapping]
        for data_row in input_data[self.TABLE_HEADER_LENGTH :]:
            new_data_rows = []

            data_row = list(data_row)
            data_row_no_json = data_row.copy()
            for expanded in self.json_columns:
                column_index = list(self.column_mapping).index(expanded)
                data_row_no_json[column_index] = ""
            new_data_rows.append(data_row_no_json)

            # expand rows with json list items
            for expanded in self.json_columns:
                column_index = list(self.column_mapping).index(expanded)

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

        # self.column_mapping = new_columns
        return new_output

    def expand_json_horizontal(
        self, input_data: List[Union[list, tuple]]
    ) -> List[Union[list, tuple]]:
        """Expands json dicts to columns horizontally.

        Args:
            input_data (List[Union[list, tuple]]): list of rows to be expanded.

        Returns:
            List[Union[list, tuple]]: list of rows with json dicts expanded to columns
        """
        # TODO ?

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
        return input_data

    # exporting part

    def _before_export(self, input_data: List[Union[list, tuple]]) -> FieldMapView:
        """Is executed when exporting almost started.

        Args:
            input_data (List[Union[list, tuple]]): data to be finalized

        Returns:
            FieldMapView: list of rows with expanded jsons a
        """
        output_mapping = OrderedDict()
        for col in self.column_mapping:
            output_mapping[self.column_mapping[col]] = col

        skip_these_columns = [
            "id",
            "error",
            "status",
            "created_at",
            "updated_at",
            "sent_to_customer",
        ]
        for skipped_column in skip_these_columns:
            if output_mapping.get(skipped_column):
                output_mapping.pop(skipped_column)

        if not self.allow_json:
            input_data = self.expand_json_vertical(input_data)
            input_data = self.expand_json_horizontal(input_data)
        for col in self.json_columns:
            output_mapping[self.column_mapping[col]] = (
                col,
                lambda value: json.dumps(value) if value else "",
            )

        return etl.fieldmap(input_data, output_mapping)

    def before_export(self, input_data: FieldMapView) -> FieldMapView:
        """User-overridable method to do anything specific with data before exporting to files.

        Args:
            input_data (FieldMapView): list of rows to be edited before exporting.

        Returns:
            FieldMapView: list of processed rows
        """
        # Example implementation:
        # return input_data
        raise NotImplementedError("before_export is not implemented!")

    def _export_data(self, output_data: FieldMapView, filename: str) -> None:
        """Exports `output_data` to files.

        Args:
            output_data (FieldMapView): list of rows to be exported
            filename (str): name of exported file

        Raises:
            RuntimeError: Unknown file_type
        """
        output_data = self._before_export(output_data)
        output_data = self.before_export(output_data)
        # update stats with exported rows count
        self.stats_per_file[filename]["modified_rows"] = len(output_data)
        # - Подготовить предложения или аргументацию почему так лучше не делать для случая
        # развертываний нескольких полей
        # - Сделать пример использования на основании scrapy команды.
        # - Результат отправить в отдельную ветку на github scrapy-boilerplate. Сделать PR
        # в ветку разработки

        # - В общем и целом команда для шаблона должна быть абстрактной.
        # Что-то похожее на rmq.Producer/rmq.Consumer, чтобы при конечной реализации достаточно
        # было определить
        # запрос для получения данных из БД/или метод предварительной подготовки данных,
        # метод маппинга полей в названия колонок файла,
        # возможно метод обновления записи в БД (поддержка поля sent_to_customer)

        if self.file_type == self.FileTypes.csv:
            output_data.tocsv(f"{filename}.csv", "utf-8", write_header=True)
        elif self.file_type == self.FileTypes.xlsx:
            output_data.toxlsx(f"{filename}.xlsx", write_header=True)
            if len(output_data) >= self.XLSX_MAX_ROW_COUNT:
                self.logger.critical(
                    "Too long file for 'xlsx' type: %s row(s)! Consider reducing file length for '%s'!",
                    len(output_data),
                    filename,
                )
        else:
            raise RuntimeError("Unknown file_type!")
