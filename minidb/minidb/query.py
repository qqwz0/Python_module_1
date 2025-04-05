from typing import List, Any
from .table import Table
from .row import Row

class SimpleQuery:
    """
    A class to represent a simple query on a table.
    """

    def __init__(self, table: Table):
        """
        Initialize the SimpleQuery with a table.

        :param table: The table to query.
        """
        self.table = table
        self.selected_columns = None
        self.filter_conditions = []
        self.sort_column = None
        self.sort_ascending = True

    def select(self, columns: List[str]):
        """
        Select specific columns to include in the result.

        :param columns: List of column names to select.
        :return: The SimpleQuery object.
        """
        self.selected_columns = columns
        return self

    def where(self, column: str, operator: str, value: Any):
        """
        Add a filter condition to the query.

        :param column: The column name to filter on.
        :param operator: The operator to use for comparison.
        :param value: The value to compare against.
        :return: The SimpleQuery object.
        """
        self.filter_conditions.append((column, operator, value))
        return self

    def order_by(self, column: str, ascending: bool = True):
        """
        Specify the column to sort the results by.

        :param column: The column name to sort by.
        :param ascending: Whether to sort in ascending order.
        :return: The SimpleQuery object.
        """
        self.sort_column = column
        self.sort_ascending = ascending
        return self

    def execute(self) -> List[Row]:
        """
        Execute the query and return the results.

        :return: List of rows that match the query.
        """
        results = []
        filtered_rows = []

        # Filter rows based on conditions
        for row in self.table:
            matches_all_conditions = True
            for column, operator, value in self.filter_conditions:
                row_value = row[column]
                if operator == "=" and row_value != value:
                    matches_all_conditions = False
                    break
                elif operator == ">" and not (row_value > value):
                    matches_all_conditions = False
                    break
                elif operator == "<" and not (row_value < value):
                    matches_all_conditions = False
                    break
                elif operator == ">=" and not (row_value >= value):
                    matches_all_conditions = False
                    break
                elif operator == "<=" and not (row_value <= value):
                    matches_all_conditions = False
                    break
            if matches_all_conditions:
                filtered_rows.append(row)

        # Sort rows if a sort column is specified
        if self.sort_column:
            filtered_rows.sort(
                key=lambda r: r[self.sort_column] if r[self.sort_column] is not None else "",
                reverse=not self.sort_ascending
            )

        # Select specified columns or all columns if none specified
        for row in filtered_rows:
            if self.selected_columns:
                new_row_data = {col: row[col] for col in self.selected_columns if col in row.data}
                results.append(Row(new_row_data))
            else:
                results.append(row)
        
        return results

class JoinedTable:
    """
    A class to represent a joined table from two tables.
    """

    def __init__(self, table1: Table, table2: Table, join_column: str):
        """
        Initialize the JoinedTable with two tables and a join column.

        :param table1: The first table to join.
        :param table2: The second table to join.
        :param join_column: The column to join on.
        """
        self.table1 = table1
        self.table2 = table2
        self.join_column = join_column
        self.rows = self.inner_join()

    def inner_join(self) -> List[Row]:
        """
        Perform an inner join on the two tables.

        :return: List of joined rows.
        """
        joined_rows = []
        for row1 in self.table1:
            for row2 in self.table2:
                if row1[self.join_column] == row2[self.join_column]:
                    combined_data = {**row1.data, **{f"{self.table2.name}.{k}": v for k, v in row2.data.items() if k != self.join_column}}
                    joined_rows.append(Row(combined_data))
        return joined_rows

    def __iter__(self):
        """
        Return an iterator over the joined rows.

        :return: Iterator over the joined rows.
        """
        return iter(self.rows)

    def __repr__(self):
        """
        Return a string representation of the JoinedTable.

        :return: String representation of the JoinedTable.
        """
        return f"JoinedTable(on={self.join_column}, rows={len(self.rows)})"