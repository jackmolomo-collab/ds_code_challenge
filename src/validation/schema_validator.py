import yaml
import polars as pl


class SchemaValidator:

    TYPE_MAPPING = {
        "string": pl.String,
        "float": pl.Float64,
        "integer": pl.Int64,
        "boolean": pl.Boolean,
        "struct": pl.Struct,
    }

    def __init__(self, schema_path):

        self.schema_path = schema_path

        with open(
            self.schema_path,
            "r",
            encoding="utf-8"
        ) as file:

            self.schema = yaml.safe_load(file)

    def check_column(
        self,
        dataframe,
        column
    ):

        return column in dataframe.columns

    def check_type(
        self,
        dataframe,
        column,
        expected_type
    ):

        actual_type = dataframe.schema[column]

        expected_polars_type = self.TYPE_MAPPING[
            expected_type
        ]

        return actual_type == expected_polars_type

    def check_required(
        self,
        dataframe,
        column
    ):

        null_count = dataframe[column].null_count()

        return null_count == 0

    def check_min(
        self,
        dataframe,
        column,
        minimum
    ):

        return dataframe[column].min() >= minimum

    def check_max(
        self,
        dataframe,
        column,
        maximum
    ):

        return dataframe[column].max() <= maximum

    def check_allowed_values(
        self,
        dataframe,
        column,
        allowed_values
    ):

        values = (
            dataframe[column]
            .unique()
            .to_list()
        )

        return all(
            value in allowed_values
            for value in values
        )

    def validate(self, dataframe):

        total_checks = 0
        passed_checks = 0

        failures = []

        # Actual schema from the dataframe
        actual_schema = dataframe.schema

        print("Actual schema:")
        print(actual_schema)

        # Loop through expected schema
        for column, rules in self.schema.items():

            # --------------------------------
            # 1. Column existence
            # --------------------------------

            total_checks += 1

            if self.check_column(
                dataframe,
                column
            ):

                passed_checks += 1

            else:

                failures.append(
                    f"Missing column: {column}"
                )

                # Cannot perform the remaining
                # checks for a missing column
                continue

            # --------------------------------
            # 2. Data type
            # --------------------------------

            total_checks += 1

            if self.check_type(
                dataframe,
                column,
                rules["type"]
            ):

                passed_checks += 1

            else:

                actual_type = actual_schema[column]

                expected_type = self.TYPE_MAPPING[
                    rules["type"]
                ]

                failures.append(
                    f"{column}: expected "
                    f"{expected_type}, "
                    f"got {actual_type}"
                )

            # --------------------------------
            # 3. Required / null check
            # --------------------------------

            if rules.get(
                "required",
                False
            ):

                total_checks += 1

                if self.check_required(
                    dataframe,
                    column
                ):

                    passed_checks += 1

                else:

                    null_count = dataframe[
                        column
                    ].null_count()

                    failures.append(
                        f"{column}: "
                        f"{null_count} null values"
                    )

            # --------------------------------
            # 4. Minimum value
            # --------------------------------

            if "min" in rules:

                total_checks += 1

                minimum = rules["min"]

                if self.check_min(
                    dataframe,
                    column,
                    minimum
                ):

                    passed_checks += 1

                else:

                    failures.append(
                        f"{column}: values "
                        f"below minimum {minimum}"
                    )

            # --------------------------------
            # 5. Maximum value
            # --------------------------------

            if "max" in rules:

                total_checks += 1

                maximum = rules["max"]

                if self.check_max(
                    dataframe,
                    column,
                    maximum
                ):

                    passed_checks += 1

                else:

                    failures.append(
                        f"{column}: values "
                        f"above maximum {maximum}"
                    )

            # --------------------------------
            # 6. Allowed values
            # --------------------------------

            if "allowed_values" in rules:

                total_checks += 1

                allowed_values = rules[
                    "allowed_values"
                ]

                if self.check_allowed_values(
                    dataframe,
                    column,
                    allowed_values
                ):

                    passed_checks += 1

                else:

                    failures.append(
                        f"{column}: values outside "
                        f"allowed values "
                        f"{allowed_values}"
                    )

        # --------------------------------
        # Conformance score
        # --------------------------------

        score = (
            passed_checks / total_checks
            if total_checks
            else 0
        )

        passed = score >= 0.95

        print(
            f"Schema validation score: "
            f"{score:.4f}"
        )

        print(
            f"Schema validation passed: "
            f"{passed}"
        )

        print(
            f"Checks passed: "
            f"{passed_checks}/{total_checks}"
        )

        if failures:

            print("Schema validation failures:")

            for failure in failures:

                print(
                    f"- {failure}"
                )

        return {
            "score": score,
            "passed_checks": passed_checks,
            "total_checks": total_checks,
            "failures": failures,
            "passed": passed,
        }