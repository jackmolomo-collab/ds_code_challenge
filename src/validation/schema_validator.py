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
            encoding="utf-8",
        ) as file:

            self.schema = yaml.safe_load(file)

        # Support both schema formats:
        #
        # Format 1:
        #
        # h3_index:
        #     type: string
        #
        # Format 2:
        #
        # dataset: service_requests
        # version: 1
        # columns:
        #     notification_number:
        #         type: integer

        if "columns" in self.schema:

            self.columns_schema = (
                self.schema["columns"]
            )

        else:

            self.columns_schema = self.schema

    def check_column(
        self,
        dataframe,
        column,
    ):

        return column in dataframe.columns

    def check_type(
        self,
        dataframe,
        column,
        expected_type,
    ):

        if expected_type not in self.TYPE_MAPPING:

            raise ValueError(
                f"Unsupported schema type: "
                f"{expected_type}"
            )

        actual_type = dataframe.schema[column]

        expected_polars_type = (
            self.TYPE_MAPPING[
                expected_type
            ]
        )

        return actual_type == expected_polars_type

    def check_required(
        self,
        dataframe,
        column,
    ):

        null_count = (
            dataframe[column]
            .null_count()
        )

        return null_count == 0

    def check_min(
        self,
        dataframe,
        column,
        minimum,
    ):

        value = dataframe[column].min()

        if value is None:
            return False

        return value >= minimum

    def check_max(
        self,
        dataframe,
        column,
        maximum,
    ):

        value = dataframe[column].max()

        if value is None:
            return False

        return value <= maximum

    def check_allowed_values(
        self,
        dataframe,
        column,
        allowed_values,
    ):

        values = (
            dataframe[column]
            .drop_nulls()
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

        actual_schema = dataframe.schema

        print("Actual schema:")
        print(actual_schema)

        # -----------------------------------------------------
        # Validate every configured dataframe column
        # -----------------------------------------------------

        for column, rules in (
            self.columns_schema.items()
        ):

            # -------------------------------------------------
            # 1. Column existence
            # -------------------------------------------------

            total_checks += 1

            if self.check_column(
                dataframe,
                column,
            ):

                passed_checks += 1

            else:

                failures.append(
                    f"Missing column: {column}"
                )

                # Cannot perform further
                # validation for this column.
                continue

            # -------------------------------------------------
            # 2. Data type
            # -------------------------------------------------

            total_checks += 1

            expected_type = rules.get(
                "type"
            )

            if self.check_type(
                dataframe,
                column,
                expected_type,
            ):

                passed_checks += 1

            else:

                actual_type = (
                    actual_schema[column]
                )

                expected_polars_type = (
                    self.TYPE_MAPPING[
                        expected_type
                    ]
                )

                failures.append(
                    f"{column}: expected "
                    f"{expected_polars_type}, "
                    f"got {actual_type}"
                )

            # -------------------------------------------------
            # 3. Required / null validation
            # -------------------------------------------------

            if rules.get(
                "required",
                False,
            ):

                total_checks += 1

                if self.check_required(
                    dataframe,
                    column,
                ):

                    passed_checks += 1

                else:

                    null_count = (
                        dataframe[column]
                        .null_count()
                    )

                    failures.append(
                        f"{column}: "
                        f"{null_count} null values"
                    )

            # -------------------------------------------------
            # 4. Minimum value
            # -------------------------------------------------

            if "min" in rules:

                total_checks += 1

                minimum = rules["min"]

                if self.check_min(
                    dataframe,
                    column,
                    minimum,
                ):

                    passed_checks += 1

                else:

                    failures.append(
                        f"{column}: values "
                        f"below minimum "
                        f"{minimum}"
                    )

            # -------------------------------------------------
            # 5. Maximum value
            # -------------------------------------------------

            if "max" in rules:

                total_checks += 1

                maximum = rules["max"]

                if self.check_max(
                    dataframe,
                    column,
                    maximum,
                ):

                    passed_checks += 1

                else:

                    failures.append(
                        f"{column}: values "
                        f"above maximum "
                        f"{maximum}"
                    )

            # -------------------------------------------------
            # 6. Allowed values
            # -------------------------------------------------

            if "allowed_values" in rules:

                total_checks += 1

                allowed_values = (
                    rules["allowed_values"]
                )

                if self.check_allowed_values(
                    dataframe,
                    column,
                    allowed_values,
                ):

                    passed_checks += 1

                else:

                    failures.append(
                        f"{column}: values outside "
                        f"allowed values "
                        f"{allowed_values}"
                    )

        # -----------------------------------------------------
        # Conformance score
        # -----------------------------------------------------

        score = (
            passed_checks / total_checks
            if total_checks
            else 0.0
        )

        # Non-binary threshold required
        # by the assessment.
        threshold = 0.95

        passed = score >= threshold

        print(
            f"Schema validation score: "
            f"{score:.4f}"
        )

        print(
            f"Schema validation threshold: "
            f"{threshold:.2%}"
        )

        print(
            f"Schema validation passed: "
            f"{passed}"
        )

        print(
            f"Checks passed: "
            f"{passed_checks}/"
            f"{total_checks}"
        )

        if failures:

            print(
                "Schema validation failures:"
            )

            for failure in failures:

                print(
                    f"- {failure}"
                )

        return {
            "score": score,
            "threshold": threshold,
            "passed_checks": passed_checks,
            "total_checks": total_checks,
            "failures": failures,
            "passed": passed,
        }