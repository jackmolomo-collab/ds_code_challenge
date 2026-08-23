class H3Validator:
    
    def validate(self, h3_df):

        total = h3_df.height

        valid = h3_df.filter(
            h3_df["h3_index"].is_not_null()
            & (h3_df["resolution"] == 8)
            & h3_df["centroid_lat"].is_not_null()
            & h3_df["centroid_lon"].is_not_null()
            & h3_df["geometry"].is_not_null()
        )

        valid_count = valid.height
        invalid_count = total - valid_count

        score = (
            valid_count / total
            if total
            else 0
        )

        print(f"Validation total: {total}")
        print(f"Validation valid: {valid_count}")
        print(f"Validation invalid: {invalid_count}")
        print(f"Validation score: {score:.4f}")
        print(f"Validation passed: {score >= 0.95}")

        if score < 0.95:
            raise ValueError(
                f"H3 validation failed: "
                f"{valid_count}/{total} valid "
                f"(score={score:.4f})"
            )

        return valid