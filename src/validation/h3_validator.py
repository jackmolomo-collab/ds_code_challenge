class H3Validator:
    
    def validate(self, h3_df):

        total = len(h3_df)

        valid = h3_df[
            (h3_df["h3_index"].notna()) &
            (h3_df["resolution"] == 8) &
            (h3_df["centroid_lat"].notna()) &
            (h3_df["centroid_lon"].notna()) &
            (h3_df["geometry"].notna())
        ]

        valid_count = len(valid)

        invalid_count = total - valid_count

        score = (
            valid_count / total
            if total
            else 0
        )

        return {
            "total": total,
            "valid": valid_count,
            "invalid": invalid_count,
            "score": score,
            "passed": score >= 0.95
        }