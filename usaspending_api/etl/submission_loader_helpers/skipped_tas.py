def update_skipped_tas(row, tas_rendering_label, skipped_tas):
    if tas_rendering_label not in skipped_tas:
        skipped_tas[tas_rendering_label] = {}
        skipped_tas[tas_rendering_label]["count"] = 1
        skipped_tas[tas_rendering_label]["rows"] = [row["row_number"]]
    else:
        skipped_tas[tas_rendering_label]["count"] += 1
        skipped_tas[tas_rendering_label]["rows"] += [row["row_number"]]
