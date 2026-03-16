from __future__ import annotations

import shutil
import sys
from pathlib import Path

from ansys.aedt.core import Hfss


INPUT_SENTINELS = {
    "coil_groups_0__count_mode": "1",
    "coil_shape_inner_margin_x": "1mm",
    "coil_spacing_tx_vertical_center_gap_mm": "2mm",
    "ferrite_present": "1",
    "tx_region_z_parts_dd_z_mm": "3mm",
}

OUTPUT_SENTINELS = {
    "k_ratio": "1",
    "Lrx_uH": "1uH",
}


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: remote_generate_sample_fixture.py <src_aedt> <dst_aedt>")
    src = Path(sys.argv[1]).expanduser().resolve()
    dst = Path(sys.argv[2]).expanduser().resolve()
    dst.parent.mkdir(parents=True, exist_ok=True)
    work_copy = dst.with_name(dst.stem + ".work.aedt")
    if work_copy.exists():
        work_copy.unlink()
    shutil.copy2(src, work_copy)

    hfss = Hfss(
        project=str(work_copy),
        design="HFSSDesign1",
        new_desktop=True,
        non_graphical=True,
    )
    try:
        for name, value in INPUT_SENTINELS.items():
            hfss[name] = value
        for name, expression in OUTPUT_SENTINELS.items():
            hfss.create_output_variable(variable=name, expression=expression)
        hfss.save_project()
    finally:
        hfss.release_desktop(close_projects=True, close_desktop=True)

    shutil.move(str(work_copy), str(dst))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
